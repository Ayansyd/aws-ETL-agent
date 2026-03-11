"""
agent/s3_agent.py

Patched S3Agent (Option A: internal schema engine + auto-persist for MCP).

Features:
- One-column-at-a-time interactive schema disambiguation
- Stores final schema in self.final_schema
- Auto-persist final schema by calling tools.use_schema(schema_text)
- Auto-injects final schema into create_glue_table calls
- Uses uploaded file /mnt/data/products-100.csv as default sample if no local_path provided
- Dry-run + confirmation for destructive operations
- Local commands: "show resolved schema", "use uploaded file"
"""

import json
import os
import traceback
from typing import Optional

from tools.tool_registry import tools, tool_schemas
from agent.models import call_ollama, SYSTEM_PROMPT
from agent.confirmation import ConfirmationManager

# Hint: the developer message says a user-uploaded file exists at /mnt/data/products-100.csv
UPLOADED_FILE_DEFAULT = "/mnt/data/products-100.csv"


class S3Agent:
    def __init__(self):
        # Conversation memory for the LLM
        self.conversation_history = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]

        # Cache of known buckets (from list_buckets)
        self.last_buckets = []

        # Confirmation manager for destructive operations
        self.confirmation = ConfirmationManager()

        # Schema disambiguation state
        self.pending_schema: Optional[dict] = None
        self.pending_columns: list = []
        self.current_column: Optional[str] = None

        # Final schema (applied automatically to create_glue_table)
        # This is set after the interactive disambiguation completes.
        self.final_schema: Optional[list] = None

        # Helper: last selected local file for ETL (defaults to uploaded file if present)
        self.selected_local_file: Optional[str] = None
        if os.path.exists(UPLOADED_FILE_DEFAULT):
            self.selected_local_file = UPLOADED_FILE_DEFAULT

    # ---------------------------
    # Public run loop
    # ---------------------------
    def run(self, user_query: str):
        """
        Main agent loop:
          - Accepts user queries (natural language)
          - Routes local commands
          - Handles confirmation / schema resolution special flows
          - Calls Ollama and processes tool calls
        """
        user_query = (user_query or "").strip()
        if not user_query:
            return

        lowered = user_query.lower()

        # Local quick commands that should not be sent to LLM
        if lowered == "show resolved schema":
            self._print_final_schema()
            return

        if lowered == "use uploaded file":
            if os.path.exists(UPLOADED_FILE_DEFAULT):
                self.selected_local_file = UPLOADED_FILE_DEFAULT
                print(f"\n✓ Selected uploaded file: {self.selected_local_file}")
            else:
                print("\n✗ No uploaded file found at", UPLOADED_FILE_DEFAULT)
            return

        # Append user input to conversation history
        self.conversation_history.append({"role": "user", "content": user_query})

        # 1) Handle confirm command
        if lowered.startswith("confirm"):
            return self._handle_confirmation()

        # 2) Cancel pending destructive actions or schema resolution
        if lowered == "cancel":
            self.confirmation.clear()
            self.pending_schema = None
            self.pending_columns = []
            self.current_column = None
            print("\n❎ Operation cancelled.")
            return

        # 3) If we are in the middle of schema disambiguation, route input there
        if self.pending_schema is not None:
            return self._handle_schema_disambiguation(user_query)

        # 4) Normal flow: call LLM
        print("\n============================================================")
        print("🤖 Processing...")
        print("============================================================")

        msg = call_ollama(self.conversation_history)
        if not msg:
            print("❌ LLM error or no response.")
            return

        content = msg.get("content", "")
        tool_calls = msg.get("tool_calls", [])

        # record assistant message (including tool_calls) to history
        self.conversation_history.append({
            "role": "assistant",
            "content": content or "",
            "tool_calls": tool_calls
        })

        if not tool_calls:
            if content:
                print("\n💬", content)
            return

        # Execute each tool call returned by model
        for call in tool_calls:
            self._execute_tool_call(call)

    # ---------------------------
    # Confirmation handling
    # ---------------------------
    def _handle_confirmation(self):
        if not self.confirmation.has_pending():
            print("\n⚠️ Nothing to confirm.")
            return

        pending = self.confirmation.get_pending()
        self.confirmation.clear()
        tool_name = pending["tool"]
        args = pending["args"]

        func = tools.get(tool_name)
        if not func:
            print(f"\n✗ Unknown tool: {tool_name}")
            return

        print("\n🔐 Confirmation received → executing real operation...\n")
        try:
            result = func(**args)
        except Exception as e:
            result = {"success": False, "error": str(e)}

        if result.get("success"):
            print("   ✓ Operation succeeded:", result.get("message", ""))
        else:
            print("   ✗ Operation failed:", result.get("error", ""))

        self._record_tool_result(tool_name, result)

    # ---------------------------
    # Print final schema helper
    # ---------------------------
    def _print_final_schema(self):
        if not self.final_schema:
            # Try reading persisted schema via tool if available
            try:
                get_schema_func = tools.get("get_resolved_schema")
                if get_schema_func:
                    resp = get_schema_func()
                    schema = resp.get("schema", [])
                    if schema:
                        print("\n📋 Final resolved schema (from persisted store):")
                        print(json.dumps(schema, indent=2))
                        return
            except Exception:
                pass

            print("\nℹ️ No resolved schema currently stored.")
            return
        print("\n📋 Final resolved schema (in-memory):")
        print(json.dumps(self.final_schema, indent=2))

    # ---------------------------
    # Schema disambiguation state machine
    # ---------------------------
    def _start_schema_disambiguation(self, inference_result: dict):
        """
        Initialize interactive schema resolution:
        - Store pending schema
        - Create pending_columns (ordered)
        - Ask the user the first question
        """
        self.pending_schema = inference_result
        ambiguous = inference_result.get("ambiguous", [])
        self.pending_columns = [a["column"] for a in ambiguous]
        if not self.pending_columns:
            self.pending_schema = None
            self.current_column = None
            return

        self.current_column = self.pending_columns[0]
        print(f"\nColumn '{self.current_column}' has ambiguous types.")
        print("Detected possibilities (sampled). Please reply with one of:")
        print("int, float, string, bool, timestamp\n")

    def _handle_schema_disambiguation(self, user_input: str):
        """
        Accept a single-type reply from user, apply it to the current ambiguous column,
        then move on to the next ambiguous column until all are resolved.
        """
        if self.pending_schema is None or not self.pending_columns:
            print("\n⚠️ No pending schema resolution.")
            return

        # Normalize user input
        t = user_input.strip().lower()
        aliases = {"boolean": "bool", "double": "float", "integer": "int"}
        if t in aliases:
            t = aliases[t]

        valid_types = {"int", "float", "string", "bool", "timestamp"}
        if t not in valid_types:
            print("\n⚠️ Please reply with one of: int, float, string, bool, timestamp")
            return

        # Apply the chosen type to the current column
        col_name = self.current_column
        columns = self.pending_schema.get("columns", [])
        applied = False
        for c in columns:
            if c.get("Name") == col_name:
                c["Type"] = t
                c["Confidence"] = 1.0
                applied = True
                break

        if not applied:
            # Defensive fallback: find ambiguous entry and update
            for amb in self.pending_schema.get("ambiguous", []):
                if amb.get("column") == col_name:
                    for c in columns:
                        if c.get("Name") == col_name:
                            c["Type"] = t
                            c["Confidence"] = 1.0
                            applied = True
                            break
                    if applied:
                        break

        print(f"\n✓ Applied type '{t}' to column '{col_name}'")

        # Remove the resolved column from the pending list and advance
        if self.pending_columns and self.pending_columns[0] == col_name:
            self.pending_columns.pop(0)
        else:
            try:
                self.pending_columns.remove(col_name)
            except ValueError:
                pass

        if not self.pending_columns:
            # All resolved
            print("\n🎉 All ambiguous columns resolved!")
            final_cols = self.pending_schema.get("columns", [])
            # Store the final schema for auto-injection
            self.final_schema = final_cols

            print("Final schema:")
            print(json.dumps(final_cols, indent=2))

            # Append final schema to conversation memory so model can act on it
            self.conversation_history.append({
                "role": "assistant",
                "content": f"Schema resolution complete. Final schema:\n{json.dumps(final_cols, indent=2)}"
            })

            # Persist final schema via the use_schema tool if available (so disk + MCP share)
            try:
                use_schema_func = tools.get("use_schema")
                if use_schema_func:
                    # Build compact schema_text: "Name=Type\n..."
                    lines = []
                    for c in final_cols:
                        name = c.get("Name")
                        typ = c.get("Type")
                        lines.append(f"{name}={typ}")
                    schema_text = "\n".join(lines)
                    persist_resp = use_schema_func(schema_text)
                    # Print persist status
                    if persist_resp.get("success"):
                        print("   ✓ Final schema persisted to .resolved_schema.json")
                    else:
                        print("   ✗ Failed to persist schema:", persist_resp.get("error"))
            except Exception as e:
                print("   ✗ Exception when persisting schema:", str(e))

            # Clear pending state
            self.current_column = None
            self.pending_schema = None
            self.pending_columns = []
            return

        # Otherwise, ask next column
        self.current_column = self.pending_columns[0]
        print(f"\nNext: Column '{self.current_column}' has ambiguous types.")
        print("Please reply with one of: int, float, string, bool, timestamp\n")
        return

    # ---------------------------
    # Execute tool call
    # ---------------------------
    def _execute_tool_call(self, call: dict):
        """
        Execute a tool call dict returned from the LLM.
        Handles:
          - destructive tool dry-run + confirmation
          - special tool infer_schema_from_csv with robust sample_limit handling
          - auto-inject final_schema into create_glue_table
          - run_production_etl default handling for uploaded file
          - normal safe tool invocation
        """
        tool_name = call["function"]["name"]
        raw_args = call["function"].get("arguments", "{}")

        try:
            args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
        except Exception:
            args = {}

        print(f"\n🔧 Requested Tool: {tool_name}({args})")

        func = tools.get(tool_name)
        if not func:
            err = {"success": False, "error": f"Unknown tool: {tool_name}"}
            print("   ✗", err["error"])
            self._record_tool_result(tool_name, err)
            return

        # Destructive operations: run dry-run first and require explicit confirmation
        destructive_tools = {
            "delete_bucket",
            "delete_all_buckets",
            "delete_object",
            "delete_all_objects_in_bucket",
            "delete_glue_database",
            "delete_glue_table",
            "move_all_objects",
            "copy_all_objects",
        }

        if tool_name in destructive_tools:
            print("⚠️ SAFETY CHECK: Running dry-run before real execution...")
            preview_args = dict(args) if isinstance(args, dict) else {}
            preview_args["dry_run"] = True
            try:
                dry_run_result = func(**preview_args)
            except TypeError:
                dry_run_result = {"success": True, "message": "(dry-run) Simulated (function has no dry_run param)."}
            except Exception as e:
                dry_run_result = {"success": False, "error": str(e)}

            print("\n🔍 Dry-run Result:", dry_run_result)
            self.confirmation.require_confirmation(tool_name, args)
            print("\n❗ This is a DESTRUCTIVE OPERATION. To proceed, type:")
            print(f"   CONFIRM {tool_name}")
            print("Or type: cancel\n")

            self._record_tool_result(tool_name, dry_run_result)
            return

        # Special handling: schema inference tool
        if tool_name == "infer_schema_from_csv":
            # be tolerant of sample_limit being omitted, None or string
            sample_limit = args.get("sample_limit", None)
            if sample_limit is None:
                args["sample_limit"] = 500  # default
            else:
                # coerce numeric strings to int
                try:
                    if isinstance(sample_limit, str) and sample_limit.strip() != "":
                        args["sample_limit"] = int(sample_limit)
                    elif isinstance(sample_limit, (int, float)):
                        args["sample_limit"] = int(sample_limit)
                    else:
                        args["sample_limit"] = 500
                except Exception:
                    args["sample_limit"] = 500

            # If local_csv_path omitted, try selected_local_file
            if not args.get("local_csv_path"):
                if self.selected_local_file:
                    args["local_csv_path"] = self.selected_local_file

            try:
                result = func(**args)
            except Exception as e:
                result = {"success": False, "error": str(e)}

            self._record_tool_result(tool_name, result)

            if not result.get("success"):
                print("   ✗ Schema inference failed:", result.get("error"))
                return

            print("   ✓ Schema inference complete.")

            if result.get("needs_user_help"):
                print("\n⚠️ Ambiguous Columns Detected:")
                for amb in result.get("ambiguous", []):
                    det = amb.get("detected_types", {})
                    try:
                        det_sorted = sorted(det.items(), key=lambda x: -x[1])
                    except Exception:
                        det_sorted = det.items()
                    det_str = ", ".join([f"{k}:{(v*100):.0f}%" if isinstance(v, float) else f"{k}:{v}" for k, v in det_sorted])
                    print(f" - {amb.get('column')}  ({det_str})")

                # Initialize the interactive resolution state
                self._start_schema_disambiguation(result)
                # Pending state now active; wait for user replies one by one
                return

            # No ambiguity -> print schema and store as final
            print("\n✓ No ambiguity. Schema ready for Glue table creation.")
            final_cols = result.get("columns", [])
            self.final_schema = final_cols
            # Persist final schema using use_schema tool if available
            try:
                use_schema_func = tools.get("use_schema")
                if use_schema_func:
                    lines = []
                    for c in final_cols:
                        lines.append(f"{c.get('Name')}={c.get('Type')}")
                    schema_text = "\n".join(lines)
                    persist_resp = use_schema_func(schema_text)
                    if persist_resp.get("success"):
                        print("   ✓ Final schema persisted to .resolved_schema.json")
                    else:
                        print("   ✗ Failed to persist schema:", persist_resp.get("error"))
            except Exception:
                pass
            print(final_cols)
            return

        # Intercept create_glue_table to auto-inject final_schema when appropriate
        if tool_name == "create_glue_table":
            # Normalize args
            args = dict(args) if isinstance(args, dict) else {}
            columns_arg = args.get("columns", None)

            # Recognize common placeholders that mean "use inferred schema"
            placeholders = {None, "", "INFERRED_SCHEMA", "INFERED_SCHEMA", "inferred_schema", "[]", [], "null", "None"}

            if (columns_arg in placeholders) and self.final_schema:
                args["columns"] = self.final_schema
                # If format missing, prefer 'csv' for backwards compatibility
                if "format" not in args or not args.get("format"):
                    args["format"] = "csv"
                print("   ℹ️ Auto-injected resolved schema into create_glue_table call.")
            elif (columns_arg in placeholders) and not self.final_schema:
                err = {"success": False, "error": "No resolved schema available to create table. Run schema inference first."}
                print("   ✗", err["error"])
                self._record_tool_result(tool_name, err)
                return

            # Now call the function with finalized args
            try:
                result = func(**args)
            except TypeError as e:
                result = {"success": False, "error": f"Wrong parameters: {str(e)}"}
            except Exception as e:
                result = {"success": False, "error": str(e)}

            if result.get("success"):
                msg = result.get("message") or result
                print("   ✓", msg)
            else:
                print("   ✗", result.get("error"))

            self._record_tool_result(tool_name, result)
            return

        # Special handling: run_production_etl orchestrator default selection
        if tool_name == "run_production_etl":
            # coerce and fill defaults
            args = dict(args) if isinstance(args, dict) else {}
            # If local_path omitted, try selected_local_file (uploaded file)
            if not args.get("local_path"):
                if self.selected_local_file:
                    args["local_path"] = self.selected_local_file
                    print(f"   ℹ️ No local_path provided — using uploaded file: {self.selected_local_file}")
                else:
                    err = {"success": False, "error": "No local_path provided and no uploaded file available"}
                    print("   ✗", err["error"])
                    self._record_tool_result(tool_name, err)
                    return

            # Call orchestrator
            try:
                result = func(**args)
            except TypeError as e:
                result = {"success": False, "error": f"Wrong parameters: {str(e)}"}
            except Exception as e:
                result = {"success": False, "error": str(e), "trace": traceback.format_exc()}

            # If orchestrator returns next:disambiguate, we should initialize pending_schema in agent
            if isinstance(result, dict) and result.get("next") == "disambiguate":
                ambiguous = result.get("ambiguous", [])
                # Convert ambiguous into a pending_schema-like structure if possible
                pending = {
                    "columns": result.get("inferred_columns", []) or [],
                    "ambiguous": ambiguous
                }
                # If ambiguous list already contains "detected_types" and others, pass through
                # but prefer calling agent's infer tool to capture full data and interactive flow
                print("   ℹ️ Orchestrator requires interactive disambiguation. Run infer_schema_from_csv or respond to prompts.")
                self._record_tool_result(tool_name, result)
                return

            if result.get("success"):
                print("   ✓ Orchestrator completed. Summary available.")
            else:
                print("   ✗ Orchestrator error:", result.get("error"))

            self._record_tool_result(tool_name, result)
            return

        # Normal safe tool call -> run directly
        try:
            result = func(**args)
        except TypeError as e:
            result = {"success": False, "error": f"Wrong parameters: {str(e)}"}
        except Exception as e:
            result = {"success": False, "error": str(e)}

        if result.get("success"):
            if tool_name == "list_buckets":
                buckets = result.get("buckets", [])
                print(f"   ✓ {len(buckets)} bucket(s): {buckets}")
                self.last_buckets = buckets
            else:
                msg = result.get("message") or result
                print("   ✓", msg)
        else:
            print("   ✗", result.get("error"))

        # Record into conversation
        self._record_tool_result(tool_name, result)

    # ---------------------------
    # Record tool result into conversation history for LLM context
    # ---------------------------
    def _record_tool_result(self, tool_name: str, content_dict: dict):
        try:
            self.conversation_history.append({
                "role": "tool",
                "tool_call_id": f"tool_{tool_name}",
                "name": tool_name,
                "content": json.dumps(content_dict)
            })
        except Exception:
            self.conversation_history.append({
                "role": "tool",
                "tool_call_id": f"tool_{tool_name}",
                "name": tool_name,
                "content": str(content_dict)
            })

    # ---------------------------
    # Utility: clear history & state
    # ---------------------------
    def clear_history(self):
        self.conversation_history = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]
        self.last_buckets = []
        self.confirmation.clear()
        self.pending_schema = None
        self.pending_columns = []
        self.current_column = None
        self.final_schema = None
        # keep selected_local_file as-is (uploaded file may still exist)
        print("\n🧹 Agent history cleared!")
