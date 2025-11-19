"""
agent/s3_agent.py

Main Agent for:
- Handling user messages
- Calling Ollama with tool schemas
- Running S3 tools
- Enforcing dry-run + confirmation for destructive operations
- Maintaining conversation memory
"""

import json
from tools.tool_registry import tools, tool_schemas
from agent.models import call_ollama, SYSTEM_PROMPT
from agent.confirmation import ConfirmationManager


class S3Agent:
    def __init__(self):
        # Stores conversation messages
        self.conversation_history = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]

        # Track what buckets exist (helps avoid hallucination)
        self.last_buckets = []

        # Manage unsafe/dangerous operations
        self.confirmation = ConfirmationManager()

    # ============================================================
    #  RUN AGENT LOOP
    # ============================================================
    def run(self, user_query):
        """
        Main loop for agent:
          - append user query
          - check if it's confirm/cancel
          - call LLM
          - run tools
        """
        self.conversation_history.append({"role": "user", "content": user_query})

        # ========================================================
        # CONFIRMATION HANDLING
        # ========================================================
        if user_query.lower().startswith("confirm"):
            if not self.confirmation.has_pending():
                print("\n⚠️ Nothing to confirm.")
                return

            print("\n🔐 Confirmation received → running real operation...\n")
            pending = self.confirmation.get_pending()
            self.confirmation.clear()

            tool_name = pending["tool"]
            tool_args = pending["args"]

            func = tools.get(tool_name)
            if not func:
                print(f"❌ Unknown tool: {tool_name}")
                return

            try:
                result = func(**tool_args)
            except Exception as e:
                result = {"success": False, "error": str(e)}

            if result.get("success"):
                print("   ✓ REAL OPERATION SUCCESS:", result.get("message", result))
            else:
                print("   ✗ FAILURE:", result.get("error", "Unknown error"))

            # Make LLM aware tool executed
            self.conversation_history.append({
                "role": "tool",
                "tool_call_id": f"confirm_{tool_name}",
                "name": tool_name,
                "content": json.dumps(result)
            })
            return

        # Cancel pending operations
        if user_query.lower() == "cancel":
            self.confirmation.clear()
            print("\n❎ Operation cancelled.")
            return

        # ========================================================
        # NORMAL FLOW (not confirm/cancel)
        # ========================================================
        print("\n============================================================")
        print("🤖 Processing...")
        print("============================================================")

        msg = call_ollama(self.conversation_history)
        if not msg:
            print("❌ LLM error")
            return

        content = msg.get("content", "")
        tool_calls = msg.get("tool_calls", [])

        self.conversation_history.append({
            "role": "assistant",
            "content": content if content else "",
            "tool_calls": tool_calls
        })

        # If LLM only replied text
        if not tool_calls:
            if content:
                print("\n💬", content)
            return

        # ========================================================
        # PROCESS TOOL CALLS
        # ========================================================
        for call in tool_calls:
            tool_name = call["function"]["name"]
            raw_args = call["function"].get("arguments", "{}")

            try:
                args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
            except json.JSONDecodeError:
                args = {}

            print(f"\n🔧 Tool request: {tool_name}({args})")

            # Check if tool exists
            func = tools.get(tool_name)
            if not func:
                err = {"success": False, "error": f"Unknown tool: {tool_name}"}
                print("   ✗", err["error"])
                self._record_tool_result(tool_name, err)
                continue

            # ====================================================
            # SAFETY CHECK (dangerous operations)
            # ====================================================
            destructive = [
                "delete_bucket",
                "delete_all_buckets",
                "delete_object",
                "delete_all_objects_in_bucket",
                "move_all_objects",
                "copy_all_objects"
            ]

            if tool_name in destructive:
                print("⚠️ SAFETY CHECK: Running dry-run before real execution…")

                args_with_dry = dict(args)
                args_with_dry["dry_run"] = True

                try:
                    dry_run_result = func(**args_with_dry)
                except Exception as e:
                    dry_run_result = {"success": False, "error": str(e)}

                print("\n🔍 Dry-run Output:", dry_run_result)

                # Ask for confirmation
                self.confirmation.require_confirmation(tool_name, args)
                print("\n❗ DESTRUCTIVE ACTION!")
                print("   To continue, type:")
                print(f"   CONFIRM {tool_name}")
                print("   Or type: cancel")

                # Record dry-run result for LLM context
                self._record_tool_result(tool_name, dry_run_result)
                return

            # ====================================================
            # SAFE TOOL → execute directly
            # ====================================================
            try:
                result = func(**args)
            except Exception as e:
                result = {"success": False, "error": str(e)}

            # Output to terminal (human)
            if result.get("success"):
                message = result.get("message", "")
                if tool_name == "list_buckets":
                    self.last_buckets = result.get("buckets", [])
                    print("   ✓ Buckets:", self.last_buckets)
                else:
                    print(f"   ✓ {message}")
            else:
                print(f"   ✗ {result.get('error')}")

            # Record in conversation
            self._record_tool_result(tool_name, result)

    # ============================================================
    # Utility to add tool results back to conversation history
    # ============================================================
    def _record_tool_result(self, tool_name, content_dict):
        self.conversation_history.append({
            "role": "tool",
            "tool_call_id": f"tool_{tool_name}",
            "name": tool_name,
            "content": json.dumps(content_dict)
        })

    # ============================================================
    # Reset Agent State
    # ============================================================
    def clear_history(self):
        self.conversation_history = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]
        self.last_buckets = []
        self.confirmation.clear()
        print("\n🧹 Agent history cleared!")
