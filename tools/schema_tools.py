"""
tools/schema_tools.py

Tiny, robust schema store + helpers so tools and agent can share resolved schema
without requiring the agent instance to be passed around.

Design:
- Uses a local JSON file (.resolved_schema.json) in the repository root to persist resolved schema.
- Exposes:
    - parse_schema_text(schema_text) -> list[{"Name":..., "Type":...}]
    - get_resolved_schema() -> {success, schema}
    - use_schema(schema_text) -> {success, schema}
- Safe to call from the agent and also callable as a tool by the LLM (deterministic JSON outputs).
"""

import json
import os
from typing import List, Dict

_STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".resolved_schema.json")


def _read_state() -> Dict:
    if not os.path.exists(_STATE_FILE):
        return {}
    try:
        with open(_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_state(data: Dict):
    try:
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        return False


def parse_schema_text(schema_text: str) -> List[Dict]:
    """
    Parse a schema block in formats such as:
      - "Index=int\nPrice=float\n..."
      - "Name string\nPrice float"
      - comma separated "Index=int, Price=float"
    Returns: list of {"Name": "<col>", "Type": "<type>"}
    """
    if not schema_text or not schema_text.strip():
        return []

    cols = []
    # allow comma-separated single-line
    if "," in schema_text and "\n" not in schema_text:
        parts = [p.strip() for p in schema_text.split(",") if p.strip()]
        for p in parts:
            if "=" in p:
                name, typ = [x.strip() for x in p.split("=", 1)]
            else:
                parts2 = p.split()
                name = parts2[0]
                typ = parts2[1] if len(parts2) > 1 else "string"
            cols.append({"Name": name, "Type": typ})
        return cols

    # otherwise treat as line separated
    for line in schema_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if "=" in line:
            name, typ = [p.strip() for p in line.split("=", 1)]
        else:
            parts = line.split()
            if len(parts) >= 2:
                name, typ = parts[0], parts[1]
            else:
                name, typ = line, "string"
        cols.append({"Name": name, "Type": typ})
    return cols


def get_resolved_schema():
    """
    Return the currently persisted resolved schema (if any).
    Output:
    {"success": True, "schema": [ {Name, Type, Confidence?}, ... ], "message": "..." }
    """
    state = _read_state()
    schema = state.get("final_schema")
    if not schema:
        return {"success": True, "schema": [], "message": "No resolved schema stored"}
    return {"success": True, "schema": schema, "message": "Returning resolved schema"}


def use_schema(schema_text: str):
    """
    Parse and persist the provided schema_text as the final resolved schema.
    Example schema_text:
      Index=int
      Price=float
      Stock=int
    Returns: {"success": True, "schema": [ ... ]}
    """
    try:
        parsed = parse_schema_text(schema_text)
        # normalize types
        alias = {"integer": "int", "boolean": "bool", "double": "float"}
        normalized = []
        for c in parsed:
            t = c.get("Type", "string").strip().lower()
            t = alias.get(t, t)
            normalized.append({"Name": c.get("Name"), "Type": t, "Confidence": 1.0})
        state = _read_state()
        state["final_schema"] = normalized
        _write_state(state)
        return {"success": True, "schema": normalized, "message": "Schema applied and persisted"}
    except Exception as e:
        return {"success": False, "error": str(e)}
