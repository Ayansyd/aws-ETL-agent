"""
tools/schema_inference.py

Hybrid CSV schema inference engine.
- Automatically infers column types (int, float, bool, timestamp, string)
- Samples up to N rows (configurable)
- Detects ambiguous columns (e.g., mixed strings + floats)
- Returns a structured schema used to create Glue tables
- Does NOT require Glue crawlers (free and local)

Output format:

{
  "success": True,
  "columns": [
     {"Name": "sensor_id", "Type": "bigint", "Confidence": 1.0},
     {"Name": "reading",   "Type": "double", "Confidence": 0.95},
     {"Name": "ts",        "Type": "timestamp", "Confidence": 0.60},
  ],
  "ambiguous": [
     {
       "column": "ts",
       "detected_types": {"timestamp": 60%, "string": 40%},
       "reason": "Inconsistent timestamp formats",
     }
  ],
  "needs_user_help": True/False,
  "message": "..."
}

"""

import csv
import os
from datetime import datetime


# ---------------------------------------------------------
# Type detection helpers
# ---------------------------------------------------------

def _is_int(val: str) -> bool:
    try:
        if val.strip() == "":
            return True  # treat empty as nullable
        int(val)
        return True
    except:
        return False


def _is_float(val: str) -> bool:
    try:
        if val.strip() == "":
            return True
        float(val)
        return True
    except:
        return False


def _is_bool(val: str) -> bool:
    if val.strip().lower() in ("true", "false", "yes", "no", "0", "1"):
        return True
    return False


def _is_timestamp(val: str) -> bool:
    # Try multiple timestamp formats
    if not val.strip():
        return True
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%d-%m-%Y",
        "%m/%d/%Y",
    ):
        try:
            datetime.strptime(val.strip(), fmt)
            return True
        except:
            pass
    return False


# ---------------------------------------------------------
# Core inference logic
# ---------------------------------------------------------

def infer_schema_from_csv(
    local_csv_path: str,
    sample_limit: int = 500
):
    """
    Hybrid schema inference:
    1. Read up to sample_limit rows
    2. Test each value for int, float, bool, timestamp
    3. Aggregate type frequencies
    4. Resolve best type OR mark column ambiguous

    Returns:
    - success flag
    - columns list with Name/Type/Confidence
    - ambiguous list requiring user clarification
    """
    if not os.path.exists(local_csv_path):
        return {"success": False, "error": f"File not found: {local_csv_path}"}

    try:
        with open(local_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = []
            for i, row in enumerate(reader):
                if i >= sample_limit:
                    break
                rows.append(row)

        if not rows:
            return {"success": False, "error": "CSV has no data rows"}

        # Columns inferred from header
        columns = reader.fieldnames
        if not columns:
            return {"success": False, "error": "Could not detect CSV header"}

        # Track counts for each type per column
        type_counts = {
            col: {
                "int": 0,
                "float": 0,
                "bool": 0,
                "timestamp": 0,
                "string": 0,
                "total": 0,
            }
            for col in columns
        }

        # Scan rows
        for row in rows:
            for col in columns:
                val = row[col]
                type_counts[col]["total"] += 1

                if _is_int(val):
                    type_counts[col]["int"] += 1
                if _is_float(val):
                    type_counts[col]["float"] += 1
                if _is_bool(val):
                    type_counts[col]["bool"] += 1
                if _is_timestamp(val):
                    type_counts[col]["timestamp"] += 1
                else:
                    # fallback: string
                    type_counts[col]["string"] += 1

        # Resolve best types + detect ambiguity
        final_columns = []
        ambiguous = []
        needs_help = False

        for col in columns:
            stats = type_counts[col]
            total = stats["total"]

            # Compute percentages
            perc = {
                t: stats[t] / total if total > 0 else 0
                for t in ["int", "float", "bool", "timestamp", "string"]
            }

            # Best type = max percentage
            best_type = max(perc, key=perc.get)
            best_conf = perc[best_type]

            # Ambiguity detection (Hybrid logic)
            # Condition: more than 1 type has >20% of values
            high_types = [t for t, p in perc.items() if p > 0.20]

            if len(high_types) > 1:
                needs_help = True
                ambiguous.append({
                    "column": col,
                    "detected_types": perc,
                    "reason": f"Column contains mixed types: {high_types}",
                })
                # Tentatively assign best guess but mark confidence low
                final_columns.append({
                    "Name": col,
                    "Type": best_type,
                    "Confidence": best_conf,
                })
            else:
                # No ambiguity → assign type with confidence
                final_columns.append({
                    "Name": col,
                    "Type": best_type,
                    "Confidence": best_conf,
                })

        return {
            "success": True,
            "columns": final_columns,
            "ambiguous": ambiguous,
            "needs_user_help": needs_help,
            "message": "Schema inference completed",
        }

    except Exception as e:
        return {"success": False, "error": str(e)}
