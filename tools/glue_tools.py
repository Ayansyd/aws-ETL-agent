"""
tools/glue_tools.py

Glue Data Catalog helper functions (manual table registration; no crawlers).
Designed for a low-cost POC: only metadata operations (Glue Data Catalog).
All functions return dicts with a 'success' boolean and helpful metadata/messages.

Functions:
- create_glue_database(name, description=None, dry_run=False)
- delete_glue_database(name, dry_run=False)
- list_glue_databases()
- create_glue_table(database, table, s3_location, columns, partition_keys=None, format='csv', dry_run=False)
- get_glue_table(database, table)
- list_glue_tables(database)
- delete_glue_table(database, table, dry_run=False)

Naming convention: hybrid (uses user-provided names when possible, then slugifies/sanitizes).
"""

import re
import uuid
from botocore.exceptions import ClientError
from config.aws_session import get_boto3_session

# Glue client (shared)
session = get_boto3_session()
glue = session.client("glue")

# ---------- Helpers ----------

def _slugify(name: str, fallback_prefix="obj") -> str:
    """
    Create a safe Glue-compatible name from free-form text.
    Rules:
      - lowercase
      - replace spaces and hyphens with underscore
      - allow letters, numbers and underscores
      - strip leading non-letter/number
      - fallback to prefix + uuid if result becomes empty
    """
    if not name:
        return f"{fallback_prefix}_{uuid.uuid4().hex[:8]}"

    s = name.strip().lower()
    s = re.sub(r"[ \-]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"^_+", "", s)
    if not s:
        return f"{fallback_prefix}_{uuid.uuid4().hex[:8]}"
    # Ensure length not too long (Glue allows fairly long names, keep safe)
    return s[:120]


def _map_column_type_to_glue(col_type: str) -> str:
    """
    Map a friendly/inferred type to Glue/Hive type.
    Accepts (case-insensitive): int, integer, long, float, double, boolean, timestamp, string
    Default: string
    """
    if not col_type:
        return "string"
    t = col_type.strip().lower()
    if t in ("int", "integer", "tinyint", "smallint"):
        return "int"
    if t in ("bigint", "long"):
        return "bigint"
    if t in ("float",):
        return "float"
    if t in ("double", "real"):
        return "double"
    if t in ("boolean", "bool"):
        return "boolean"
    if t in ("timestamp", "ts", "datetime", "date"):
        return "timestamp"
    # default
    return "string"


def _build_storage_descriptor(s3_location: str, columns: list, partition_keys: list = None, fmt: str = "csv"):
    """
    Build StorageDescriptor + SerDeInfo according to format.
    columns: list of dicts: [{"Name": "col1", "Type": "string", "Comment": "..."}]
    partition_keys: same shape as columns for partitions
    fmt: 'csv' or 'parquet'
    """
    if fmt.lower() == "parquet":
        input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        serde_info = {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "Parameters": {}}
    else:
        # default CSV using LazySimpleSerDe
        input_format = "org.apache.hadoop.mapred.TextInputFormat"
        output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        serde_info = {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "Parameters": {"field.delim": ",", "skip.header.line.count": "0"}
        }

    sd = {
        "Columns": columns,
        "Location": s3_location,
        "InputFormat": input_format,
        "OutputFormat": output_format,
        "Compressed": False,
        "NumberOfBuckets": -1,
        "SerdeInfo": serde_info,
        "BucketColumns": [],
        "SortColumns": [],
        "Parameters": {}
    }

    if partition_keys:
        sd_partition_keys = partition_keys
    else:
        sd_partition_keys = []

    return sd, sd_partition_keys


# ---------- Glue operations ----------

def create_glue_database(name: str, description: str = None, dry_run: bool = False):
    """
    Create a Glue database (DatabaseInput). Returns dict.
    Uses sanitized name (slugified). Returns the actual database name used.
    """
    try:
        db_name = _slugify(name, fallback_prefix="db")
        db_input = {"Name": db_name}
        if description:
            db_input["Description"] = description

        if dry_run:
            return {"success": True, "message": f"(dry-run) Would create Glue database '{db_name}'", "database": db_name}

        resp = glue.create_database(DatabaseInput=db_input)
        # Glue create_database returns empty response on success
        return {"success": True, "message": f"Created Glue database '{db_name}'", "database": db_name}
    except ClientError as e:
        # If already exists, report gracefully
        err = str(e)
        if "AlreadyExistsException" in err or "AlreadyExists" in err:
            return {"success": True, "message": f"Glue database '{db_name}' already exists", "database": db_name}
        return {"success": False, "error": err}


def delete_glue_database(name: str, dry_run: bool = False):
    """
    Delete Glue database by name (expects user-provided or sanitized name).
    WARNING: Glue won't delete tables under the db automatically (you must delete tables first).
    """
    try:
        db_name = _slugify(name, fallback_prefix="db")
        if dry_run:
            return {"success": True, "message": f"(dry-run) Would delete Glue database '{db_name}'", "database": db_name}

        glue.delete_database(Name=db_name)
        return {"success": True, "message": f"Deleted Glue database '{db_name}'", "database": db_name}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def list_glue_databases():
    """List Glue databases (paginated)."""
    try:
        paginator = glue.get_paginator("get_databases")
        dbs = []
        for page in paginator.paginate():
            for d in page.get("DatabaseList", []):
                dbs.append(d.get("Name"))
        return {"success": True, "databases": dbs, "count": len(dbs)}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def create_glue_table(database: str, table: str, s3_location: str, columns: list, partition_keys: list = None, format: str = "csv", table_comment: str = None, dry_run: bool = False):
    """
    Create a Glue table pointing at s3_location with the provided column spec.
    - database: human-friendly name (will be slugified)
    - table: human-friendly name (slugified)
    - s3_location: full s3 path e.g. s3://bucket/path/
    - columns: list of {"Name": "col", "Type": "string", "Comment": "..."} - types should be Glue types
    - partition_keys: same shape as columns (optional)
    - format: 'csv' or 'parquet'
    - dry_run: if True, preview only
    Returns dict with success and actual names
    """
    try:
        db_name = _slugify(database, fallback_prefix="db")
        tbl_name = _slugify(table, fallback_prefix="tbl")

        # Map types defensively
        safe_columns = []
        for c in columns:
            name = c.get("Name")
            t = c.get("Type")
            glue_t = _map_column_type_to_glue(t)
            safe_columns.append({"Name": name, "Type": glue_t, "Comment": c.get("Comment", "")})

        safe_partitions = []
        if partition_keys:
            for p in partition_keys:
                pname = p.get("Name")
                pt = p.get("Type")
                glue_t = _map_column_type_to_glue(pt)
                safe_partitions.append({"Name": pname, "Type": glue_t, "Comment": p.get("Comment", "")})

        sd, partition_defs = _build_storage_descriptor(s3_location, safe_columns, safe_partitions, fmt=format)

        table_input = {
            "Name": tbl_name,
            "Description": table_comment or f"Table {tbl_name} registered by ETL agent",
            "Owner": "etl-agent",
            "Parameters": {"classification": format.lower()},
            "StorageDescriptor": sd,
            "PartitionKeys": partition_defs
        }

        if dry_run:
            return {
                "success": True,
                "message": f"(dry-run) Would create table '{tbl_name}' in database '{db_name}' pointing to {s3_location}",
                "database": db_name,
                "table": tbl_name,
                "table_input": table_input
            }

        # Ensure database exists (idempotent behavior)
        try:
            glue.get_database(Name=db_name)
        except ClientError as e:
            # If DB not found, create it automatically (safe convenience)
            create_resp = create_glue_database(db_name)
            if not create_resp.get("success"):
                return {"success": False, "error": f"Failed to create database '{db_name}': {create_resp.get('error')}"}

        # Create table
        glue.create_table(DatabaseName=db_name, TableInput=table_input)
        return {"success": True, "message": f"Created table '{tbl_name}' in database '{db_name}'", "database": db_name, "table": tbl_name}
    except ClientError as e:
        # If already exists, return success with message
        err = str(e)
        if "AlreadyExistsException" in err or "AlreadyExists" in err:
            return {"success": True, "message": f"Table '{tbl_name}' already exists in database '{db_name}'", "database": db_name, "table": tbl_name}
        return {"success": False, "error": err}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_glue_table(database: str, table: str):
    """Retrieve table metadata."""
    try:
        db_name = _slugify(database, fallback_prefix="db")
        tbl_name = _slugify(table, fallback_prefix="tbl")
        resp = glue.get_table(DatabaseName=db_name, Name=tbl_name)
        return {"success": True, "table": resp.get("Table")}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def list_glue_tables(database: str):
    """List tables in a glue database (paginated)."""
    try:
        db_name = _slugify(database, fallback_prefix="db")
        paginator = glue.get_paginator("get_tables")
        tables = []
        for page in paginator.paginate(DatabaseName=db_name):
            for t in page.get("TableList", []):
                tables.append(t.get("Name"))
        return {"success": True, "database": db_name, "tables": tables, "count": len(tables)}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def delete_glue_table(database: str, table: str, dry_run: bool = False):
    """Delete a glue table (preview with dry_run)."""
    try:
        db_name = _slugify(database, fallback_prefix="db")
        tbl_name = _slugify(table, fallback_prefix="tbl")

        if dry_run:
            return {"success": True, "message": f"(dry-run) Would delete table '{tbl_name}' from database '{db_name}'", "database": db_name, "table": tbl_name}

        glue.delete_table(DatabaseName=db_name, Name=tbl_name)
        return {"success": True, "message": f"Deleted table '{tbl_name}' from '{db_name}'", "database": db_name, "table": tbl_name}
    except ClientError as e:
        return {"success": False, "error": str(e)}
