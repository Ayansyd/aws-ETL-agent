"""
tools/tool_registry.py

Central registry of all tools + JSON schemas for LLM tool calling.
Now includes:
- S3 tools
- Glue Data Catalog tools
- Schema inference tools
- Upload-local-folder
- Schema tools (get_resolved_schema, use_schema)
- Production ETL orchestrator (run_production_etl)
"""

# -------------------------------------
# IMPORT ALL TOOL FUNCTIONS
# -------------------------------------

from tools.s3_tools import (
    create_bucket,
    list_buckets,
    delete_bucket,
    delete_all_buckets,
    list_objects,
    delete_all_objects_in_bucket,
    put_object,
    get_object,
    delete_object,
    copy_object,
    move_object,
    copy_all_objects,
    move_all_objects,
)

from tools.glue_tools import (
    create_glue_database,
    delete_glue_database,
    list_glue_databases,
    create_glue_table,
    get_glue_table,
    list_glue_tables,
    delete_glue_table,
)

from tools.schema_inference import infer_schema_from_csv
from tools.upload_local_folder import upload_local_folder_to_s3

# NEW
from tools.schema_tools import get_resolved_schema, use_schema
from tools.etl_orchestrator import run_production_etl


# -------------------------------------
# TOOL FUNCTION REGISTRY
# -------------------------------------

tools = {
    # S3
    "create_bucket": create_bucket,
    "list_buckets": list_buckets,
    "delete_bucket": delete_bucket,
    "delete_all_buckets": delete_all_buckets,
    "list_objects": list_objects,
    "delete_all_objects_in_bucket": delete_all_objects_in_bucket,
    "put_object": put_object,
    "get_object": get_object,
    "delete_object": delete_object,
    "copy_object": copy_object,
    "move_object": move_object,
    "copy_all_objects": copy_all_objects,
    "move_all_objects": move_all_objects,

    # Glue
    "create_glue_database": create_glue_database,
    "delete_glue_database": delete_glue_database,
    "list_glue_databases": list_glue_databases,
    "create_glue_table": create_glue_table,
    "get_glue_table": get_glue_table,
    "list_glue_tables": list_glue_tables,
    "delete_glue_table": delete_glue_table,

    # Inference / Upload
    "infer_schema_from_csv": infer_schema_from_csv,
    "upload_local_folder_to_s3": upload_local_folder_to_s3,

    # NEW: Schema tools
    "get_resolved_schema": get_resolved_schema,
    "use_schema": use_schema,

    # NEW: Production ETL Orchestrator
    "run_production_etl": run_production_etl,
}


# -------------------------------------
# JSON SCHEMAS FOR LLM TOOL CALLS
# -------------------------------------

tool_schemas = [

    #
    # UPLOAD LOCAL FOLDER
    #
    {
        "type": "function",
        "function": {
            "name": "upload_local_folder_to_s3",
            "description": "Upload a local folder recursively into an S3 bucket.",
            "parameters": {
                "type": "object",
                "properties": {
                    "local_path": {"type": "string"},
                    "bucket": {"type": "string"},
                    "s3_prefix": {"type": "string"}
                },
                "required": ["local_path", "bucket", "s3_prefix"]
            }
        }
    },

    #
    # S3 – Create bucket
    #
    {
        "type": "function",
        "function": {
            "name": "create_bucket",
            "description": "Create an S3 bucket.",
            "parameters": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"]
            }
        }
    },

    #
    # S3 – List buckets
    #
    {
        "type": "function",
        "function": {
            "name": "list_buckets",
            "description": "List S3 buckets.",
            "parameters": {"type": "object", "properties": {}}
        }
    },

    #
    # S3 – Delete bucket
    #
    {
        "type": "function",
        "function": {
            "name": "delete_bucket",
            "description": "Delete an S3 bucket (optional dry_run).",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "dry_run": {"type": "boolean"}
                },
                "required": ["name"]
            }
        }
    },

    #
    # S3 – Delete all buckets
    #
    {
        "type": "function",
        "function": {
            "name": "delete_all_buckets",
            "description": "Delete ALL S3 buckets (optional dry_run).",
            "parameters": {
                "type": "object",
                "properties": {"dry_run": {"type": "boolean"}}
            }
        }
    },

    #
    # S3 – List objects
    #
    {
        "type": "function",
        "function": {
            "name": "list_objects",
            "description": "List objects in an S3 bucket.",
            "parameters": {
                "type": "object",
                "properties": {"bucket": {"type": "string"}},
                "required": ["bucket"]
            }
        }
    },

    #
    # S3 – Delete all objects in bucket
    #
    {
        "type": "function",
        "function": {
            "name": "delete_all_objects_in_bucket",
            "description": "Delete all objects in an S3 bucket (optional dry_run).",
            "parameters": {
                "type": "object",
                "properties": {
                    "bucket": {"type": "string"},
                    "dry_run": {"type": "boolean"}
                },
                "required": ["bucket"]
            }
        }
    },

    #
    # S3 – put/get/delete
    #
    {
        "type": "function",
        "function": {
            "name": "put_object",
            "description": "Upload a string object to S3.",
            "parameters": {
                "type": "object",
                "properties": {
                    "bucket": {"type": "string"},
                    "key": {"type": "string"},
                    "body": {"type": "string"}
                },
                "required": ["bucket", "key", "body"]
            }
        }
    },

    {
        "type": "function",
        "function": {
            "name": "get_object",
            "description": "Get an object from S3.",
            "parameters": {
                "type": "object",
                "properties": {
                    "bucket": {"type": "string"},
                    "key": {"type": "string"}
                },
                "required": ["bucket", "key"]
            }
        }
    },

    {
        "type": "function",
        "function": {
            "name": "delete_object",
            "description": "Delete an S3 object (optional dry_run).",
            "parameters": {
                "type": "object",
                "properties": {
                    "bucket": {"type": "string"},
                    "key": {"type": "string"},
                    "dry_run": {"type": "boolean"}
                },
                "required": ["bucket", "key"]
            }
        }
    },

    #
    # S3 – copy/move
    #
    {
        "type": "function",
        "function": {
            "name": "copy_object",
            "description": "Copy a single S3 object.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_bucket": {"type": "string"},
                    "source_key": {"type": "string"},
                    "dest_bucket": {"type": "string"},
                    "dest_key": {"type": "string"}
                },
                "required": ["source_bucket", "source_key", "dest_bucket"]
            }
        }
    },

    {
        "type": "function",
        "function": {
            "name": "move_object",
            "description": "Move a single S3 object.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_bucket": {"type": "string"},
                    "source_key": {"type": "string"},
                    "dest_bucket": {"type": "string"},
                    "dest_key": {"type": "string"}
                },
                "required": ["source_bucket", "source_key", "dest_bucket"]
            }
        }
    },

    #
    # S3 – copy/move all objects
    #
    {
        "type": "function",
        "function": {
            "name": "copy_all_objects",
            "description": "Copy ALL objects between buckets (optional dry_run).",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_bucket": {"type": "string"},
                    "dest_bucket": {"type": "string"},
                    "dry_run": {"type": "boolean"}
                },
                "required": ["source_bucket", "dest_bucket"]
            }
        }
    },

    {
        "type": "function",
        "function": {
            "name": "move_all_objects",
            "description": "Move ALL objects between buckets (optional dry_run).",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_bucket": {"type": "string"},
                    "dest_bucket": {"type": "string"},
                    "dry_run": {"type": "boolean"}
                },
                "required": ["source_bucket", "dest_bucket"]
            }
        }
    },

    # -------------------------------------
    # Glue Tools
    # -------------------------------------

    {
        "type": "function",
        "function": {
            "name": "create_glue_database",
            "description": "Create a Glue database (optional dry_run).",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "description": {"type": "string"},
                    "dry_run": {"type": "boolean"}
                },
                "required": ["name"]
            }
        }
    },

    {
        "type": "function",
        "function": {
            "name": "delete_glue_database",
            "description": "Delete a Glue database (optional dry_run).",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "dry_run": {"type": "boolean"}
                },
                "required": ["name"]
            }
        }
    },

    {
        "type": "function",
        "function": {
            "name": "list_glue_databases",
            "description": "List Glue databases.",
            "parameters": {"type": "object", "properties": {}}
        }
    },

    {
        "type": "function",
        "function": {
            "name": "create_glue_table",
            "description": "Create a Glue table. If columns=None, agent auto-injects final schema.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {"type": "string"},
                    "table": {"type": "string"},
                    "s3_location": {"type": "string"},
                    "columns": {"type": "array"},
                    "partition_keys": {"type": "array"},
                    "format": {"type": "string"},
                    "table_comment": {"type": "string"},
                    "dry_run": {"type": "boolean"}
                },
                "required": ["database", "table", "s3_location", "columns"]
            }
        }
    },

    {
        "type": "function",
        "function": {
            "name": "delete_glue_table",
            "description": "Delete a Glue table (optional dry_run).",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {"type": "string"},
                    "table": {"type": "string"},
                    "dry_run": {"type": "boolean"}
                },
                "required": ["database", "table"]
            }
        }
    },

    {
        "type": "function",
        "function": {
            "name": "list_glue_tables",
            "description": "List Glue tables in a database.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {"type": "string"}
                },
                "required": ["database"]
            }
        }
    },

    # -------------------------------------
    # Schema Inference
    # -------------------------------------

    {
        "type": "function",
        "function": {
            "name": "infer_schema_from_csv",
            "description": "Infer schema from a CSV using hybrid inference + user disambiguation.",
            "parameters": {
                "type": "object",
                "properties": {
                    "local_csv_path": {"type": "string"},
                    "sample_limit": {"type": "number"}
                },
                "required": ["local_csv_path"]
            }
        }
    },

    # -------------------------------------
    # NEW — Schema Tools
    # -------------------------------------

    {
        "type": "function",
        "function": {
            "name": "get_resolved_schema",
            "description": "Return the current resolved schema (persisted).",
            "parameters": {"type": "object", "properties": {}}
        }
    },

    {
        "type": "function",
        "function": {
            "name": "use_schema",
            "description": "Apply an explicit schema (persisted).",
            "parameters": {
                "type": "object",
                "properties": {
                    "schema_text": {"type": "string"}
                },
                "required": ["schema_text"]
            }
        }
    },

    # -------------------------------------
    # NEW — ETL Orchestrator (Production)
    # -------------------------------------

    {
        "type": "function",
        "function": {
            "name": "run_production_etl",
            "description": "Run the production ETL orchestrator pipeline.",
            "parameters": {
                "type": "object",
                "properties": {
                    "local_path": {"type": "string"},
                    "bucket": {"type": "string"},
                    "database": {"type": "string"},
                    "table": {"type": "string"},
                    "convert_to_parquet": {"type": "boolean"}
                },
                "required": ["local_path"]
            }
        }
    },
]
