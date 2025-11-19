"""
tools/tool_registry.py

This file exposes:
- tools: function registry for the agent
- tool_schemas: JSON schemas for Ollama tool-calling
"""

from .s3_tools import (
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

# ==========================
# TOOL REGISTRY
# ==========================

tools = {
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
}

# ==========================
# TOOL SCHEMAS
# ==========================

tool_schemas = [

    # CREATE BUCKET
    {
        "type": "function",
        "function": {
            "name": "create_bucket",
            "description": "Create a new S3 bucket.",
            "parameters": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"]
            }
        }
    },

    # LIST BUCKETS
    {
        "type": "function",
        "function": {
            "name": "list_buckets",
            "description": "List all S3 buckets.",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },

    # DELETE BUCKET
    {
        "type": "function",
        "function": {
            "name": "delete_bucket",
            "description": "Delete an S3 bucket. Optional: dry_run (boolean).",
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

    # DELETE ALL BUCKETS
    {
        "type": "function",
        "function": {
            "name": "delete_all_buckets",
            "description": "Delete ALL S3 buckets in the account. Optional: dry_run.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dry_run": {"type": "boolean"}
                },
                "required": []
            }
        }
    },

    # LIST OBJECTS
    {
        "type": "function",
        "function": {
            "name": "list_objects",
            "description": "List all object keys in a bucket.",
            "parameters": {
                "type": "object",
                "properties": {
                    "bucket": {"type": "string"}
                },
                "required": ["bucket"]
            }
        }
    },

    # DELETE ALL OBJECTS IN BUCKET
    {
        "type": "function",
        "function": {
            "name": "delete_all_objects_in_bucket",
            "description": "Delete ALL objects in a bucket. Optional: dry_run.",
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

    # PUT OBJECT
    {
        "type": "function",
        "function": {
            "name": "put_object",
            "description": "Upload an object to S3.",
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

    # GET OBJECT
    {
        "type": "function",
        "function": {
            "name": "get_object",
            "description": "Retrieve object content.",
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

    # DELETE SINGLE OBJECT
    {
        "type": "function",
        "function": {
            "name": "delete_object",
            "description": "Delete a single object. Optional: dry_run.",
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

    # COPY OBJECT
    {
        "type": "function",
        "function": {
            "name": "copy_object",
            "description": "Copy one object to another bucket.",
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

    # MOVE OBJECT
    {
        "type": "function",
        "function": {
            "name": "move_object",
            "description": "Move a single object (copy + delete).",
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

    # COPY ALL OBJECTS
    {
        "type": "function",
        "function": {
            "name": "copy_all_objects",
            "description": "Copy ALL objects between buckets. Optional: dry_run.",
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

    # MOVE ALL OBJECTS
    {
        "type": "function",
        "function": {
            "name": "move_all_objects",
            "description": "Move ALL objects between buckets. Optional: dry_run.",
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
    }
]
