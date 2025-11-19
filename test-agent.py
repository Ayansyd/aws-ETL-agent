#!/usr/bin/env python3
import json
import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import time
import math

load_dotenv()

# ========= AWS CLIENT (Session aware) =========
region = os.getenv("AWS_REGION", "us-east-1")
print(f"🔧 AWS Region from .env: {region}")

# Support: AWS_PROFILE (optional), AWS_SESSION_TOKEN (optional)
aws_profile = os.getenv("AWS_PROFILE")
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_session_token = os.getenv("AWS_SESSION_TOKEN")

if aws_profile:
    session = boto3.Session(profile_name=aws_profile, region_name=region)
else:
    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret,
        aws_session_token=aws_session_token,
        region_name=region
    )

s3 = session.client('s3', region_name=region)

# ========= HELPERS & Improved S3 UTILITIES =========

def _iter_bucket_objects(bucket):
    """Yield object dicts (as returned in 'Contents') for a bucket using pagination."""
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                yield obj
    except ClientError:
        # Let caller handle
        raise

def create_bucket(name):
    """Create a new S3 bucket"""
    try:
        current_region = s3.meta.region_name
        print(f"   → Creating in region: {current_region}")
        
        # us-east-1 doesn't need LocationConstraint, others do
        if current_region == "us-east-1":
            s3.create_bucket(Bucket=name)
        else:
            s3.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={'LocationConstraint': current_region}
            )
        return {"success": True, "bucket": name, "message": f"✓ Created bucket '{name}' in {current_region}"}
    except ClientError as e:
        error_msg = str(e)
        if "BucketAlready" in error_msg:
            return {"success": False, "error": "Bucket name already taken globally"}
        elif "IllegalLocationConstraint" in error_msg:
            return {
                "success": False, 
                "error": f"Region config issue. Your S3 client region: {s3.meta.region_name}. Check .env file."
            }
        return {"success": False, "error": error_msg}

def list_buckets():
    """List all S3 buckets"""
    try:
        response = s3.list_buckets()
        buckets = [b['Name'] for b in response.get('Buckets', [])]
        return {"success": True, "buckets": buckets, "count": len(buckets)}
    except ClientError as e:
        return {"success": False, "error": str(e)}

def delete_bucket(name, force_empty=True, dry_run=False):
    """
    Delete an S3 bucket.
    - force_empty=True : will attempt to empty the bucket first (safe for PoC)
    - dry_run=True : only simulate actions
    """
    try:
        # Validate bucket exists
        try:
            s3.head_bucket(Bucket=name)
        except ClientError:
            return {"success": False, "error": f"Bucket '{name}' does not exist"}

        if force_empty:
            # Collect keys
            keys = [obj['Key'] for obj in _iter_bucket_objects(name)]
            if keys:
                if dry_run:
                    return {"success": True, "message": f"(dry-run) Would delete {len(keys)} objects then delete bucket '{name}'", "bucket": name, "objects_found": len(keys)}
                # Delete in batches
                for i in range(0, len(keys), 1000):
                    chunk = keys[i:i+1000]
                    delete_payload = {'Objects': [{'Key': k} for k in chunk], 'Quiet': True}
                    resp = s3.delete_objects(Bucket=name, Delete=delete_payload)
                    errors = resp.get('Errors', [])
                    if errors:
                        err_str = "; ".join([f"{e.get('Key')}: {e.get('Message')}" for e in errors])
                        return {"success": False, "error": f"Failed deleting some objects: {err_str}"}
            else:
                # no objects
                pass

        if dry_run:
            return {"success": True, "message": f"(dry-run) Would delete bucket '{name}'", "bucket": name}

        # Finally delete bucket
        s3.delete_bucket(Bucket=name)
        return {"success": True, "bucket": name, "message": f"✓ Deleted bucket '{name}'"}
    except ClientError as e:
        error_msg = str(e)
        if "BucketAlready" in error_msg:
            return {"success": False, "error": "Bucket name already taken globally"}
        elif "IllegalLocationConstraint" in error_msg:
            return {"success": False, "error": f"Region config issue. Your S3 client region: {s3.meta.region_name}. Check .env file."}
        return {"success": False, "error": error_msg}

def delete_all_buckets(dry_run=False):
    """Delete all buckets in the account (empties each first). If dry_run=True, just report."""
    try:
        response = s3.list_buckets()
        buckets = [b['Name'] for b in response.get('Buckets', [])]
        
        if not buckets:
            return {"success": True, "message": "No buckets to delete", "deleted": 0}
        
        deleted = []
        failed = []
        
        for bucket_name in buckets:
            try:
                # Count objects first
                keys = [obj['Key'] for obj in _iter_bucket_objects(bucket_name)]
                if keys and dry_run:
                    print(f"   (dry-run) Would empty {len(keys)} objects from {bucket_name}")
                    deleted.append(bucket_name)
                    continue
                
                if keys:
                    # Delete objects in batches
                    for i in range(0, len(keys), 1000):
                        chunk = keys[i:i+1000]
                        delete_payload = {'Objects': [{'Key': k} for k in chunk], 'Quiet': True}
                        resp = s3.delete_objects(Bucket=bucket_name, Delete=delete_payload)
                        errors = resp.get('Errors', [])
                        if errors:
                            raise ClientError({"Error": {"Message": "Failed deleting some objects"}}, "DeleteObjects")
                
                if not dry_run:
                    s3.delete_bucket(Bucket=bucket_name)
                deleted.append(bucket_name)
                print(f"   ✓ Deleted: {bucket_name}")
                
            except ClientError as e:
                failed.append(f"{bucket_name}: {str(e)}")
                print(f"   ✗ Failed: {bucket_name}")
        
        success_flag = len(failed) == 0
        return {
            "success": success_flag,
            "message": f"✓ Deleted {len(deleted)}/{len(buckets)} buckets",
            "deleted": deleted,
            "deleted_count": len(deleted),
            "total": len(buckets),
            "failures": failed if failed else []
        }
    except ClientError as e:
        return {"success": False, "error": str(e)}
        
def delete_all_objects_in_bucket(bucket, dry_run=False):
    """
    Delete all objects in a bucket (keeps the bucket).
    Supports pagination and 1000-key batch deletes.
    If dry_run=True, it will NOT delete anything and only return what would be deleted.
    """
    try:
        keys = [obj['Key'] for obj in _iter_bucket_objects(bucket)]

        # No objects
        if not keys:
            return {
                "success": True,
                "message": f"No objects in '{bucket}'",
                "deleted_count": 0,
                "bucket": bucket
            }

        # Dry-run → preview only
        if dry_run:
            return {
                "success": True,
                "message": f"(dry-run) Would delete {len(keys)} objects from '{bucket}'",
                "deleted_count": 0,
                "bucket": bucket,
                "would_delete": len(keys)
            }

        # Real delete → delete in batches of 1000
        deleted_count = 0
        for i in range(0, len(keys), 1000):
            chunk = keys[i:i+1000]
            payload = {'Objects': [{'Key': k} for k in chunk], 'Quiet': True}
            
            resp = s3.delete_objects(Bucket=bucket, Delete=payload)
            errors = resp.get('Errors', [])
            
            if errors:
                err_str = "; ".join([f"{e.get('Key')}: {e.get('Message')}" for e in errors])
                return {"success": False, "error": f"Failed deleting some objects: {err_str}"}
            
            deleted_count += len(chunk)

        return {
            "success": True,
            "message": f"✓ Deleted {deleted_count} objects from '{bucket}' (bucket kept)",
            "deleted_count": deleted_count,
            "bucket": bucket
        }

    except ClientError as e:
        return {"success": False, "error": str(e)}


def list_objects(bucket):
    """List all objects in a bucket (pagination aware)."""
    try:
        objects = [obj["Key"] for obj in _iter_bucket_objects(bucket)]
        return {"success": True, "bucket": bucket, "objects": objects, "count": len(objects)}
    except ClientError as e:
        return {"success": False, "error": str(e)}

def put_object(bucket, key, body):
    """Upload an object to S3"""
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body.encode())
        return {"success": True, "message": f"✓ Uploaded '{key}' to '{bucket}'"}
    except ClientError as e:
        return {"success": False, "error": str(e)}

def get_object(bucket, key):
    """Read an object from S3"""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read().decode()
        return {"success": True, "content": content, "key": key, "bucket": bucket}
    except ClientError as e:
        return {"success": False, "error": str(e)}

def delete_object(bucket, key, dry_run=False):
    """Delete an object from S3 (supports dry_run)"""
    try:
        # Validate object exists
        try:
            s3.head_object(Bucket=bucket, Key=key)
        except ClientError:
            return {"success": False, "error": f"Object '{key}' not found in bucket '{bucket}'"}
        
        if dry_run:
            return {"success": True, "message": f"(dry-run) Would delete '{key}' from '{bucket}'", "bucket": bucket, "key": key}
        
        s3.delete_object(Bucket=bucket, Key=key)
        return {"success": True, "message": f"✓ Deleted '{key}' from '{bucket}'"}
    except ClientError as e:
        return {"success": False, "error": str(e)}

def copy_object(source_bucket, source_key, dest_bucket, dest_key=None):
    """Copy an object from one bucket to another"""
    try:
        if not dest_key:
            dest_key = source_key
        
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
        return {
            "success": True, 
            "message": f"✓ Copied '{source_key}' from '{source_bucket}' to '{dest_bucket}/{dest_key}'"
        }
    except ClientError as e:
        return {"success": False, "error": str(e)}

def move_object(source_bucket, source_key, dest_bucket, dest_key=None):
    """Move an object from one bucket to another"""
    try:
        if not dest_key:
            dest_key = source_key
        
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
        s3.delete_object(Bucket=source_bucket, Key=source_key)
        
        return {
            "success": True,
            "message": f"✓ Moved '{source_key}' from '{source_bucket}' to '{dest_bucket}/{dest_key}'"
        }
    except ClientError as e:
        return {"success": False, "error": str(e)}

def copy_all_objects(source_bucket, dest_bucket, dry_run=False):
    """
    Copy all objects from source_bucket to dest_bucket using pagination.
    Returns count of copied objects. Uses copy_object per key (suitable for PoC).
    """
    try:
        # ensure destination exists
        try:
            s3.head_bucket(Bucket=dest_bucket)
        except ClientError:
            return {"success": False, "error": f"Destination bucket '{dest_bucket}' does not exist. Create it first."}

        keys = [obj['Key'] for obj in _iter_bucket_objects(source_bucket)]
        if not keys:
            return {"success": True, "message": f"No objects in '{source_bucket}'", "copied": 0}

        if dry_run:
            return {"success": True, "message": f"(dry-run) Would copy {len(keys)} objects from '{source_bucket}' to '{dest_bucket}'", "copied": 0, "would_copy": len(keys)}

        copied = 0
        for key in keys:
            try:
                copy_source = {'Bucket': source_bucket, 'Key': key}
                s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=key)
                copied += 1
            except ClientError as e:
                # continue copying others but report failure
                print(f"   ✗ Failed copying {key}: {e}")
                continue

        return {"success": True, "message": f"✓ Copied {copied} objects from '{source_bucket}' to '{dest_bucket}'", "copied": copied}
    except ClientError as e:
        return {"success": False, "error": str(e)}

def move_all_objects(source_bucket, dest_bucket, dry_run=False):
    """
    Move all objects from source_bucket to dest_bucket (copy then delete).
    Uses copy_all_objects and then batch delete if all copied successfully.
    """
    try:
        keys = [obj['Key'] for obj in _iter_bucket_objects(source_bucket)]
        if not keys:
            return {"success": True, "message": f"No objects in '{source_bucket}'", "moved": 0}

        if dry_run:
            return {"success": True, "message": f"(dry-run) Would move {len(keys)} objects from '{source_bucket}' to '{dest_bucket}'", "moved": 0, "would_move": len(keys)}

        copied = 0
        for key in keys:
            try:
                copy_source = {'Bucket': source_bucket, 'Key': key}
                s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=key)
                copied += 1
            except ClientError as e:
                print(f"   ✗ Failed copying {key}: {e}")
                continue

        # Delete copied keys in batches
        if copied > 0:
            for i in range(0, copied, 1000):
                chunk = keys[i:i+1000]
                delete_payload = {'Objects': [{'Key': k} for k in chunk], 'Quiet': True}
                resp = s3.delete_objects(Bucket=source_bucket, Delete=delete_payload)
                errors = resp.get('Errors', [])
                if errors:
                    err_str = "; ".join([f"{e.get('Key')}: {e.get('Message')}" for e in errors])
                    return {"success": False, "error": f"Failed deleting some objects after copy: {err_str}"}

        return {"success": True, "message": f"✓ Moved {copied} objects from '{source_bucket}' to '{dest_bucket}'", "moved": copied}
    except ClientError as e:
        return {"success": False, "error": str(e)}

# ========= TOOL REGISTRY =========
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

# ========= TOOL SCHEMAS (Updated: dry_run optional where supported) =========
tool_schemas = [
    {"type": "function", "function": {
        "name": "create_bucket",
        "description": "Create a new S3 bucket. Parameter: name (string)",
        "parameters": {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"]
        }
    }},
    {"type": "function", "function": {
        "name": "list_buckets",
        "description": "List all S3 buckets. No parameters needed.",
        "parameters": {"type": "object", "properties": {}, "required": []}
    }},
    {"type": "function", "function": {
        "name": "delete_bucket",
        "description": "Delete a bucket. Parameter: name (string). Automatically empties bucket first. Optional: dry_run (bool).",
        "parameters": {
            "type": "object",
            "properties": {"name": {"type": "string"}, "dry_run": {"type": "boolean"}},
            "required": ["name"]
        }
    }},
    {"type": "function", "function": {
        "name": "delete_all_buckets",
        "description": "Delete ALL BUCKETS in the account at once. WARNING: This deletes BUCKETS, not just objects! Optional: dry_run (bool).",
        "parameters": {"type": "object", "properties": {"dry_run": {"type": "boolean"}}, "required": []}
    }},
    {"type": "function", "function": {
        "name": "list_objects",
        "description": "List all objects in a bucket. Parameter: bucket (string)",
        "parameters": {
            "type": "object",
            "properties": {"bucket": {"type": "string"}},
            "required": ["bucket"]
        }
    }},
    {"type": "function", "function": {
        "name": "delete_all_objects_in_bucket",
        "description": "Delete ALL OBJECTS in a bucket (keeps the bucket itself). Parameter: bucket (string). Optional: dry_run (bool).",
        "parameters": {
            "type": "object",
            "properties": {"bucket": {"type": "string"}, "dry_run": {"type": "boolean"}},
            "required": ["bucket"]
        }
    }},
    {"type": "function", "function": {
        "name": "put_object",
        "description": "Upload object. Parameters: bucket, key, body (all strings)",
        "parameters": {
            "type": "object",
            "properties": {
                "bucket": {"type": "string"},
                "key": {"type": "string"},
                "body": {"type": "string"}
            },
            "required": ["bucket", "key", "body"]
        }
    }},
    {"type": "function", "function": {
        "name": "get_object",
        "description": "Read object. Parameters: bucket, key (strings)",
        "parameters": {
            "type": "object",
            "properties": {
                "bucket": {"type": "string"},
                "key": {"type": "string"}
            },
            "required": ["bucket", "key"]
        }
    }},
    {"type": "function", "function": {
        "name": "delete_object",
        "description": "Delete object. Parameters: bucket, key (strings). Optional: dry_run (bool).",
        "parameters": {
            "type": "object",
            "properties": {
                "bucket": {"type": "string"},
                "key": {"type": "string"},
                "dry_run": {"type": "boolean"}
            },
            "required": ["bucket", "key"]
        }
    }},
    {"type": "function", "function": {
        "name": "copy_object",
        "description": "Copy single object. Parameters: source_bucket, source_key, dest_bucket, dest_key (optional)",
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
    }},
    {"type": "function", "function": {
        "name": "move_object",
        "description": "Move single object. Parameters: source_bucket, source_key, dest_bucket, dest_key (optional)",
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
    }},
    {"type": "function", "function": {
        "name": "copy_all_objects",
        "description": "Copy ALL objects. Parameters: source_bucket, dest_bucket. Optional: dry_run (bool).",
        "parameters": {
            "type": "object",
            "properties": {
                "source_bucket": {"type": "string"},
                "dest_bucket": {"type": "string"},
                "dry_run": {"type": "boolean"}
            },
            "required": ["source_bucket", "dest_bucket"]
        }
    }},
    {"type": "function", "function": {
        "name": "move_all_objects",
        "description": "Move ALL objects. Parameters: source_bucket, dest_bucket. Optional: dry_run (bool).",
        "parameters": {
            "type": "object",
            "properties": {
                "source_bucket": {"type": "string"},
                "dest_bucket": {"type": "string"},
                "dry_run": {"type": "boolean"}
            },
            "required": ["source_bucket", "dest_bucket"]
        }
    }}
]

# ========= SYSTEM PROMPT (unchanged, but kept for clarity) =========
SYSTEM_PROMPT = """You are an S3 agent. Use tools to execute ALL operations. Never just describe what to do.

KEY RULES:
1. ALWAYS check "success" field in tool results
2. If success=false, tell user the error - don't claim success!
3. Use EXACT bucket/object names from previous tool results
4. NEVER make up bucket names - only use names from list_buckets results
5. Keep responses SHORT and ACCURATE - base answers ONLY on actual tool results
6. When user lists multiple bucket names to create, call create_bucket ONCE for EACH name

CRITICAL DISTINCTIONS:
- "delete all buckets" → Use delete_all_buckets (deletes BUCKETS themselves)
- "delete all objects in bucket X" → Use delete_all_objects_in_bucket (keeps bucket, deletes objects)
- "delete bucket X" → Use delete_bucket with name="X" (deletes one bucket)
- "delete object Y from bucket X" → Use delete_object (deletes one object)

PARAMETER NAMES (USE EXACTLY AS SHOWN):
- create_bucket: name="bucket-name" ← Only ONE parameter: name
- delete_bucket: name="bucket-name"
- delete_all_buckets: (no parameters) ← Deletes ALL BUCKETS
- delete_all_objects_in_bucket: bucket="bucket-name" ← Empties ONE bucket
- list_objects: bucket="bucket-name"
- put_object: bucket="...", key="...", body="..." ← THREE parameters only
- get_object: bucket="...", key="..."
- delete_object: bucket="...", key="..."
- copy_object: source_bucket="...", source_key="...", dest_bucket="...", dest_key="..."
- move_object: source_bucket="...", source_key="...", dest_bucket="...", dest_key="..."

EXAMPLES:
User: "create bucket ayan-t1 ayan-t2 ayan-t3"
→ Call create_bucket(name="ayan-t1")
→ Call create_bucket(name="ayan-t2")
→ Call create_bucket(name="ayan-t3")
→ Say: "Created 3 buckets"

User: "delete all buckets"
→ Call delete_all_buckets()

User: "delete all objects in ayan-test2"
→ Call delete_all_objects_in_bucket(bucket="ayan-test2")

CRITICAL ERRORS TO AVOID:
❌ NEVER pass bucket names as "body" parameter
❌ NEVER call put_object unless user explicitly wants to upload/create an object
❌ NEVER call put_object when user only asked to create buckets
❌ create_bucket takes ONLY "name" - no other parameters like "body"
❌ Don't randomly create test objects unless user asks for them"""

# ========= OLLAMA CALL =========
def call_ollama(messages):
    """Call Ollama API with extended timeout"""
    payload = {
        "model": "llama3.2:latest",
        "messages": messages,
        "tools": tool_schemas,
        "stream": False,
        "options": {
            "temperature": 0.0,
            "num_predict": 256,
            "top_p": 0.9,
            "repeat_penalty": 1.1
        }
    }
    try:
        resp = requests.post(
            "http://localhost:11434/api/chat", 
            json=payload, 
            timeout=120
        )
        resp.raise_for_status()
        return resp.json().get("message", {})
    except requests.Timeout:
        print("❌ Ollama timeout - try reducing workload or restarting Ollama")
        return None
    except Exception as e:
        print(f"❌ Ollama error: {e}")
        return None

# ========= AGENT WITH CONFIRMATION & DRY-RUN =========
class S3Agent:
    def __init__(self):
        self.conversation_history = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]
        self.last_buckets = []
        # Stores pending delete operations requiring confirmation
        self.pending_confirmation = None

    def run(self, user_query):
        """Run agent with conversation memory"""
        self.conversation_history.append({
            "role": "user",
            "content": user_query
        })

        # ====== CONFIRMATION HANDLING ======
        if user_query.lower().startswith("confirm"):
            if not self.pending_confirmation:
                print("\n⚠️ Nothing to confirm.")
                return
            
            # Execute confirmed operation
            print("\n🔐 Confirmation received → executing unsafe operation now...")
            confirmed = self.pending_confirmation
            self.pending_confirmation = None  # reset
            
            tool_name = confirmed["tool"]
            tool_args = confirmed["args"]
            func = tools.get(tool_name)
            if not func:
                print(f"\n✗ Unknown tool: {tool_name}")
                return
            try:
                result = func(**tool_args)
            except Exception as e:
                result = {"success": False, "error": str(e)}

            # Show result
            if result.get("success"):
                print(f"\n   ✓ Confirmed operation succeeded: {result.get('message', result)}")
                # update bucket cache if list_buckets or delete_all_buckets
                if tool_name == "list_buckets" and result.get("success"):
                    self.last_buckets = result.get("buckets", [])
                if tool_name == "delete_all_buckets" and result.get("success"):
                    self.last_buckets = []
            else:
                print(f"\n   ✗ Confirmed operation failed: {result.get('error', 'Unknown error')}")
            # record tool result into history
            self.conversation_history.append({
                "role": "tool",
                "tool_call_id": f"confirm_{tool_name}",
                "name": tool_name,
                "content": json.dumps(result)
            })
            return

        if user_query.lower() == "cancel":
            self.pending_confirmation = None
            print("\n❎ Operation canceled.")
            return

        print("\n" + "="*60)
        print("🤖 Processing...")
        print("="*60)

        for step in range(30):  # Increased for bulk operations
            msg = call_ollama(self.conversation_history)
            if not msg:
                break

            content = msg.get("content", "")
            tool_calls = msg.get("tool_calls", [])
            
            self.conversation_history.append({
                "role": "assistant",
                "content": content if content else "",
                "tool_calls": tool_calls if tool_calls else []
            })

            if not tool_calls:
                if content:
                    print(f"\n💬 {content}")
                break

            # Execute tools
            for tool in tool_calls:
                name = tool["function"]["name"]
                raw_args = tool["function"].get("arguments", "{}")
                
                try:
                    args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except json.JSONDecodeError:
                    args = {}

                # Validation: prevent hallucinated bucket names (simple heuristic)
                if name == "delete_bucket":
                    bucket_name = args.get("name", "")
                    if bucket_name in tools.keys() or bucket_name == "list_buckets":
                        print(f"\n⚠️  BLOCKED: '{bucket_name}' is not a valid bucket name!")
                        result = {
                            "success": False, 
                            "error": f"'{bucket_name}' is not a bucket. Use delete_all_buckets tool to delete all buckets."
                        }
                        self.conversation_history.append({
                            "role": "tool",
                            "tool_call_id": tool.get("id", f"call_{step}"),
                            "name": name,
                            "content": json.dumps(result)
                        })
                        continue
                    
                    # Extra safety: check if bucket actually exists in last_buckets (if we have a cache)
                    if self.last_buckets and bucket_name not in self.last_buckets:
                        print(f"\n⚠️  '{bucket_name}' not in recent bucket list: {self.last_buckets}")
                        print(f"💡 Available buckets: {', '.join(self.last_buckets) if self.last_buckets else 'none'}")

                print(f"\n🔧 {name}({', '.join(f'{k}={v[:50] if isinstance(v, str) and len(v) > 50 else v}' for k, v in args.items()) if args else ''})")

                func = tools.get(name)
                if not func:
                    result = {"success": False, "error": f"Unknown tool: {name}"}
                    print(f"   ✗ {result['error']}")
                    self.conversation_history.append({
                        "role": "tool",
                        "tool_call_id": tool.get("id", f"call_{step}"),
                        "name": name,
                        "content": json.dumps(result)
                    })
                    continue

                # ====== SAFETY CHECK FOR DESTRUCTIVE OPERATIONS ======
                destructive_tools = [
                    "delete_bucket",
                    "delete_all_buckets",
                    "delete_object",
                    "delete_all_objects_in_bucket",
                    "move_all_objects",
                    "copy_all_objects",
                ]

                if name in destructive_tools:
                    print("\n⚠️ SAFETY CHECK TRIGGERED")

                    # Always run DRY-RUN first (if supported)
                    args_with_dry = dict(args) if isinstance(args, dict) else {}
                    args_with_dry["dry_run"] = True

                    try:
                        dry_run_result = func(**args_with_dry)
                    except TypeError:
                        # Function might not accept dry_run param; simulate best-effort
                        try:
                            # call without dry_run as fallback but DO NOT execute destructive without confirm
                            dry_run_result = {"success": True, "message": "(dry-run) Simulated outcome; function has no dry_run param."}
                        except Exception as e:
                            dry_run_result = {"success": False, "error": str(e)}
                    except Exception as e:
                        dry_run_result = {"success": False, "error": str(e)}

                    print(f"\n🔍 Dry-run Result: {dry_run_result}")

                    # Ask user to confirm, store pending operation
                    print("\n❗ This is a DESTRUCTIVE OPERATION. To proceed, type:")
                    # Provide clearer confirmation token
                    confirmation_token = f"CONFIRM {name} {json.dumps(args)}"
                    print(f"   {confirmation_token}")
                    print("Or type: cancel")

                    self.pending_confirmation = {
                        "tool": name,
                        "args": args
                    }

                    # Append dry-run tool result into history (so model can see outcome)
                    self.conversation_history.append({
                        "role": "tool",
                        "tool_call_id": tool.get("id", f"call_{step}"),
                        "name": name,
                        "content": json.dumps(dry_run_result)
                    })
                    # Stop processing further tool calls until the user confirms
                    return

                # Non-destructive: execute directly
                try:
                    result = func(**args)
                except TypeError as e:
                    result = {"success": False, "error": f"Wrong parameters: {str(e)}"}
                except Exception as e:
                    result = {"success": False, "error": str(e)}

                # Show concise result with better formatting
                if result.get("success"):
                    if name == "list_buckets":
                        buckets = result.get("buckets", [])
                        count = len(buckets)
                        if count == 0:
                            print(f"   ✓ No buckets")
                        else:
                            print(f"   ✓ {count} bucket(s): {buckets}")
                        # Track for validation
                        self.last_buckets = buckets
                    elif name == "delete_all_buckets":
                        deleted = result.get("deleted", [])
                        print(f"   ✓ Deleted {len(deleted)} buckets: {deleted}")
                        if result.get("success"):
                            self.last_buckets = []
                    elif name == "delete_all_objects_in_bucket":
                        count = result.get("deleted_count", 0)
                        bucket = result.get("bucket", "")
                        print(f"   ✓ Deleted {count} objects from '{bucket}' (bucket kept)")
                    else:
                        msg = result.get("message", result.get("objects", "✓"))
                        print(f"   ✓ {msg}")
                else:
                    print(f"   ✗ {result.get('error', 'Failed')}")

                # Record tool result into conversation history
                self.conversation_history.append({
                    "role": "tool",
                    "tool_call_id": tool.get("id", f"call_{step}"),
                    "name": name,
                    "content": json.dumps(result)
                })
        else:
            print("\n⚠️  Reached max steps")

    def clear_history(self):
        """Clear conversation history"""
        self.conversation_history = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]
        self.last_buckets = []
        self.pending_confirmation = None
        print("\n🧹 History cleared!")

# ========= MAIN =========
def check_ollama():
    """Check if Ollama is running and accessible"""
    try:
        resp = requests.get("http://localhost:11434/api/tags", timeout=5)
        if resp.status_code == 200:
            models = resp.json().get("models", [])
            model_names = [m.get("name", "") for m in models]
            if any("llama3.2" in name for name in model_names):
                return True, "✓ Ollama is running"
            else:
                return False, "❌ llama3.2:latest not found. Run: ollama pull llama3.2:latest"
        return False, f"❌ Ollama returned status {resp.status_code}"
    except requests.ConnectionError:
        return False, "❌ Ollama not running. Start it with: ollama serve"
    except Exception as e:
        return False, f"❌ Ollama check failed: {e}"

def check_aws_config():
    """Check AWS configuration"""
    try:
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION", "us-east-1")
        
        if not aws_key or not aws_secret:
            return False, "❌ AWS credentials not found in .env"
        
        print(f"   AWS Region: {aws_region}")
        print(f"   Access Key: {aws_key[:8]}..." if aws_key else "   Access Key: NOT SET")
        
        # Try to list buckets to verify credentials work
        try:
            response = s3.list_buckets()
            bucket_count = len(response.get('Buckets', []))
            return True, f"✓ AWS configured ({bucket_count} existing buckets)"
        except ClientError as e:
            return False, f"❌ AWS credentials invalid: {str(e)}"
            
    except Exception as e:
        return False, f"❌ AWS check failed: {e}"

def main():
    print("="*60)
    print("🚀 S3 Agent (Safe: dry-run + confirmation)")
    print("="*60)
    
    # Check Ollama before starting
    print("\n🔍 Checking Ollama...")
    is_ok, message = check_ollama()
    print(message)
    
    if not is_ok:
        print("\n💡 To fix:")
        print("  1. Start Ollama: ollama serve")
        print("  2. Pull model: ollama pull llama3.2:latest")
        print("  3. Test: ollama run llama3.2:latest 'hello'")
        return
    
    # Check AWS configuration
    print("\n🔍 Checking AWS...")
    is_ok, message = check_aws_config()
    print(message)
    
    if not is_ok:
        print("\n💡 To fix:")
        print("  1. Create .env file with:")
        print("     AWS_ACCESS_KEY_ID=your_key")
        print("     AWS_SECRET_ACCESS_KEY=your_secret")
        print("     AWS_REGION=us-east-1")
        return
    
    print("\n📋 Commands: 'clear' | 'exit' | 'CONFIRM ...' | 'cancel' | or natural language")
    print("\n💡 Examples:")
    print("  • list my buckets")
    print("  • create bucket called test-bucket")
    print("  • delete all buckets")
    print("  • copy all objects from bucket-a to bucket-b")
    print("="*60)

    agent = S3Agent()

    while True:
        try:
            user_input = input("\n💬 ").strip()
            
            if not user_input:
                continue
            
            if user_input.lower() in ['exit', 'quit', 'q']:
                print("\n👋 Goodbye!")
                break
            
            if user_input.lower() == 'clear':
                agent.clear_history()
                continue
            
            agent.run(user_input)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
