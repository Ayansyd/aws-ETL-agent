"""
tools/s3_tools.py

All S3 utility functions (pagination-aware, batch deletes, dry-run support).
These are plain functions returning dicts with a "success" boolean and helpful metadata/messages.
"""

from botocore.exceptions import ClientError
from config.aws_session import get_s3_client

# Shared S3 client for this module
s3 = get_s3_client()


def _iter_bucket_objects(bucket):
    """
    Yield object dicts (as returned in 'Contents') for a bucket using pagination.
    Raises ClientError to caller if there is an AWS error.
    """
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            yield obj


def create_bucket(name):
    """Create a new S3 bucket in the configured region."""
    try:
        current_region = s3.meta.region_name
        if current_region == "us-east-1":
            s3.create_bucket(Bucket=name)
        else:
            s3.create_bucket(Bucket=name, CreateBucketConfiguration={'LocationConstraint': current_region})
        return {"success": True, "bucket": name, "message": f"✓ Created bucket '{name}' in {current_region}"}
    except ClientError as e:
        error_msg = str(e)
        if "BucketAlready" in error_msg:
            return {"success": False, "error": "Bucket name already taken globally"}
        elif "IllegalLocationConstraint" in error_msg:
            return {"success": False, "error": f"Region config issue. Your S3 client region: {s3.meta.region_name}. Check .env file."}
        return {"success": False, "error": error_msg}


def list_buckets():
    """Return a list of buckets in the account."""
    try:
        resp = s3.list_buckets()
        buckets = [b["Name"] for b in resp.get("Buckets", [])]
        return {"success": True, "buckets": buckets, "count": len(buckets)}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def list_objects(bucket):
    """List all object keys in a bucket (pagination-aware)."""
    try:
        keys = [obj["Key"] for obj in _iter_bucket_objects(bucket)]
        return {"success": True, "bucket": bucket, "objects": keys, "count": len(keys)}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def put_object(bucket, key, body):
    """Upload an object to S3 (body is string)."""
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body.encode())
        return {"success": True, "message": f"✓ Uploaded '{key}' to '{bucket}'"}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def get_object(bucket, key):
    """Download and return object content (decoded to string)."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode()
        return {"success": True, "bucket": bucket, "key": key, "content": content}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def delete_object(bucket, key, dry_run=False):
    """
    Delete a single object. If dry_run=True, only simulate and return what would be deleted.
    """
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


def delete_all_objects_in_bucket(bucket, dry_run=False):
    """
    Delete all objects in a bucket (keeps bucket).
    Handles pagination and batch deletes (1000 per request).
    If dry_run=True, returns a preview without deleting.
    """
    try:
        keys = [obj["Key"] for obj in _iter_bucket_objects(bucket)]
        if not keys:
            return {"success": True, "message": f"No objects in '{bucket}'", "deleted_count": 0, "bucket": bucket}

        if dry_run:
            return {"success": True, "message": f"(dry-run) Would delete {len(keys)} objects from '{bucket}'", "would_delete": len(keys), "bucket": bucket}

        deleted_count = 0
        for i in range(0, len(keys), 1000):
            chunk = keys[i:i+1000]
            delete_payload = {"Objects": [{"Key": k} for k in chunk], "Quiet": True}
            resp = s3.delete_objects(Bucket=bucket, Delete=delete_payload)
            errors = resp.get("Errors", [])
            if errors:
                err_str = "; ".join([f"{e.get('Key')}: {e.get('Message')}" for e in errors])
                return {"success": False, "error": f"Failed deleting some objects: {err_str}"}
            deleted_count += len(chunk)

        return {"success": True, "message": f"✓ Deleted {deleted_count} objects from '{bucket}' (bucket kept)", "deleted_count": deleted_count, "bucket": bucket}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def delete_bucket(name, force_empty=True, dry_run=False):
    """
    Delete a bucket (optionally emptying it first).
    - force_empty: if True, empties object contents
    - dry_run: preview only, do not actually delete
    """
    try:
        # Validate bucket exists
        try:
            s3.head_bucket(Bucket=name)
        except ClientError:
            return {"success": False, "error": f"Bucket '{name}' does not exist"}

        if force_empty:
            keys = [obj["Key"] for obj in _iter_bucket_objects(name)]
            if keys:
                if dry_run:
                    return {"success": True, "message": f"(dry-run) Would delete {len(keys)} objects then delete bucket '{name}'", "bucket": name, "objects_found": len(keys)}
                # Delete in batches
                for i in range(0, len(keys), 1000):
                    chunk = keys[i:i+1000]
                    delete_payload = {"Objects": [{"Key": k} for k in chunk], "Quiet": True}
                    resp = s3.delete_objects(Bucket=name, Delete=delete_payload)
                    errors = resp.get("Errors", [])
                    if errors:
                        err_str = "; ".join([f"{e.get('Key')}: {e.get('Message')}" for e in errors])
                        return {"success": False, "error": f"Failed deleting some objects: {err_str}"}
            # else: no objects to delete

        if dry_run:
            return {"success": True, "message": f"(dry-run) Would delete bucket '{name}'", "bucket": name}

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
    """
    Delete all buckets in the account. If dry_run=True, returns a report only.
    WARNING: This deletes buckets and their contents when dry_run=False.
    """
    try:
        resp = s3.list_buckets()
        buckets = [b["Name"] for b in resp.get("Buckets", [])]
        if not buckets:
            return {"success": True, "message": "No buckets to delete", "deleted": 0}

        deleted = []
        failed = []
        for bucket_name in buckets:
            try:
                keys = [obj["Key"] for obj in _iter_bucket_objects(bucket_name)]
                if keys and dry_run:
                    deleted.append(bucket_name)
                    continue

                if keys:
                    for i in range(0, len(keys), 1000):
                        chunk = keys[i:i+1000]
                        delete_payload = {"Objects": [{"Key": k} for k in chunk], "Quiet": True}
                        resp_del = s3.delete_objects(Bucket=bucket_name, Delete=delete_payload)
                        errors = resp_del.get("Errors", [])
                        if errors:
                            raise ClientError({"Error": {"Message": "Failed deleting some objects"}}, "DeleteObjects")

                if not dry_run:
                    s3.delete_bucket(Bucket=bucket_name)
                deleted.append(bucket_name)
            except ClientError as e:
                failed.append(f"{bucket_name}: {str(e)}")

        success_flag = len(failed) == 0
        return {"success": success_flag, "message": f"✓ Deleted {len(deleted)}/{len(buckets)} buckets", "deleted": deleted, "deleted_count": len(deleted), "total": len(buckets), "failures": failed}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def copy_object(source_bucket, source_key, dest_bucket, dest_key=None):
    """Copy a single object from source_bucket/source_key to dest_bucket/dest_key."""
    try:
        if not dest_key:
            dest_key = source_key
        copy_source = {"Bucket": source_bucket, "Key": source_key}
        s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
        return {"success": True, "message": f"✓ Copied '{source_key}' from '{source_bucket}' to '{dest_bucket}/{dest_key}'"}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def move_object(source_bucket, source_key, dest_bucket, dest_key=None):
    """Move a single object (copy then delete original)."""
    try:
        if not dest_key:
            dest_key = source_key
        copy_source = {"Bucket": source_bucket, "Key": source_key}
        s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
        s3.delete_object(Bucket=source_bucket, Key=source_key)
        return {"success": True, "message": f"✓ Moved '{source_key}' from '{source_bucket}' to '{dest_bucket}/{dest_key}'"}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def copy_all_objects(source_bucket, dest_bucket, dry_run=False):
    """
    Copy all objects from source_bucket to dest_bucket.
    If dry_run=True, just report count.
    """
    try:
        # Ensure destination exists
        try:
            s3.head_bucket(Bucket=dest_bucket)
        except ClientError:
            return {"success": False, "error": f"Destination bucket '{dest_bucket}' does not exist. Create it first."}

        keys = [obj["Key"] for obj in _iter_bucket_objects(source_bucket)]
        if not keys:
            return {"success": True, "message": f"No objects in '{source_bucket}'", "copied": 0}

        if dry_run:
            return {"success": True, "message": f"(dry-run) Would copy {len(keys)} objects from '{source_bucket}' to '{dest_bucket}'", "would_copy": len(keys)}

        copied = 0
        for key in keys:
            try:
                copy_source = {"Bucket": source_bucket, "Key": key}
                s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=key)
                copied += 1
            except ClientError:
                # continue copying others, but don't fail entire operation
                continue

        return {"success": True, "message": f"✓ Copied {copied} objects from '{source_bucket}' to '{dest_bucket}'", "copied": copied}
    except ClientError as e:
        return {"success": False, "error": str(e)}


def move_all_objects(source_bucket, dest_bucket, dry_run=False):
    """
    Move all objects from source_bucket to dest_bucket (copy then delete).
    If dry_run=True, only report count.
    """
    try:
        keys = [obj["Key"] for obj in _iter_bucket_objects(source_bucket)]
        if not keys:
            return {"success": True, "message": f"No objects in '{source_bucket}'", "moved": 0}

        if dry_run:
            return {"success": True, "message": f"(dry-run) Would move {len(keys)} objects from '{source_bucket}' to '{dest_bucket}'", "would_move": len(keys)}

        copied = 0
        for key in keys:
            try:
                copy_source = {"Bucket": source_bucket, "Key": key}
                s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=key)
                copied += 1
            except ClientError:
                continue

        # Delete copied keys in batches of 1000
        if copied > 0:
            for i in range(0, copied, 1000):
                chunk = keys[i:i+1000]
                delete_payload = {"Objects": [{"Key": k} for k in chunk], "Quiet": True}
                resp = s3.delete_objects(Bucket=source_bucket, Delete=delete_payload)
                errors = resp.get("Errors", [])
                if errors:
                    err_str = "; ".join([f"{e.get('Key')}: {e.get('Message')}" for e in errors])
                    return {"success": False, "error": f"Failed deleting some objects after copy: {err_str}"}

        return {"success": True, "message": f"✓ Moved {copied} objects from '{source_bucket}' to '{dest_bucket}'", "moved": copied}
    except ClientError as e:
        return {"success": False, "error": str(e)}
