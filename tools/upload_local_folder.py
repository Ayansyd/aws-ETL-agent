"""
tools/upload_local_folder.py

This tool lets the agent upload an entire LOCAL folder recursively into S3.
It preserves subfolder structure and is fully compatible with the S3 ETL pipeline.
"""

import os
from config.aws_session import get_s3_client

s3 = get_s3_client()


def upload_local_folder_to_s3(local_path: str, bucket: str, s3_prefix: str):
    """
    Upload a local folder (recursively) to S3.
    - local_path: local folder path
    - bucket: destination bucket
    - s3_prefix: prefix inside S3 bucket (e.g., 'raw/')
    
    Returns:
    {
        "success": True,
        "uploaded": [
            "raw/file1.csv",
            "raw/subfolder/file2.csv"
        ],
        "count": 2,
        "message": "Uploaded 2 files"
    }
    """

    if not os.path.exists(local_path):
        return {"success": False, "error": f"Local folder not found: {local_path}"}

    if not os.path.isdir(local_path):
        return {"success": False, "error": f"Not a directory: {local_path}"}

    uploaded = []

    try:
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file_path = os.path.join(root, file)

                # Compute S3 key
                relative_path = os.path.relpath(local_file_path, local_path)
                s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")

                # Upload
                with open(local_file_path, "rb") as f:
                    s3.put_object(Bucket=bucket, Key=s3_key, Body=f.read())

                uploaded.append(s3_key)

        return {
            "success": True,
            "uploaded": uploaded,
            "count": len(uploaded),
            "message": f"Uploaded {len(uploaded)} files to s3://{bucket}/{s3_prefix}"
        }

    except Exception as e:
        return {"success": False, "error": str(e)}
