"""
tools/etl_orchestrator.py

Production ETL orchestrator (cost-conscious).

Exports: run_production_etl(local_path, bucket=None, s3_prefix_raw='raw/', s3_prefix_processed='processed/',
                           database=None, table=None, convert_to_parquet=True, keep_original=True)

Notes:
- Uses existing tools: upload_local_folder_to_s3, infer_schema_from_csv, create_glue_database, create_glue_table
- If infer_schema returns needs_user_help == True, returns {"next":"disambiguate", "ambiguous": [...]}
  so your interactive agent UI can prompt the user and set the final schema.
- If pandas is available, will convert CSV -> Parquet and upload to processed/ (cost saving).
- Uses /mnt/data/products-100.csv as an example local path if you want to test quickly (that file was uploaded earlier).
"""

import os
import uuid
import traceback
from typing import Optional

from config.aws_session import get_s3_client
from tools.upload_local_folder import upload_local_folder_to_s3
from tools.schema_inference import infer_schema_from_csv
from tools.glue_tools import create_glue_database, create_glue_table
from tools.s3_tools import create_bucket as s3_create_bucket

# optional parquet conversion dependencies
try:
    import pandas as pd
    _HAS_PANDAS = True
except Exception:
    _HAS_PANDAS = False

s3 = get_s3_client()


def _ensure_bucket(bucket_name: str):
    try:
        s3.head_bucket(Bucket=bucket_name)
        return {"success": True, "bucket": bucket_name, "message": "Bucket exists"}
    except Exception:
        try:
            resp = s3_create_bucket(bucket_name)
            return {"success": resp.get("success", False), "bucket": bucket_name, "message": resp.get("message", "")}
        except Exception as e:
            return {"success": False, "error": str(e)}


def _local_csv_to_parquet(local_csv_path: str, parquet_out_path: str):
    if not _HAS_PANDAS:
        return {"success": False, "error": "pandas not installed; cannot convert to parquet locally"}
    try:
        df = pd.read_csv(local_csv_path)
        # write using pyarrow if available
        df.to_parquet(parquet_out_path, index=False)
        return {"success": True, "parquet": parquet_out_path}
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_production_etl(local_path: str,
                       bucket: Optional[str] = None,
                       s3_prefix_raw: str = "raw/",
                       s3_prefix_processed: str = "processed/",
                       database: Optional[str] = None,
                       table: Optional[str] = None,
                       convert_to_parquet: bool = True,
                       keep_original: bool = True):
    """
    Orchestrator steps:
      1. Ensure bucket
      2. Upload local files to s3://bucket/s3_prefix_raw/
      3. Find first CSV and infer schema (agent may need to disambiguate)
      4. Create Glue DB
      5. Create Glue table for raw CSV using inferred schema
      6. Convert CSV -> Parquet locally and upload to processed/ (optional)
      7. Create Glue table for parquet (optional)
    Returns JSON summary.
    """

    summary = {"steps": [], "errors": [], "locations": {}}

    # allow the caller to pass a full local file path or folder
    if not local_path:
        return {"success": False, "error": "local_path is required", "summary": summary}

    # choose bucket name if not provided
    if not bucket:
        bucket = f"etl-pipeline-{uuid.uuid4().hex[:8]}"

    # 1) Ensure bucket exists
    resp = _ensure_bucket(bucket)
    summary["steps"].append({"ensure_bucket": resp})
    if not resp.get("success"):
        summary["errors"].append({"step": "ensure_bucket", "error": resp.get("error")})
        return {"success": False, "summary": summary}

    # 2) Upload local file/folder to S3 raw prefix
    upload_resp = upload_local_folder_to_s3(local_path, bucket, s3_prefix_raw)
    summary["steps"].append({"upload": upload_resp})
    if not upload_resp.get("success"):
        summary["errors"].append({"step": "upload", "error": upload_resp.get("error")})
        return {"success": False, "summary": summary}

    summary["locations"]["raw_s3"] = f"s3://{bucket}/{s3_prefix_raw}"

    # 3) Find first CSV key among uploaded files
    csv_keys = [k for k in upload_resp.get("uploaded", []) if k.lower().endswith(".csv")]
    if not csv_keys:
        summary["steps"].append({"success": True, "message": "No CSV files found in uploaded folder"})
        return {"success": True, "summary": summary}

    first_csv_key = csv_keys[0]
    s3_csv_path = f"s3://{bucket}/{first_csv_key}"
    summary["locations"]["sample_csv_s3_path"] = s3_csv_path

    # Determine a local CSV path if possible (best effort)
    local_csv_path = None
    if os.path.isfile(local_path) and local_path.lower().endswith(".csv"):
        local_csv_path = local_path
    else:
        possible_local = os.path.join(local_path, os.path.basename(first_csv_key))
        if os.path.exists(possible_local):
            local_csv_path = possible_local

    if local_csv_path:
        summary["locations"]["sample_csv_local_path"] = local_csv_path

    # 4) Infer schema locally (agent will handle interactive disambiguation if required)
    sample_limit = 500
    try:
        infer_resp = infer_schema_from_csv(local_csv_path or possible_local, sample_limit=sample_limit)
    except Exception as e:
        infer_resp = {"success": False, "error": str(e)}

    summary["steps"].append({"infer_schema": infer_resp})
    if not infer_resp.get("success"):
        summary["errors"].append({"step": "infer_schema", "error": infer_resp.get("error")})
        return {"success": False, "summary": summary}

    # If infer_schema requires interactive disambiguation, return control to agent/user
    if infer_resp.get("needs_user_help"):
        return {"success": True, "summary": summary, "next": "disambiguate", "ambiguous": infer_resp.get("ambiguous", [])}

    inferred_cols = infer_resp.get("columns", [])
    summary["locations"]["inferred_schema"] = inferred_cols

    # 5) Create Glue Database
    if database:
        db_resp = create_glue_database(database, dry_run=False)
    else:
        db_name = f"db_{uuid.uuid4().hex[:8]}"
        db_resp = create_glue_database(db_name, dry_run=False)
        database = db_name

    summary["steps"].append({"create_database": db_resp})
    if not db_resp.get("success"):
        summary["errors"].append({"step": "create_database", "error": db_resp.get("error")})
        return {"success": False, "summary": summary}

    # 6) Create Glue table for CSV raw
    table_name = table or f"tbl_{uuid.uuid4().hex[:8]}"
    try:
        table_resp = create_glue_table(database=database, table=table_name,
                                      s3_location=f"s3://{bucket}/{s3_prefix_raw}",
                                      columns=inferred_cols, partition_keys=None,
                                      format="csv", dry_run=False)
        summary["steps"].append({"create_table_raw": table_resp})
        if not table_resp.get("success"):
            summary["errors"].append({"step": "create_table_raw", "error": table_resp.get("error")})
    except Exception as e:
        summary["errors"].append({"step": "create_table_raw", "error": str(e), "trace": traceback.format_exc()})
        return {"success": False, "summary": summary}

    # 7) Optional CSV -> Parquet conversion and upload
    if convert_to_parquet and _HAS_PANDAS and local_csv_path:
        try:
            parquet_local = os.path.splitext(local_csv_path)[0] + ".parquet"
            conv = _local_csv_to_parquet(local_csv_path, parquet_local)
            summary["steps"].append({"local_parquet_conversion": conv})
            if conv.get("success"):
                parquet_s3_key = os.path.join(s3_prefix_processed, os.path.basename(parquet_local)).replace("\\", "/")
                with open(parquet_local, "rb") as f:
                    s3.put_object(Bucket=bucket, Key=parquet_s3_key, Body=f.read())
                summary["steps"].append({"upload_parquet": f"s3://{bucket}/{parquet_s3_key}"})
                summary["locations"]["processed_s3"] = f"s3://{bucket}/{s3_prefix_processed}"
                parquet_table_name = f"{table_name}_parquet"
                pt_resp = create_glue_table(database=database, table=parquet_table_name,
                                           s3_location=f"s3://{bucket}/{s3_prefix_processed}",
                                           columns=inferred_cols, partition_keys=None,
                                           format="parquet", dry_run=False)
                summary["steps"].append({"create_table_parquet": pt_resp})
            else:
                summary["steps"].append({"parquet_conversion_error": conv.get("error")})
        except Exception as e:
            summary["errors"].append({"step": "parquet_conversion", "error": str(e), "trace": traceback.format_exc()})

    return {"success": True, "summary": summary}
