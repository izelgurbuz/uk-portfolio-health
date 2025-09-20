import os
from pathlib import Path

import boto3

from ..utils.logging import log


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "eu-west-2"),
    )


def upload_file_to_s3(local_path: Path, s3_key: str):
    bucket = os.getenv("AWS_S3_BUCKET")
    if not bucket:
        raise ValueError("AWS_S3_BUCKET not set in environment")

    s3 = get_s3_client()
    log(f"Uploading {local_path} → s3://{bucket}/{s3_key}")
    s3.upload_file(str(local_path), bucket, s3_key)
    log("Upload complete")


def upload_latest_snapshot():
    """
    Upload the most recent portfolio_metrics.parquet to S3.
    """
    out_file = Path("data/processed/snapshots/portfolio_metrics.parquet")
    if not out_file.exists():
        raise FileNotFoundError(f"Snapshot file not found: {out_file}")

    s3_key = f"snapshots/portfolio_metrics/{out_file.name}"
    upload_file_to_s3(out_file, s3_key)
