import boto3
from .settings import (
    AWS_REGION,
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    AWS_SESSION_TOKEN,
    AWS_PROFILE
)

def get_boto3_session():
    """Return a configured boto3 session."""
    if AWS_PROFILE:
        return boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

    return boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION
    )

def get_s3_client():
    """Shared S3 client available to all tools."""
    session = get_boto3_session()
    return session.client('s3', region_name=AWS_REGION)
