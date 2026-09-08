"""Create the `bronze` bucket on the CI MinIO service container.

Lifted out of an inline heredoc in `ci.yml` by Plan 162 Stage E. The job split
gave three jobs a MinIO service, and three copies of the same twelve lines is
three places for the endpoint and credentials to drift apart.

CI infrastructure, deliberately under `.github/` rather than `scripts/`:
nothing ships it, no service imports it, and it is not part of the coverage
denominator `scripts/` is.
"""

import os

import boto3
from botocore.client import Config


def main() -> None:
    bucket = os.environ.get("MINIO_BUCKET", "bronze")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "cartracker"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "cartracker123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket=bucket)
    print(f"Created bucket: {bucket}")


if __name__ == "__main__":
    main()
