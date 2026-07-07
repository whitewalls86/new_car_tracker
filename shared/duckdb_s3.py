"""
Shared DuckDB connection helper for reading MinIO/S3 Parquet.

Mirrors the config already read by shared/minio.py (boto3/s3fs) but produces
a DuckDB connection instead, for callers that want to run SQL directly over
Parquet in object storage (selectors, audits, ad-hoc scripts).
"""
from shared.minio import ACCESS, ENDPOINT, SECRET


def _normalize_endpoint(endpoint: str) -> str:
    """Strip scheme from a MinIO endpoint (e.g. 'http://minio:9000' -> 'minio:9000')."""
    return endpoint.replace("http://", "").replace("https://", "")


def get_duckdb_s3_connection():
    """Return a new DuckDB connection configured to read MinIO over S3.

    Credentials are passed as bound parameters, not interpolated into SQL.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("SET s3_endpoint=?", [_normalize_endpoint(ENDPOINT)])
    con.execute("SET s3_access_key_id=?", [ACCESS])
    con.execute("SET s3_secret_access_key=?", [SECRET])
    con.execute("SET s3_use_ssl=?", [False])
    con.execute("SET s3_url_style=?", ["path"])
    return con
