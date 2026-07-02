"""One-off audit for normalized Parquet prefixes.

This reuses scripts.audit_parquet_layout but swaps the dataset configs from the
legacy prefixes to the normalized prefixes created by rewrite_parquet_layout.py.
It is intentionally read-only for MinIO and writes reports to /tmp.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from scripts import audit_parquet_layout as audit


def _ops_re(table: str) -> re.Pattern:
    return re.compile(
        rf"^ops_normalized/{re.escape(table)}/year=\d+/month=\d+/[^/]+\.parquet$"
    )


def _configure_normalized_prefixes() -> None:
    audit.DATASET_CONFIGS["silver_observations"] = {
        "prefix": "silver_normalized/observations/",
        "expected_pattern": re.compile(
            r"^silver_normalized/observations/source=[^/]+"
            r"/obs_year=\d+/obs_month=\d+/[^/]+\.parquet$"
        ),
        "partition_template": "source=<source>/obs_year=<Y>/obs_month=<M>/",
    }

    for table in audit.SUPPORTED_DATASETS:
        if table == "silver_observations":
            continue
        audit.DATASET_CONFIGS[table] = {
            "prefix": f"ops_normalized/{table}/",
            "expected_pattern": _ops_re(table),
            "partition_template": "year=<Y>/month=<M>/",
        }


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    _configure_normalized_prefixes()

    client = audit.get_boto3_client()
    fs = audit.get_s3fs()

    results = {}
    for name in audit.SUPPORTED_DATASETS:
        results[name] = audit.audit_dataset(
            client,
            fs,
            audit.BUCKET,
            name,
            audit.DATASET_CONFIGS[name],
            sample_files=3,
        )

    report = audit.build_json_report(results)
    audit.print_stdout_summary(report)

    Path("/tmp/audit_parquet_layout_after_normalize.json").write_text(
        json.dumps(report, indent=2)
    )
    Path("/tmp/audit_parquet_layout_after_normalize.md").write_text(
        audit.build_markdown_report(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
