"""
Seed a deterministic set of business-state scenarios into MinIO for the
Plan 120 Gate B dbt/selector equivalence guard.

    python scripts/seed_dbt_selector_equivalence_fixture.py

Run this *before* `dbt build --profiles-dir dbt --target duckdb` in CI, then
after that build completes, `tests/integration/dbt/test_selector_dbt_equivalence.py`
compares the real dbt-materialized `int_listing_state_runs`/
`int_latest_observation` tables against the production selector SQL
(`archiver/processors/lake_snapshot_selectors.py`) run over the same MinIO
data — the actual coupling guard, not a hand-copied paraphrase of either
side. The test imports the VIN constants and row builder from this module so
seeding and assertions cannot drift apart.

Uses distinct obs_year/obs_month partitions (2099-01) so this fixture never
collides with the minimal empty-schema seed the CI `dbt` job already writes
for compilation checks.
"""
from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

from shared.minio import BUCKET, ensure_bucket, get_boto3_client

_PARTITION = "obs_year=2099/obs_month=1"


def _vin17(tag: str) -> str:
    """Deterministic 17-char alphanumeric VIN — same construction the
    selector's fingerprint CTE and dbt's stg_observations.sql both validate
    against (17 chars, [A-Z0-9] only)."""
    return (tag.upper() + "0" * 17)[:17]


def _ts(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Scenario VINs (dbt-equivalence: stable_state_run / state_change_run)
# ---------------------------------------------------------------------------

VIN_IDENT = _vin17("IDENT")       # identical consecutive detail states -> stable
VIN_PRICE = _vin17("PRICE")       # price changes -> state change
VIN_STATECHG = _vin17("STCHG")    # listing_state changes -> state change
VIN_RELIST2 = _vin17("RELST2")    # listing_id changes for the same VIN -> state change
VIN_ABA = _vin17("ABA")           # A -> B -> A sequence -> two state changes

# Scenario VINs (dbt-equivalence: detail_beats_srp / srp_fallback)
VIN_DETAIL_WINS = _vin17("DETWIN")  # detail (earlier) must win over a later srp row
VIN_SRP_ONLY = _vin17("SRPONLY")    # usable srp row, no detail row at all

_OBS_DEFAULTS: Dict[str, Any] = dict(
    listing_id="L0",
    artifact_id=0,
    source="detail",
    fetched_at=_ts(2026, 7, 1),
    price=10000,
    mileage=5000,
    msrp=30000,
    make="Toyota",
    model="Camry",
    trim="LE",
    year=2022,
    stock_type="Used",
    fuel_type="Gasoline",
    body_style="Sedan",
    listing_state="active",
    dealer_name="ABC Motors",
    dealer_zip="90210",
    dealer_city="Beverly Hills",
    dealer_state="CA",
    customer_id="CUST1",
    canonical_detail_url=None,
    seller_id=None,
    dealer_street=None,
    dealer_phone=None,
    dealer_website=None,
    dealer_cars_com_url=None,
    dealer_rating=None,
    financing_type=None,
    seller_zip=None,
    seller_customer_id=None,
    page_number=None,
    position_on_page=None,
    trid=None,
    isa_context=None,
    body=None,
    condition=None,
)


def _obs_row(vin: str, **overrides: Any) -> Dict[str, Any]:
    row = dict(_OBS_DEFAULTS)
    row["vin"] = vin
    row.update(overrides)
    return row


def build_scenario_rows() -> List[Dict[str, Any]]:
    """The fixture rows shared by the dbt build and the equivalence test."""
    return [
        # identical consecutive detail states -> no boundary (stable_state_run)
        _obs_row(VIN_IDENT, listing_id="LI", artifact_id=90001, fetched_at=_ts(2026, 7, 1)),
        _obs_row(VIN_IDENT, listing_id="LI", artifact_id=90002, fetched_at=_ts(2026, 7, 2)),
        # changed price -> boundary (state_change_run)
        _obs_row(
            VIN_PRICE, listing_id="LP", artifact_id=90011,
            fetched_at=_ts(2026, 7, 1), price=20000,
        ),
        _obs_row(
            VIN_PRICE, listing_id="LP", artifact_id=90012,
            fetched_at=_ts(2026, 7, 2), price=19000,
        ),
        # changed listing_state -> boundary
        _obs_row(
            VIN_STATECHG, listing_id="LS", artifact_id=90021,
            fetched_at=_ts(2026, 7, 1), listing_state="active",
        ),
        _obs_row(
            VIN_STATECHG, listing_id="LS", artifact_id=90022,
            fetched_at=_ts(2026, 7, 2), listing_state="unlisted",
        ),
        # changed listing_id for the same VIN -> boundary (listing_id is in the fingerprint)
        _obs_row(VIN_RELIST2, listing_id="LR1", artifact_id=90031, fetched_at=_ts(2026, 7, 1)),
        _obs_row(VIN_RELIST2, listing_id="LR2", artifact_id=90032, fetched_at=_ts(2026, 7, 2)),
        # A -> B -> A sequence -> two boundaries
        _obs_row(
            VIN_ABA, listing_id="LA", artifact_id=90041,
            fetched_at=_ts(2026, 7, 1), price=100,
        ),
        _obs_row(
            VIN_ABA, listing_id="LA", artifact_id=90042,
            fetched_at=_ts(2026, 7, 2), price=200,
        ),
        _obs_row(
            VIN_ABA, listing_id="LA", artifact_id=90043,
            fetched_at=_ts(2026, 7, 3), price=100,
        ),
        # detail (earlier) must win over a later srp row (detail_beats_srp)
        _obs_row(
            VIN_DETAIL_WINS, listing_id="LDW", artifact_id=90051,
            source="detail", fetched_at=_ts(2026, 7, 1),
        ),
        _obs_row(
            VIN_DETAIL_WINS, listing_id="LDW", artifact_id=90052,
            source="srp", fetched_at=_ts(2026, 7, 3),
        ),
        # usable srp row, no detail row at all (srp_fallback)
        _obs_row(
            VIN_SRP_ONLY, listing_id="LSO", artifact_id=90061,
            source="srp", fetched_at=_ts(2026, 7, 1),
        ),
    ]


_SCHEMA = pa.schema([
    pa.field("artifact_id",          pa.int64()),
    pa.field("listing_id",           pa.string()),
    pa.field("vin",                  pa.string()),
    pa.field("canonical_detail_url", pa.string()),
    pa.field("source",               pa.string()),
    pa.field("listing_state",        pa.string()),
    pa.field("fetched_at",           pa.timestamp("us", tz="UTC")),
    pa.field("written_at",           pa.timestamp("us", tz="UTC")),
    pa.field("price",                pa.int32()),
    pa.field("make",                 pa.string()),
    pa.field("model",                pa.string()),
    pa.field("trim",                 pa.string()),
    pa.field("year",                 pa.int16()),
    pa.field("mileage",              pa.int32()),
    pa.field("msrp",                 pa.int32()),
    pa.field("stock_type",           pa.string()),
    pa.field("fuel_type",            pa.string()),
    pa.field("body_style",           pa.string()),
    pa.field("dealer_name",          pa.string()),
    pa.field("dealer_zip",           pa.string()),
    pa.field("customer_id",          pa.string()),
    pa.field("seller_id",            pa.string()),
    pa.field("dealer_street",        pa.string()),
    pa.field("dealer_city",          pa.string()),
    pa.field("dealer_state",         pa.string()),
    pa.field("dealer_phone",         pa.string()),
    pa.field("dealer_website",       pa.string()),
    pa.field("dealer_cars_com_url",  pa.string()),
    pa.field("dealer_rating",        pa.float32()),
    pa.field("financing_type",       pa.string()),
    pa.field("seller_zip",           pa.string()),
    pa.field("seller_customer_id",   pa.string()),
    pa.field("page_number",          pa.int16()),
    pa.field("position_on_page",     pa.int16()),
    pa.field("trid",                 pa.string()),
    pa.field("isa_context",          pa.string()),
    pa.field("body",                 pa.string()),
    pa.field("condition",            pa.string()),
])


def _upload_source_group(source: str, rows: List[Dict[str, Any]]) -> str:
    """Write one source's rows to its own hive partition folder. Writing
    mixed-source rows under a single `source=X` folder would let DuckDB's
    hive-partition inference silently override the real `source` column
    value for every row in that folder."""
    for row in rows:
        row.setdefault("written_at", datetime.now(timezone.utc))
    table = pa.Table.from_pylist(rows, schema=_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    key = (
        f"silver_normalized/observations/source={source}/{_PARTITION}"
        "/dbt_selector_equivalence_fixture.parquet"
    )
    ensure_bucket()
    get_boto3_client().put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    return key


def seed() -> List[str]:
    """Upload all scenario rows, grouped by source. Returns the uploaded keys."""
    rows = build_scenario_rows()
    by_source: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        by_source.setdefault(row["source"], []).append(row)
    return [_upload_source_group(source, source_rows) for source, source_rows in by_source.items()]


def main() -> None:
    keys = seed()
    for key in keys:
        print(f"Uploaded s3://{BUCKET}/{key}")


if __name__ == "__main__":
    main()
