"""
Single source of truth for the synthetic MinIO fixture used by the Plan 120
lake-snapshot integration tests.

    python scripts/seed_lake_snapshot_fixture.py [--phase base|observation_fingerprint_incremental]

This module seeds deterministic business-state scenarios into MinIO across all
four supported source tables:

    silver_observations       (silver_normalized/observations/…)
    price_observation_events  (ops_normalized/price_observation_events/…)
    vin_to_listing_events     (ops_normalized/vin_to_listing_events/…)
    blocked_cooldown_events   (ops_normalized/blocked_cooldown_events/…)

Three consumers read this fixture, and all import their expected entities from
here so seeding and assertions cannot drift apart:

  1. tests/integration/dbt/test_selector_dbt_equivalence.py — after the CI
     `dbt build --target duckdb`, compares dbt's materialized
     int_listing_state_runs / int_latest_observation against the archiver
     selector SQL run over this same data.
  2. tests/integration/archiver/test_lake_snapshot_selectors.py and
     test_lake_snapshot_cohort.py — run the archiver selector / cohort SQL
     (`run_lake_selectors`, `build_snapshot_cohort`) directly against this
     MinIO data to prove the SQL is correct against real, production-shaped
     Parquet.
  3. tests/integration/dbt/test_observation_fingerprints_real_build.py (Plan 123
     Phase 2b) — seeds the "observation_fingerprint_incremental" phase after
     the base phase has already been built once, reruns `dbt build --select
     int_listing_observation_fingerprints` against the real dbt project (no
     throwaway shadow project), and asserts on the real materialized output —
     both the single-build base-phase behavior and the second phase's
     incremental/late-arrival/correction behavior.

`seed(phase=...)` (default "base") controls which rows get written:

  * "base" — the original single-shot fixture (silver + all three ops
    tables), seeded once before the first `dbt build`.
  * "observation_fingerprint_incremental" — silver-only, written under
    distinct filenames (see `_write_dataset`) so it lands alongside, not over,
    the base phase's files. A subsequent non-full-refresh `dbt build` then
    sees base+phase-2 data combined, exercising real incremental behavior.

Two design rules make this safe to share:

  * Reserved partition. Everything is written under obs_year=2099/obs_month=1
    (silver) and year=2099/month=1 (ops). No production flush or other CI seed
    writes to 2099, so this fixture never collides with the empty-schema
    compilation seed the CI `dbt` job also writes (which uses 2026 partitions).
    Do NOT reuse the 2099 partition for any other seed data.

  * Physically production-shaped. The Arrow schema is imported from the real
    writers (never re-declared), and rows are written with the same
    `pq.write_to_dataset(partition_cols=...)` call the flush uses. So the
    on-disk layout matches production byte-for-byte: source/obs_year/obs_month
    (silver) and year/month (ops) are stripped into the hive path, obs_day
    stays in the file. This matters because the archiver reads with
    `read_parquet(union_by_name=true)` and relies on DuckDB auto-detecting the
    hive partitions to see `source`; a fixture that kept `source` in the file
    would let a broken hive-partition read pass here while failing against real
    Parquet. If a writer renames or drops a column, this fixture (and the
    selector SQL that references it) breaks loudly, which is the drift signal
    we want.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

# Import the production writer schemas as the single source of truth for the
# on-disk column set. These are module-private in the writers, but importing
# them (rather than re-declaring) is exactly what makes schema drift fail here.
from archiver.processors.flush_silver_observations import _SCHEMA as _SILVER_WRITER_SCHEMA
from archiver.processors.flush_staging_events import (
    _BLOCKED_COOLDOWN_EVENTS_SCHEMA as _COOLDOWN_WRITER_SCHEMA,
)
from archiver.processors.flush_staging_events import (
    _PRICE_OBSERVATION_EVENTS_SCHEMA as _PRICE_WRITER_SCHEMA,
)
from archiver.processors.flush_staging_events import (
    _VIN_TO_LISTING_EVENTS_SCHEMA as _VIN_TO_LISTING_WRITER_SCHEMA,
)
from shared.minio import BUCKET, ensure_bucket, get_s3fs

# Reserved partition. Everything lands under 2099, which no production flush or
# other CI seed writes to.
_RESERVED_OBS_YEAR = 2099
_RESERVED_OBS_MONTH = 1
_RESERVED_YEAR = 2099
_RESERVED_MONTH = 1

# Use the production writer schemas verbatim (they include the hive partition
# columns). Rows are written with pq.write_to_dataset using the same
# partition_cols as the real flush, so the physical layout is identical:
# source/obs_year/obs_month (silver) and year/month (ops) are stripped into the
# object path, while obs_day stays in the file. This forces the reader side to
# resolve `source` (and the date partitions) from the hive path exactly as it
# must in production — a fixture that kept `source` in-file would let a broken
# hive-partition read pass here while failing against real Parquet.
_SILVER_SCHEMA = _SILVER_WRITER_SCHEMA
_PRICE_SCHEMA = _PRICE_WRITER_SCHEMA
_VIN_TO_LISTING_SCHEMA = _VIN_TO_LISTING_WRITER_SCHEMA
_COOLDOWN_SCHEMA = _COOLDOWN_WRITER_SCHEMA

_SILVER_PARTITION_COLS = ["source", "obs_year", "obs_month"]
_OPS_PARTITION_COLS = ["year", "month"]


def _vin17(tag: str) -> str:
    """Deterministic 17-char alphanumeric VIN — same construction the
    selector's fingerprint CTE and dbt's stg_observations.sql both validate
    against (17 chars, [A-Z0-9] only)."""
    return (tag.upper() + "0" * 17)[:17]


def _ts(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


# ===========================================================================
# silver_observations scenarios
# ===========================================================================

# --- dbt-equivalence VINs (stable_state_run / state_change_run) ------------
VIN_IDENT = _vin17("IDENT")       # identical consecutive detail states -> stable
VIN_PRICE = _vin17("PRICE")       # price changes -> state change
VIN_STATECHG = _vin17("STCHG")    # listing_state changes -> state change
VIN_RELIST2 = _vin17("RELST2")    # listing_id changes for the same VIN -> state change
VIN_ABA = _vin17("ABA")           # A -> B -> A sequence -> two state changes

# --- dbt-equivalence VINs (detail_beats_srp / srp_fallback) ----------------
VIN_DETAIL_WINS = _vin17("DETWIN")  # detail (earlier) must win over a later srp row
VIN_SRP_ONLY = _vin17("SRPONLY")    # usable srp row, no detail row at all

# --- selector / cohort scenario entities -----------------------------------
VIN_UNLIST = "VIN_UNLIST_TARGET"     # active_to_unlisted
VIN_CAROUSEL = "VIN_CAROUSEL_X"      # carousel_only_or_low_priority
VIN_NO_PRICE = "VIN_NO_PRICE_X"      # no_price_history (absent from price events)
VIN_FRESH = "VIN_FRESH"              # fresh_recent_listing
VIN_STALE = "VIN_STALE"              # stale_listing

# vin_to_listing / price / closure entities
VIN_RELISTED = "VIN_RELISTED"        # relisted across L1 -> L2 (remap)
VIN_SINGLE = "VIN_SINGLE"            # single listing L3
VIN_L16 = "VIN_L16"                  # price_changed_7d listing L16
VIN_L17 = "VIN_L17"                  # price_changed_30d_only listing L17

# artifact_id constants used by cohort closure assertions
ARTIFACT_NULL_VIN = 108              # invalid_or_null_vin row (vin=None, listing L8a)
ARTIFACT_SHORT_VIN = 109             # invalid_or_null_vin row (vin='SHORTVIN', listing L8b)
ARTIFACT_RELISTED_1 = 101            # VIN_RELISTED on L1
ARTIFACT_RELISTED_2 = 102            # VIN_RELISTED on L2
LISTING_NULL_VIN = "L8a"

# artifact co-occurrence scenario: one artifact_id (e.g. an SRP/carousel page)
# spans two otherwise-unrelated VIN/listing rows. Used to prove closure does
# not treat artifact co-occurrence as a vehicle-identity edge.
ARTIFACT_SRP_SHARED = 150
VIN_SRP_COOCCUR_A = "VIN_SRP_COOCCUR_A"
VIN_SRP_COOCCUR_B = "VIN_SRP_COOCCUR_B"
LISTING_SRP_COOCCUR_A = "L30"
LISTING_SRP_COOCCUR_B = "L31"

# --- Plan 123 Phase 2b: int_listing_observation_fingerprints scenarios -----
# One SRP artifact_id carrying two listing_ids: the all-source observation
# fingerprint model keys on artifact_id + listing_id, not bare artifact_id, so
# these two rows must produce two distinct fingerprint rows, not a collision.
ARTIFACT_SRP_MULTI = 400
VIN_SRP_MULTI_A = _vin17("SRPMULTA")
VIN_SRP_MULTI_B = _vin17("SRPMULTB")
LISTING_SRP_MULTI_A = "L40"
LISTING_SRP_MULTI_B = "L41"

# One carousel artifact_id carrying two listing_ids, one with a resolved VIN
# and one without — carousel rows commonly lack a resolved VIN, and the
# observation fingerprint model must retain such a row as long as listing_id
# is present.
ARTIFACT_CAROUSEL_MULTI = 410
VIN_CAROUSEL_MULTI_A = _vin17("CARMULTA")
LISTING_CAROUSEL_MULTI_A = "L42"
LISTING_CAROUSEL_MULTI_B = "L43"

# Phase 2b incremental-run scenario (seeded separately via
# seed(phase="observation_fingerprint_incremental"), never part of the base
# phase): a late-arriving SRP artifact whose fetched_at falls inside the
# model's lookback window, and a corrected observation replacing
# ARTIFACT_SRP_MULTI/LISTING_SRP_MULTI_A's price.
VIN_OBSFP_LATE_ARRIVAL = _vin17("LATEARR")
LISTING_OBSFP_LATE_ARRIVAL = "L44"
ARTIFACT_OBSFP_LATE_ARRIVAL = 420
OBSFP_CORRECTED_PRICE = 17500

# listing_id constants asserted on by the selector/cohort integration tests,
# exported so seeding and assertions cannot drift apart.
LISTING_RELISTED_1 = "L1"            # relisted VIN first listing (+ price drop/increase, cooldown)
LISTING_RELISTED_2 = "L2"            # relisted VIN second listing (remap of L1)
LISTING_PRICE_7D = "L16"            # price_changed_7d
LISTING_PRICE_30D = "L17"          # price_changed_30d_only
LISTING_COOLDOWN_SINGLE = "L5"      # cooldown_blocked (one first-attempt block)
LISTING_COOLDOWN_3_4 = "L20"        # cooldown_bucket_3_4
LISTING_COOLDOWN_5_10 = "L21"       # cooldown_bucket_5_10
LISTING_COOLDOWN_11_PLUS = "L22"    # cooldown_bucket_11_plus
LISTING_ACTIVE_UNLIST = "L9"        # active_to_unlisted
LISTING_FRESH = "L14"               # fresh_recent_listing (freshest listing in the seed)
LISTING_STALE = "L15"               # stale_listing (oldest listing in the seed)

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
)


def _obs_row(vin: str | None, **overrides: Any) -> Dict[str, Any]:
    row = dict(_OBS_DEFAULTS)
    row["vin"] = vin
    row.update(overrides)
    return row


def _dbt_equivalence_rows() -> List[Dict[str, Any]]:
    """Rows the dbt/selector equivalence guard asserts on (stable/change runs,
    detail-beats-srp, srp-fallback)."""
    return [
        # identical consecutive detail states -> no boundary (stable_state_run)
        _obs_row(VIN_IDENT, listing_id="LI", artifact_id=90001, fetched_at=_ts(2026, 7, 1)),
        _obs_row(VIN_IDENT, listing_id="LI", artifact_id=90002, fetched_at=_ts(2026, 7, 2)),
        # changed price -> boundary (state_change_run)
        _obs_row(VIN_PRICE, listing_id="LP", artifact_id=90011,
                 fetched_at=_ts(2026, 7, 1), price=20000),
        _obs_row(VIN_PRICE, listing_id="LP", artifact_id=90012,
                 fetched_at=_ts(2026, 7, 2), price=19000),
        # changed listing_state -> boundary
        _obs_row(VIN_STATECHG, listing_id="LS", artifact_id=90021,
                 fetched_at=_ts(2026, 7, 1), listing_state="active"),
        _obs_row(VIN_STATECHG, listing_id="LS", artifact_id=90022,
                 fetched_at=_ts(2026, 7, 2), listing_state="unlisted"),
        # changed listing_id for the same VIN -> boundary (listing_id is in the fingerprint)
        _obs_row(VIN_RELIST2, listing_id="LR1", artifact_id=90031, fetched_at=_ts(2026, 7, 1)),
        _obs_row(VIN_RELIST2, listing_id="LR2", artifact_id=90032, fetched_at=_ts(2026, 7, 2)),
        # A -> B -> A sequence -> two boundaries
        _obs_row(VIN_ABA, listing_id="LA", artifact_id=90041,
                 fetched_at=_ts(2026, 7, 1), price=100),
        _obs_row(VIN_ABA, listing_id="LA", artifact_id=90042,
                 fetched_at=_ts(2026, 7, 2), price=200),
        _obs_row(VIN_ABA, listing_id="LA", artifact_id=90043,
                 fetched_at=_ts(2026, 7, 3), price=100),
        # detail (earlier) must win over a later srp row (detail_beats_srp)
        _obs_row(VIN_DETAIL_WINS, listing_id="LDW", artifact_id=90051,
                 source="detail", fetched_at=_ts(2026, 7, 1)),
        _obs_row(VIN_DETAIL_WINS, listing_id="LDW", artifact_id=90052,
                 source="srp", fetched_at=_ts(2026, 7, 3)),
        # usable srp row, no detail row at all (srp_fallback)
        _obs_row(VIN_SRP_ONLY, listing_id="LSO", artifact_id=90061,
                 source="srp", fetched_at=_ts(2026, 7, 1)),
    ]


def _dense_rows(n: int = 20) -> List[Dict[str, Any]]:
    """benchmark_dense_make_model — a Honda Civic group dense enough for stable
    percentile/median benchmarks."""
    return [
        _obs_row(f"DENSE{i:012d}", listing_id=f"LDENSE{i}", artifact_id=2000 + i,
                 make="Honda", model="Civic", fetched_at=_ts(2026, 7, 1))
        for i in range(n)
    ]


def _sparse_rows(n: int = 2) -> List[Dict[str, Any]]:
    """benchmark_sparse_make_model — a Rare Bird group with only a few rows."""
    return [
        _obs_row(f"SPARSE{i:011d}", listing_id=f"LSPARSE{i}", artifact_id=3000 + i,
                 make="Rare", model="Bird", fetched_at=_ts(2026, 7, 1))
        for i in range(n)
    ]


def _selector_scenario_rows() -> List[Dict[str, Any]]:
    """Rows exercising the remaining silver-observation selectors and the
    entity relationships the cohort closure tests assert on."""
    rows = [
        # invalid_or_null_vin: null vin and a too-short/invalid vin
        _obs_row(None, listing_id=LISTING_NULL_VIN, artifact_id=ARTIFACT_NULL_VIN,
                 fetched_at=_ts(2026, 7, 1)),
        _obs_row("SHORTVIN", listing_id="L8b", artifact_id=ARTIFACT_SHORT_VIN,
                 fetched_at=_ts(2026, 7, 1)),
        # active_to_unlisted: active row followed by an unlisted row, same listing
        _obs_row(VIN_UNLIST, listing_id="L9", artifact_id=1091, fetched_at=_ts(2026, 7, 1),
                 listing_state="active", price=10000),
        _obs_row(VIN_UNLIST, listing_id="L9", artifact_id=1092, fetched_at=_ts(2026, 7, 3),
                 listing_state="unlisted", price=None, make=None, model=None),
        # carousel_only_or_low_priority: only a carousel row for this VIN
        _obs_row(VIN_CAROUSEL, listing_id="L12", artifact_id=112, source="carousel",
                 fetched_at=_ts(2026, 7, 1), make=None, model=None),
        # no_price_history: VIN present in silver, absent from price events entirely
        _obs_row(VIN_NO_PRICE, listing_id="L13", artifact_id=113, fetched_at=_ts(2026, 7, 1)),
        # fresh_recent_listing: freshest listing in the seed — must be seen
        # more recently than any other scenario's silver observation.
        _obs_row(VIN_FRESH, listing_id="L14", artifact_id=1141, fetched_at=_ts(2026, 7, 25)),
        _obs_row(VIN_FRESH, listing_id="L14", artifact_id=1142, fetched_at=_ts(2026, 7, 28)),
        # stale_listing: last seen far before the window anchor
        _obs_row(VIN_STALE, listing_id="L15", artifact_id=1151, fetched_at=_ts(2026, 1, 1)),
        _obs_row(VIN_STALE, listing_id="L15", artifact_id=1152, fetched_at=_ts(2026, 1, 5)),
        # closure: observations tying the relisted VIN's listings/artifacts together
        _obs_row(VIN_RELISTED, listing_id="L1", artifact_id=101, fetched_at=_ts(2026, 7, 1)),
        _obs_row(VIN_RELISTED, listing_id="L2", artifact_id=102, fetched_at=_ts(2026, 7, 2)),
        _obs_row(VIN_L16, listing_id="L16", artifact_id=116, fetched_at=_ts(2026, 7, 20)),
        # artifact co-occurrence: same artifact_id (an SRP/carousel page) spans
        # two unrelated VIN/listing rows — closure must not fuse them.
        _obs_row(VIN_SRP_COOCCUR_A, listing_id=LISTING_SRP_COOCCUR_A,
                 artifact_id=ARTIFACT_SRP_SHARED, fetched_at=_ts(2026, 7, 1)),
        _obs_row(VIN_SRP_COOCCUR_B, listing_id=LISTING_SRP_COOCCUR_B,
                 artifact_id=ARTIFACT_SRP_SHARED, fetched_at=_ts(2026, 7, 1)),
    ]
    rows += _dense_rows()
    rows += _sparse_rows()
    return rows


def _observation_fingerprint_rows() -> List[Dict[str, Any]]:
    """Plan 123 Phase 2b base-phase rows for int_listing_observation_fingerprints:
    an SRP artifact and a carousel artifact each carrying two listing_ids, proving
    the model's artifact_id + listing_id key does not collide."""
    return [
        _obs_row(VIN_SRP_MULTI_A, listing_id=LISTING_SRP_MULTI_A, artifact_id=ARTIFACT_SRP_MULTI,
                 source="srp", fetched_at=_ts(2026, 7, 1), price=20000),
        _obs_row(VIN_SRP_MULTI_B, listing_id=LISTING_SRP_MULTI_B, artifact_id=ARTIFACT_SRP_MULTI,
                 source="srp", fetched_at=_ts(2026, 7, 1), price=21000),
        _obs_row(VIN_CAROUSEL_MULTI_A, listing_id=LISTING_CAROUSEL_MULTI_A,
                 artifact_id=ARTIFACT_CAROUSEL_MULTI, source="carousel",
                 fetched_at=_ts(2026, 7, 1), price=18000),
        _obs_row(None, listing_id=LISTING_CAROUSEL_MULTI_B, artifact_id=ARTIFACT_CAROUSEL_MULTI,
                 source="carousel", fetched_at=_ts(2026, 7, 1), price=19000),
    ]


def build_silver_rows() -> List[Dict[str, Any]]:
    """All silver_observations fixture rows (dbt-equivalence + selector/cohort)."""
    return _dbt_equivalence_rows() + _selector_scenario_rows() + _observation_fingerprint_rows()


def build_observation_fingerprint_incremental_rows() -> List[Dict[str, Any]]:
    """Plan 123 Phase 2b incremental-phase rows, seeded only via
    seed(phase="observation_fingerprint_incremental") after the base phase has
    already been built once. Exercises int_listing_observation_fingerprints'
    late-arrival lookback and observation_id replace-on-correction behavior on
    a subsequent (non-full-refresh) dbt build:

      * a late-arriving SRP artifact, never seen in the base phase, whose
        fetched_at falls inside the model's default 3-day lookback window
        relative to the base phase's global max fetched_at (2026-07-28, from
        VIN_FRESH's second row in _selector_scenario_rows) — it must appear
        after the incremental rebuild;
      * a corrected observation for ARTIFACT_SRP_MULTI/LISTING_SRP_MULTI_A
        (base phase price 20000) with a later fetched_at inside the lookback
        window and a different price — the model must replace the existing
        target row for that observation_id, not duplicate it.
    """
    return [
        _obs_row(VIN_OBSFP_LATE_ARRIVAL, listing_id=LISTING_OBSFP_LATE_ARRIVAL,
                 artifact_id=ARTIFACT_OBSFP_LATE_ARRIVAL, source="srp",
                 fetched_at=_ts(2026, 7, 26), price=22000),
        _obs_row(VIN_SRP_MULTI_A, listing_id=LISTING_SRP_MULTI_A, artifact_id=ARTIFACT_SRP_MULTI,
                 source="srp", fetched_at=_ts(2026, 7, 27), price=OBSFP_CORRECTED_PRICE),
    ]


# Back-compat alias for the dbt-equivalence test, which imports the row builder
# by its original name.
def build_scenario_rows() -> List[Dict[str, Any]]:
    return build_silver_rows()


# ===========================================================================
# ops event scenarios
# ===========================================================================

def build_vin_to_listing_rows() -> List[Dict[str, Any]]:
    """relisted_vin + remap closure (VIN_RELISTED: L1 -> L2, previous L1)."""
    return [
        dict(event_id=1, vin=VIN_RELISTED, listing_id="L1", artifact_id=101,
             event_type="mapped", previous_listing_id=None, event_at=_ts(2026, 7, 1)),
        dict(event_id=2, vin=VIN_RELISTED, listing_id="L2", artifact_id=102,
             event_type="remapped", previous_listing_id="L1", event_at=_ts(2026, 7, 2)),
        dict(event_id=3, vin=VIN_SINGLE, listing_id="L3", artifact_id=103,
             event_type="mapped", previous_listing_id=None, event_at=_ts(2026, 7, 1)),
    ]


def build_price_event_rows() -> List[Dict[str, Any]]:
    """price_drop / price_increase / price_changed_7d / price_changed_30d_only."""
    specs = [
        # L1: 20000 -> 19000 (drop) -> 21000 (increase)
        (1, "L1", VIN_RELISTED, 101, 20000, _ts(2026, 7, 1)),
        (2, "L1", VIN_RELISTED, 101, 19000, _ts(2026, 7, 2)),
        (3, "L1", VIN_RELISTED, 101, 21000, _ts(2026, 7, 3)),
        (4, "L4", "VIN_OTHER", 104, 15000, _ts(2026, 7, 1)),
        # L16: change on 8/1 -> within 7d of an 8/1-ish window end
        (5, "L16", VIN_L16, 116, 12000, _ts(2026, 7, 20)),
        (6, "L16", VIN_L16, 116, 13000, _ts(2026, 8, 1)),
        # L17: change on 7/10 -> within 30d but outside 7d
        (7, "L17", VIN_L17, 117, 8000, _ts(2026, 7, 1)),
        (8, "L17", VIN_L17, 117, 8500, _ts(2026, 7, 10)),
    ]
    return [
        dict(event_id=eid, listing_id=lid, vin=vin, artifact_id=aid, price=price,
             make="Honda", model="Civic", event_type="upserted", source="detail",
             event_at=event_at)
        for (eid, lid, vin, aid, price, event_at) in specs
    ]


def build_cooldown_event_rows() -> List[Dict[str, Any]]:
    """cooldown_blocked / cooldown_incremented / cooldown_bucket_{3_4,5_10,11_plus}."""
    specs = [
        (1, "L1", 1, _ts(2026, 7, 1)),
        (2, "L1", 2, _ts(2026, 7, 2)),
        (3, "L5", 1, _ts(2026, 7, 1)),
        (4, "L20", 3, _ts(2026, 7, 1)),
        (5, "L21", 7, _ts(2026, 7, 1)),
        (6, "L22", 15, _ts(2026, 7, 1)),
    ]
    return [
        dict(event_id=eid, listing_id=lid, event_type="blocked",
             num_of_attempts=attempts, event_at=event_at)
        for (eid, lid, attempts, event_at) in specs
    ]


# ===========================================================================
# Upload
# ===========================================================================

def _write_dataset(
    schema: pa.Schema, rows: List[Dict[str, Any]], prefix: str, partition_cols: List[str],
    basename_prefix: str = "lake_snapshot_fixture",
) -> str:
    """Write rows as a hive-partitioned Parquet dataset, mirroring the
    production flush (pq.write_to_dataset with the same partition_cols).

    basename_prefix must be distinct per phase: pyarrow's `{i}` numbering in
    basename_template restarts at 0 on every call, so two phases writing into
    the same partition directory with the same prefix would silently
    overwrite each other's file (existing_data_behavior="overwrite_or_ignore"
    keeps pre-existing *differently-named* files but replaces same-named
    ones). Distinct prefixes let a phase-2 seed add rows alongside phase-1's
    without touching its files, exactly like separate production flush runs.
    """
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_to_dataset(
        table,
        root_path=f"s3://{BUCKET}/{prefix}",
        partition_cols=partition_cols,
        filesystem=get_s3fs(),
        existing_data_behavior="overwrite_or_ignore",
        basename_template=f"{basename_prefix}-{{i}}.parquet",
    )
    return prefix


def _seed_silver(rows: List[Dict[str, Any]], basename_prefix: str = "lake_snapshot_fixture") -> str:
    now = datetime.now(timezone.utc)
    for row in rows:
        # Force every row into the reserved partition (like the fixed 2099
        # location before). obs_day stays an in-file column, as in production.
        fetched_at = row["fetched_at"]
        row["obs_year"] = _RESERVED_OBS_YEAR
        row["obs_month"] = _RESERVED_OBS_MONTH
        row["obs_day"] = fetched_at.day if fetched_at is not None else 1
        row.setdefault("written_at", now)
    return _write_dataset(
        _SILVER_SCHEMA, rows, "silver_normalized/observations", _SILVER_PARTITION_COLS,
        basename_prefix=basename_prefix,
    )


def _seed_ops_table(schema: pa.Schema, rows: List[Dict[str, Any]], prefix: str) -> str:
    for row in rows:
        row["year"] = _RESERVED_YEAR
        row["month"] = _RESERVED_MONTH
    return _write_dataset(schema, rows, prefix, _OPS_PARTITION_COLS)


def _seed_ops() -> List[str]:
    return [
        _seed_ops_table(
            _PRICE_SCHEMA, build_price_event_rows(),
            "ops_normalized/price_observation_events",
        ),
        _seed_ops_table(
            _VIN_TO_LISTING_SCHEMA, build_vin_to_listing_rows(),
            "ops_normalized/vin_to_listing_events",
        ),
        _seed_ops_table(
            _COOLDOWN_SCHEMA, build_cooldown_event_rows(),
            "ops_normalized/blocked_cooldown_events",
        ),
    ]


# Phases: "base" is the original single-shot fixture (silver + all three ops
# tables), seeded once by the CI `dbt` job before the real `dbt build`.
# "observation_fingerprint_incremental" is Plan 123 Phase 2b's second wave —
# silver-only, written under distinct filenames so it lands alongside (not
# over) the base phase's files, letting a later non-full-refresh `dbt build`
# exercise incremental behavior against the combined base+phase-2 data.
PHASES = ("base", "observation_fingerprint_incremental")


def seed(phase: str = "base") -> List[str]:
    """Upload fixture data for the given phase. Returns the written keys."""
    if phase not in PHASES:
        raise ValueError(f"unknown phase {phase!r}; expected one of {PHASES}")
    ensure_bucket()
    if phase == "base":
        return [_seed_silver(build_silver_rows())] + _seed_ops()
    return [
        _seed_silver(
            build_observation_fingerprint_incremental_rows(),
            basename_prefix="lake_snapshot_fixture_obsfp_incremental",
        )
    ]


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=PHASES, default="base")
    args = parser.parse_args()

    for key in seed(phase=args.phase):
        print(f"Uploaded s3://{BUCKET}/{key} (phase={args.phase})")


if __name__ == "__main__":
    main()
