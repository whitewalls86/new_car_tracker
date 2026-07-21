"""
Single source of truth for the synthetic MinIO fixture used by the Plan 120
lake-snapshot integration tests.

    python scripts/seed_lake_snapshot_fixture.py --phase <phase>
    # phase in: base, observation_fingerprint_incremental,
    #           detail_fingerprint_incremental, price_history_incremental,
    #           listing_state_runs_incremental, scrape_volume_incremental,
    #           latest_observation_incremental, observation_runs_incremental

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
  4. tests/integration/dbt/test_incremental_models_real_build.py — the same
     real-build pattern as (3), extended to int_listing_state_fingerprints,
     int_price_history, int_listing_state_runs (replacing the throwaway
     dbt-duckdb shadow-project tests those models used to have),
     mart_scrape_volume (Plan 123 Phase 5), int_latest_observation (also
     Plan 123 Phase 5), and int_listing_observation_runs (Plan 123 final
     modeling correction: the all-source, listing_id-grain observation-state
     run model).

`seed(phase=...)` (default "base") controls which rows get written:

  * "base" — the original single-shot fixture (silver + all three ops
    tables), seeded once before the first `dbt build`.
  * "observation_fingerprint_incremental", "detail_fingerprint_incremental",
    "price_history_incremental", "listing_state_runs_incremental",
    "scrape_volume_incremental", "latest_observation_incremental" — each a
    silver- or ops-only second wave for one real-build incremental test,
    written under distinct filenames (see `_write_dataset`) so they land
    alongside, not over, the base phase's (and each other's) files. A
    subsequent non-full-refresh `dbt build` then sees base+phase-2 data
    combined, exercising real incremental behavior.

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

from datetime import datetime, timedelta, timezone
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
# model's lookback window, and a newer fetch of an existing listing (later
# fetched_at) that supersedes it.
VIN_OBSFP_LATE_ARRIVAL = _vin17("LATEARR")
LISTING_OBSFP_LATE_ARRIVAL = "L44"
ARTIFACT_OBSFP_LATE_ARRIVAL = 420
OBSFP_CORRECTED_PRICE = 17500

# A true reprocessing correction: same artifact_id/listing_id/fetched_at as
# the base-phase row, but re-landed in phase 2 with a different price. Since
# fetched_at is identical, only written_at (later here, because this row is
# seeded after the base phase in wall-clock time — see _seed_silver's
# row.setdefault("written_at", now)) distinguishes the corrected row from the
# original, proving the model's written_at dedupe tiebreaker actually works,
# not just its fetched_at-desc ordering.
VIN_OBSFP_CORRECTION = _vin17("CORRECT")
LISTING_OBSFP_CORRECTION = "L45"
ARTIFACT_OBSFP_CORRECTION = 430
OBSFP_CORRECTION_FETCHED_AT = _ts(2026, 7, 26)
OBSFP_CORRECTION_ORIGINAL_PRICE = 23000
OBSFP_CORRECTION_FIXED_PRICE = 23500

# --- Plan 123 replacement for the shadow-project incremental tests: real-build
# coverage for int_listing_state_fingerprints, int_price_history, and
# int_listing_state_runs (see tests/integration/dbt/test_incremental_models_real_build.py) ---

# int_listing_state_fingerprints ("detail_fingerprint_incremental" phase).
# ARTIFACT_FP_ANCHOR fetches at 2026-07-26, close to the other detail/vin17
# scenario rows added below for int_listing_state_runs (2026-07-26 to
# 2026-07-27) — all clustered together so the shared 3-day lookback window
# comfortably covers every phase-2 addition regardless of which of these
# base rows ends up the actual global max(fetched_at). (Rows added for other
# scenarios earlier in this file, e.g. the dbt-equivalence VINs, top out at
# 2026-07-03 and are unrelated to this cluster.)
VIN_FP_TARGET = _vin17("FPTARGT")
LISTING_FP_TARGET = "L50"
ARTIFACT_FP_ANCHOR = 500
ARTIFACT_FP_DUP = 501
FP_ANCHOR_FETCHED_AT = _ts(2026, 7, 26)
FP_DUP_BASE_FETCHED_AT = _ts(2026, 7, 24)
FP_DUP_BASE_PRICE = 24000

# Phase-2 rows: a never-before-seen artifact (late arrival), a re-publish of
# ARTIFACT_FP_DUP with a later fetched_at (correction — the model's
# row_number() dedupe has no written_at tiebreaker, only fetched_at desc, so
# the corrected row must use a strictly later fetched_at to win
# deterministically), and a same-batch retry duplicate (ARTIFACT_FP_RETRY
# appears twice in the very same phase-2 seed, simulating an ingestion retry
# landing two rows for one artifact_id in a single source scan).
ARTIFACT_FP_LATE = 502
FP_LATE_FETCHED_AT = _ts(2026, 7, 25)
FP_DUP_CORRECTED_FETCHED_AT = _ts(2026, 7, 27)
FP_DUP_CORRECTED_PRICE = 26000
ARTIFACT_FP_RETRY = 503
FP_RETRY_FETCHED_AT_EARLY = _ts(2026, 7, 26, 8)
FP_RETRY_FETCHED_AT_LATE = _ts(2026, 7, 26, 14)
FP_RETRY_LATE_PRICE = 200

# int_price_history ("price_history_incremental" phase). VIN_PH_AFFECTED gets
# base-phase events far outside any future lookback window, plus phase-2
# events both inside the lookback and interleaved with the existing history —
# proving the affected-VIN replacement rereads the VIN's COMPLETE history, not
# just the new rows. VIN_PH_STABLE is a control VIN never touched by phase 2.
VIN_PH_AFFECTED = "VIN_PH_AFFECTED"
LISTING_PH_AFFECTED = "LPH1"
VIN_PH_STABLE = "VIN_PH_STABLE"
LISTING_PH_STABLE = "LPH2"
PH_AFFECTED_EVENT_1 = _ts(2026, 7, 1)
PH_AFFECTED_EVENT_2 = _ts(2026, 7, 2)
PH_STABLE_EVENT = _ts(2026, 7, 1)
PH_AFFECTED_LATE_EVENT = _ts(2026, 7, 29)     # late/corrected event inside lookback
PH_AFFECTED_NEW_EVENT = _ts(2026, 7, 30)      # new event inside lookback

# int_listing_state_runs ("listing_state_runs_incremental" phase).
# VIN_RUNS_A: base rows form two runs (fp_a x2, fp_b x1); phase 2 inserts a
# late artifact between the fp_a rows with a distinct fingerprint, splitting
# the original fp_a run into fp_a -> fp_c -> fp_a (3 runs) + the open fp_b run
# = 4 runs total.
# VIN_RUNS_B: base rows form three runs (fp_m, fp_n, fp_m again); phase 2
# republishes the middle artifact with a later fetched_at matching fp_m,
# merging all three into one continuous open run.
# VIN_RUNS_STABLE: untouched by phase 2, must be unaffected.
VIN_RUNS_A = _vin17("RUNSA")
LISTING_RUNS_A = "L60"
ARTIFACT_RUNS_1 = 600
ARTIFACT_RUNS_2 = 601
ARTIFACT_RUNS_3 = 602
ARTIFACT_RUNS_LATE_SPLIT = 603
VIN_RUNS_B = _vin17("RUNSB")
LISTING_RUNS_B = "L61"
ARTIFACT_RUNS_M1 = 610
ARTIFACT_RUNS_M2 = 611
ARTIFACT_RUNS_M3 = 612
ARTIFACT_RUNS_M2_CORRECTED_FETCHED_AT = _ts(2026, 7, 26, 6, 1)
VIN_RUNS_STABLE = _vin17("RUNSTBL")
LISTING_RUNS_STABLE = "L62"
ARTIFACT_RUNS_STABLE = 620

# --- Plan 123 Phase 5: mart_scrape_volume real-build incremental test ------
# ("scrape_volume_incremental" phase). Base build's global max(fetched_at)
# across the whole fixture is 2026-07-28 (VIN_FRESH's second row above), so
# mart_scrape_volume's base max(hour) is 2026-07-28T00:00.
#
# SV_AFFECTED_HOUR (2026-07-27T10:00) sits 14 hours before that max hour —
# inside the default 72-hour scrape_volume_incremental_lookback_hours window —
# and gets one base-phase detail/valid-vin row. Phase 2 adds a second,
# invalid-vin detail row inside the SAME hour, proving the affected-hour
# rebuild rereads the WHOLE hour (not just the new row): observation_count
# 1 -> 2, artifact_count 1 -> 2, valid_vin_count stays 1,
# vin_extraction_pct 100.0 -> 50.0.
#
# SV_NEW_HOUR (2026-07-28T14:00) exists only in phase 2 — a brand new
# (hour, source) row that must appear after the incremental rebuild.
#
# SV_STABLE_HOUR (2026-07-01T10:00) is a base-only control far outside the
# lookback window (>72 hours before the base max hour) — untouched by phase
# 2, proving unaffected hours are left alone.
SV_AFFECTED_HOUR = _ts(2026, 7, 27, 10)
LISTING_SV_AFFECTED_1 = "L70"
LISTING_SV_AFFECTED_2 = "L71"
ARTIFACT_SV_AFFECTED_1 = 700
ARTIFACT_SV_AFFECTED_2 = 701
VIN_SV_AFFECTED = _vin17("SVAFFCT")
SV_NEW_HOUR = _ts(2026, 7, 28, 14)
LISTING_SV_NEW = "L72"
ARTIFACT_SV_NEW = 702
SV_STABLE_HOUR = _ts(2026, 7, 1, 10)
LISTING_SV_STABLE = "L73"
ARTIFACT_SV_STABLE = 703

# --- Plan 123 Phase 5: int_latest_observation real-build incremental test --
# ("latest_observation_incremental" phase). All four VINs are distinct from
# every other scenario's VINs. Base build's max(fetched_at) among
# int_latest_observation's own winning rows is 2026-07-27T10:05 (from
# VIN_SV_AFFECTED above, a single-row winner) — comfortably above the other
# scenario clusters (fingerprints/runs top out at 2026-07-27T00:00) — so the
# default 3-day latest_observation_incremental_lookback_days window anchors on
# it: 2026-07-24T10:05.
#
# VIN_LO_PRIORITY: base detail row (LO_PRIORITY_DETAIL_FETCHED_AT, well
# outside the lookback window) is the base-phase winner. Phase 2 adds a newer
# SRP row inside the lookback window — the VIN becomes "affected" and its
# FULL history is reread, but the base detail row must still win because
# source priority (detail > srp) is checked before recency. Proves the
# affected-VIN replacement doesn't just look at the new row.
VIN_LO_PRIORITY = _vin17("LOPRI")
LISTING_LO_PRIORITY = "L80"
ARTIFACT_LO_PRIORITY_DETAIL = 800
ARTIFACT_LO_PRIORITY_SRP = 801
LO_PRIORITY_DETAIL_FETCHED_AT = _ts(2026, 7, 20)
LO_PRIORITY_DETAIL_MAKE = "Honda"
LO_PRIORITY_SRP_FETCHED_AT = _ts(2026, 7, 26)
LO_PRIORITY_SRP_MAKE = "SRP-Make"

# VIN_LO_DETAIL_UPGRADE: base detail row is the winner. Phase 2 adds a NEWER
# detail row inside the lookback window — same source tier, so recency
# decides and the phase-2 row must win.
VIN_LO_DETAIL_UPGRADE = _vin17("LODETUP")
LISTING_LO_DETAIL_UPGRADE = "L81"
ARTIFACT_LO_DETAIL_OLD = 810
ARTIFACT_LO_DETAIL_NEW = 811
LO_DETAIL_OLD_FETCHED_AT = _ts(2026, 7, 15)
LO_DETAIL_OLD_MAKE = "Honda"
LO_DETAIL_NEW_FETCHED_AT = _ts(2026, 7, 26)
LO_DETAIL_NEW_MAKE = "Ford"

# VIN_LO_NEW: absent from the base phase entirely. Its only row is added in
# phase 2, with fetched_at inside the lookback window — a late-arriving
# observation for a brand-new VIN, which must appear after the incremental
# rebuild.
VIN_LO_NEW = _vin17("LONEWVIN")
LISTING_LO_NEW = "L82"
ARTIFACT_LO_NEW = 820
LO_NEW_FETCHED_AT = _ts(2026, 7, 25)
LO_NEW_MAKE = "Mazda"

# VIN_LO_STABLE: base-only detail row, never referenced by phase 2 — proves
# an unaffected VIN is left unchanged by the incremental rebuild.
VIN_LO_STABLE = _vin17("LOSTABLE")
LISTING_LO_STABLE = "L83"
ARTIFACT_LO_STABLE = 830
LO_STABLE_FETCHED_AT = _ts(2026, 7, 10)
LO_STABLE_MAKE = "Subaru"

# --- Plan 123 final modeling correction: int_listing_observation_runs
# real-build incremental test (all-source, listing_id-grain observation-state
# runs; see tests/integration/dbt/test_incremental_models_real_build.py) ---
# LISTING_OBSRUN_A: base rows form two runs — a detail+srp pair sharing one
# observation_state_key (price/mileage/listing_state, price=30000), followed
# by a carousel artifact at a different price (open run, price=29000). Phase 2
# inserts a late detail artifact between the first two at a distinct price
# (31000), splitting the original single run into three
# (30000 -> 31000 -> 30000), leaving the open 29000 run untouched (four runs
# total for this listing).
# LISTING_OBSRUN_B: base rows alternate 40000 (detail) -> 41000 (srp) -> 40000
# (carousel) — three runs. Phase 2 republishes the middle (srp) artifact_id
# with a later fetched_at and a price matching the first/third run (40000),
# which replaces the original 41000 row (same artifact_id+listing_id
# observation_id) and merges all three runs into one continuous open run.
# LISTING_OBSRUN_STABLE: single open run, never touched by phase 2 — proves an
# unaffected listing_id is left unchanged by another listing's incremental
# rebuild.
LISTING_OBSRUN_A = "L90"
ARTIFACT_OBSRUN_A1 = 900
ARTIFACT_OBSRUN_A2 = 901
ARTIFACT_OBSRUN_A3 = 902
ARTIFACT_OBSRUN_LATE_SPLIT = 903
LISTING_OBSRUN_B = "L91"
ARTIFACT_OBSRUN_B1 = 910
ARTIFACT_OBSRUN_B2 = 911
ARTIFACT_OBSRUN_B3 = 912
ARTIFACT_OBSRUN_B2_CORRECTED_FETCHED_AT = _ts(2026, 7, 26, 6, 1)
LISTING_OBSRUN_STABLE = "L92"
ARTIFACT_OBSRUN_STABLE = 920

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
        # base row for the phase-2 true-correction scenario (see
        # VIN_OBSFP_CORRECTION above) — same fetched_at as the phase-2 row,
        # only written_at differs.
        _obs_row(VIN_OBSFP_CORRECTION, listing_id=LISTING_OBSFP_CORRECTION,
                 artifact_id=ARTIFACT_OBSFP_CORRECTION, source="srp",
                 fetched_at=OBSFP_CORRECTION_FETCHED_AT, price=OBSFP_CORRECTION_ORIGINAL_PRICE),
    ]


def _detail_fingerprint_incremental_base_rows() -> List[Dict[str, Any]]:
    """Base-phase rows for int_listing_state_fingerprints' real-build
    incremental test (see build_detail_fingerprint_incremental_rows below)."""
    return [
        _obs_row(VIN_FP_TARGET, listing_id=LISTING_FP_TARGET, artifact_id=ARTIFACT_FP_ANCHOR,
                 source="detail", fetched_at=FP_ANCHOR_FETCHED_AT, price=25000),
        _obs_row(VIN_FP_TARGET, listing_id=LISTING_FP_TARGET, artifact_id=ARTIFACT_FP_DUP,
                 source="detail", fetched_at=FP_DUP_BASE_FETCHED_AT, price=FP_DUP_BASE_PRICE),
    ]


def _listing_state_runs_base_rows() -> List[Dict[str, Any]]:
    """Base-phase rows for int_listing_state_runs' real-build incremental test
    (see build_listing_state_runs_incremental_rows below)."""
    return [
        # VIN_RUNS_A: fp_a (artifacts 600, 601) -> fp_b (artifact 602, open)
        _obs_row(VIN_RUNS_A, listing_id=LISTING_RUNS_A, artifact_id=ARTIFACT_RUNS_1,
                 source="detail", fetched_at=_ts(2026, 7, 26), price=30000),
        _obs_row(VIN_RUNS_A, listing_id=LISTING_RUNS_A, artifact_id=ARTIFACT_RUNS_2,
                 source="detail", fetched_at=_ts(2026, 7, 26, 6), price=30000),
        _obs_row(VIN_RUNS_A, listing_id=LISTING_RUNS_A, artifact_id=ARTIFACT_RUNS_3,
                 source="detail", fetched_at=_ts(2026, 7, 27), price=29000),
        # VIN_RUNS_B: fp_m (610) -> fp_n (611) -> fp_m (612), three runs
        _obs_row(VIN_RUNS_B, listing_id=LISTING_RUNS_B, artifact_id=ARTIFACT_RUNS_M1,
                 source="detail", fetched_at=_ts(2026, 7, 26), price=40000),
        _obs_row(VIN_RUNS_B, listing_id=LISTING_RUNS_B, artifact_id=ARTIFACT_RUNS_M2,
                 source="detail", fetched_at=_ts(2026, 7, 26, 6), price=41000),
        _obs_row(VIN_RUNS_B, listing_id=LISTING_RUNS_B, artifact_id=ARTIFACT_RUNS_M3,
                 source="detail", fetched_at=_ts(2026, 7, 27), price=40000),
        # VIN_RUNS_STABLE: single open run, never touched by phase 2
        _obs_row(VIN_RUNS_STABLE, listing_id=LISTING_RUNS_STABLE, artifact_id=ARTIFACT_RUNS_STABLE,
                 source="detail", fetched_at=_ts(2026, 7, 26), price=20000),
    ]


def _scrape_volume_base_rows() -> List[Dict[str, Any]]:
    """Base-phase rows for mart_scrape_volume's real-build incremental test
    (see build_scrape_volume_incremental_rows below)."""
    return [
        _obs_row(VIN_SV_AFFECTED, listing_id=LISTING_SV_AFFECTED_1,
                 artifact_id=ARTIFACT_SV_AFFECTED_1, source="detail",
                 fetched_at=SV_AFFECTED_HOUR + timedelta(minutes=5)),
        _obs_row(None, listing_id=LISTING_SV_STABLE, artifact_id=ARTIFACT_SV_STABLE,
                 source="carousel", fetched_at=SV_STABLE_HOUR),
    ]


def _latest_observation_base_rows() -> List[Dict[str, Any]]:
    """Base-phase rows for int_latest_observation's real-build incremental
    test (see build_latest_observation_incremental_rows below)."""
    return [
        _obs_row(VIN_LO_PRIORITY, listing_id=LISTING_LO_PRIORITY,
                 artifact_id=ARTIFACT_LO_PRIORITY_DETAIL, source="detail",
                 fetched_at=LO_PRIORITY_DETAIL_FETCHED_AT, make=LO_PRIORITY_DETAIL_MAKE),
        _obs_row(VIN_LO_DETAIL_UPGRADE, listing_id=LISTING_LO_DETAIL_UPGRADE,
                 artifact_id=ARTIFACT_LO_DETAIL_OLD, source="detail",
                 fetched_at=LO_DETAIL_OLD_FETCHED_AT, make=LO_DETAIL_OLD_MAKE),
        _obs_row(VIN_LO_STABLE, listing_id=LISTING_LO_STABLE,
                 artifact_id=ARTIFACT_LO_STABLE, source="detail",
                 fetched_at=LO_STABLE_FETCHED_AT, make=LO_STABLE_MAKE),
    ]


def _observation_runs_base_rows() -> List[Dict[str, Any]]:
    """Base-phase rows for int_listing_observation_runs' real-build incremental
    test (see build_observation_runs_incremental_rows below)."""
    return [
        # LISTING_OBSRUN_A: detail+srp share one observation-state (price
        # 30000) -> carousel opens a new, open observation-state (price 29000)
        _obs_row(_vin17("OBSRUNA"), listing_id=LISTING_OBSRUN_A, artifact_id=ARTIFACT_OBSRUN_A1,
                 source="detail", fetched_at=_ts(2026, 7, 26), price=30000, mileage=5000),
        _obs_row(None, listing_id=LISTING_OBSRUN_A, artifact_id=ARTIFACT_OBSRUN_A2,
                 source="srp", fetched_at=_ts(2026, 7, 26, 6), price=30000, mileage=5000),
        _obs_row(None, listing_id=LISTING_OBSRUN_A, artifact_id=ARTIFACT_OBSRUN_A3,
                 source="carousel", fetched_at=_ts(2026, 7, 27), price=29000, mileage=5000),
        # LISTING_OBSRUN_B: 40000 (detail) -> 41000 (srp) -> 40000 (carousel),
        # three observation-state runs
        _obs_row(_vin17("OBSRUNB"), listing_id=LISTING_OBSRUN_B, artifact_id=ARTIFACT_OBSRUN_B1,
                 source="detail", fetched_at=_ts(2026, 7, 26), price=40000, mileage=6000),
        _obs_row(None, listing_id=LISTING_OBSRUN_B, artifact_id=ARTIFACT_OBSRUN_B2,
                 source="srp", fetched_at=_ts(2026, 7, 26, 6), price=41000, mileage=6000),
        _obs_row(None, listing_id=LISTING_OBSRUN_B, artifact_id=ARTIFACT_OBSRUN_B3,
                 source="carousel", fetched_at=_ts(2026, 7, 27), price=40000, mileage=6000),
        # LISTING_OBSRUN_STABLE: single open run, never touched by phase 2
        _obs_row(_vin17("OBSRUNC"), listing_id=LISTING_OBSRUN_STABLE,
                 artifact_id=ARTIFACT_OBSRUN_STABLE,
                 source="detail", fetched_at=_ts(2026, 7, 26), price=20000, mileage=1000),
    ]


def build_observation_runs_incremental_rows() -> List[Dict[str, Any]]:
    """Phase-2 rows for int_listing_observation_runs (and its upstream
    int_listing_observation_fingerprints), seeded via
    seed(phase="observation_runs_incremental") after the base phase has
    already been built once:

      * ARTIFACT_OBSRUN_LATE_SPLIT (LISTING_OBSRUN_A) — a late detail artifact
        landing between the base phase's two 30000-price artifacts with a
        distinct price (31000), splitting the original single run into three
        (30000 -> 31000 -> 30000), leaving the open 29000 run untouched (four
        runs total for this listing).
      * ARTIFACT_OBSRUN_B2 (LISTING_OBSRUN_B) — a correction of the base
        phase's middle (srp) artifact, republished with a later fetched_at and
        a price matching the first/third run (40000) — since this is the same
        artifact_id+listing_id observation_id, it replaces the original 41000
        row rather than adding a fourth, merging all three base-phase runs
        into a single continuous open run.

    LISTING_OBSRUN_STABLE is intentionally never touched here, to prove an
    unaffected listing_id is unchanged by this phase's incremental rebuild.
    """
    return [
        _obs_row(None, listing_id=LISTING_OBSRUN_A, artifact_id=ARTIFACT_OBSRUN_LATE_SPLIT,
                 source="detail", fetched_at=_ts(2026, 7, 26, 3), price=31000, mileage=5000),
        _obs_row(None, listing_id=LISTING_OBSRUN_B, artifact_id=ARTIFACT_OBSRUN_B2,
                 source="srp", fetched_at=ARTIFACT_OBSRUN_B2_CORRECTED_FETCHED_AT,
                 price=40000, mileage=6000),
    ]


def build_silver_rows() -> List[Dict[str, Any]]:
    """All silver_observations fixture rows (dbt-equivalence + selector/cohort)."""
    return (
        _dbt_equivalence_rows()
        + _selector_scenario_rows()
        + _observation_fingerprint_rows()
        + _detail_fingerprint_incremental_base_rows()
        + _listing_state_runs_base_rows()
        + _scrape_volume_base_rows()
        + _latest_observation_base_rows()
        + _observation_runs_base_rows()
    )


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
      * a newer fetch of ARTIFACT_SRP_MULTI/LISTING_SRP_MULTI_A (base phase
        price 20000) with a later fetched_at inside the lookback window and a
        different price — the model must replace the existing target row for
        that observation_id, not duplicate it. This is fetched_at-desc
        ordering doing the work: a real re-scrape with a new observation time;
      * a true reprocessing correction of ARTIFACT_OBSFP_CORRECTION/
        LISTING_OBSFP_CORRECTION: the SAME fetched_at as the base-phase row
        (OBSFP_CORRECTION_FETCHED_AT), re-landed here with a different price.
        Because fetched_at ties, only the written_at tiebreaker (this row's
        written_at is naturally later — see _seed_silver — since it's seeded
        after the base phase) picks the corrected row over the original.
    """
    return [
        _obs_row(VIN_OBSFP_LATE_ARRIVAL, listing_id=LISTING_OBSFP_LATE_ARRIVAL,
                 artifact_id=ARTIFACT_OBSFP_LATE_ARRIVAL, source="srp",
                 fetched_at=_ts(2026, 7, 26), price=22000),
        _obs_row(VIN_SRP_MULTI_A, listing_id=LISTING_SRP_MULTI_A, artifact_id=ARTIFACT_SRP_MULTI,
                 source="srp", fetched_at=_ts(2026, 7, 27), price=OBSFP_CORRECTED_PRICE),
        _obs_row(VIN_OBSFP_CORRECTION, listing_id=LISTING_OBSFP_CORRECTION,
                 artifact_id=ARTIFACT_OBSFP_CORRECTION, source="srp",
                 fetched_at=OBSFP_CORRECTION_FETCHED_AT, price=OBSFP_CORRECTION_FIXED_PRICE),
    ]


# Back-compat alias for the dbt-equivalence test, which imports the row builder
# by its original name.
def build_scenario_rows() -> List[Dict[str, Any]]:
    return build_silver_rows()


def build_detail_fingerprint_incremental_rows() -> List[Dict[str, Any]]:
    """Phase-2 rows for int_listing_state_fingerprints, seeded via
    seed(phase="detail_fingerprint_incremental") after the base phase has
    already been built once:

      * ARTIFACT_FP_LATE — never seen in the base phase, fetched_at inside
        the lookback window relative to the base phase's max fetched_at
        (2026-07-26, from ARTIFACT_FP_ANCHOR) — must appear after the
        incremental rebuild.
      * ARTIFACT_FP_DUP — a correction of the base-phase row, republished
        here with a later fetched_at and a different price. Since this
        model's row_number() dedupe has no written_at tiebreaker (only
        fetched_at desc, parsed_fingerprint), the corrected row must have a
        strictly later fetched_at than the original to win deterministically.
      * ARTIFACT_FP_RETRY — appears twice in this SAME phase-2 batch (an
        ingestion-retry shape), proving the model's row_number() dedupe
        collapses same-batch duplicates to the latest fetched_at, not just
        duplicates against the existing target row.
    """
    return [
        _obs_row(VIN_FP_TARGET, listing_id=LISTING_FP_TARGET, artifact_id=ARTIFACT_FP_LATE,
                 source="detail", fetched_at=FP_LATE_FETCHED_AT, price=23000),
        _obs_row(VIN_FP_TARGET, listing_id=LISTING_FP_TARGET, artifact_id=ARTIFACT_FP_DUP,
                 source="detail", fetched_at=FP_DUP_CORRECTED_FETCHED_AT,
                 price=FP_DUP_CORRECTED_PRICE),
        _obs_row(VIN_FP_TARGET, listing_id=LISTING_FP_TARGET, artifact_id=ARTIFACT_FP_RETRY,
                 source="detail", fetched_at=FP_RETRY_FETCHED_AT_EARLY, price=100),
        _obs_row(VIN_FP_TARGET, listing_id=LISTING_FP_TARGET, artifact_id=ARTIFACT_FP_RETRY,
                 source="detail", fetched_at=FP_RETRY_FETCHED_AT_LATE, price=FP_RETRY_LATE_PRICE),
    ]


def build_listing_state_runs_incremental_rows() -> List[Dict[str, Any]]:
    """Phase-2 rows for int_listing_state_runs (and its upstream
    int_listing_state_fingerprints), seeded via
    seed(phase="listing_state_runs_incremental") after the base phase has
    already been built once:

      * ARTIFACT_RUNS_LATE_SPLIT (VIN_RUNS_A) — a late artifact landing
        between the base phase's two fp_a artifacts with a distinct price/
        fingerprint, splitting the original single fp_a run into
        fp_a -> fp_c -> fp_a (three runs), leaving the open fp_b run
        untouched (four runs total for this VIN).
      * ARTIFACT_RUNS_M2 (VIN_RUNS_B) — a correction of the base phase's
        middle (fp_n) artifact, republished with a later fetched_at and a
        price matching fp_m — merges all three base-phase runs
        (fp_m -> fp_n -> fp_m) into a single continuous open run.

    VIN_RUNS_STABLE is intentionally never touched here, to prove unaffected
    VINs are unchanged by this phase's incremental rebuild.
    """
    return [
        _obs_row(VIN_RUNS_A, listing_id=LISTING_RUNS_A, artifact_id=ARTIFACT_RUNS_LATE_SPLIT,
                 source="detail", fetched_at=_ts(2026, 7, 26, 3), price=31000),
        _obs_row(VIN_RUNS_B, listing_id=LISTING_RUNS_B, artifact_id=ARTIFACT_RUNS_M2,
                 source="detail", fetched_at=ARTIFACT_RUNS_M2_CORRECTED_FETCHED_AT, price=40000),
    ]


def build_scrape_volume_incremental_rows() -> List[Dict[str, Any]]:
    """Phase-2 rows for mart_scrape_volume, seeded via
    seed(phase="scrape_volume_incremental") after the base phase has already
    been built once:

      * ARTIFACT_SV_AFFECTED_2 — a second, invalid-vin detail row landing in
        the SAME hour as the base-phase ARTIFACT_SV_AFFECTED_1 row
        (SV_AFFECTED_HOUR), proving the affected-hour rebuild recomputes the
        WHOLE hour's aggregates, not just the new row.
      * ARTIFACT_SV_NEW — a brand new (hour, source) row in SV_NEW_HOUR,
        never seen in the base phase.

    SV_STABLE_HOUR is intentionally never touched here, to prove an hour
    outside the lookback window is left unchanged by the incremental rebuild.
    """
    return [
        _obs_row(None, listing_id=LISTING_SV_AFFECTED_2, artifact_id=ARTIFACT_SV_AFFECTED_2,
                 source="detail", fetched_at=SV_AFFECTED_HOUR + timedelta(minutes=45)),
        _obs_row(None, listing_id=LISTING_SV_NEW, artifact_id=ARTIFACT_SV_NEW,
                 source="srp", fetched_at=SV_NEW_HOUR + timedelta(minutes=10)),
    ]


def build_latest_observation_incremental_rows() -> List[Dict[str, Any]]:
    """Phase-2 rows for int_latest_observation, seeded via
    seed(phase="latest_observation_incremental") after the base phase has
    already been built once:

      * ARTIFACT_LO_PRIORITY_SRP (VIN_LO_PRIORITY) — a newer SRP row, inside
        the lookback window, for a VIN whose base-phase winner is an older
        detail row. The VIN becomes affected and its full history is reread,
        but the older detail row must still win: source priority is checked
        before recency.
      * ARTIFACT_LO_DETAIL_NEW (VIN_LO_DETAIL_UPGRADE) — a newer detail row,
        same source tier as the base-phase winner, which must win on recency.
      * ARTIFACT_LO_NEW (VIN_LO_NEW) — a brand-new VIN, absent from the base
        phase, whose only row lands inside the lookback window.

    VIN_LO_STABLE is intentionally never touched here, to prove an unaffected
    VIN is unchanged by this phase's incremental rebuild.
    """
    return [
        _obs_row(VIN_LO_PRIORITY, listing_id=LISTING_LO_PRIORITY,
                 artifact_id=ARTIFACT_LO_PRIORITY_SRP, source="srp",
                 fetched_at=LO_PRIORITY_SRP_FETCHED_AT, make=LO_PRIORITY_SRP_MAKE),
        _obs_row(VIN_LO_DETAIL_UPGRADE, listing_id=LISTING_LO_DETAIL_UPGRADE,
                 artifact_id=ARTIFACT_LO_DETAIL_NEW, source="detail",
                 fetched_at=LO_DETAIL_NEW_FETCHED_AT, make=LO_DETAIL_NEW_MAKE),
        _obs_row(VIN_LO_NEW, listing_id=LISTING_LO_NEW,
                 artifact_id=ARTIFACT_LO_NEW, source="detail",
                 fetched_at=LO_NEW_FETCHED_AT, make=LO_NEW_MAKE),
    ]


def build_price_history_incremental_rows() -> List[Dict[str, Any]]:
    """Phase-2 price_observation_events rows for int_price_history, seeded via
    seed(phase="price_history_incremental") after the base phase has already
    been built once:

      * PH_AFFECTED_LATE_EVENT — a late/corrected event landing chronologically
        before PH_AFFECTED_NEW_EVENT but inside the lookback window, reordering
        VIN_PH_AFFECTED's drop/increase sequence.
      * PH_AFFECTED_NEW_EVENT — a brand new event inside the lookback window,
        making VIN_PH_AFFECTED "affected" on this incremental run.

    Both events force the model's affected-VIN replacement to reread
    VIN_PH_AFFECTED's COMPLETE history (base phase's two events plus these
    two), not just the new rows, to get price_drop_count/price_increase_count
    right. VIN_PH_STABLE is never referenced here, to prove it is unaffected.
    """
    specs = [
        (203, LISTING_PH_AFFECTED, VIN_PH_AFFECTED, 5001, 38000, PH_AFFECTED_LATE_EVENT),
        (204, LISTING_PH_AFFECTED, VIN_PH_AFFECTED, 5002, 42000, PH_AFFECTED_NEW_EVENT),
    ]
    return [
        dict(event_id=eid, listing_id=lid, vin=vin, artifact_id=aid, price=price,
             make="Honda", model="Civic", event_type="upserted", source="detail",
             event_at=event_at)
        for (eid, lid, vin, aid, price, event_at) in specs
    ]


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
        # int_price_history real-build incremental test (Plan 123 replacement
        # for test_price_history_incremental.py): VIN_PH_AFFECTED's base-phase
        # history, far outside any future lookback window, plus a stable
        # control VIN never touched by the phase-2 seed.
        (200, LISTING_PH_AFFECTED, VIN_PH_AFFECTED, 5000, 40000, PH_AFFECTED_EVENT_1),
        (201, LISTING_PH_AFFECTED, VIN_PH_AFFECTED, 5000, 39000, PH_AFFECTED_EVENT_2),
        (202, LISTING_PH_STABLE, VIN_PH_STABLE, 5003, 15000, PH_STABLE_EVENT),
    ]
    return [
        dict(event_id=eid, listing_id=lid, vin=vin, artifact_id=aid, price=price,
             make="Honda", model="Civic", event_type="upserted", source="detail",
             event_at=event_at)
        for (eid, lid, vin, aid, price, event_at) in specs
    ]


def build_cooldown_event_rows() -> List[Dict[str, Any]]:
    """cooldown_blocked / cooldown_incremented / cooldown_bucket_{3_4,5_10,11_plus}
    plus a full blocked→cleared lifecycle (L23).

    The 'cleared' row is load-bearing for CI: it exercises the event_type
    accepted_values domain (['blocked','incremented','cleared']) and the
    mart_cooldown_cohorts drop-out (a listing whose latest event is 'cleared'
    is excluded from the backlog). Without it, CI's dbt build never sees a
    'cleared' value and the accepted_values test can't catch a missing entry.
    """
    specs = [
        (1, "L1", 1, "blocked", _ts(2026, 7, 1)),
        (2, "L1", 2, "blocked", _ts(2026, 7, 2)),
        (3, "L5", 1, "blocked", _ts(2026, 7, 1)),
        (4, "L20", 3, "blocked", _ts(2026, 7, 1)),
        (5, "L21", 7, "blocked", _ts(2026, 7, 1)),
        (6, "L22", 15, "blocked", _ts(2026, 7, 1)),
        # Full lifecycle: blocked, then resolved by a successful scrape.
        (7, "L23", 1, "blocked", _ts(2026, 7, 1)),
        (8, "L23", 1, "cleared", _ts(2026, 7, 3)),
    ]
    return [
        dict(event_id=eid, listing_id=lid, event_type=event_type,
             num_of_attempts=attempts, event_at=event_at)
        for (eid, lid, attempts, event_type, event_at) in specs
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


def _seed_ops_table(
    schema: pa.Schema, rows: List[Dict[str, Any]], prefix: str,
    basename_prefix: str = "lake_snapshot_fixture",
) -> str:
    for row in rows:
        row["year"] = _RESERVED_YEAR
        row["month"] = _RESERVED_MONTH
    return _write_dataset(
        schema, rows, prefix, _OPS_PARTITION_COLS, basename_prefix=basename_prefix,
    )


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
# The remaining phases are each a second wave for one real-build incremental
# test, seeded only after the base phase has already been built once, and
# written under distinct filenames so they land alongside (not over) the base
# phase's (and each other's) files:
#   * "observation_fingerprint_incremental" — Plan 123 Phase 2b,
#     int_listing_observation_fingerprints (silver-only).
#   * "detail_fingerprint_incremental" — int_listing_state_fingerprints
#     (silver-only).
#   * "price_history_incremental" — int_price_history (ops price events only).
#   * "listing_state_runs_incremental" — int_listing_state_runs, via new
#     silver rows that also feed its upstream int_listing_state_fingerprints
#     (silver-only).
#   * "scrape_volume_incremental" — Plan 123 Phase 5, mart_scrape_volume
#     (silver-only).
#   * "latest_observation_incremental" — Plan 123 Phase 5,
#     int_latest_observation (silver-only).
#   * "observation_runs_incremental" — Plan 123 final modeling correction,
#     int_listing_observation_runs (silver-only, via its upstream
#     int_listing_observation_fingerprints).
PHASES = (
    "base",
    "observation_fingerprint_incremental",
    "detail_fingerprint_incremental",
    "price_history_incremental",
    "listing_state_runs_incremental",
    "scrape_volume_incremental",
    "latest_observation_incremental",
    "observation_runs_incremental",
)


def seed(phase: str = "base") -> List[str]:
    """Upload fixture data for the given phase. Returns the written keys."""
    if phase not in PHASES:
        raise ValueError(f"unknown phase {phase!r}; expected one of {PHASES}")
    ensure_bucket()
    if phase == "base":
        return [_seed_silver(build_silver_rows())] + _seed_ops()
    if phase == "observation_fingerprint_incremental":
        return [
            _seed_silver(
                build_observation_fingerprint_incremental_rows(),
                basename_prefix="lake_snapshot_fixture_obsfp_incremental",
            )
        ]
    if phase == "detail_fingerprint_incremental":
        return [
            _seed_silver(
                build_detail_fingerprint_incremental_rows(),
                basename_prefix="lake_snapshot_fixture_fp_incremental",
            )
        ]
    if phase == "price_history_incremental":
        return [
            _seed_ops_table(
                _PRICE_SCHEMA, build_price_history_incremental_rows(),
                "ops_normalized/price_observation_events",
                basename_prefix="lake_snapshot_fixture_ph_incremental",
            )
        ]
    if phase == "listing_state_runs_incremental":
        return [
            _seed_silver(
                build_listing_state_runs_incremental_rows(),
                basename_prefix="lake_snapshot_fixture_runs_incremental",
            )
        ]
    if phase == "scrape_volume_incremental":
        return [
            _seed_silver(
                build_scrape_volume_incremental_rows(),
                basename_prefix="lake_snapshot_fixture_sv_incremental",
            )
        ]
    if phase == "latest_observation_incremental":
        return [
            _seed_silver(
                build_latest_observation_incremental_rows(),
                basename_prefix="lake_snapshot_fixture_lo_incremental",
            )
        ]
    return [
        _seed_silver(
            build_observation_runs_incremental_rows(),
            basename_prefix="lake_snapshot_fixture_obsrun_incremental",
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
