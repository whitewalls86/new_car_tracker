"""
Plan 125 Gate C: unit tests for the scale-reproduction harness
(scripts/lakehouse_scale_harness.py).

No Docker, no Spark, no MinIO, no production credentials -- the harness keeps
every pyspark/boto3/dbt import inside the function that needs it, so the
config, guard, probe-matrix, and evidence layers are all importable and
testable in the plain `unit-tests` CI job.

What these tests are actually protecting, in priority order:

  1. The isolation guard. The harness writes multi-million-row synthetic rows
     into `<bucket>/silver_normalized/observations/`, which is exactly where
     real silver lives. A regression that let --bucket default to, or accept,
     `bronze` would not fail loudly -- it would overwrite the lake.
  2. The synthetic schema's coverage of the failing model. The harness's whole
     purpose is reproducing a failure in the 28-field fingerprint. If the
     generator stopped emitting a column that model hashes, the run would
     still "pass" -- while no longer reproducing anything. That is the silent
     failure mode this file exists for, so it is asserted against the real
     model SQL rather than a hardcoded list.
  3. Evidence integrity: no credentials in a bundle that is meant to be
     committed into the plan doc, and errors captured rather than raised.
"""
import argparse
import json
import re
from pathlib import Path

import pytest

from scripts.lakehouse_scale_harness import (
    DEFAULT_HARNESS_BUCKET,
    HASHED_NUMERIC_BYTES,
    HASHED_STRING_FIELDS,
    OBSERVATIONS_PREFIX,
    PRICE_EVENTS_PREFIX,
    PRODUCTION_BUCKET,
    PRODUCTION_PROFILE_PROVENANCE,
    SNAPSHOT_PROFILE_PROVENANCE,
    HarnessError,
    SparkSizing,
    StringWidths,
    assert_isolated_bucket,
    assert_isolated_path,
    bucket_and_prefix,
    build_parser,
    dataset_profile_sql,
    evidence_bundle,
    format_profile_summary,
    harness_spark_conf,
    observations_expr,
    parse_string_field_stats,
    price_events_expr,
    probe_cases,
    redact_conf,
    row_bytes_expr,
    run_step,
    sizing_from_args,
    write_evidence,
)
from shared.iceberg_catalog import CATALOG_NAME

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = REPO_ROOT / "dbt" / "models" / "intermediate"
SOURCES_YML = REPO_ROOT / "dbt" / "models" / "sources.yml"


@pytest.fixture
def catalog_env(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://lakekeeper:8181/catalog")
    monkeypatch.setenv("MINIO_ROOT_USER", "cartracker")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "not-a-real-password")
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")


class TestIsolationGuard:
    def test_rejects_the_production_bucket(self):
        """The guard that stops the harness overwriting real silver."""
        with pytest.raises(HarnessError, match=PRODUCTION_BUCKET):
            assert_isolated_bucket(PRODUCTION_BUCKET)

    def test_rejects_empty_bucket(self):
        with pytest.raises(HarnessError):
            assert_isolated_bucket("")

    def test_accepts_the_default_harness_bucket(self):
        assert_isolated_bucket(DEFAULT_HARNESS_BUCKET)

    def test_default_bucket_is_not_production(self):
        assert DEFAULT_HARNESS_BUCKET != PRODUCTION_BUCKET

    def test_spark_conf_refuses_production_bucket(self, catalog_env):
        """The guard has to sit on the conf builder too, not only the CLI --
        every subcommand routes through here."""
        with pytest.raises(HarnessError):
            harness_spark_conf(SparkSizing(), PRODUCTION_BUCKET)


class TestSparkSizing:
    def test_sizing_is_explicit_in_the_conf(self, catalog_env):
        """The VM run that OOMed set none of these. Acceptance item 4 is about
        bounded, EXPLICIT sizing, so when a value IS given it has to actually
        reach the session config rather than be silently dropped."""
        conf = harness_spark_conf(
            SparkSizing(driver_memory="7g", shuffle_partitions=64), DEFAULT_HARNESS_BUCKET
        )

        assert conf["spark.driver.memory"] == "7g"
        assert conf["spark.sql.shuffle.partitions"] == "64"

    def test_pins_master_rather_than_local_star(self, catalog_env):
        """local[*] takes every core on the host, which makes a dev-box timing
        incomparable with the VM's. Reproduction evidence is worthless if the
        parallelism silently differs per machine."""
        conf = harness_spark_conf(SparkSizing(), DEFAULT_HARNESS_BUCKET)

        assert conf["spark.master"].startswith("local[")
        assert conf["spark.master"] != "local[*]"

    def test_inherits_the_production_catalog_config(self, catalog_env):
        """The point of reproducing here is that the session matches what
        run_dbt_spark builds. If the harness forked the catalog wiring, a
        local pass would prove nothing about the VM."""
        conf = harness_spark_conf(SparkSizing(), DEFAULT_HARNESS_BUCKET)

        assert conf["spark.sql.defaultCatalog"] == CATALOG_NAME
        assert conf["spark.sql.session.timeZone"] == "UTC"
        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.type"] == "rest"

    def test_unset_sizing_emits_no_key_at_all(self, catalog_env):
        """Reproducing the VM means setting NOTHING, which is not the same as
        passing Spark's documented default. If the harness quietly wrote '1g'
        for an unset driver memory, every run would be testing an assumption
        about the default rather than the default itself."""
        conf = harness_spark_conf(
            SparkSizing(driver_memory=None, master=None), DEFAULT_HARNESS_BUCKET
        )

        assert "spark.driver.memory" not in conf
        assert "spark.master" not in conf

    def test_sizing_from_args_maps_the_unset_sentinel(self):
        args = argparse.Namespace(
            driver_memory="unset", master="unset", shuffle_partitions=32
        )

        sizing = sizing_from_args(args)

        assert sizing.driver_memory is None
        assert sizing.master is None

    def test_sizing_from_args_passes_real_values_through(self):
        args = argparse.Namespace(
            driver_memory="2g", master="local[8]", shuffle_partitions=16
        )

        sizing = sizing_from_args(args)

        assert sizing.driver_memory == "2g"
        assert sizing.master == "local[8]"
        assert sizing.shuffle_partitions == 16

    def test_extra_overrides_win(self, catalog_env):
        conf = harness_spark_conf(
            SparkSizing(), DEFAULT_HARNESS_BUCKET, extra={"spark.driver.memory": "2g"}
        )

        assert conf["spark.driver.memory"] == "2g"


class TestProbeMatrix:
    def test_case_names_are_unique(self):
        names = [c.name for c in probe_cases()]

        assert len(names) == len(set(names))

    def test_every_case_states_its_hypothesis(self):
        """A probe result table is an argument, not a log. A case with no
        stated hypothesis cannot be read as evidence for anything."""
        for case in probe_cases():
            assert len(case.hypothesis) > 40, case.name

    def test_every_case_queries_the_parquet_path_form(self):
        """Each case must actually exercise `parquet.`<path>`` -- the exact
        relation syntax that failed on the VM. A case that lost the reference
        would silently pass and be counted as an elimination."""
        for case in probe_cases():
            statements = case.statements("s3a://b/p", "cat.ns")

            assert statements, case.name
            assert any("parquet.`s3a://b/p`" in s for s in statements), case.name

    def test_matrix_isolates_default_catalog_and_run_sql_on_files(self):
        """These two are the config variables that can produce this error
        class; dropping either from the matrix would leave a real cause
        untested while the run still looked thorough."""
        overrides = {k for c in probe_cases() for k in c.conf_overrides}

        assert "spark.sql.defaultCatalog" in overrides
        assert "spark.sql.runSQLOnFiles" in overrides

    def test_matrix_covers_read_write_and_merge_shapes(self):
        """dbt-spark emits a CTAS on an incremental model's first run and a
        MERGE on later ones. A matrix testing only reads could not tell those
        apart."""
        statements = [
            s for c in probe_cases() for s in c.statements("s3a://b/p", "cat.ns")
        ]
        joined = " ".join(statements).lower()

        assert "using iceberg as" in joined
        assert "merge into" in joined
        assert "temporary view" in joined


class TestSyntheticSchema:
    """The generator has to emit every column the failing models read.

    Asserted against the model SQL itself, not a copy of the column list: a
    hardcoded list would drift with the model and the drift is exactly what
    would make a harness run stop reproducing anything while still passing.
    """

    @staticmethod
    def _generated_columns(exprs):
        return {e.rsplit(" AS ", 1)[1].strip().lower() for e in exprs}

    @staticmethod
    def _hashed_fields(model_name):
        """Pull the field names out of the model's md5(concat_ws(...)) hash."""
        sql = (MODEL_DIR / f"{model_name}.sql").read_text()
        body = sql[sql.index("md5(concat_ws(") :]
        # coalesce(<field>, '') and cast_to_string('<field>') spellings.
        fields = set(re.findall(r"coalesce\(\s*([a-z_0-9]+)\s*,", body))
        fields |= set(re.findall(r"cast_to_string\('([a-z_0-9]+)'\)", body))
        return fields

    # stg_observations renames three columns on the way through; the generator
    # emits the SOURCE spelling (sources.yml), the models read the renamed one.
    RENAMES = {"vehicle_trim": "trim", "model_year": "year", "vin17": "vin"}

    def test_generator_covers_the_28_field_fingerprint(self):
        """The widest hash in the project, and the model that OOMed. A missing
        column here means the reproduction is not reproducing that model."""
        generated = self._generated_columns(observations_expr(10, 5))
        required = self._hashed_fields("int_listing_observation_fingerprints")

        missing = {
            f for f in required if self.RENAMES.get(f, f) not in generated
        }

        assert not missing, f"generator omits fingerprint fields: {sorted(missing)}"

    def test_generator_covers_the_18_field_state_fingerprint(self):
        generated = self._generated_columns(observations_expr(10, 5))
        required = self._hashed_fields("int_listing_state_fingerprints")

        missing = {f for f in required if self.RENAMES.get(f, f) not in generated}

        assert not missing, f"generator omits state fields: {sorted(missing)}"

    def test_generator_emits_the_hive_partition_columns(self):
        """sources.yml declares source/obs_year/obs_month; Spark discovers
        them from the directory layout, so they must exist to partition by."""
        generated = self._generated_columns(observations_expr(10, 5))

        assert {"source", "obs_year", "obs_month", "obs_day"} <= generated

    def test_price_events_cover_stg_price_events(self):
        sql = (REPO_ROOT / "dbt/models/staging/stg_price_events.sql").read_text()
        generated = self._generated_columns(price_events_expr(10, 5))
        # The select list of stg_price_events, minus the upper(vin) rename.
        required = {"event_id", "listing_id", "vin", "price", "make", "model",
                    "artifact_id", "event_type", "source", "event_at"}

        assert required <= generated
        for field in required:
            assert field in sql

    @staticmethod
    def _expr_for(exprs, alias):
        return next(e for e in exprs if e.endswith(f"AS {alias}"))

    def test_artifact_fanout_makes_the_composite_window_key_repeat(self):
        """The regression this file previously FAILED to catch.

        `int_listing_observation_fingerprints` ranks within
        `partition by artifact_id, listing_id`. The original generator emitted
        `id AS artifact_id`, so every window partition was size 1, the
        row_number() dedupe was a no-op, and Iceberg's MERGE cardinality check
        never met its precondition -- while the old version of this test
        asserted only that listing_id repeated, and passed.

        Assert on the artifact key itself: with fan-out, artifact_id must be a
        DIVISION of the row id, not the id.
        """
        exprs = observations_expr(
            rows=1000, distinct_vins=100, listings_per_artifact=8
        )
        artifact = self._expr_for(exprs, "artifact_id")

        assert artifact != "id AS artifact_id"
        assert "/ 8" in artifact

    def test_fanout_of_one_is_the_degenerate_case(self):
        """Kept reachable on purpose -- it is the control for a fan-out run --
        but it must be an explicit choice, not the shape you get by accident."""
        exprs = observations_expr(rows=10, distinct_vins=5, listings_per_artifact=1)

        assert "/ 1" in self._expr_for(exprs, "artifact_id")

    def test_listing_id_still_varies_within_one_artifact(self):
        """Fan-out is only meaningful if the listings inside an artifact
        DIFFER; identical rows would collapse to a duplicate-key case instead
        of the many-listings-per-artifact case being modelled."""
        exprs = observations_expr(
            rows=1000, distinct_vins=100, listings_per_artifact=8
        )

        assert "% 100" in self._expr_for(exprs, "listing_id")

    def test_duplicate_modulus_folds_rows_onto_their_predecessor(self):
        """The reprocessing-correction case: same (artifact_id, listing_id),
        same fetched_at, later written_at. Without it the dedupe's
        `written_at desc` tiebreak is never exercised."""
        exprs = observations_expr(
            rows=1000, distinct_vins=100, listings_per_artifact=4, duplicate_modulus=10
        )

        for alias in ("artifact_id", "listing_id", "fetched_at"):
            assert "id - 1" in self._expr_for(exprs, alias), alias
        # written_at must NOT fold, or the correction would be indistinguishable
        # from the original and the tiebreak still would not be exercised.
        assert "id - 1" not in self._expr_for(exprs, "written_at")

    def test_duplicates_are_off_unless_asked_for(self):
        exprs = observations_expr(rows=10, distinct_vins=5)

        assert "id - 1" not in self._expr_for(exprs, "artifact_id")

    def test_string_width_profiles_actually_widen_the_hashed_fields(self):
        """The 28-field hash is string-bound, so width is a first-class knob.
        A profile that did not reach the SQL would make a 'wide' run a
        relabelled narrow one."""
        narrow = observations_expr(
            rows=10, distinct_vins=5, widths=StringWidths.profile("narrow")
        )
        wide = observations_expr(
            rows=10, distinct_vins=5, widths=StringWidths.profile("wide")
        )

        assert ", 1, 24) AS body" in self._expr_for(narrow, "body")
        assert ", 1, 512) AS body" in self._expr_for(wide, "body")

    def test_wide_strings_are_high_entropy_not_constant_padding(self):
        """Constant padding would compress away in Parquet, so files would
        stay small while rows grew -- making bytes-per-row on disk a lie."""
        exprs = observations_expr(
            rows=10, distinct_vins=5, widths=StringWidths.profile("wide")
        )

        assert "md5(" in self._expr_for(exprs, "body")

    def test_unknown_width_profile_is_rejected(self):
        with pytest.raises(HarnessError, match="narrow/snapshot/wide/extreme"):
            StringWidths.profile("enormous")

    def test_source_prefixes_match_sources_yml(self):
        """The generator writes where sources.yml reads. If either moved, the
        harness would generate into one prefix and dbt would read an empty
        other one -- and an empty source is a PASS, not an error."""
        sources = SOURCES_YML.read_text()

        assert OBSERVATIONS_PREFIX in sources
        assert PRICE_EVENTS_PREFIX in sources


class TestSnapshotProfileIsolation:
    """describe-dataset reads; --path is free-form, so the bucket flag alone
    does not constrain it. These guard the read path specifically."""

    def test_path_in_the_production_bucket_is_refused(self):
        with pytest.raises(HarnessError, match=PRODUCTION_BUCKET):
            assert_isolated_path(f"s3a://{PRODUCTION_BUCKET}/silver_normalized/observations")

    def test_plain_s3_scheme_is_also_checked(self):
        """s3:// and s3a:// address the same object store; guarding only one
        would leave the other as a way in."""
        with pytest.raises(HarnessError, match=PRODUCTION_BUCKET):
            assert_isolated_path(f"s3://{PRODUCTION_BUCKET}/silver_normalized")

    def test_path_without_a_parseable_bucket_is_refused(self):
        """A local filesystem path names no bucket, so the guard cannot verify
        it. Refusing beats assuming it is safe."""
        with pytest.raises(HarnessError, match="cannot verify"):
            assert_isolated_path("/mnt/data/observations")

    def test_isolated_snapshot_bucket_is_allowed(self):
        assert_isolated_path("s3a://snapshot-profile/silver_normalized/observations")

    def test_storage_stats_follow_the_path_not_the_bucket_flag(self):
        """The two diverge exactly when profiling production.

        `--bucket scale-harness --path s3a://bronze/...` is the intended
        production-read invocation: the flag stays isolated (it governs
        WRITES) while the path names bronze. Sizing storage off the flag
        listed the isolated bucket with an empty prefix and divided ITS bytes
        by production's row count -- a fabricated bytes/row that looks
        entirely normal. Nothing in the output would have flagged it.
        """
        bucket, prefix = bucket_and_prefix(
            "s3a://bronze/silver_normalized/observations"
        )

        assert bucket == "bronze"
        assert prefix == "silver_normalized/observations"

    def test_bucket_and_prefix_rejects_a_non_s3_uri(self):
        with pytest.raises(HarnessError):
            bucket_and_prefix("/mnt/data/observations")

    def test_production_read_is_opt_in_and_off_by_default(self):
        """The escape hatch must never be the default.

        --allow-production-read exists so a deliberate, read-only VM
        measurement is possible; the guard it relaxes is about PROVENANCE, so
        the flag's job is to force the caller to say out loud that the numbers
        came from production. A default of True would silently un-label every
        profile ever taken.
        """
        args = build_parser().parse_args(["describe-dataset"])

        assert args.allow_production_read is False

    def test_production_read_flag_does_not_widen_the_write_guard(self):
        """Relaxing the READ guard must not make the production bucket a legal
        write target -- the generator writes into silver_normalized/, so that
        would be data loss rather than a mislabelled bundle."""
        args = build_parser().parse_args(
            ["describe-dataset", "--allow-production-read"]
        )

        assert args.allow_production_read is True
        with pytest.raises(HarnessError):
            assert_isolated_bucket(PRODUCTION_BUCKET)


class TestMeasuredSnapshotProfile:
    def test_snapshot_profile_carries_its_provenance(self):
        """This is the only profile that is measurement rather than guess, so
        a reader must be able to tell which snapshot and how many rows it came
        from -- otherwise it is indistinguishable from the invented ones."""
        assert SNAPSHOT_PROFILE_PROVENANCE["snapshot_id"]
        assert SNAPSHOT_PROFILE_PROVENANCE["rows_measured"] > 0
        assert "caveat" in str(SNAPSHOT_PROFILE_PROVENANCE).lower()

    def test_snapshot_profile_records_measured_null_rates(self):
        """trid and isa_context are ~90% null in the real snapshot. A profile
        that populated them densely would overstate row size -- which is
        exactly what `wide` did."""
        snap = StringWidths.profile("snapshot")

        assert snap.null_pct["trid"] > 50
        assert snap.null_pct["isa_context"] > 50

    def test_wide_profile_is_materially_heavier_than_measured_reality(self):
        """Guards the comparison the plan reports: the OOM-triggering
        treatment is several times wider per row than production. If someone
        'corrected' wide down to snapshot values, the reproduction would stop
        reproducing and this test should force that to be a deliberate act."""
        snap = StringWidths.profile("snapshot")
        wide = StringWidths.profile("wide")

        assert wide.body > 5 * snap.body
        assert wide.isa_context > 5 * snap.isa_context

    def test_null_rate_reaches_the_generated_sql(self):
        exprs = observations_expr(
            rows=100, distinct_vins=10, widths=StringWidths.profile("snapshot")
        )
        trid = next(e for e in exprs if e.endswith("AS trid"))

        assert "THEN NULL" in trid

    def test_zero_null_rate_emits_no_null_branch(self):
        """Narrow/wide have no measured null rates; they should generate plain
        values rather than a no-op CASE."""
        exprs = observations_expr(
            rows=100, distinct_vins=10, widths=StringWidths.profile("wide")
        )

        assert "THEN NULL" not in next(e for e in exprs if e.endswith("AS trid"))


class TestDatasetProfile:
    """describe-dataset is what would have caught the flattened-fan-out bug.

    Row and file counts were the only shape evidence the harness reported, and
    both looked correct while every window partition was a singleton. These
    stats are the ones that distinguish a faithful reproduction from a
    same-sized-but-differently-shaped one.
    """

    def test_profiles_the_real_window_partition_key(self):
        sql = dataset_profile_sql("s3a://b/p")["observation_key_groups"]

        assert "GROUP BY artifact_id, listing_id" in sql
        assert "groups_with_duplicates" in sql

    def test_profiles_artifact_fanout_distribution_not_just_a_count(self):
        """A mean would hide skew; the failing model's cost lives in the tail."""
        sql = dataset_profile_sql("s3a://b/p")["artifact_fanout"]

        assert "GROUP BY artifact_id" in sql
        for pct in ("0.5", "0.95", "0.99"):
            assert pct in sql
        assert "max(n)" in sql

    def test_production_profile_nulls_every_field_it_measured(self):
        """The gap that made the Finding 4 baseline 336 B/row against
        production's 269.

        _wide_string() modelled nulls for six fields; every other hashed
        string was emitted 100% populated. Fields that are 61-97% null in
        production (make, model, seller_customer_id, financing_type, ...)
        were therefore paying their full width on every row. A profile whose
        null map lists them is useless unless the generator applies it, so
        this asserts the generated SQL, not the dataclass.
        """
        widths = StringWidths.profile("production")
        sql = " ".join(
            observations_expr(1000, 100, listings_per_artifact=9, widths=widths)
        )

        for f in ("make", "model", "seller_customer_id", "financing_type",
                  "fuel_type", "stock_type", "body_style", "vin"):
            assert f"AS {f}" in sql
        # Each of those has a measured non-zero null rate, so each must have
        # been wrapped in a NULL-producing CASE.
        assert sql.count("THEN NULL ELSE") >= 15

    def test_production_profile_uses_the_measured_listing_id_width(self):
        """listing_id is 36-char UUIDs in production and was ~8 here -- ~28
        bytes understated on a field that is never null, so it was paid on
        every row."""
        assert StringWidths.profile("production").listing_id == 36

    def test_production_provenance_records_the_flat_fanout_caveat(self):
        """The profile models fan-out FLAT at 9 while production is bimodal
        (mean 6.43). That makes a run HEAVIER than production, not equal to
        it -- which is a fine direction for a floor test and a claim that
        must travel with the numbers."""
        caveat = PRODUCTION_PROFILE_PROVENANCE["caveat"]

        assert "HEAVIER" in caveat
        assert PRODUCTION_PROFILE_PROVENANCE["artifact_fanout"]["mean"] == 6.43

    def test_profiles_the_skew_tail_not_only_percentiles(self):
        """The gap that made the sweep's negative result weaker than it read.

        Percentiles describe the bulk of a distribution and say nothing about
        its largest member. With millions of artifacts, one partition of
        500k rows moves no percentile at all -- and it is one partition that
        must be sorted in the driver heap. So the profile must report the
        heaviest groups directly, ordered, or "p99=1" will keep being read as
        "no big partitions exist".
        """
        sql = dataset_profile_sql("s3a://b/p")

        for key, group_by in (
            ("artifact_skew_top", "GROUP BY artifact_id"),
            ("observation_key_skew_top", "GROUP BY artifact_id, listing_id"),
        ):
            assert group_by in sql[key]
            assert "ORDER BY n DESC" in sql[key]
            assert "LIMIT" in sql[key]

    def test_skew_queries_are_named_so_the_runner_keeps_all_rows(self):
        """Coupling worth asserting: cmd_describe_dataset dispatches multi-row
        handling on the `_skew_top` suffix. Renaming a skew query without
        renaming that branch would silently collapse the tail to one row --
        the exact failure the query exists to prevent, and a green one."""
        skew = [k for k in dataset_profile_sql("s3a://b/p") if "skew" in k]

        assert skew
        assert all(k.endswith("_skew_top") for k in skew)

    def test_profiles_every_hashed_string_field_width(self):
        sql = dataset_profile_sql("s3a://b/p")["string_fields"]

        for field in HASHED_STRING_FIELDS:
            assert f"length({field})" in sql
        assert "percentile_approx" in sql

    def test_profile_reads_the_given_path_as_parquet_files(self):
        """Must work against a real snapshot path, not only the harness
        bucket -- that comparison is the point of the subcommand."""
        for sql in dataset_profile_sql("s3a://real/silver").values():
            assert "parquet.`s3a://real/silver`" in sql

    def test_hashed_string_fields_match_the_models_hash(self):
        """The drift guard that matters most here.

        The profile's whole job is measuring what the 28-field hash carries.
        If a field is added to the model's hash and not to this list, sizing
        would be derived from an incomplete row -- silently, since every query
        would still succeed. Asserted against the model SQL, in silver's own
        column naming (stg_observations renames two columns on the way).
        """
        sql = (MODEL_DIR / "int_listing_observation_fingerprints.sql").read_text()
        body = sql[sql.index("md5(concat_ws(") :]
        hashed = set(re.findall(r"coalesce\(\s*([a-z_0-9]+)\s*,", body))
        hashed |= set(re.findall(r"cast_to_string\('([a-z_0-9]+)'\)", body))
        # Model spelling -> silver source spelling, and the numeric fields
        # which are counted by HASHED_NUMERIC_BYTES rather than measured.
        renames = {"vehicle_trim": "trim", "model_year": "year", "vin17": "vin"}
        hashed = {renames.get(f, f) for f in hashed}
        numeric = set(HASHED_NUMERIC_BYTES)

        missing = hashed - numeric - set(HASHED_STRING_FIELDS)

        assert not missing, f"hashed fields not measured by the profile: {sorted(missing)}"

    def test_row_bytes_counts_every_hashed_field(self):
        """Bytes-per-row is the number sizing will be argued from, so it must
        cover the whole hashed payload, strings and numerics alike."""
        expr = row_bytes_expr()

        for f in HASHED_STRING_FIELDS:
            assert f"length({f})" in expr
        assert str(sum(HASHED_NUMERIC_BYTES.values())) in expr

    def test_row_bytes_treats_null_as_zero_width(self):
        """The hash coalesces nulls to '', so a null field genuinely costs
        nothing. Counting it as anything else would overstate the row."""
        assert "coalesce(length(" in row_bytes_expr()

    def test_profile_reports_every_requested_percentile(self):
        """p50/p95/p99/max on each distribution -- a mean would hide the tail,
        and the tail is where a skewed key's memory cost lives."""
        sql = dataset_profile_sql("s3a://b/p")
        for name in ("artifact_fanout", "observation_key_groups", "row_bytes"):
            for pct in ("0.5", "0.95", "0.99"):
                assert pct in sql[name], f"{name} missing p{pct}"
            assert "max(" in sql[name], name

    def test_profile_reports_null_counts_per_string_field(self):
        sql = dataset_profile_sql("s3a://b/p")["string_fields"]

        for f in HASHED_STRING_FIELDS:
            assert f"{f}_nulls" in sql

    def test_profile_reports_source_distribution(self):
        """detail is one-listing-per-artifact; srp and carousel are where
        fan-out comes from, so the mix is needed to judge whether a measured
        fan-out is representative."""
        sql = dataset_profile_sql("s3a://b/p")["source_distribution"]

        assert "GROUP BY source" in sql

    def test_parse_string_field_stats_regroups_and_computes_null_pct(self):
        parsed = parse_string_field_stats(
            {"rows": 200, "body_p95": 47, "body_max": 55, "body_nulls": 50}
        )

        assert parsed["body"]["p95"] == 47
        assert parsed["body"]["null_pct"] == 25.0

    def test_parse_string_field_stats_survives_a_zero_row_dataset(self):
        """An empty source is a PASS, not an error -- so the formatter must not
        divide by zero on one."""
        parsed = parse_string_field_stats({"rows": 0, "body_nulls": 0})

        assert "null_pct" not in parsed["body"]


class TestProfileSummary:
    def test_summary_renders_the_headline_numbers(self, catalog_env):
        bundle = {
            "path": "s3a://snapshot-profile/silver_normalized/observations",
            "steps": [
                {"name": "row_count", "ok": True, "detail": {"stats": {"rows": 16847}}},
                {"name": "artifact_fanout", "ok": True,
                 "detail": {"stats": {"fanout_p50": 1, "fanout_p95": 5,
                                      "fanout_p99": 7, "fanout_max": 10,
                                      "artifacts": 11761}}},
            ],
        }

        text = format_profile_summary(bundle)

        assert "16847" in text
        assert "p95=5" in text

    def test_summary_prints_the_tail_beside_the_percentiles(self):
        """`p99=1, max=412880` only lands as one sentence. Splitting the two
        halves across sections is how a percentile summary talks someone out
        of noticing skew."""
        bundle = {
            "path": "s3a://bronze/silver_normalized/observations",
            "steps": [
                {"name": "observation_key_skew_top", "ok": True, "detail": {"stats": [
                    {"artifact_id": "hot", "listing_id": "L1", "n": 412880},
                    {"artifact_id": "b", "listing_id": "L2", "n": 12},
                ]}},
            ],
        }

        text = format_profile_summary(bundle)

        assert "412880" in text
        assert "hot" in text

    def test_summary_tolerates_failed_steps(self):
        """A partial profile is still worth reading; the formatter must not
        raise on the step that failed."""
        bundle = {"path": "p", "steps": [
            {"name": "row_count", "ok": False, "detail": {}, "error_message": "boom"}
        ]}

        assert "Dataset profile" in format_profile_summary(bundle)


class TestEvidence:
    def test_run_step_captures_failure_instead_of_raising(self):
        """The probe matrix needs every case attempted -- the passing cases
        are what bound the failing one. A harness that aborted on the first
        failure could only ever report one finding per run."""

        def boom():
            raise ValueError("kaboom")

        result = run_step("explode", boom)

        assert result.ok is False
        assert result.error_class == "ValueError"
        assert "kaboom" in result.error_message
        assert "kaboom" in result.traceback_text

    def test_run_step_records_success_detail(self):
        result = run_step("fine", lambda: {"rows": 5})

        assert result.ok is True
        assert result.detail == {"rows": 5}
        assert result.seconds >= 0

    def test_conf_is_redacted(self):
        conf = {
            "spark.sql.catalog.cartracker.s3.secret-access-key": "hunter2",
            "spark.hadoop.fs.s3a.secret.key": "hunter2",
            "spark.hadoop.fs.s3a.access.key": "cartracker",
            "spark.driver.memory": "4g",
        }

        redacted = redact_conf(conf)

        assert "hunter2" not in json.dumps(redacted)
        assert redacted["spark.driver.memory"] == "4g"

    def test_bundle_carries_no_credentials(self, catalog_env):
        """The bundle is meant to be pasted into the plan doc and committed."""
        conf = harness_spark_conf(SparkSizing(), DEFAULT_HARNESS_BUCKET)

        bundle = evidence_bundle("probe-parquet", conf, [run_step("x", lambda: {})])

        assert "not-a-real-password" not in json.dumps(bundle)

    def test_bundle_ok_is_false_when_any_step_failed(self, catalog_env):
        conf = harness_spark_conf(SparkSizing(), DEFAULT_HARNESS_BUCKET)

        def boom():
            raise RuntimeError("no")

        bundle = evidence_bundle(
            "probe-parquet", conf, [run_step("a", lambda: {}), run_step("b", boom)]
        )

        assert bundle["ok"] is False

    def test_write_evidence_creates_readable_json(self, catalog_env, tmp_path):
        conf = harness_spark_conf(SparkSizing(), DEFAULT_HARNESS_BUCKET)
        bundle = evidence_bundle("generate", conf, [run_step("x", lambda: {"n": 1})])

        path = write_evidence(tmp_path / "nested", "generate", bundle)

        assert json.loads(path.read_text())["command"] == "generate"
