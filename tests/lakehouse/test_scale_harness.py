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
    OBSERVATIONS_PREFIX,
    PRICE_EVENTS_PREFIX,
    PRODUCTION_BUCKET,
    HarnessError,
    SparkSizing,
    assert_isolated_bucket,
    evidence_bundle,
    harness_spark_conf,
    observations_expr,
    price_events_expr,
    probe_cases,
    redact_conf,
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

    def test_keys_repeat_so_window_partitions_are_not_singletons(self):
        """The failing models rank within (artifact_id, listing_id) windows.
        A unique-per-row key would make every partition size 1 and hide the
        very skew that makes the widest model expensive -- the run would pass
        while reproducing nothing."""
        exprs = observations_expr(rows=1000, distinct_vins=10)
        listing = next(e for e in exprs if e.endswith("AS listing_id"))

        assert "% 10" in listing

    def test_source_prefixes_match_sources_yml(self):
        """The generator writes where sources.yml reads. If either moved, the
        harness would generate into one prefix and dbt would read an empty
        other one -- and an empty source is a PASS, not an error."""
        sources = SOURCES_YML.read_text()

        assert OBSERVATIONS_PREFIX in sources
        assert PRICE_EVENTS_PREFIX in sources


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
