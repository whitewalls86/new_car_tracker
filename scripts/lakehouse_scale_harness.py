"""
Plan 125 Gate C: scale-reproduction harness for the two failures found by the
2026-07-17 VM shadow build.

    1. OutOfMemoryError: Java heap space   -- int_listing_observation_fingerprints
    2. UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY on `parquet`
                                           -- int_listing_state_fingerprints,
                                              int_price_history

Both were found on the production VM, against real silver/ops Parquet, in an
ad hoc container run with no explicit Spark or container sizing. Neither has
been root-caused. The point of this module is to move both off production
infrastructure and onto something cheap to iterate on, so the next fix is
backed by a reproduction rather than a hypothesis.

Three subcommands, deliberately independent -- the Parquet failure needs no
scale at all, and coupling it to a multi-GB data generation step would make
the cheapest evidence the slowest to get:

    probe-parquet   Minimize failure 2. Writes ONE tiny Parquet file, then runs
                    a matrix of SQL shapes / catalog states against it and
                    records which combination raises. No dbt, no scale.

    generate        Synthesize silver observations + price events at a chosen
                    row count into an ISOLATED MinIO bucket, laid out in the
                    same Hive partitioning production uses.

    run-model       Run dbt against the spark target over that synthetic data
                    with explicit, bounded Spark sizing, capturing config,
                    timings, peak driver heap, and full error output.

Isolation
---------
Nothing here reads production. `generate` and `run-model` address a bucket
named by --bucket (default `scale-harness`, never `bronze`), which is passed
to dbt as MINIO_BUCKET -- the same env var sources.yml already interpolates
into both external_location and spark_external_location. So the synthetic
data is picked up with NO change to sources.yml, and a harness run cannot
address the real silver prefix even by accident: assert_isolated_bucket()
rejects the production bucket name outright.

Evidence
--------
Every subcommand writes a JSON bundle under --evidence-dir (default
.cache/lakehouse_scale_harness/<run-id>/). The bundle is the deliverable --
"it OOMed again" is not evidence, "it OOMed at driver.memory=2g and passed at
4g, here is the config and the timing" is.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import time
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

from shared.iceberg_catalog import (
    CATALOG_NAME,
    WAREHOUSE_NAME,
    spark_conf_for_dbt_session,
)

# The production silver/bronze bucket. The harness must never address it --
# it holds the real lake, and this module writes multi-million-row synthetic
# junk. Guarded by assert_isolated_bucket(), not by convention.
PRODUCTION_BUCKET = "bronze"
DEFAULT_HARNESS_BUCKET = "scale-harness"

DEFAULT_EVIDENCE_DIR = ".cache/lakehouse_scale_harness"

# Where `generate` writes, and what sources.yml resolves to given MINIO_BUCKET.
# Kept in sync with dbt/models/sources.yml by test_source_paths_match_sources_yml.
OBSERVATIONS_PREFIX = "silver_normalized/observations"
PRICE_EVENTS_PREFIX = "ops_normalized/price_observation_events"

# The VM shadow build's real source scale (docs/plan_125_...md section 5).
VM_OBSERVATION_ROWS = 38_600_000


class HarnessError(RuntimeError):
    """Actionable harness misconfiguration."""


def assert_isolated_bucket(bucket: str) -> None:
    """Refuse to run against the production bucket.

    The harness generates synthetic rows into `<bucket>/silver_normalized/`,
    which is exactly where real silver lives. A typo'd --bucket would not
    fail, it would quietly corrupt the lake, so this is a hard guard rather
    than a documented convention.
    """
    if bucket == PRODUCTION_BUCKET:
        raise HarnessError(
            f"Refusing to run the scale harness against the production bucket "
            f"{PRODUCTION_BUCKET!r}. It writes synthetic rows into "
            f"{OBSERVATIONS_PREFIX}/, which would overwrite real silver. Use an "
            f"isolated bucket (default {DEFAULT_HARNESS_BUCKET!r})."
        )
    if not bucket:
        raise HarnessError("--bucket must be a non-empty bucket name.")


# ---------------------------------------------------------------------------
# Spark sizing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SparkSizing:
    """Explicit, bounded Spark sizing for a harness run.

    The VM run that OOMed set NONE of these. What an unset config actually
    resolves to is a question for jvm_runtime_facts() to answer per run, not
    something to assume here -- and the answer matters, because a JVM that
    raises `OutOfMemoryError: Java heap space` has hit a bound, whereas a
    genuinely unbounded one is SIGKILLed by the kernel and raises no Java
    exception at all.

    Acceptance item 4 is "establish whether the OOM is fixed by bounded,
    explicit sizing", so sizing has to be a first-class, recorded input rather
    than an ambient property of the container -- and it must be possible to
    run with it UNSET, or the VM's own configuration cannot be reproduced.
    """

    # None means "set nothing", reproducing the VM's ad hoc run, which set no
    # sizing at all. This is NOT the same as passing Spark's default value:
    # the whole question is what an unset config actually resolves to, and
    # hardcoding the documented default here would assume the answer.
    driver_memory: Optional[str] = "4g"
    # local[*] takes every core on the host, so a dev-box timing is not
    # comparable with the VM's -- but it is also what the VM itself ran, since
    # nothing in spark_conf_for_dbt_session() sets spark.master. Pinning it
    # makes runs comparable; None reproduces the VM's unbounded parallelism.
    # Both are needed: the pinned value is the control, None is the treatment.
    master: Optional[str] = "local[4]"
    shuffle_partitions: int = 32
    driver_max_result_size: str = "1g"

    def as_conf(self) -> Dict[str, str]:
        conf: Dict[str, str] = {
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.driver.maxResultSize": self.driver_max_result_size,
        }
        if self.driver_memory is not None:
            conf["spark.driver.memory"] = self.driver_memory
        if self.master is not None:
            conf["spark.master"] = self.master
        return conf


def sizing_from_args(args) -> SparkSizing:
    """Translate the CLI's 'unset' sentinel into "set nothing".

    A sentinel rather than a separate --no-driver-memory flag because the
    interesting experiment is a MATRIX over this one axis (unset / 1g / 4g),
    and a boolean flag plus a value flag can express contradictory states.
    """
    return SparkSizing(
        driver_memory=None if args.driver_memory == "unset" else args.driver_memory,
        master=None if getattr(args, "master", None) == "unset" else args.master,
        shuffle_partitions=args.shuffle_partitions,
    )


def harness_spark_conf(
    sizing: SparkSizing,
    bucket: str,
    extra: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Full Spark config for a harness session.

    Built ON TOP of spark_conf_for_dbt_session() rather than beside it: the
    whole value of reproducing here is that the session matches what
    run_dbt_spark builds in production-shaped runs. Diverging config would
    make a local pass meaningless. Sizing and the isolated bucket are the
    only deliberate deltas.
    """
    assert_isolated_bucket(bucket)
    conf = dict(spark_conf_for_dbt_session())
    conf.update(sizing.as_conf())
    if extra:
        conf.update(extra)
    return conf


def build_spark(conf: Dict[str, str], app_name: str):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Evidence capture
# ---------------------------------------------------------------------------


def container_limits() -> Dict[str, object]:
    """Read the cgroup memory limit the JVM is actually confined to.

    Recorded because the VM failure report could not say what the container
    was allowed -- "no explicit container memory limit" was inferred from the
    command, not measured. cgroup v2 first, then v1; both absent (e.g. a
    non-Linux host) is reported as such rather than guessed.
    """
    for path, name in (
        ("/sys/fs/cgroup/memory.max", "cgroup_v2"),
        ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "cgroup_v1"),
    ):
        try:
            raw = Path(path).read_text().strip()
        except OSError:
            continue
        return {"source": name, "raw": raw, "bytes": None if raw == "max" else int(raw)}
    return {"source": "unavailable", "raw": None, "bytes": None}


def jvm_runtime_facts(spark) -> Dict[str, object]:
    """What the driver JVM ACTUALLY got, as opposed to what was configured.

    This exists because "the VM took Spark's 1g default" was an inference from
    documentation, and the difference between an unset config and its
    documented default is precisely what the OOM investigation turns on. A
    JVM that throws `OutOfMemoryError: Java heap space` has hit its -Xmx; one
    with no bound gets SIGKILLed by the kernel instead and produces no Java
    exception at all. So the real -Xmx, and the core count Spark decided to
    use, both have to be measured rather than assumed.
    """
    facts: Dict[str, object] = {}
    try:
        runtime = spark._jvm.java.lang.Runtime.getRuntime()
        facts["jvm_max_heap_bytes"] = int(runtime.maxMemory())
        facts["jvm_available_processors"] = int(runtime.availableProcessors())
    except Exception as exc:  # noqa: BLE001
        facts["jvm_error"] = str(exc)[:200]
    for key in ("spark.driver.memory", "spark.master", "spark.sql.shuffle.partitions"):
        facts[key] = spark.conf.get(key, "<unset>")
    facts["default_parallelism"] = spark.sparkContext.defaultParallelism
    return facts


def peak_heap_bytes(spark) -> Optional[int]:
    """Peak driver JVM heap used, via the JVM's own memory bean.

    Best-effort: this reaches through py4j into the driver JVM, and a session
    that already died (the OOM case) has no JVM to ask. Returns None rather
    than raising, so evidence capture never masks the failure it is recording.
    """
    try:
        jvm = spark._jvm
        bean = jvm.java.lang.management.ManagementFactory.getMemoryMXBean()
        return int(bean.getHeapMemoryUsage().getUsed())
    except Exception:
        return None


@dataclass
class StepResult:
    """One recorded action: what ran, whether it raised, and what it raised."""

    name: str
    ok: bool
    seconds: float
    detail: Dict[str, object] = field(default_factory=dict)
    error_class: Optional[str] = None
    error_message: Optional[str] = None
    traceback_text: Optional[str] = None

    def as_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "ok": self.ok,
            "seconds": round(self.seconds, 3),
            "detail": self.detail,
            "error_class": self.error_class,
            "error_message": self.error_message,
            "traceback": self.traceback_text,
        }


def run_step(name: str, fn: Callable[[], Dict[str, object]]) -> StepResult:
    """Run one action, recording the FULL error rather than re-raising.

    A harness that aborts on the first failure can only ever report one
    finding per run; the probe matrix specifically needs every case attempted
    so the passing cases bound the failing one.
    """
    started = time.monotonic()
    try:
        detail = fn() or {}
    except BaseException as exc:  # noqa: BLE001 -- OOM is a BaseException path
        return StepResult(
            name=name,
            ok=False,
            seconds=time.monotonic() - started,
            error_class=type(exc).__name__,
            error_message=str(exc)[:4000],
            traceback_text="".join(traceback.format_exception(exc))[:8000],
        )
    return StepResult(name=name, ok=True, seconds=time.monotonic() - started, detail=detail)


def evidence_bundle(
    command: str,
    conf: Dict[str, str],
    steps: Sequence[StepResult],
    extra: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    """Assemble the JSON evidence document.

    Credentials are stripped: this file is meant to be pasted into the plan
    doc and committed, and the catalog config carries MinIO keys.
    """
    return {
        "command": command,
        "run_id": os.environ.get("HARNESS_RUN_ID", ""),
        "platform": {
            "machine": platform.machine(),
            "python": platform.python_version(),
            "system": platform.system(),
        },
        "container_limits": container_limits(),
        "spark_conf": redact_conf(conf),
        "steps": [s.as_dict() for s in steps],
        "ok": all(s.ok for s in steps),
        **(extra or {}),
    }


_SECRET_HINTS = ("secret", "password", "access-key", "access.key")


def redact_conf(conf: Dict[str, str]) -> Dict[str, str]:
    return {
        key: ("<redacted>" if any(h in key.lower() for h in _SECRET_HINTS) else value)
        for key, value in conf.items()
    }


def write_evidence(evidence_dir: Path, name: str, bundle: Dict[str, object]) -> Path:
    evidence_dir.mkdir(parents=True, exist_ok=True)
    path = evidence_dir / f"{name}.json"
    path.write_text(json.dumps(bundle, indent=2, default=str))
    return path


# ---------------------------------------------------------------------------
# probe-parquet: minimize UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProbeCase:
    """One SQL shape / catalog state to try against a known-good Parquet file.

    `hypothesis` names the variable the case isolates, so a result table reads
    as an argument rather than a log. Acceptance item 5 asks which of SQL
    shape / adapter compilation / relation syntax / source scale causes the
    error; each case moves exactly one of those.
    """

    name: str
    hypothesis: str
    # Statements to execute in order. The LAST one is the one under test;
    # earlier ones are setup (USE, CREATE TABLE, ...).
    statements: Callable[[str, str], List[str]]
    # Session state to set before the statements, restored afterwards.
    conf_overrides: Dict[str, str] = field(default_factory=dict)
    # Catalog to make current before running (None = leave as configured).
    use_catalog: Optional[str] = None


def probe_cases() -> List[ProbeCase]:
    """The matrix.

    Read as a bisection, not a checklist: the cases are ordered so that the
    first failure localizes the cause. If `direct_select` alone fails, the
    cause is catalog/session state and has nothing to do with dbt or model
    shape. If it passes and `ctas_iceberg` fails, the cause is the write
    wrapper. If everything here passes, the cause is in dbt's compilation and
    the probe has ruled the SQL out -- which is itself a finding.
    """

    def q(path: str) -> str:
        return f"parquet.`{path}`"

    return [
        ProbeCase(
            name="direct_select",
            hypothesis=(
                "Bare direct query on files, with the Iceberg REST catalog as "
                "spark.sql.defaultCatalog -- exactly the session run_dbt_spark "
                "builds. If this fails, the error is catalog resolution and "
                "every model reading Parquet is affected equally."
            ),
            statements=lambda path, ns: [f"SELECT count(*) AS n FROM {q(path)}"],
        ),
        ProbeCase(
            name="direct_select_default_catalog_spark",
            hypothesis=(
                "Same query with spark.sql.defaultCatalog left at the built-in "
                "spark_catalog. Isolates defaultCatalog as the variable: pass "
                "here + fail above means the Iceberg catalog is intercepting "
                "the `parquet` identifier."
            ),
            statements=lambda path, ns: [f"SELECT count(*) AS n FROM {q(path)}"],
            conf_overrides={"spark.sql.defaultCatalog": "spark_catalog"},
        ),
        ProbeCase(
            name="direct_select_current_catalog_iceberg",
            hypothesis=(
                "USE <iceberg catalog>.<namespace> first, making the Iceberg "
                "catalog CURRENT rather than merely default. Iceberg's "
                "SparkCatalog resolves a 2-part name against itself, so this "
                "is the most likely spelling of the VM failure."
            ),
            statements=lambda path, ns: [f"SELECT count(*) AS n FROM {q(path)}"],
            use_catalog=f"{CATALOG_NAME}.{WAREHOUSE_NAME}",
        ),
        ProbeCase(
            name="cte_wrapped",
            hypothesis=(
                "The reference inside a CTE, which is what an `ephemeral` "
                "staging model compiles to. Isolates SQL shape from the bare "
                "reference."
            ),
            statements=lambda path, ns: [
                f"WITH src AS (SELECT * FROM {q(path)}) SELECT count(*) AS n FROM src"
            ],
        ),
        ProbeCase(
            name="ctas_iceberg",
            hypothesis=(
                "CREATE TABLE ... USING iceberg AS SELECT from the file -- what "
                "dbt-spark emits for an incremental model's FIRST run. Isolates "
                "the write wrapper from the read."
            ),
            statements=lambda path, ns: [
                f"DROP TABLE IF EXISTS {ns}.probe_ctas",
                f"CREATE TABLE {ns}.probe_ctas USING iceberg AS "
                f"SELECT * FROM {q(path)}",
            ],
        ),
        ProbeCase(
            name="temp_view_then_select",
            hypothesis=(
                "A TEMPORARY view over the file, then a read of it. dbt-spark "
                "creates a temp view for the incremental tmp relation, and a "
                "view re-analyzes its body against its own catalog on read -- "
                "the documented Gate A failure mode for PERSISTED views."
            ),
            statements=lambda path, ns: [
                f"CREATE OR REPLACE TEMPORARY VIEW probe_tmp AS SELECT * FROM {q(path)}",
                "SELECT count(*) AS n FROM probe_tmp",
            ],
        ),
        ProbeCase(
            name="merge_using_file_subquery",
            hypothesis=(
                "MERGE INTO an Iceberg table USING a subquery over the file -- "
                "what dbt-spark emits for an incremental model's SUBSEQUENT "
                "runs. Isolates the merge path, which the first-run CTAS never "
                "exercises."
            ),
            statements=lambda path, ns: [
                f"DROP TABLE IF EXISTS {ns}.probe_merge",
                f"CREATE TABLE {ns}.probe_merge USING iceberg AS "
                f"SELECT * FROM {q(path)} WHERE 1=0",
                f"MERGE INTO {ns}.probe_merge AS t "
                f"USING (SELECT * FROM {q(path)}) AS s "
                "ON t.id = s.id "
                "WHEN MATCHED THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT *",
            ],
        ),
        ProbeCase(
            name="direct_select_run_sql_on_files_disabled",
            hypothesis=(
                "Direct query with spark.sql.runSQLOnFiles=false. Confirms "
                "which config governs the code path -- the error class only "
                "reachable through ResolveSQLOnFile should change or vanish."
            ),
            statements=lambda path, ns: [f"SELECT count(*) AS n FROM {q(path)}"],
            conf_overrides={"spark.sql.runSQLOnFiles": "false"},
        ),
    ]


def cmd_probe_oom_cascade(args: argparse.Namespace) -> int:
    """Test whether a driver OOM POISONS the shared session, so that later
    Parquet reads in the same run fail with UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY.

    Why this probe exists: probe-parquet showed every SQL shape reads Parquet
    fine, and a dbt run of both "failing" models passes on a first run. So the
    error is not shape, syntax, or compilation. What the VM run had that
    neither of those had is ORDER: dbt ran on one long-lived session, with
    threads=1, and `int_listing_observation_fingerprints` OOMed BEFORE
    `int_listing_state_fingerprints` and `int_price_history` were attempted.
    `int_latest_observation`, the one model that passed, ran BEFORE the OOM.

    That makes "failure 2 is a consequence of failure 1" the hypothesis this
    probe is built to confirm or kill. It runs the same direct query twice in
    one session -- once on a healthy driver, once after deliberately
    exhausting the driver heap -- and records both results. Deliberately small
    heap and small data: if the cascade is real, it needs neither production
    scale nor production infrastructure to show.
    """
    assert_isolated_bucket(args.bucket)
    ensure_bucket(args.bucket)

    # Small enough that a modest collect() exhausts it, so the probe is fast
    # and does not need 38.6M rows to make the driver die.
    sizing = SparkSizing(driver_memory=args.driver_memory)
    conf = harness_spark_conf(
        sizing,
        args.bucket,
        # Without this, a big collect() trips maxResultSize and raises a clean
        # SparkException instead of actually exhausting the heap -- which would
        # test the wrong thing.
        extra={"spark.driver.maxResultSize": "0"},
    )
    spark = build_spark(conf, "cartracker-oom-cascade-probe")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{WAREHOUSE_NAME}")

    steps: List[StepResult] = []
    fixture = run_step(
        "write_probe_fixture", lambda: {"path": _write_probe_fixture(spark, args.bucket)}
    )
    steps.append(fixture)
    if not fixture.ok:
        print("could not write probe fixture; aborting")
        return 1
    path = str(fixture.detail["path"])
    direct = f"SELECT count(*) AS n FROM parquet.`{path}`"

    steps.append(
        run_step(
            "direct_select_before_oom",
            lambda: {"n": spark.sql(direct).collect()[0]["n"], "sql": direct},
        )
    )

    def induce_oom() -> Dict[str, object]:
        # Collect a result far larger than the driver heap. This is the
        # closest small-scale analogue of what the 28-field hash over 38.6M
        # rows did to the VM driver.
        rows = spark.range(0, args.oom_rows).selectExpr(
            "id", f"repeat('x', {args.oom_string_width}) AS pad"
        )
        rows.collect()
        return {"unexpected": "collect() did not exhaust the driver heap"}

    steps.append(run_step("induce_driver_oom", induce_oom))

    steps.append(
        run_step(
            "direct_select_after_oom",
            lambda: {"n": spark.sql(direct).collect()[0]["n"], "sql": direct},
        )
    )
    # Read an Iceberg table too: if only the Parquet path breaks, the poisoning
    # is specific to datasource lookup rather than a wholly dead session.
    steps.append(
        run_step(
            "iceberg_ctas_after_oom",
            lambda: {
                "sql": spark.sql(
                    f"CREATE OR REPLACE TABLE {CATALOG_NAME}.{WAREHOUSE_NAME}"
                    f".probe_after_oom USING iceberg AS SELECT 1 AS x"
                )
                and "ok"
            },
        )
    )

    bundle = evidence_bundle(
        "probe-oom-cascade",
        conf,
        steps,
        extra={
            "hypothesis": (
                "A driver OutOfMemoryError leaves the shared SparkSession in a "
                "state where subsequent `parquet.`path`` direct queries fail with "
                "UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY, making the VM's failure "
                "2 a cascade from failure 1 rather than an independent defect."
            ),
            "oom_rows": args.oom_rows,
        },
    )
    out = write_evidence(Path(args.evidence_dir), "probe_oom_cascade", bundle)
    print("\nOOM cascade probe")
    for step in steps:
        status = "PASS" if step.ok else f"FAIL {step.error_class}"
        print(f"  {step.name:30s} {status}")
        if not step.ok:
            first = (step.error_message or "").splitlines()
            print(f"      {(first[0] if first else '')[:200]}")
    print(f"\nEvidence: {out}")
    return 0


def _write_probe_fixture(spark, bucket: str) -> str:
    """One tiny Parquet file, written the same way silver is: s3a, directory,
    Hive-partitioned. Scale is deliberately trivial -- if the error needs
    scale, that is a finding, and the probe passing here says so."""
    path = f"s3a://{bucket}/probe/observations"
    df = spark.range(0, 100).selectExpr(
        "id",
        "cast(id % 3 as int) as part",
        "concat('vin', cast(id as string)) as vin",
    )
    df.write.mode("overwrite").partitionBy("part").parquet(path)
    return path


def cmd_probe_parquet(args: argparse.Namespace) -> int:
    assert_isolated_bucket(args.bucket)
    ensure_bucket(args.bucket)

    sizing = SparkSizing(driver_memory=args.driver_memory)
    conf = harness_spark_conf(sizing, args.bucket)
    spark = build_spark(conf, "cartracker-parquet-probe")
    namespace = f"{CATALOG_NAME}.{WAREHOUSE_NAME}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    steps: List[StepResult] = []
    fixture = run_step(
        "write_probe_fixture", lambda: {"path": _write_probe_fixture(spark, args.bucket)}
    )
    steps.append(fixture)
    if not fixture.ok:
        bundle = evidence_bundle("probe-parquet", conf, steps)
        print(json.dumps(bundle, indent=2, default=str))
        write_evidence(Path(args.evidence_dir), "probe_parquet", bundle)
        return 1

    path = str(fixture.detail["path"])

    for case in probe_cases():
        steps.append(_run_probe_case(spark, case, path, namespace))

    bundle = evidence_bundle(
        "probe-parquet",
        conf,
        steps,
        extra={
            "hypotheses": {c.name: c.hypothesis for c in probe_cases()},
            "spark_version": spark.version,
        },
    )
    out = write_evidence(Path(args.evidence_dir), "probe_parquet", bundle)

    print(f"\nProbe matrix (fixture: {path})")
    for step in steps[1:]:
        status = "PASS" if step.ok else f"FAIL {step.error_class}"
        print(f"  {step.name:45s} {status}")
        if not step.ok:
            print(f"      {(step.error_message or '').splitlines()[0][:160]}")
    print(f"\nEvidence: {out}")
    # A probe that reproduces the failure has done its job -- exit 0 either
    # way, and let the bundle say what happened. Exiting nonzero on a
    # successful reproduction would make CI-style wrappers treat evidence as
    # breakage.
    return 0


def _run_probe_case(spark, case: ProbeCase, path: str, namespace: str) -> StepResult:
    original: Dict[str, Optional[str]] = {}

    def execute() -> Dict[str, object]:
        statements = case.statements(path, namespace)
        if case.use_catalog:
            spark.sql(f"USE {case.use_catalog}")
        for key, value in case.conf_overrides.items():
            original[key] = spark.conf.get(key, None)
            spark.conf.set(key, value)
        try:
            for statement in statements:
                spark.sql(statement).collect()
            return {"statements": statements}
        finally:
            for key, value in original.items():
                if value is None:
                    spark.conf.unset(key)
                else:
                    spark.conf.set(key, value)
            if case.use_catalog:
                spark.sql(f"USE {CATALOG_NAME}.{WAREHOUSE_NAME}")

    result = run_step(case.name, execute)
    result.detail.setdefault("statements", case.statements(path, namespace))
    result.detail["hypothesis"] = case.hypothesis
    return result


# ---------------------------------------------------------------------------
# generate: synthetic silver at scale
# ---------------------------------------------------------------------------


def ensure_bucket(bucket: str) -> None:
    """Create the isolated harness bucket if absent (idempotent)."""
    assert_isolated_bucket(bucket)
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)


# Shared between the observation and price-event projections -- the two must
# agree on make/model/source or a join between them would match nothing.
_SOURCE_EXPR = (
    "CASE cast(id % 3 as int) WHEN 0 THEN 'detail' WHEN 1 THEN 'srp' "
    "ELSE 'carousel' END AS source"
)
_MAKE_EXPR = (
    "CASE cast(id % 5 as int) WHEN 0 THEN 'Honda' WHEN 1 THEN 'Toyota' "
    "WHEN 2 THEN 'Ford' WHEN 3 THEN 'Subaru' ELSE 'Mazda' END AS make"
)
_MODEL_EXPR = (
    "CASE cast(id % 7 as int) WHEN 0 THEN 'Accord' WHEN 1 THEN 'Camry' "
    "WHEN 2 THEN 'F-150' WHEN 3 THEN 'Outback' WHEN 4 THEN 'CX-5' "
    "WHEN 5 THEN 'Civic' ELSE 'Corolla' END AS model"
)
_VIN_EXPR = "concat('1HGCM82633A', lpad(cast(id % {n} as string), 6, '0')) AS vin"
_EVENT_AT_EXPR = (
    "timestamp_millis(1700000000000 + cast(id % 2592000 as bigint) * 1000) AS {alias}"
)


def observations_expr(rows: int, distinct_vins: int) -> List[str]:
    """The synthetic silver observation projection, as Spark SQL expressions.

    Shape matters more than realism here. Two properties drive both failures
    and are therefore modelled deliberately:

      * every column the 28-field fingerprint hashes exists and is a non-null
        string/number of realistic width -- a table of nulls would compress to
        nothing and never reproduce a memory-bound failure;
      * vin17/listing_id have realistic REPEAT, because the failing models
        window over (artifact_id, listing_id) and rank within partitions. A
        unique-per-row key would make every window partition size 1 and hide
        exactly the skew that makes the widest model expensive.

    Kept as SQL expressions over spark.range so generation itself streams and
    never materializes the dataset in the driver.
    """
    return [
        "id AS artifact_id",
        f"concat('L', cast(id % {distinct_vins} as string)) AS listing_id",
        _VIN_EXPR.format(n=distinct_vins),
        "concat('https://www.cars.com/vehicledetail/', cast(id as string), '/') "
        "AS canonical_detail_url",
        _SOURCE_EXPR,
        "CASE WHEN id % 17 = 0 THEN 'unavailable' ELSE 'active' END AS listing_state",
        _EVENT_AT_EXPR.format(alias="fetched_at"),
        "timestamp_millis(1700000060000 + cast(id % 2592000 as bigint) * 1000) AS written_at",
        "cast(15000 + (id % 60000) as int) AS price",
        _MAKE_EXPR,
        _MODEL_EXPR,
        "concat('Trim-', cast(id % 11 as string)) AS trim",
        "cast(2018 + (id % 8) as smallint) AS year",
        "cast(id % 120000 as int) AS mileage",
        "cast(20000 + (id % 55000) as int) AS msrp",
        "CASE WHEN id % 2 = 0 THEN 'New' ELSE 'Used' END AS stock_type",
        "CASE cast(id % 4 as int) WHEN 0 THEN 'Gasoline' WHEN 1 THEN 'Hybrid' "
        "WHEN 2 THEN 'Electric' ELSE 'Diesel' END AS fuel_type",
        "CASE cast(id % 6 as int) WHEN 0 THEN 'Sedan' WHEN 1 THEN 'SUV' "
        "WHEN 2 THEN 'Truck' WHEN 3 THEN 'Coupe' WHEN 4 THEN 'Wagon' "
        "ELSE 'Van' END AS body_style",
        "concat('Dealer Number ', cast(id % 900 as string)) AS dealer_name",
        "lpad(cast(id % 99999 as string), 5, '0') AS dealer_zip",
        "concat('City', cast(id % 400 as string)) AS dealer_city",
        "CASE cast(id % 5 as int) WHEN 0 THEN 'CA' WHEN 1 THEN 'TX' "
        "WHEN 2 THEN 'NY' WHEN 3 THEN 'FL' ELSE 'WA' END AS dealer_state",
        "concat('cust-', cast(id % 900 as string)) AS customer_id",
        "concat('seller-', cast(id % 900 as string)) AS seller_id",
        "CASE WHEN id % 3 = 0 THEN 'cash' ELSE 'lease' END AS financing_type",
        "lpad(cast(id % 99999 as string), 5, '0') AS seller_zip",
        "concat('sc-', cast(id % 700 as string)) AS seller_customer_id",
        "cast(id % 50 as smallint) AS page_number",
        "cast(id % 30 as smallint) AS position_on_page",
        "concat('trid-', cast(id % 5000 as string)) AS trid",
        "concat('isa-', cast(id % 40 as string)) AS isa_context",
        "concat('body-', cast(id % 25 as string)) AS body",
        "CASE WHEN id % 2 = 0 THEN 'new' ELSE 'used' END AS condition",
        "cast(2024 + (id % 2) as int) AS obs_year",
        "cast(1 + (id % 12) as int) AS obs_month",
        "cast(1 + (id % 28) as int) AS obs_day",
    ]


def price_events_expr(rows: int, distinct_vins: int) -> List[str]:
    return [
        "id AS event_id",
        f"concat('L', cast(id % {distinct_vins} as string)) AS listing_id",
        _VIN_EXPR.format(n=distinct_vins),
        "cast(15000 + (id % 60000) as int) AS price",
        _MAKE_EXPR,
        _MODEL_EXPR,
        "id AS artifact_id",
        "CASE WHEN id % 23 = 0 THEN 'added' ELSE 'upserted' END AS event_type",
        _SOURCE_EXPR,
        _EVENT_AT_EXPR.format(alias="event_at"),
        "cast(2024 + (id % 2) as int) AS year",
        "cast(1 + (id % 12) as int) AS month",
    ]


def cmd_generate(args: argparse.Namespace) -> int:
    assert_isolated_bucket(args.bucket)
    ensure_bucket(args.bucket)

    sizing = sizing_from_args(args)
    conf = harness_spark_conf(sizing, args.bucket)
    spark = build_spark(conf, "cartracker-scale-generate")

    steps: List[StepResult] = []

    def write(prefix: str, rows: int, exprs: List[str], partitions: List[str]) -> Dict[str, object]:
        path = f"s3a://{args.bucket}/{prefix}"
        (
            spark.range(0, rows, numPartitions=args.write_partitions)
            .selectExpr(*exprs)
            .write.mode("overwrite")
            .partitionBy(*partitions)
            .parquet(path)
        )
        return {"path": path, "rows": rows}

    steps.append(
        run_step(
            "write_observations",
            lambda: write(
                OBSERVATIONS_PREFIX,
                args.rows,
                observations_expr(args.rows, args.distinct_vins),
                ["source", "obs_year", "obs_month"],
            ),
        )
    )
    steps.append(
        run_step(
            "write_price_events",
            lambda: write(
                PRICE_EVENTS_PREFIX,
                args.price_event_rows,
                price_events_expr(args.price_event_rows, args.distinct_vins),
                ["year", "month"],
            ),
        )
    )

    bundle = evidence_bundle(
        "generate",
        conf,
        steps,
        extra={"bucket": args.bucket, "peak_driver_heap_bytes": peak_heap_bytes(spark)},
    )
    out = write_evidence(Path(args.evidence_dir), "generate", bundle)
    for step in steps:
        print(f"  {step.name:24s} {'PASS' if step.ok else 'FAIL ' + str(step.error_class)} "
              f"({step.seconds:.1f}s) {step.detail}")
    print(f"Evidence: {out}")
    return 0 if all(s.ok for s in steps) else 1


# ---------------------------------------------------------------------------
# run-model: dbt against the synthetic data, bounded and instrumented
# ---------------------------------------------------------------------------


def cmd_run_model(args: argparse.Namespace) -> int:
    assert_isolated_bucket(args.bucket)
    # sources.yml interpolates MINIO_BUCKET into spark_external_location, so
    # this is the whole isolation mechanism -- set before dbt parses.
    os.environ["MINIO_BUCKET"] = args.bucket
    os.environ.setdefault("POSTGRES_URL", "postgresql://unused:unused@unused:5432/unused")

    sizing = sizing_from_args(args)
    conf = harness_spark_conf(sizing, args.bucket)
    spark = build_spark(conf, "cartracker-scale-run-model")
    jvm_facts = jvm_runtime_facts(spark)
    print(f"  driver JVM: {jvm_facts}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{WAREHOUSE_NAME}")

    dbt_args = list(args.dbt_args)
    if dbt_args and dbt_args[0] == "--":
        dbt_args = dbt_args[1:]
    if not dbt_args:
        raise HarnessError("no dbt command given, e.g. `run --select int_price_history`")

    def invoke() -> Dict[str, object]:
        from dbt.cli.main import dbtRunner

        result = dbtRunner().invoke(
            [*dbt_args, "--project-dir", "/app/dbt", "--profiles-dir", "/app/dbt",
             "--target", "spark"]
        )
        node_timings = []
        if getattr(result, "result", None) is not None:
            for node in getattr(result.result, "results", []) or []:
                node_timings.append(
                    {
                        "node": getattr(node.node, "name", "?"),
                        "status": str(node.status),
                        "seconds": round(getattr(node, "execution_time", 0.0), 2),
                        "message": (node.message or "")[:4000],
                    }
                )
        if not result.success:
            return {"dbt_success": False, "nodes": node_timings}
        return {"dbt_success": True, "nodes": node_timings}

    steps = [run_step("dbt_invoke", invoke)]

    # Iceberg verification is a separate step so a dbt PASS with nothing
    # written -- the Gate A trap run_dbt_spark exists to catch -- still shows
    # up as a failed step rather than an overall pass.
    if args.verify_tables:
        steps.append(
            run_step(
                "verify_iceberg",
                lambda: {"tables": _verify_tables(spark, args.verify_tables)},
            )
        )

    bundle = evidence_bundle(
        "run-model",
        conf,
        steps,
        extra={
            "dbt_args": dbt_args,
            "bucket": args.bucket,
            "peak_driver_heap_bytes": peak_heap_bytes(spark),
            "jvm_runtime_facts": jvm_facts,
        },
    )
    out = write_evidence(Path(args.evidence_dir), args.evidence_name, bundle)
    for step in steps:
        print(f"  {step.name:16s} {'PASS' if step.ok else 'FAIL ' + str(step.error_class)} "
              f"({step.seconds:.1f}s)")
        if not step.ok:
            print((step.error_message or "")[:2000])
    for node in steps[0].detail.get("nodes", []) if steps[0].ok else []:
        print(f"    {node['node']:45s} {node['status']:10s} {node['seconds']}s")
    print(f"Evidence: {out}")
    return 0 if all(s.ok for s in steps) else 1


def _verify_tables(spark, names: Sequence[str]) -> List[Dict[str, object]]:
    out = []
    for name in names:
        fqn = f"{CATALOG_NAME}.{WAREHOUSE_NAME}.{name}"
        rows = spark.sql(f"SELECT count(*) AS n FROM {fqn}").collect()[0]["n"]
        described = {
            r["col_name"].strip(): (r["data_type"] or "").strip()
            for r in spark.sql(f"DESCRIBE EXTENDED {fqn}").collect()
        }
        out.append(
            {
                "table": fqn,
                "rows": rows,
                "provider": described.get("Provider", ""),
                "location": described.get("Location", ""),
            }
        )
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plan 125 Gate C scale-reproduction harness.",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_HARNESS_BUCKET,
        help=f"Isolated MinIO bucket (default {DEFAULT_HARNESS_BUCKET!r}; "
             f"{PRODUCTION_BUCKET!r} is rejected).",
    )
    parser.add_argument(
        "--driver-memory",
        default="4g",
        help="spark.driver.memory; 'unset' sets nothing, reproducing the VM run.",
    )
    parser.add_argument(
        "--master",
        default="local[4]",
        help="spark.master; 'unset' sets nothing (Spark then uses local[*]).",
    )
    parser.add_argument(
        "--shuffle-partitions", type=int, default=32, help="spark.sql.shuffle.partitions"
    )
    parser.add_argument(
        "--evidence-dir",
        default=None,
        help=f"Evidence output directory (default {DEFAULT_EVIDENCE_DIR}/<run-id>).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    probe = sub.add_parser(
        "probe-parquet",
        help="Minimize UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY. No scale needed.",
    )
    probe.set_defaults(func=cmd_probe_parquet)

    cascade = sub.add_parser(
        "probe-oom-cascade",
        help="Test whether a driver OOM poisons the session's Parquet reads.",
    )
    cascade.add_argument("--oom-rows", type=int, default=8_000_000)
    cascade.add_argument("--oom-string-width", type=int, default=200)
    cascade.set_defaults(func=cmd_probe_oom_cascade)

    gen = sub.add_parser("generate", help="Write synthetic silver/ops Parquet at scale.")
    gen.add_argument(
        "--rows",
        type=int,
        default=1_000_000,
        help=f"Observation rows (VM scale is {VM_OBSERVATION_ROWS}).",
    )
    gen.add_argument("--price-event-rows", type=int, default=1_000_000)
    gen.add_argument(
        "--distinct-vins",
        type=int,
        default=400_000,
        help="Controls window-partition size; too high hides ranking skew.",
    )
    gen.add_argument("--write-partitions", type=int, default=32)
    gen.set_defaults(func=cmd_generate)

    run = sub.add_parser("run-model", help="Run dbt over the synthetic data, instrumented.")
    run.add_argument("--verify-table", action="append", dest="verify_tables", default=None)
    run.add_argument("--evidence-name", default="run_model")
    run.add_argument("dbt_args", nargs=argparse.REMAINDER)
    run.set_defaults(func=cmd_run_model)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.evidence_dir is None:
        run_id = os.environ.setdefault("HARNESS_RUN_ID", uuid.uuid4().hex[:8])
        args.evidence_dir = str(Path(DEFAULT_EVIDENCE_DIR) / run_id)
    try:
        return args.func(args)
    except HarnessError as exc:
        print(f"harness error: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
