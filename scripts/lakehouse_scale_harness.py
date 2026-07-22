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
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

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


def assert_isolated_path(path: str) -> None:
    """Refuse to READ a path in the production bucket.

    Separate from assert_isolated_bucket because `--path` bypasses it: the
    bucket flag governs where the harness writes, while --path is free-form
    and could name any bucket. describe-dataset is read-only, so this is not
    about corruption -- it is about not pointing a local profiling run at
    production silver and reporting the result as snapshot-derived.
    """
    if not path:
        raise HarnessError("--path must be a non-empty s3a:// path.")
    for scheme in ("s3a://", "s3://"):
        if path.startswith(scheme):
            bucket = path[len(scheme):].split("/", 1)[0]
            assert_isolated_bucket(bucket)
            return
    raise HarnessError(
        f"--path must be an s3a:// URI naming its bucket, got {path!r}; the "
        "isolation guard cannot verify a path whose bucket it cannot read."
    )


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
        # Per-key try/except, and NO default passed to conf.get().
        #
        # Spark type-checks the default against the config's declared type, so
        # a string sentinel raises IllegalArgumentException on an int-typed
        # key ("spark.sql.shuffle.partitions should be int, but was <unset>").
        # Every harness run SETS that key via SparkSizing, so this was
        # invisible here -- it only fires when the key is genuinely unset,
        # which is exactly the shadow-build configuration the replay
        # reproduces. Reading facts must never be what kills the run whose
        # facts are being read.
        try:
            facts[key] = spark.conf.get(key)
        except Exception:  # noqa: BLE001
            facts[key] = "<unset>"
    facts["default_parallelism"] = spark.sparkContext.defaultParallelism
    return facts


def heap_used_bytes_at_sampling(spark) -> Optional[int]:
    """Driver JVM heap in use *at this instant*, via the JVM's memory bean.

    **This is NOT a peak or high-water measurement.** It is one
    `getHeapMemoryUsage().getUsed()` sample taken whenever the caller asks —
    in practice after the run, so whatever GC last left behind. It cannot
    support sizing conclusions: a low number may mean the workload was cheap
    or merely that a collection ran just before sampling, and those are
    indistinguishable from this value alone.

    Genuine peak evidence needs either periodic sampling during execution or
    `getPeakUsage().getUsed()` summed across the heap `MemoryPoolMXBean`s.
    Neither is implemented; do not read this field as if one were.

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
# Value-only forms, so observations_expr can wrap them in a measured null
# rate while price_events_expr (which has no null modelling and needs none)
# keeps using the aliased versions unchanged.
_MAKE_VALUE = (
    "CASE cast(id % 5 as int) WHEN 0 THEN 'Honda' WHEN 1 THEN 'Toyota' "
    "WHEN 2 THEN 'Ford' WHEN 3 THEN 'Subaru' ELSE 'Mazda' END"
)
_MODEL_VALUE = (
    "CASE cast(id % 7 as int) WHEN 0 THEN 'Accord' WHEN 1 THEN 'Camry' "
    "WHEN 2 THEN 'F-150' WHEN 3 THEN 'Outback' WHEN 4 THEN 'CX-5' "
    "WHEN 5 THEN 'Civic' ELSE 'Corolla' END"
)
_VIN_VALUE = "concat('1HGCM82633A', lpad(cast(id % {n} as string), 6, '0'))"
_MAKE_EXPR = f"{_MAKE_VALUE} AS make"
_MODEL_EXPR = f"{_MODEL_VALUE} AS model"
_VIN_EXPR = f"{_VIN_VALUE} AS vin"
_EVENT_AT_EXPR = (
    "timestamp_millis(1700000000000 + cast(id % 2592000 as bigint) * 1000) AS {alias}"
)


@dataclass(frozen=True)
class StringWidths:
    """Target character widths for the hashed string fields.

    The 28-field fingerprint is string-bound: its cost in shuffle and window
    state is dominated by how many BYTES each row carries, not by how many
    rows there are. The first version of this generator emitted uniformly
    short values (`body-7`, `isa-12`), which is a plausible reason 38.6M
    synthetic rows fit in a 1 GiB heap that real data did not.

    These profiles are DELIBERATELY GUESSES until measured. `describe-dataset`
    reports real per-field p95/max widths from a production snapshot; those
    numbers should replace these. Until they do, a `wide` run proves only that
    width matters -- not that it matches production.
    """

    body: int = 24
    canonical_detail_url: int = 48
    trid: int = 12
    isa_context: int = 12
    dealer_name: int = 24
    vehicle_trim: int = 12
    # Fields whose width was previously hardcoded in the generator. listing_id
    # is the one that mattered: production uses 36-char UUIDs and the
    # generator emitted ~8 chars, understating every row by ~28 bytes on a
    # field that is NEVER null and so pays that cost on all of them.
    listing_id: int = 8
    dealer_city: int = 7
    seller_customer_id: int = 6
    financing_type: int = 5
    condition: int = 4
    # Per-field null percentage. Load-bearing, not cosmetic: the hash
    # coalesces every field to '', so a field that is 90% null in production
    # contributes almost nothing to row size. A synthetic profile that
    # populates it densely overstates memory pressure -- which is exactly what
    # the `wide` profile did to trid and isa_context.
    null_pct: Mapping[str, float] = field(default_factory=dict)

    @classmethod
    def profile(cls, name: str) -> "StringWidths":
        if name == "narrow":
            return cls()
        if name == "snapshot":
            # MEASURED from a real Plan 120 lake snapshot (2026-07-21,
            # adaptive-refresh-2026-07-15-181719, 16,847 rows). Widths are the
            # measured p99; null rates are measured directly. This is the only
            # profile here that is evidence rather than guesswork -- see
            # SNAPSHOT_PROFILE_PROVENANCE and the plan's measured-profile
            # table for the caveats on how representative it is.
            return cls(
                body=47,
                canonical_detail_url=72,
                trid=22,
                isa_context=8,
                dealer_name=35,
                vehicle_trim=23,
                null_pct={
                    "body": 25.05,
                    "trid": 90.42,
                    "isa_context": 90.42,
                    "dealer_name": 10.54,
                    "trim": 75.91,
                },
            )
        if name == "production":
            # MEASURED from real production silver on the VM (2026-07-21,
            # read-only: s3a://bronze/silver_normalized/observations,
            # 40,450,715 rows / 1,030 files). Widths are the measured p99,
            # null rates the measured percentages. This is the ONLY profile
            # taken from production rather than a fixture -- see
            # PRODUCTION_PROFILE_PROVENANCE and Finding 5.
            #
            # The snapshot profile it supersedes had accurate widths but
            # understated fan-out (p50 1 vs 9) and reported zero duplicates
            # where production has 232,247 groups.
            return cls(
                body=55,
                canonical_detail_url=72,
                trid=22,
                isa_context=12,
                dealer_name=44,
                vehicle_trim=28,
                listing_id=36,
                dealer_city=18,
                seller_customer_id=36,
                financing_type=11,
                condition=9,
                null_pct={
                    "canonical_detail_url": 0.0,
                    "body": 18.94,
                    "condition": 18.94,
                    "vin": 14.03,
                    "dealer_name": 32.13,
                    "customer_id": 32.99,
                    "dealer_zip": 33.58,
                    "dealer_city": 36.65,
                    "dealer_state": 36.65,
                    "make": 61.32,
                    "model": 61.32,
                    "stock_type": 85.41,
                    "trim": 86.85,
                    "fuel_type": 86.98,
                    "body_style": 89.82,
                    "seller_customer_id": 97.04,
                    "trid": 97.04,
                    "financing_type": 97.04,
                    "seller_zip": 97.04,
                    "isa_context": 97.04,
                },
            )
        if name == "wide":
            return cls(
                body=512,
                canonical_detail_url=180,
                trid=64,
                isa_context=96,
                dealer_name=64,
                vehicle_trim=48,
            )
        if name == "extreme":
            return cls(
                body=4096,
                canonical_detail_url=512,
                trid=128,
                isa_context=256,
                dealer_name=128,
                vehicle_trim=96,
            )
        raise HarnessError(
            f"unknown string-width profile {name!r}; expected "
            "narrow/snapshot/wide/extreme"
        )


# Where the `snapshot` profile's numbers came from, carried in every evidence
# bundle that uses it so a future reader can tell measurement from guess.
PRODUCTION_PROFILE_PROVENANCE = {
    "source": "production silver, read-only VM measurement",
    "path": "s3a://bronze/silver_normalized/observations",
    "measured_at": "2026-07-21",
    "rows": 40450715,
    "files": 1030,
    "hashed_bytes_per_row": {"p50": 235, "p95": 269, "p99": 282, "max": 330},
    "artifact_fanout": {"p50": 9, "p95": 9, "p99": 9, "max": 112, "mean": 6.43},
    "observation_key_groups": {
        "max": 6,
        "groups_with_duplicates": 232247,
        "groups": 40186331,
    },
    "caveat": (
        "Fan-out is modelled FLAT at 9 while production is bimodal (carousel "
        "81.1% at ~9, detail 16.0% at 1, mean 6.43), so a flat-9 run is "
        "HEAVIER than production, not equal to it. Duplicate groups reach 6 "
        "in production; --duplicate-modulus tops out at 2."
    ),
}


SNAPSHOT_PROFILE_PROVENANCE = {
    "snapshot_id": "adaptive-refresh-2026-07-15-181719",
    "measured_at": "2026-07-21",
    "rows_measured": 16847,
    "widths": "measured p99",
    "null_rates": "measured",
    "caveats": (
        "Seed-VIN-filtered CI fixture, not a uniform production sample: 282 "
        "distinct listing_ids, source mix 74.9% carousel / 15.5% detail / "
        "9.6% srp, and zero duplicate (artifact_id, listing_id) groups. "
        "Per-row WIDTHS are trustworthy (real rows, real values). Source mix, "
        "fan-out, and duplicate rate are likely biased by the seed selection "
        "and should not be treated as production-representative."
    ),
}


def _wide_string(
    seed_expr: str, width: int, alias: str, null_pct: float = 0.0
) -> str:
    """A high-entropy string of exactly `width` characters, null `null_pct`
    percent of the time.

    Entropy matters: padding with a repeated constant would compress to almost
    nothing in Parquet, so the FILES would stay small while the in-memory rows
    grew -- making bytes-per-row on disk a lie. Repeated md5 keeps on-disk and
    in-heap size honest with each other.

    Nulls are applied on `id` (not the seed) so the null pattern does not
    correlate with the value pattern, and so a field seeded off a low-
    cardinality expression still gets its nulls spread across all rows.
    """
    repeats = max(1, -(-width // 32))
    value = f"substr(repeat(md5(cast({seed_expr} as string)), {repeats}), 1, {width})"
    if null_pct > 0:
        threshold = max(1, int(round(null_pct * 100)))
        value = f"CASE WHEN id % 10000 < {threshold} THEN NULL ELSE {value} END"
    return f"{value} AS {alias}"


def _nullable(value_expr: str, alias: str, null_pct: float = 0.0) -> str:
    """Apply a measured null rate to ANY generated column.

    _wide_string() could already do this, but only the six fields it builds.
    Everything else was emitted 100% populated, which is why the Finding 4
    baseline carried 336 hashed bytes/row against production's measured 269:
    fields that are 61-97% null in reality (seller_customer_id, financing_type,
    make, model, fuel_type, ...) were contributing their full width to every
    single row.

    Nulls are keyed on the same `id % 10000` window as _wide_string, so fields
    sharing a null rate go null on the SAME rows. That is deliberate rather
    than a shortcut: production's rates cluster into obviously-correlated
    groups (97.04% across seller_customer_id/trid/financing_type/seller_zip/
    isa_context; 61.32% across make/model), which is what a field-group that
    is present or absent together looks like. Independent nulls would model a
    row that production does not produce.
    """
    if null_pct and null_pct > 0:
        threshold = max(1, int(round(null_pct * 100)))
        value_expr = (
            f"CASE WHEN id % 10000 < {threshold} THEN NULL ELSE {value_expr} END"
        )
    return f"{value_expr} AS {alias}"


def _base_id(duplicate_modulus: int) -> str:
    """Identity source for a row, folding a fraction of rows onto their
    predecessor's (artifact_id, listing_id).

    This is the reprocessing-correction case the model documents: the same
    observation re-landing with the SAME fetched_at but a later written_at.
    It is the only thing that exercises three paths at once -- the
    row_number() dedupe, the `unique` contract on observation_id, and
    Iceberg's MERGE cardinality check, which raises outright if the source
    carries two rows for one key. With no duplicates the dedupe is dead code
    and MERGE never meets the precondition it enforces.
    """
    if duplicate_modulus <= 0:
        return "id"
    return f"(CASE WHEN id % {duplicate_modulus} = 0 AND id > 0 THEN id - 1 ELSE id END)"


def observations_expr(
    rows: int,
    distinct_vins: int,
    listings_per_artifact: int = 1,
    widths: Optional[StringWidths] = None,
    duplicate_modulus: int = 0,
) -> List[str]:
    """The synthetic silver observation projection, as Spark SQL expressions.

    Shape matters more than realism here, and the shape that matters is the
    one the FAILING models window over.

    `int_listing_observation_fingerprints` ranks within
    `partition by artifact_id, listing_id`; `int_listing_state_fingerprints`
    ranks within `partition by artifact_id`. The first version of this
    generator set `artifact_id = id`, which made both keys unique per row,
    every window partition size 1, and the ranking a no-op -- so a passing run
    exercised none of the sort/window state it was meant to stress, and the
    MERGE cardinality precondition was never met either.

    `listings_per_artifact` fixes that by giving one artifact many listing
    rows, which is what SRP and carousel artifacts genuinely look like: the
    model's own header says bare artifact_id "does not hold here because a
    single SRP or carousel artifact can carry many listing_ids".
    `duplicate_modulus` additionally folds a fraction of rows onto an existing
    (artifact_id, listing_id).

    Kept as SQL expressions over spark.range so generation itself streams and
    never materializes the dataset in the driver.
    """
    widths = widths or StringWidths()
    base = _base_id(duplicate_modulus)
    fanout = max(1, listings_per_artifact)
    # Rows sharing an artifact_id are consecutive, so `base % distinct_vins`
    # still differs within the group -- one artifact, many distinct listings.
    artifact = f"cast({base} / {fanout} as bigint)"
    return [
        f"{artifact} AS artifact_id",
        # Width matters and was wrong: production listing_ids are 36-char
        # UUIDs, the generator emitted ~8 chars, understating every row by
        # ~28 bytes on a field that is never null. lpad preserves
        # distinctness while hitting the measured width exactly.
        f"concat('L', lpad(cast({base} % {distinct_vins} as string), "
        f"{max(1, widths.listing_id - 1)}, '0')) AS listing_id",
        _nullable(
            f"concat('1HGCM82633A', lpad(cast({base} % {distinct_vins} as string), "
            "6, '0'))", "vin", widths.null_pct.get("vin", 0.0)),
        _wide_string(base, widths.canonical_detail_url, "canonical_detail_url",
                     widths.null_pct.get("canonical_detail_url", 0.0)),
        _SOURCE_EXPR,
        "CASE WHEN id % 17 = 0 THEN 'unavailable' ELSE 'active' END AS listing_state",
        # fetched_at keyed off `base` so a correction row shares its
        # predecessor's fetched_at, and written_at off `id` so it lands later
        # -- exactly the tie the dedupe's `written_at desc` exists to break.
        f"timestamp_millis(1700000000000 + cast({base} % 2592000 as bigint) * 1000) "
        "AS fetched_at",
        "timestamp_millis(1700000060000 + cast(id % 2592000 as bigint) * 1000) AS written_at",
        "cast(15000 + (id % 60000) as int) AS price",
        _nullable(_MAKE_VALUE, "make", widths.null_pct.get("make", 0.0)),
        _nullable(_MODEL_VALUE, "model", widths.null_pct.get("model", 0.0)),
        _wide_string("id % 11", widths.vehicle_trim, "trim",
                     widths.null_pct.get("trim", 0.0)),
        "cast(2018 + (id % 8) as smallint) AS year",
        "cast(id % 120000 as int) AS mileage",
        "cast(20000 + (id % 55000) as int) AS msrp",
        _nullable("CASE WHEN id % 2 = 0 THEN 'New' ELSE 'Used' END", "stock_type",
                  widths.null_pct.get("stock_type", 0.0)),
        _nullable("CASE cast(id % 4 as int) WHEN 0 THEN 'Gasoline' WHEN 1 THEN 'Hybrid' "
                  "WHEN 2 THEN 'Electric' ELSE 'Diesel' END", "fuel_type",
                  widths.null_pct.get("fuel_type", 0.0)),
        _nullable("CASE cast(id % 6 as int) WHEN 0 THEN 'Sedan' WHEN 1 THEN 'SUV' "
                  "WHEN 2 THEN 'Truck' WHEN 3 THEN 'Coupe' WHEN 4 THEN 'Wagon' "
                  "ELSE 'Van' END", "body_style",
                  widths.null_pct.get("body_style", 0.0)),
        _wide_string("id % 900", widths.dealer_name, "dealer_name",
                     widths.null_pct.get("dealer_name", 0.0)),
        _nullable("lpad(cast(id % 99999 as string), 5, '0')", "dealer_zip",
                  widths.null_pct.get("dealer_zip", 0.0)),
        _nullable(f"concat('City', lpad(cast(id % 400 as string), "
                  f"{max(1, widths.dealer_city - 4)}, '0'))", "dealer_city",
                  widths.null_pct.get("dealer_city", 0.0)),
        _nullable("CASE cast(id % 5 as int) WHEN 0 THEN 'CA' WHEN 1 THEN 'TX' "
                  "WHEN 2 THEN 'NY' WHEN 3 THEN 'FL' ELSE 'WA' END", "dealer_state",
                  widths.null_pct.get("dealer_state", 0.0)),
        _nullable("concat('cust-', cast(id % 900 as string))", "customer_id",
                  widths.null_pct.get("customer_id", 0.0)),
        "concat('seller-', cast(id % 900 as string)) AS seller_id",
        _nullable(f"substr(repeat('financing-', 3), 1, {widths.financing_type})",
                  "financing_type", widths.null_pct.get("financing_type", 0.0)),
        _nullable("lpad(cast(id % 99999 as string), 5, '0')", "seller_zip",
                  widths.null_pct.get("seller_zip", 0.0)),
        _nullable(f"concat('sc-', lpad(cast(id % 700 as string), "
                  f"{max(1, widths.seller_customer_id - 3)}, '0'))",
                  "seller_customer_id",
                  widths.null_pct.get("seller_customer_id", 0.0)),
        "cast(id % 50 as smallint) AS page_number",
        "cast(id % 30 as smallint) AS position_on_page",
        _wide_string("id % 5000", widths.trid, "trid",
                     widths.null_pct.get("trid", 0.0)),
        _wide_string("id % 40", widths.isa_context, "isa_context",
                     widths.null_pct.get("isa_context", 0.0)),
        _wide_string("id", widths.body, "body",
                     widths.null_pct.get("body", 0.0)),
        _nullable(f"substr(repeat('condition-', 2), 1, {widths.condition})",
                  "condition", widths.null_pct.get("condition", 0.0)),
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


# Every STRING field the 28-field fingerprint hashes, in silver's own column
# naming (stg_observations renames trim->vehicle_trim and year->model_year on
# the way through, so the model's spelling differs from the source's).
#
# Load-bearing for sizing, not just reporting: the hash concatenates all of
# these, so their combined width is what the window/sort state carries per
# row. test_hashed_string_fields_match_the_model asserts this list against the
# model SQL, so a field added to the hash cannot silently go unmeasured.
HASHED_STRING_FIELDS = (
    "listing_id",
    "vin",
    "source",
    "make",
    "model",
    "trim",
    "listing_state",
    "canonical_detail_url",
    "stock_type",
    "fuel_type",
    "body_style",
    "dealer_name",
    "dealer_zip",
    "dealer_city",
    "dealer_state",
    "customer_id",
    "seller_customer_id",
    "seller_zip",
    "financing_type",
    "trid",
    "isa_context",
    "body",
    "condition",
)

# Numeric fields in the same hash. Fixed-width, so they contribute a constant
# to row size rather than a distribution -- but they are not free, and a
# bytes-per-row figure that ignored them would understate the row.
HASHED_NUMERIC_BYTES = {
    # Hashed into observation_id rather than parsed_fingerprint, but it is
    # still payload the row carries through the window.
    "artifact_id": 8,
    "price": 4,
    "mileage": 4,
    "year": 2,
    "msrp": 4,
    "page_number": 2,
    "position_on_page": 2,
}

PERCENTILES = (0.5, 0.95, 0.99)


def row_bytes_expr() -> str:
    """Per-row byte width of the hashed payload.

    Deliberately an IN-MEMORY proxy, not a stored size. Parquet's on-disk
    bytes/row is compressed and dictionary-encoded, so it can understate the
    heap cost of the same row by a large factor -- and heap is what OOMs. This
    sums actual string lengths plus fixed numeric widths, which is what the
    window/sort state carries.

    Both figures are reported: this one as `row_bytes_*` percentiles, and the
    on-disk one as `bytes_per_row` under `storage`. They answer different
    questions and neither substitutes for the other.
    """
    parts = [f"coalesce(length({f}), 0)" for f in HASHED_STRING_FIELDS]
    parts.append(str(sum(HASHED_NUMERIC_BYTES.values())))
    return " + ".join(parts)


def _pct(expr: str, alias: str) -> str:
    """percentile_approx at the standard set, plus max -- the tail is where
    the memory cost of a skewed key lives, so a mean would hide it."""
    cols = [
        f"percentile_approx({expr}, {p}) AS {alias}_p{int(p * 100)}" for p in PERCENTILES
    ]
    cols.append(f"max({expr}) AS {alias}_max")
    cols.append(f"avg({expr}) AS {alias}_avg")
    return ", ".join(cols)


def dataset_profile_sql(path: str) -> Dict[str, str]:
    """The stat queries describe-dataset runs, as {name: SQL}.

    Split out as data so the query shapes are unit-testable without Spark,
    and so the same profile can be pointed at a real Plan 120 snapshot --
    which is the whole point. Row and file counts alone were what let the
    earlier harness claim a faithful reproduction while every window
    partition was size 1; these are the numbers that would have caught it.
    """
    src = f"parquet.`{path}`"
    return {
        "row_count": f"SELECT count(*) AS rows FROM {src}",
        # Which sources the rows come from. detail is one-listing-per-artifact;
        # srp and carousel are where fan-out comes from, so a profile skewed
        # toward detail would understate it.
        "source_distribution": (
            f"SELECT source, count(*) AS rows FROM {src} GROUP BY source ORDER BY source"
        ),
        # Rows per artifact -- the SRP/carousel fan-out the first generator
        # flattened to 1.
        "artifact_fanout": (
            f"SELECT {_pct('n', 'fanout')}, min(n) AS fanout_min, "
            "count(*) AS artifacts "
            f"FROM (SELECT artifact_id, count(*) AS n FROM {src} GROUP BY artifact_id)"
        ),
        # Rows per (artifact_id, listing_id) -- the actual window partition of
        # int_listing_observation_fingerprints. Anything above 1 is what the
        # row_number() dedupe and Iceberg's MERGE cardinality check act on.
        "observation_key_groups": (
            f"SELECT {_pct('n', 'group')}, count(*) AS groups, "
            "sum(CASE WHEN n > 1 THEN 1 ELSE 0 END) AS groups_with_duplicates "
            f"FROM (SELECT artifact_id, listing_id, count(*) AS n FROM {src} "
            "GROUP BY artifact_id, listing_id)"
        ),
        # SKEW. The three queries above summarise the distribution; these two
        # describe its TAIL, which is what a window function actually OOMs on.
        # A single hot artifact_id is invisible at p99 -- with millions of
        # artifacts, one partition of 500k rows moves no percentile at all,
        # and yet it is one partition that must be sorted in the driver heap.
        # Percentiles said "p50 1, p95 5" for a snapshot whose real spread was
        # never in question; the number that would change a sizing decision is
        # the largest group, not the median one.
        "artifact_skew_top": (
            "SELECT artifact_id, count(*) AS n "
            f"FROM {src} GROUP BY artifact_id ORDER BY n DESC LIMIT 20"
        ),
        "observation_key_skew_top": (
            "SELECT artifact_id, listing_id, count(*) AS n "
            f"FROM {src} GROUP BY artifact_id, listing_id ORDER BY n DESC LIMIT 20"
        ),
        # listing_id ALONE -- the window key of int_listing_observation_runs,
        # which is the model that actually OOMed on the VM (2026-07-22 replay,
        # UnsafeSorterSpillReader inside WindowExec).
        #
        # Finding 5 measured artifact_id and (artifact_id, listing_id) because
        # those are int_listing_observation_fingerprints' keys, found both
        # bounded (112 and 6), and concluded "skew is bounded". That
        # conclusion did not cover this key at all. int_listing_observation_runs
        # runs gaps-and-islands over ALL history for a listing_id, so its
        # window partition is every observation of one listing across its
        # lifetime -- an unbounded-in-time group that the other two keys say
        # nothing about.
        "listing_fanout": (
            f"SELECT {_pct('n', 'listing')}, count(*) AS listings "
            f"FROM (SELECT listing_id, count(*) AS n FROM {src} "
            "GROUP BY listing_id)"
        ),
        "listing_skew_top": (
            "SELECT listing_id, count(*) AS n "
            f"FROM {src} GROUP BY listing_id ORDER BY n DESC LIMIT 20"
        ),
        "row_bytes": f"SELECT {_pct(row_bytes_expr(), 'row_bytes')} FROM {src}",
        # Per-field width AND null rate. Nulls matter as much as widths here:
        # the hash coalesces every field to '', so a field that is mostly null
        # in production contributes nothing to row size, and a synthetic
        # profile that populates it densely would overstate memory pressure.
        "string_fields": (
            "SELECT count(*) AS rows, "
            + ", ".join(
                f"{_pct(f'length({f})', f)}, "
                f"sum(CASE WHEN {f} IS NULL THEN 1 ELSE 0 END) AS {f}_nulls"
                for f in HASHED_STRING_FIELDS
            )
            + f" FROM {src}"
        ),
    }


def parse_string_field_stats(row: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    """Regroup the flat string_fields row into {field: {stat: value}}.

    The query has to be one flat SELECT so Spark makes a single pass over the
    data; this makes the resulting bundle readable rather than 100+ columns
    of `body_p95`-style keys.
    """
    rows = row.get("rows")
    out: Dict[str, Dict[str, object]] = {}
    for name in HASHED_STRING_FIELDS:
        stats: Dict[str, object] = {}
        for suffix in ("p50", "p95", "p99", "max", "avg", "nulls"):
            key = f"{name}_{suffix}"
            if key in row:
                stats[suffix] = row[key]
        if isinstance(rows, int) and rows and isinstance(stats.get("nulls"), int):
            stats["null_pct"] = round(100.0 * stats["nulls"] / rows, 2)
        out[name] = stats
    return out


def cmd_describe_dataset(args: argparse.Namespace) -> int:
    """Profile a dataset's SHAPE, not just its size.

    Point it at the synthetic bucket to verify the generator produces the
    fan-out and widths it claims; point it at a real Plan 120 snapshot to get
    the production percentiles that should replace the StringWidths guesses.
    Comparing the two bundles is the bridge between "1 GiB passes locally"
    and "1 GiB OOMed on the VM".
    """
    assert_isolated_bucket(args.bucket)
    path = args.path or f"s3a://{args.bucket}/{OBSERVATIONS_PREFIX}"
    # --path is free-form, so the bucket guard above does not cover it. Without
    # this, `--bucket snapshot-profile --path s3a://bronze/...` would read
    # production silver through a command that looks isolated.
    #
    # The guard's purpose is PROVENANCE, not safety -- profiling is read-only,
    # and the risk it defends against is a production-derived number being
    # filed as snapshot-derived. So the opt-in does not bypass it; it changes
    # the labelling, stamping `reads_production: true` into the bundle and
    # saying so on stdout. A run that reads production is then impossible to
    # mistake for one that did not, which is what the guard was protecting.
    if args.allow_production_read:
        print(
            f"*** READING PRODUCTION DATA: {path}\n"
            "*** Read-only profiling. Evidence is stamped reads_production=true "
            "and must NOT be cited as snapshot-derived."
        )
    else:
        assert_isolated_path(path)

    sizing = sizing_from_args(args)
    conf = harness_spark_conf(sizing, args.bucket)
    spark = build_spark(conf, "cartracker-describe-dataset")

    steps: List[StepResult] = []

    def collect_one(sql: str) -> Dict[str, object]:
        return {"stats": spark.sql(sql).collect()[0].asDict()}

    for name, sql in dataset_profile_sql(path).items():
        if name == "source_distribution":
            steps.append(
                run_step(
                    name,
                    lambda sql=sql: {
                        "stats": {
                            r["source"]: r["rows"] for r in spark.sql(sql).collect()
                        }
                    },
                )
            )
        elif name == "string_fields":
            steps.append(
                run_step(
                    name,
                    lambda sql=sql: {
                        "stats": parse_string_field_stats(
                            spark.sql(sql).collect()[0].asDict()
                        )
                    },
                )
            )
        elif name.endswith("_skew_top"):
            # Multi-row by construction: collect_one would silently keep only
            # the largest group and discard the shape of the tail, which is
            # the entire reason these queries exist.
            steps.append(
                run_step(
                    name,
                    lambda sql=sql: {
                        "stats": [r.asDict() for r in spark.sql(sql).collect()]
                    },
                )
            )
        else:
            steps.append(run_step(name, lambda sql=sql: collect_one(sql)))

    # Bytes/row and bytes/file, which row counts alone cannot give -- a
    # narrow-string dataset and a wide-string one of identical row and file
    # count differ here by an order of magnitude, and that difference is what
    # the fingerprint's memory cost tracks.
    def storage() -> Dict[str, object]:
        files = spark.read.parquet(path).inputFiles()
        total = _prefix_bytes(path)
        rows = steps[0].detail["stats"]["rows"] if steps[0].ok else 0
        return {
            "files": len(files),
            "total_bytes": total,
            "bytes_per_row": round(total / rows, 2) if rows else None,
            "bytes_per_file": round(total / len(files), 2) if files else None,
        }

    steps.append(run_step("storage", storage))

    bundle = evidence_bundle(
        "describe-dataset",
        conf,
        steps,
        extra={"path": path, "reads_production": bool(args.allow_production_read)},
    )
    out = write_evidence(Path(args.evidence_dir), args.evidence_name, bundle)
    print(format_profile_summary(bundle))
    print(f"Evidence: {out}")
    return 0 if all(s.ok for s in steps) else 1


def format_profile_summary(bundle: Dict[str, object]) -> str:
    """Concise human-readable profile.

    The JSON bundle is the record; this is what makes a run readable without
    opening it. Kept pure (takes the bundle, returns a string) so it is
    testable and so the same formatting works on a bundle read back from disk
    months later.
    """
    steps = {s["name"]: s for s in bundle.get("steps", [])}

    def stats(name: str) -> Dict[str, object]:
        step = steps.get(name) or {}
        return (step.get("detail") or {}).get("stats") or {} if step.get("ok") else {}

    lines = [f"\nDataset profile: {bundle.get('path', '?')}"]

    rows = stats("row_count").get("rows")
    storage = (steps.get("storage", {}).get("detail") or {}) if steps.get("storage") else {}
    lines.append(f"  rows={rows}  files={storage.get('files')}  "
                 f"stored_bytes_per_row={storage.get('bytes_per_row')}")

    src = stats("source_distribution")
    if src:
        total = sum(v for v in src.values() if isinstance(v, int)) or 1
        parts = ", ".join(
            f"{k}={v} ({100.0 * v / total:.1f}%)" for k, v in sorted(src.items())
        )
        lines.append(f"  sources: {parts}")

    fan = stats("artifact_fanout")
    if fan:
        lines.append(
            f"  artifact fan-out: p50={fan.get('fanout_p50')} "
            f"p95={fan.get('fanout_p95')} p99={fan.get('fanout_p99')} "
            f"max={fan.get('fanout_max')} over {fan.get('artifacts')} artifacts"
        )

    grp = stats("observation_key_groups")
    if grp:
        lines.append(
            f"  (artifact_id, listing_id) groups: p50={grp.get('group_p50')} "
            f"p95={grp.get('group_p95')} p99={grp.get('group_p99')} "
            f"max={grp.get('group_max')}  duplicates={grp.get('groups_with_duplicates')}"
            f"/{grp.get('groups')}"
        )

    # The tail, printed next to the percentiles it hides. Showing the top few
    # groups inline is the point: "p99=1, max=412,880" is a sentence that
    # changes a sizing decision, and it is unreadable if the two halves live
    # in different sections of the bundle.
    lf = stats("listing_fanout")
    if lf:
        lines.append(
            f"  rows per listing_id: p50={lf.get('listing_p50')} "
            f"p95={lf.get('listing_p95')} p99={lf.get('listing_p99')} "
            f"max={lf.get('listing_max')} over {lf.get('listings')} listings"
        )

    for label, key, cols in (
        ("artifact_id", "artifact_skew_top", ("artifact_id",)),
        ("listing_id", "listing_skew_top", ("listing_id",)),
        (
            "(artifact_id, listing_id)",
            "observation_key_skew_top",
            ("artifact_id", "listing_id"),
        ),
    ):
        top = stats(key)
        if isinstance(top, list) and top:
            head = ", ".join(str(r.get("n")) for r in top[:5])
            biggest = top[0]
            ident = " / ".join(str(biggest.get(c)) for c in cols)
            lines.append(
                f"  {label} SKEW: largest 5 groups = [{head}]  "
                f"heaviest = {ident}"
            )

    rb = stats("row_bytes")
    if rb:
        lines.append(
            f"  hashed payload bytes/row (in-memory): p50={rb.get('row_bytes_p50')} "
            f"p95={rb.get('row_bytes_p95')} p99={rb.get('row_bytes_p99')} "
            f"max={rb.get('row_bytes_max')}"
        )

    fields = stats("string_fields")
    if fields:
        lines.append("  hashed string fields (p50/p95/p99/max chars, null%):")
        # Widest first -- those are the ones that drive the row size, and the
        # ones a synthetic profile most needs to match.
        ordered = sorted(
            fields.items(),
            key=lambda kv: (kv[1].get("p95") or 0),
            reverse=True,
        )
        for name, s in ordered:
            lines.append(
                f"    {name:22s} {str(s.get('p50')):>5}/{str(s.get('p95')):>5}/"
                f"{str(s.get('p99')):>5}/{str(s.get('max')):>6}   "
                f"{s.get('null_pct')}%"
            )
    return "\n".join(lines)


def bucket_and_prefix(path: str) -> Tuple[str, str]:
    """Split an s3a:// URI into (bucket, key prefix).

    Derived from the PATH, never from the --bucket flag. Those two are the
    same object for a normal harness run and different the moment --path
    names another bucket -- which is exactly the production-profiling case.
    Reading the flag there listed the isolated bucket with an empty prefix
    and reported its size against production's row count: a bytes/row figure
    that is entirely fictitious and looks completely ordinary.
    """
    for scheme in ("s3a://", "s3://"):
        if path.startswith(scheme):
            rest = path[len(scheme):]
            bucket, _, prefix = rest.partition("/")
            return bucket, prefix
    raise HarnessError(f"expected an s3a:// URI, got {path!r}")


def _prefix_bytes(path: str) -> int:
    """Total stored bytes under a dataset path, via the object store rather
    than Spark -- Spark reports uncompressed sizes, and the interesting ratio
    here is stored-bytes-per-row."""
    import boto3

    bucket, prefix = bucket_and_prefix(path)
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )
    paginator = client.get_paginator("list_objects_v2")
    return sum(
        obj["Size"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    )


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

    widths = StringWidths.profile(args.string_widths)
    steps.append(
        run_step(
            "write_observations",
            lambda: write(
                OBSERVATIONS_PREFIX,
                args.rows,
                observations_expr(
                    args.rows,
                    args.distinct_vins,
                    listings_per_artifact=args.listings_per_artifact,
                    widths=widths,
                    duplicate_modulus=args.duplicate_modulus,
                ),
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
        extra={
            "bucket": args.bucket,
            "driver_heap_used_bytes_at_sampling": heap_used_bytes_at_sampling(spark),
        },
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
            "driver_heap_used_bytes_at_sampling": heap_used_bytes_at_sampling(spark),
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
    gen.add_argument(
        "--listings-per-artifact",
        type=int,
        default=1,
        help=(
            "Listing rows sharing one artifact_id (SRP/carousel fan-out). "
            "1 makes every (artifact_id, listing_id) window partition a "
            "singleton and the models' ranking a no-op -- use >1 to exercise it."
        ),
    )
    gen.add_argument(
        "--duplicate-modulus",
        type=int,
        default=0,
        help=(
            "Every Nth row reuses its predecessor's (artifact_id, listing_id) "
            "with a later written_at -- the reprocessing-correction case. 0 "
            "disables, leaving the dedupe and MERGE cardinality paths untested."
        ),
    )
    gen.add_argument(
        "--string-widths",
        default="narrow",
        choices=("narrow", "snapshot", "production", "wide", "extreme"),
        help=(
            "Width profile for the hashed string fields. The 28-field hash is "
            "string-bound, so this drives memory more than row count does."
        ),
    )
    gen.set_defaults(func=cmd_generate)

    desc = sub.add_parser(
        "describe-dataset",
        help="Profile a dataset's fan-out, key skew, string widths, and bytes/row.",
    )
    desc.add_argument(
        "--path",
        default=None,
        help="s3a:// path to profile (default: the harness bucket's observations).",
    )
    desc.add_argument("--evidence-name", default="describe_dataset")
    desc.add_argument(
        "--allow-production-read",
        action="store_true",
        help="Permit profiling a path in the production bucket. Read-only, and "
             "stamps reads_production=true into the evidence bundle so the "
             "result can never be cited as snapshot-derived.",
    )
    desc.set_defaults(func=cmd_describe_dataset)

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
