"""
Plan 125 Gate C: instrumented replay of the 2026-07-17 VM shadow-build failure.

Runs the SAME selector that failed on the VM, against the SAME production
Parquet sources, on the SAME (unset -> 1 GiB) driver heap, and captures enough
evidence to say *where* the heap goes -- which the original run could not,
because `~/gate_c_shadow` was removed and the run used `--rm`.

What this is NOT: a sizing experiment. It sets no driver memory at all (the
shadow build did not either, so Spark's 1g default applies), and it must not
be used to pick a production heap. Its only job is to reproduce or refute a
failure and to leave a trace behind either way.

Isolation, and why reading production is safe here:

  * SOURCES ARE READ-ONLY. `MINIO_BUCKET` is interpolated by sources.yml into
    `spark_external_location`, so pointing it at `bronze` makes dbt *read*
    real silver Parquet. Nothing in the selected DAG writes to a source.
  * WRITES ARE NAMESPACE-CONFINED. Models materialise into the Iceberg
    catalog under WAREHOUSE_NAME, whose Lakekeeper storage profile is pinned
    to the `lakehouse_spike/warehouse` key prefix -- disjoint from
    `silver_normalized/`, `ops_normalized/`, and bronze html. Production
    analytics is DuckDB/Postgres and never reads this namespace.
  * The guard is asserted, not assumed: `require_spike_namespace()` runs
    before dbt is invoked.

Evidence captured, all under one run directory:

  pre-run      image digest, pip freeze, JVM input args (the effective -Xmx),
               free memory, cgroup limits, catalog state, source file
               inventory incl. Parquet row-group metadata
  during       a sampler thread polling JVM heap-pool *peak* usage and cgroup
               memory.current every few seconds, appended to JSONL so the
               trace survives a JVM that dies mid-run
  post-run     cgroup memory.peak and memory.events (kernel high-water and
               OOM counters, which outlive the JVM), per-node timings and
               statuses, full exception text, and a phase classification
  always       dbt's own log file and the captured driver stdout/stderr

The sampler is the piece that answers the question Finding 4 could not. A
single post-run `getHeapMemoryUsage().getUsed()` is not a high-water mark;
`MemoryPoolMXBean.getPeakUsage()` is, and cgroup `memory.peak` is the
kernel's own. Both are recorded here.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, "/app")

from scripts.lakehouse_scale_harness import (  # noqa: E402
    jvm_runtime_facts,
    redact_conf,
)
from shared.iceberg_catalog import (  # noqa: E402
    CATALOG_NAME,
    WAREHOUSE_NAME,
    require_spike_namespace,
    spark_conf_for_dbt_session,
)

PRODUCTION_BUCKET = "bronze"
OBSERVATIONS_PREFIX = "silver_normalized/observations"
DBT_PROJECT_DIR = "/app/dbt"

# The exact selector the 2026-07-17 shadow build ran.
DEFAULT_SELECTOR = "+int_listing_volatility_features"

CGROUP = Path("/sys/fs/cgroup")


# ---------------------------------------------------------------------------
# Environment capture
# ---------------------------------------------------------------------------


def _read(path: Path) -> Optional[str]:
    try:
        return path.read_text().strip()
    except Exception:
        return None


def cgroup_facts() -> Dict[str, object]:
    """cgroup v2 memory facts.

    `memory.peak` is the kernel's high-water mark for the whole container and
    `memory.events` counts `oom` / `oom_kill` -- both survive a JVM that died,
    which is exactly the case this script exists for. `memory.max` is the
    limit the container was given.
    """
    facts: Dict[str, object] = {}
    for name in ("memory.max", "memory.high", "memory.current", "memory.peak",
                 "memory.events", "memory.stat"):
        raw = _read(CGROUP / name)
        if raw is None:
            continue
        if name in ("memory.events", "memory.stat"):
            parsed = {}
            for line in raw.splitlines():
                parts = line.split()
                if len(parts) == 2:
                    parsed[parts[0]] = int(parts[1]) if parts[1].isdigit() else parts[1]
            # memory.stat is long; keep only the fields that bear on pressure.
            if name == "memory.stat":
                parsed = {
                    k: v for k, v in parsed.items()
                    if k in ("anon", "file", "slab", "sock", "pgmajfault")
                }
            facts[name] = parsed
        else:
            facts[name] = int(raw) if raw.isdigit() else raw
    return facts


def meminfo_facts() -> Dict[str, object]:
    raw = _read(Path("/proc/meminfo")) or ""
    keep = ("MemTotal", "MemFree", "MemAvailable", "SwapTotal", "SwapFree")
    out = {}
    for line in raw.splitlines():
        key = line.split(":")[0]
        if key in keep:
            out[key] = line.split(":", 1)[1].strip()
    return out


def dependency_facts() -> Dict[str, object]:
    """Pinned versions, so a passing replay can be compared against the
    failing run's environment rather than assumed identical."""
    out: Dict[str, object] = {"python": sys.version.split()[0]}
    try:
        freeze = subprocess.run(
            [sys.executable, "-m", "pip", "freeze"],
            capture_output=True, text=True, timeout=120,
        ).stdout
        interesting = ("pyspark", "dbt-core", "dbt-spark", "pyarrow", "boto3",
                       "py4j", "duckdb")
        out["packages"] = sorted(
            line for line in freeze.splitlines()
            if line.split("==")[0].lower() in interesting
        )
    except Exception as exc:  # pragma: no cover - environment probe
        out["packages_error"] = str(exc)
    return out


def jvm_input_arguments(spark) -> Dict[str, object]:
    """The JVM's own command line.

    This is what settles whether an explicit -Xmx was passed and what it was
    -- Spark's launcher derives it from spark.driver.memory's 1g default and
    it OVERRIDES JVM ergonomics, so the flag list is the authoritative record
    of the heap the run actually had.
    """
    try:
        jvm = spark._jvm
        bean = jvm.java.lang.management.ManagementFactory.getRuntimeMXBean()
        args = [str(a) for a in bean.getInputArguments()]
        return {
            "input_arguments": args,
            "xmx_flags": [a for a in args if a.startswith("-Xmx")],
        }
    except Exception as exc:
        return {"error": str(exc)}


def heap_pool_peaks(spark) -> Dict[str, object]:
    """Per-pool PEAK heap usage -- a real high-water mark.

    This is the measurement Finding 4 lacked. getPeakUsage() is maintained by
    the JVM across the run, so unlike a post-hoc getUsed() sample it cannot be
    fooled by a collection that happened to fire just before sampling.
    """
    try:
        jvm = spark._jvm
        mgmt = jvm.java.lang.management.ManagementFactory
        pools = {}
        total = 0
        for pool in mgmt.getMemoryPoolMXBeans():
            if str(pool.getType()) != "Heap memory":
                continue
            peak = pool.getPeakUsage()
            used = int(peak.getUsed())
            pools[str(pool.getName())] = {
                "peak_used": used,
                "peak_committed": int(peak.getCommitted()),
                "max": int(peak.getMax()),
            }
            total += used
        return {"pools": pools, "peak_heap_used_total": total}
    except Exception as exc:
        return {"error": str(exc)}


def source_inventory(bucket: str, prefix: str, sample_footers: int = 12) -> Dict:
    """File count/size for the whole source, plus row-group metadata from a
    sample of footers.

    Row-group size bears directly on driver memory: the scan buffers whole
    row groups, so a source written with very large row groups costs more per
    task than the file count alone suggests.
    """
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )
    sizes: List[int] = []
    keys: List[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                sizes.append(obj["Size"])
                keys.append(obj["Key"])
    inv: Dict[str, object] = {
        "files": len(sizes),
        "total_bytes": sum(sizes),
        "largest_file_bytes": max(sizes) if sizes else 0,
        "smallest_file_bytes": min(sizes) if sizes else 0,
    }
    footers = []
    for key in keys[:sample_footers]:
        try:
            import io

            import pyarrow.parquet as pq

            body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
            md = pq.ParquetFile(io.BytesIO(body)).metadata
            groups = [
                md.row_group(i).total_byte_size for i in range(md.num_row_groups)
            ]
            footers.append({
                "key": key,
                "rows": md.num_rows,
                "row_groups": md.num_row_groups,
                "max_row_group_bytes": max(groups) if groups else 0,
                "columns": md.num_columns,
            })
        except Exception as exc:
            footers.append({"key": key, "error": str(exc)})
    inv["row_group_sample"] = footers
    return inv


def catalog_state(spark) -> Dict[str, object]:
    """What already exists in the namespace, recorded BEFORE the replay.

    A --full-refresh rebuilds these, so without this the prior shadow build's
    surviving table (int_latest_observation, landed 2026-07-17) would be
    silently overwritten rather than superseded on the record.
    """
    try:
        rows = spark.sql(
            f"SHOW TABLES IN {CATALOG_NAME}.{WAREHOUSE_NAME}"
        ).collect()
        tables = []
        for row in rows:
            name = row["tableName"]
            entry: Dict[str, object] = {"table": name}
            try:
                entry["rows"] = spark.sql(
                    f"SELECT count(*) AS n FROM {CATALOG_NAME}.{WAREHOUSE_NAME}.{name}"
                ).collect()[0]["n"]
            except Exception as exc:
                entry["rows_error"] = str(exc)[:400]
            tables.append(entry)
        return {"namespace": WAREHOUSE_NAME, "tables": tables}
    except Exception as exc:
        return {"error": str(exc)[:1000]}


# ---------------------------------------------------------------------------
# Sampler
# ---------------------------------------------------------------------------


class HeapSampler:
    """Polls heap-pool peaks and cgroup memory into JSONL while dbt runs.

    Appends and flushes every tick rather than accumulating in memory: if the
    driver JVM dies, everything up to the moment of death is already on disk.
    That is the entire design requirement -- an in-memory trace written at the
    end would be lost in exactly the case worth studying.
    """

    def __init__(self, spark, out_path: Path, interval: float = 3.0):
        self.spark = spark
        self.out_path = out_path
        self.interval = interval
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _tick(self) -> Dict[str, object]:
        sample: Dict[str, object] = {"t": round(time.time(), 2)}
        cg = cgroup_facts()
        sample["cgroup_memory_current"] = cg.get("memory.current")
        sample["cgroup_memory_peak"] = cg.get("memory.peak")
        pools = heap_pool_peaks(self.spark)
        sample["peak_heap_used_total"] = pools.get("peak_heap_used_total")
        try:
            bean = self.spark._jvm.java.lang.management.ManagementFactory \
                .getMemoryMXBean()
            sample["heap_used_now"] = int(bean.getHeapMemoryUsage().getUsed())
        except Exception:
            sample["heap_used_now"] = None
        return sample

    def _loop(self) -> None:
        with self.out_path.open("a", encoding="utf-8") as fh:
            while not self._stop.is_set():
                try:
                    fh.write(json.dumps(self._tick()) + "\n")
                    fh.flush()
                except Exception:
                    # A dead JVM must end the sampler quietly, not raise into
                    # a thread nobody is joining yet.
                    break
                self._stop.wait(self.interval)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)


# ---------------------------------------------------------------------------
# Failure classification
# ---------------------------------------------------------------------------


def classify_failure(message: str) -> str:
    """Bucket a failure into the phase it happened in.

    Deliberately coarse and keyword-driven; the raw text is always kept
    alongside so this can be re-judged. The distinction that matters is
    whether the driver died BEFORE touching data (planning/listing), while
    executing (scan/shuffle/window), or while committing to Iceberg -- those
    imply different fixes and the original report could not tell them apart.
    """
    if not message:
        return "unknown"
    low = message.lower()
    if "outofmemoryerror" in low or "java heap space" in low:
        if any(k in low for k in ("listleaffiles", "inmemoryfileindex",
                                  "listing leaf files", "filestatuscache")):
            return "oom_during_planning_or_listing"
        if any(k in low for k in ("commit", "snapshotproducer", "manifest",
                                  "writeto", "icebergwrite")):
            return "oom_during_iceberg_write_or_commit"
        if any(k in low for k in ("shuffle", "sort", "window", "unsafe",
                                  "externalsorter", "exchange")):
            return "oom_during_scan_shuffle_or_window"
        return "oom_phase_unclassified"
    if "connection refused" in low or "py4j" in low and "gateway" in low:
        return "driver_gateway_died_post_oom"
    if "unsupported_datasource_for_direct_query" in low:
        return "post_oom_session_cascade"
    return "non_oom_failure"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def build_session(evidence_dir: Path) -> object:
    from pyspark.sql import SparkSession

    conf = dict(spark_conf_for_dbt_session())
    # Deliberately NOT setting spark.driver.memory: the shadow build did not,
    # and Spark's 1g default is the condition under test. Recording it is the
    # point; changing it would answer a different question.
    builder = SparkSession.builder.appName("cartracker-gate-c-shadow-replay")
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    (evidence_dir / "spark_conf.json").write_text(
        json.dumps(redact_conf(conf), indent=2), encoding="utf-8"
    )
    return spark


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selector", default=DEFAULT_SELECTOR)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--image-digest", default=os.environ.get("IMAGE_DIGEST", ""))
    parser.add_argument(
        "--source-bucket", default=PRODUCTION_BUCKET,
        help="Bucket dbt reads sources from. Read-only.",
    )
    parser.add_argument("--sample-interval", type=float, default=3.0)
    args = parser.parse_args(argv)

    evidence = Path(args.evidence_dir)
    evidence.mkdir(parents=True, exist_ok=True)

    # Writes are namespace-confined; assert it before anything runs.
    require_spike_namespace(WAREHOUSE_NAME)

    # sources.yml interpolates this into spark_external_location.
    os.environ["MINIO_BUCKET"] = args.source_bucket
    os.environ.setdefault(
        "POSTGRES_URL", "postgresql://unused:unused@unused:5432/unused"
    )

    bundle: Dict[str, object] = {
        "command": "gate-c-shadow-replay",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "selector": args.selector,
        "source_bucket": args.source_bucket,
        "write_namespace": f"{CATALOG_NAME}.{WAREHOUSE_NAME}",
        "image_digest": args.image_digest,
        "pre": {
            "cgroup": cgroup_facts(),
            "meminfo": meminfo_facts(),
            "dependencies": dependency_facts(),
        },
    }

    spark = build_session(evidence)
    bundle["pre"]["jvm"] = jvm_runtime_facts(spark)
    bundle["pre"]["jvm_input_arguments"] = jvm_input_arguments(spark)
    bundle["pre"]["catalog_state"] = catalog_state(spark)
    try:
        bundle["pre"]["source_inventory"] = source_inventory(
            args.source_bucket, OBSERVATIONS_PREFIX
        )
    except Exception as exc:
        bundle["pre"]["source_inventory_error"] = str(exc)[:1000]

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{WAREHOUSE_NAME}")

    # Write the pre-run half NOW. If the JVM dies hard, this survives.
    (evidence / "replay.json").write_text(
        json.dumps(bundle, indent=2, default=str), encoding="utf-8"
    )

    sampler = HeapSampler(spark, evidence / "heap_samples.jsonl",
                          interval=args.sample_interval)
    sampler.start()

    started = time.time()
    nodes: List[Dict[str, object]] = []
    failure: Dict[str, object] = {}
    success = False
    try:
        from dbt.cli.main import dbtRunner

        result = dbtRunner().invoke([
            "run", "--full-refresh", "--select", args.selector,
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROJECT_DIR,
            "--target", "spark",
        ])
        success = bool(result.success)
        if getattr(result, "result", None) is not None:
            for node in getattr(result.result, "results", []) or []:
                nodes.append({
                    "node": getattr(node.node, "name", "?"),
                    "status": str(node.status),
                    "seconds": round(getattr(node, "execution_time", 0.0), 2),
                    "message": (node.message or "")[:8000],
                })
        if not success and getattr(result, "exception", None) is not None:
            failure["exception"] = str(result.exception)[:16000]
    except BaseException as exc:  # noqa: BLE001 - OOM is an Error, not Exception
        failure["exception"] = f"{type(exc).__name__}: {exc}"[:16000]
        failure["traceback"] = traceback.format_exc()[:16000]
    finally:
        sampler.stop()

    # The first node that did not pass IS the failure site; dbt reports the
    # rest as skipped, and the original VM report named the wrong culprit by
    # reading the last error rather than the first.
    first_failed = next(
        (n for n in nodes if "success" not in n["status"].lower()), None
    )
    combined = " ".join(filter(None, [
        str(failure.get("exception", "")),
        str(failure.get("traceback", "")),
        str((first_failed or {}).get("message", "")),
    ]))

    bundle["run"] = {
        "seconds": round(time.time() - started, 2),
        "dbt_success": success,
        "nodes": nodes,
        "first_failed_node": (first_failed or {}).get("node"),
        "failure": failure,
        "phase": classify_failure(combined) if not success else "n/a_passed",
    }
    # Post-run facts are read from the KERNEL, so they are valid even if the
    # JVM is gone by now.
    bundle["post"] = {
        "cgroup": cgroup_facts(),
        "meminfo": meminfo_facts(),
        "heap_pool_peaks": heap_pool_peaks(spark),
        "catalog_state": catalog_state(spark),
    }

    (evidence / "replay.json").write_text(
        json.dumps(bundle, indent=2, default=str), encoding="utf-8"
    )

    print(f"\ndbt_success={success} phase={bundle['run']['phase']}")
    print(f"first_failed_node={bundle['run']['first_failed_node']}")
    for node in nodes:
        print(f"  {node['node']:44s} {node['status']:10s} {node['seconds']}s")
    print(f"cgroup memory.peak={bundle['post']['cgroup'].get('memory.peak')}")
    print(f"cgroup memory.events={bundle['post']['cgroup'].get('memory.events')}")
    print(f"heap peak={bundle['post']['heap_pool_peaks'].get('peak_heap_used_total')}")
    print(f"Evidence: {evidence}")
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
