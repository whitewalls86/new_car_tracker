"""Plan 135 Stage 4: what is filling each disk.

A **fixed watchlist**, not a filesystem explorer. Roughly twenty series that
between them account for most of both volumes, published through
node-exporter's textfile collector. When a disk grows, one band on the graph
grows, and that is the answer -- no SSH session, no ``du`` at 3am.

Two things about the cost, because the naive version of this job is a
twenty-minute disk thrash that competes with live scraping:

* The root disk is cheap (~558k inodes) and is walked on every run.
* ``/mnt/data`` is not. The expensive volumes are walked weekly, off-peak, and
  **carried forward** in between.

**Walk cost is O(inodes), not O(bytes)** -- see ``HIGH_INODE_VOLUMES``. That
distinction is not a detail; classifying a volume by its size on disk is the
assumption that failed on the first production run.

Carry-forward is why this writes one file rather than a cheap-daily file plus
an expensive-weekly file. Splitting a metric family across two ``.prom`` files
makes node-exporter rewrite the HELP text to name both files and then reject
the second one as inconsistent -- prometheus/node_exporter#1885. So the
previous file is the input to the next one.

The rule that falls out is worth stating once: **every series either gets a
fresh measurement, or keeps its previous value together with its previous
timestamp.** A skipped weekly walk and a failed walk are deliberately
indistinguishable in the value, and ``cartracker_disk_usage_measured_timestamp_seconds``
is the only thing that separates either from a live reading. A carried value
with no timestamp beside it is how a gauge goes stale in silence.
"""
import logging
import os
import re
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("archiver")

TEXTFILE_NAME = "cartracker_disk_usage.prom"

PATH_METRIC = "cartracker_path_bytes"
VOLUME_METRIC = "cartracker_volume_bytes"
# Plan 136 Stage 1 is building cartracker_metrics_last_success_timestamp_seconds
# as the project-wide staleness convention. This is not a competing one: it is
# per-series rather than per-run, because carry-forward means two series in the
# same file can legitimately have been measured a week apart. Fold it into
# Plan 136's pattern if that turns out to subsume it.
MEASURED_AT_METRIC = "cartracker_disk_usage_measured_timestamp_seconds"

# The root disk. Measured every run.
ROOT_PATHS: Tuple[str, ...] = (
    "/var/lib/containerd",
    "/var/lib/docker/containers",
    "/var/log/journal",
    "/usr",
    "/var/lib/snapd",
)

# /mnt/data, few enough inodes to walk on every run.
DAILY_VOLUMES: Tuple[str, ...] = (
    "cartracker_loki_data",
    "cartracker_analytics_db",
    "cartracker_pgdata",
    "cartracker_prometheus_data",
)

# Inode counts that put a volume in the slow tier, and where the number came
# from. A volume may only be promoted here on a measurement recorded in this
# dict -- see ``test_tier_membership_is_decided_by_inodes``.
#
# ``cartracker_airflow_logs`` is why this dict exists. It was originally daily,
# classified on size: 6.3 GiB is obviously small. But it holds ~1.2M inodes,
# roughly twice the entire root filesystem, and on the first production run it
# was **397s of a 456s walk -- 87%**. Size said cheap; inodes said the opposite,
# and inodes were right.
MEASURED_INODES = {
    "cartracker_parquet_data": 4_000_000,   # MinIO bronze; du has run 20+ min
    "cartracker_airflow_logs": 1_200_000,   # 6.3 GiB, 397s of a 456s walk
}

# /mnt/data, too many inodes for the daily walk. Measured weekly, carried
# forward in between. Plan 135 Stage 5d prunes Airflow task logs to 30 days,
# after which ``cartracker_airflow_logs`` can legitimately move back to daily.
HIGH_INODE_VOLUMES: Tuple[str, ...] = (
    "cartracker_parquet_data",
    "cartracker_airflow_logs",
)

DEFAULT_ROOT_PREFIX = "/measure/root"
DEFAULT_VOLUME_PREFIX = "/measure/volumes"
DEFAULT_DU_TIMEOUT_SECONDS = 3600

_LABEL_RE = re.compile(r'(\w+)="([^"]*)"')
_SERIES_RE = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}\s+([0-9.eE+-]+)\s*$")


def textfile_dir() -> Optional[str]:
    """Where node-exporter reads from, or None if this process cannot publish.

    Absence is the signal that the job is on the wrong service: only
    pack-worker carries the read-only host mounts and the writable textfile
    volume. See ``_require_disk_usage_host_mounts`` in ``archiver/app.py``.
    """
    return os.environ.get("DISK_USAGE_TEXTFILE_DIR") or None


def measure_path(path: str, timeout: int = DEFAULT_DU_TIMEOUT_SECONDS) -> Dict[str, Any]:
    """Physical bytes used by *path*.

    ``--block-size=1`` reports allocated blocks rather than apparent size,
    which is the entire point: MinIO's per-object floor -- 4 KiB for the
    object directory plus a 4 KiB-rounded ``xl.meta`` -- is invisible in
    apparent size, and that floor is what this plan exists to make visible.

    ``-x`` stops at the filesystem boundary so a nested bind mount cannot be
    counted into a total it does not belong to.
    """
    try:
        proc = subprocess.run(
            ["du", "-s", "-x", "--block-size=1", path],
            capture_output=True, text=True, timeout=timeout,
        )
    except FileNotFoundError:
        return {"bytes": None, "error": "du is not available in this image"}
    except subprocess.TimeoutExpired:
        return {"bytes": None, "error": f"du exceeded {timeout}s on {path}"}

    total = (proc.stdout or "").strip().split("\t", 1)[0].strip()
    if total.isdigit():
        # du exits non-zero when it could not descend into *some* subtree while
        # still printing a valid total for the rest. A partial total beats no
        # series at all, so take it and say so.
        if proc.returncode != 0:
            logger.warning(
                "disk_usage: du reported errors under %s but returned a total: %s",
                path, (proc.stderr or "").strip()[:200],
            )
        return {"bytes": int(total), "error": None}

    detail = (proc.stderr or "").strip()[:200] or f"du exited {proc.returncode}"
    return {"bytes": None, "error": detail}


def parse_previous(text: str) -> Dict[Tuple[str, str], float]:
    """Read a previously written .prom back into {(metric, target): value}.

    Anything unparseable is skipped rather than raised on: a corrupt or
    truncated previous file must not stop this run from writing a good one.
    """
    previous: Dict[Tuple[str, str], float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _SERIES_RE.match(line)
        if not match:
            continue
        metric, label_text, value = match.groups()
        labels = dict(_LABEL_RE.findall(label_text))
        target = labels.get("path") or labels.get("volume") or labels.get("target")
        if not target:
            continue
        try:
            previous[(metric, target)] = float(value)
        except ValueError:
            continue
    return previous


def read_previous(directory: str) -> Dict[Tuple[str, str], float]:
    try:
        with open(os.path.join(directory, TEXTFILE_NAME), "r") as handle:
            return parse_previous(handle.read())
    except FileNotFoundError:
        return {}
    except OSError as e:
        logger.warning("disk_usage: could not read the previous textfile: %s", e)
        return {}


def render_prom(readings: List[Dict[str, Any]]) -> str:
    """Render readings as Prometheus text format.

    Label values are not escaped because every one of them comes from the
    module-level watchlist above. Keep it that way -- node-exporter emits
    garbage for a malformed file rather than skipping the bad line.
    """
    lines = [
        f"# HELP {PATH_METRIC} Physical bytes used by a watched path on the root disk (du -s -x).",
        f"# TYPE {PATH_METRIC} gauge",
    ]
    lines += [
        f'{PATH_METRIC}{{disk="root",path="{r["target"]}"}} {r["bytes"]}'
        for r in readings if r["metric"] == PATH_METRIC and r["bytes"] is not None
    ]
    lines += [
        f"# HELP {VOLUME_METRIC} Physical bytes used by a Docker volume on /mnt/data (du -s -x).",
        f"# TYPE {VOLUME_METRIC} gauge",
    ]
    lines += [
        f'{VOLUME_METRIC}{{volume="{r["target"]}"}} {r["bytes"]}'
        for r in readings if r["metric"] == VOLUME_METRIC and r["bytes"] is not None
    ]
    lines += [
        f"# HELP {MEASURED_AT_METRIC} When this series was last walked, not last written.",
        f"# TYPE {MEASURED_AT_METRIC} gauge",
    ]
    lines += [
        f'{MEASURED_AT_METRIC}{{target="{r["target"]}"}} {r["measured_at"]:.0f}'
        for r in readings if r["bytes"] is not None and r["measured_at"] is not None
    ]
    return "\n".join(lines) + "\n"


def write_textfile(directory: str, text: str) -> str:
    """Write the .prom atomically: temp file in the same directory, then rename.

    node-exporter reads whatever is on disk at scrape time. Write in place and
    a scrape landing mid-write reads half a file and exports garbage, so the
    rename has to be the only thing the collector ever observes.
    """
    os.makedirs(directory, exist_ok=True)
    destination = os.path.join(directory, TEXTFILE_NAME)
    handle = tempfile.NamedTemporaryFile(
        mode="w", dir=directory, prefix=f".{TEXTFILE_NAME}.", delete=False,
    )
    try:
        with handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        # NamedTemporaryFile creates 0600 and node-exporter runs as nobody.
        os.chmod(handle.name, 0o644)
        os.replace(handle.name, destination)
    except BaseException:
        try:
            os.unlink(handle.name)
        except OSError:
            pass
        raise
    return destination


def _watchlist(root_prefix: str, volume_prefix: str, include_slow: bool):
    """[(metric, target, absolute_path, fresh)] for this run."""
    targets = [
        (PATH_METRIC, path, f"{root_prefix}{path}", True)
        for path in ROOT_PATHS
    ]
    # Not os.path.join: these are always POSIX paths inside a Linux container,
    # whatever OS happens to be running the tests.
    targets += [
        (VOLUME_METRIC, volume, f"{volume_prefix}/{volume}", True)
        for volume in DAILY_VOLUMES
    ]
    targets += [
        (VOLUME_METRIC, volume, f"{volume_prefix}/{volume}", include_slow)
        for volume in HIGH_INODE_VOLUMES
    ]
    return targets


def run_disk_usage(
    include_slow: bool = False,
    directory: Optional[str] = None,
    root_prefix: Optional[str] = None,
    volume_prefix: Optional[str] = None,
    timeout: int = DEFAULT_DU_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """Walk the watchlist and publish it (POST /disk-usage/run).

    Pass ``include_slow=True`` on the weekly run only: it adds the
    ``HIGH_INODE_VOLUMES``, which between them have run 20+ minutes.
    Everything else is walked every time.
    """
    directory = directory or textfile_dir()
    if not directory:
        raise RuntimeError(
            "DISK_USAGE_TEXTFILE_DIR is not set; this job needs the host mounts "
            "and textfile volume that only pack-worker carries"
        )
    root_prefix = root_prefix or os.environ.get("DISK_USAGE_ROOT_PREFIX") or DEFAULT_ROOT_PREFIX
    volume_prefix = (
        volume_prefix or os.environ.get("DISK_USAGE_VOLUME_PREFIX") or DEFAULT_VOLUME_PREFIX
    )

    previous = read_previous(directory)
    readings: List[Dict[str, Any]] = []

    for metric, target, path, fresh in _watchlist(root_prefix, volume_prefix, include_slow):
        carried = {
            "metric": metric,
            "target": target,
            "bytes": previous.get((metric, target)),
            "measured_at": previous.get((MEASURED_AT_METRIC, target)),
            "carried_forward": True,
            "error": None,
        }
        if not fresh:
            carried["error"] = "not scheduled this run"
            readings.append(carried)
            continue

        started = time.time()
        result = measure_path(path, timeout=timeout)
        if result["bytes"] is None:
            logger.warning("disk_usage: %s could not be measured: %s", target, result["error"])
            carried["error"] = result["error"]
            readings.append(carried)
            continue

        readings.append({
            "metric": metric,
            "target": target,
            "bytes": result["bytes"],
            "measured_at": started,
            "carried_forward": False,
            "error": None,
        })

    written = write_textfile(directory, render_prom(readings))

    fresh_count = sum(1 for r in readings if not r["carried_forward"])
    failed = [r for r in readings if r["error"] and r["error"] != "not scheduled this run"]
    lost = [r for r in readings if r["bytes"] is None]
    return {
        "include_slow": include_slow,
        "textfile": written,
        "measured": fresh_count,
        "carried_forward": len(readings) - fresh_count,
        "failed": len(failed),
        "unpublished": [r["target"] for r in lost],
        "results": readings,
    }
