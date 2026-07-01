# Plan 116: Estimate Recompression Savings Script

## Goal

Design and implement a safe, read-only evidence-gathering script that estimates
how much bronze HTML storage could be recovered by recompressing existing
`.html.zst` objects from zstd level 3 (current) to level 9.

This script is a prerequisite for deciding whether Plan 110 Track A (the
`ZSTD_LEVEL = 9` bump) is worth retrofitting to existing objects via a
one-off recompression pass.

---

## Context

From Plan 110:

- Current write path (`write_html()`) uses `ZSTD_LEVEL = 3`.
- Plan 110 Track A proposes bumping to level 9 for new writes.
- Expected gain for new objects: 15–25%.
- Existing ~5.8M detail objects remain at level 3 indefinitely unless a
  recompression pass is added.

This script answers: what is the actual savings distribution on real
production data, before we build a recompression tool?

**Key layout** (from `shared/minio.py` `make_key()`):

```
html/year={year}/month={month}/artifact_type={artifact_type}/{uuid}.html.zst
```

There is no day partition in the key path. Day tracking in existing scripts
derives from object modification metadata, not the key itself.

---

## Script: `scripts/estimate_recompression_savings.py`

### CLI Interface

```
scripts/estimate_recompression_savings.py [OPTIONS]

Selector (mutually exclusive groups):
  --prefix PREFIX         Exact MinIO prefix, e.g.
                          html/year=2026/month=5/artifact_type=detail_page/
                          (overrides --year/--month/--artifact-type)

  --year YEAR             Calendar year, e.g. 2026
  --month MONTH           Calendar month integer, e.g. 5 (requires --year)
  --artifact-type TYPE    detail_page | results_page  [default: detail_page]

Safety/performance:
  --limit N               Stop after scanning N objects total [default: 0 = no limit]
  --sample-rate RATE      Float 0.0–1.0: fraction of listed objects to actually
                          download and measure  [default: 0.05]
  --max-bytes BYTES       Stop after downloading this many compressed bytes
                          [default: 0 = no limit]
  --progress-every N      Print a progress line every N objects scanned [default: 500]
  --random-sample         Use Bernoulli sampling (random.random() < rate) instead of
                          systematic every-Nth. Both are unbiased for UUID-named
                          objects; systematic is reproducible, random is simpler.
  --json-out PATH         Write final summary JSON to this path

Other:
  --bucket BUCKET         MinIO bucket  [default: $MINIO_BUCKET or 'bronze']
  --log-level LEVEL       DEBUG | INFO | WARNING  [default: INFO]
```

#### Concrete examples

```bash
# Quick probe: 5% of one recent month, stop after 100MB read
python scripts/estimate_recompression_savings.py \
  --year 2026 --month 6 --artifact-type detail_page \
  --sample-rate 0.05 --max-bytes 104857600

# Exact prefix, 1% sample, save JSON
python scripts/estimate_recompression_savings.py \
  --prefix html/year=2026/month=5/artifact_type=results_page/ \
  --sample-rate 0.01 --json-out /tmp/savings_results_2026_05.json

# Tiny smoke test: first 50 objects at 100% sample rate
python scripts/estimate_recompression_savings.py \
  --year 2025 --month 12 --limit 50 --sample-rate 1.0

# Full-year scan at 2% sample (estimate total corpus savings)
python scripts/estimate_recompression_savings.py \
  --year 2025 --artifact-type detail_page --sample-rate 0.02
```

---

### Internal Functions and Classes

```python
# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ObjectInfo:
    key: str           # bare key, no bucket prefix
    size: int          # compressed bytes (from listing metadata)

@dataclass
class MeasurementResult:
    key: str
    old_compressed: int    # bytes as stored (level 3)
    raw_bytes: int         # decompressed size
    new_compressed: int    # bytes after level-9 recompression
    saved_bytes: int       # old_compressed - new_compressed
    error: str | None      # non-None if this object failed

@dataclass
class Stats:
    scanned: int = 0
    sampled: int = 0
    skipped: int = 0
    failed: int = 0
    listed_bytes: int = 0          # sum of .size for all listed objects
    old_compressed_bytes: int = 0  # sampled objects, compressed as-stored
    raw_bytes_total: int = 0       # sampled objects, decompressed
    new_compressed_bytes: int = 0  # sampled objects, level-9 recompressed
    failed_keys: list[str] = ...   # up to 5 example failed keys

# ── Listing ───────────────────────────────────────────────────────────────────

def build_prefixes(args) -> list[str]:
    """
    Resolve CLI selectors to one or more MinIO prefixes.
    --prefix  → single-element list, passed through as-is.
    --year [+ --month] + --artifact-type → discover all matching month
    directories under html/ and return one prefix per month.
    If --year given without --month, walks all month= subdirs for that year.
    """

def discover_months_for_year(fs, bucket, year, artifact_type) -> list[tuple[int, int]]:
    """List month integers present under html/year=Y/."""

def iter_prefix(fs, bucket, prefix) -> Iterator[ObjectInfo]:
    """
    Yield ObjectInfo for every .html.zst file under prefix.
    Uses fs.ls(prefix, detail=True) — same pattern as
    estimate_html_duplicate_storage.py.
    """

# ── Sampling ─────────────────────────────────────────────────────────────────

def make_sampler(sample_rate, random_sample) -> Callable[[int], bool]:
    """
    Return sampler(scan_index) -> bool.
    Systematic: scan_index % round(1/sample_rate) == 0.
    Random (Bernoulli): random.random() < sample_rate.
    """

# ── Measurement ──────────────────────────────────────────────────────────────

def measure_object(client, bucket, obj) -> MeasurementResult:
    """
    Download obj.key, decompress with zstd, recompress at level 9.
    Returns MeasurementResult. Never raises — errors go into result.error.
    Never calls put_object, delete_object, or any write API.

    Steps:
      1. client.get_object(Bucket=bucket, Key=obj.key)["Body"].read()
      2. zstd.ZstdDecompressor().decompress(compressed)
      3. zstd.ZstdCompressor(level=9).compress(raw)
      4. saved = len(compressed) - len(recompressed)
    """

# ── Progress + output ─────────────────────────────────────────────────────────

def log_progress(stats, current_key, log_every) -> None:
    """Emit a single INFO log line when stats.scanned % log_every == 0."""

def print_summary(stats, sample_rate) -> None:
    """Print final table + extrapolation + recommendation to stdout."""

def recommendation(savings_pct) -> str:
    """Return recommendation string based on savings_pct."""

def parse_args() -> argparse.Namespace
def main() -> int
```

---

### MinIO Access Approach

**Listing**: `get_s3fs()` → `fs.ls(prefix, detail=True)`. Metadata only; no
object bodies downloaded. Consistent with `estimate_html_duplicate_storage.py`.
Returns `size` in the detail dict.

**Downloading** (sampled objects only): `get_boto3_client()` →
`client.get_object(Bucket=bucket, Key=key)["Body"].read()`. Returns raw
compressed bytes. This is the same client as `read_html()` in
`shared/minio.py`, but we skip the final decompression to keep control of both
sizes.

`read_html()` is not called directly because it discards the compressed bytes
after decompression, making size comparison impossible.

**No DuckDB. No Parquet reads. No cross-service joins.**

---

### Sampling Strategy

**Systematic (default)**: compute `stride = round(1 / sample_rate)`, sample
object `i` if `i % stride == 0`. With `sample_rate=0.05`, stride = 20, so
objects 0, 20, 40, … are sampled. Reproducible — re-running the same args
produces the same sample.

**Why this is safe for MinIO bronze keys**: filenames are `uuid4()`. Listing
order is lexicographic on UUIDs, which is effectively random. There is no
ordering bias (no "small objects first" or similar artifact).

**Why this avoids the expensive full-scan problem**: the previous audit scripts
joined Parquet files via DuckDB httpfs, triggering full-corpus Parquet reads to
find artifact paths. Here there is no join. We list objects in MinIO metadata
and download only the sampled fraction. At 5% on ~5.8M detail objects, we
download ~290K objects. At ~15KB average compressed size, that is ~4.3 GB of
compressed reads — tractable in under an hour on the server.

**`--max-bytes` as a hard ceiling**: to spend at most 500MB of bandwidth on a
quick probe, set `--max-bytes 524288000`. The estimate from whatever was
measured before the ceiling is still valid.

---

### Progress Logging Format

Each progress line is a single `LOG.info()` call:

```
2026-07-01 10:23:45 INFO  PROGRESS | scanned=500 sampled=25 bytes_read=375.2 KiB savings=19.2% failures=0 | html/year=2026/month=6/artifact_type=detail_page/3f2a...html.zst
2026-07-01 10:24:12 INFO  PROGRESS | scanned=1000 sampled=50 bytes_read=748.1 KiB savings=18.7% failures=1 | html/year=2026/month=6/artifact_type=detail_page/7c9b...html.zst
2026-07-01 10:31:44 INFO  MONTH DONE | year=2026 month=6 listed=84203 bytes=1.23 GiB
```

Final summary is a formatted block to stdout (not via logging):

```
=== Recompression Savings Estimate ===
Scanned objects:         84,203
Sampled objects:          4,211  (5.0% sample rate)
Skipped objects:         79,969
Failed objects:               6  (keys: [...])

Sampled bytes (old, level-3):   62.7 MiB
Decompressed raw bytes:        341.4 MiB
Estimated bytes (new, level-9): 51.3 MiB
Estimated saved bytes:          11.4 MiB
Estimated savings:              18.2%

--- Extrapolated to full scanned prefix ---
Listed prefix size:         1.23 GiB
Projected savings:          223.9 MiB  (~18.2%)

=== Recommendation ===
Savings >= 15% -> WORTH IT
Adding a manual recompression pass (or shipping Track A level-9 bump now) is
justified. Existing objects account for ~224 MiB of recoverable storage.
Consider a one-off recompression batch after bumping ZSTD_LEVEL for new writes.
```

---

### Failure Handling

Inside `measure_object`:
- Wraps `client.get_object()` in `try/except (ClientError, Exception)`.
- Wraps `zstd.ZstdDecompressor().decompress()` in `try/except ZstdError`.
- Returns a `MeasurementResult` with `error=str(exc)` on any failure.
- Never raises.

In the main loop:
- `stats.failed += 1`
- `stats.failed_keys.append(obj.key)` if `len(stats.failed_keys) < 5`
- `LOG.warning("measure failed: %s — %s", obj.key, result.error)` at WARNING level

At summary:
- Failed keys printed if non-empty.
- If `failed / sampled > 0.1` (>10% failure rate), additional WARNING suggesting
  corrupt objects or a credential issue.

---

### Recommendation Thresholds

| Savings | Output |
|---------|--------|
| ≥ 15% | **WORTH IT** — add manual recompression tool |
| 5–15% | **MAYBE** — worth it if storage pressure is real |
| < 5% | **SKIP** — focus on Plan 114 section decomp or refresh reduction instead |

---

## Testing

### File: `tests/scripts/test_estimate_recompression_savings.py`

#### Group A — zstd round-trip math
- Compress known HTML bytes at level 3, feed to `measure_object` mock, assert
  `old_compressed`, `raw_bytes`, and `new_compressed` are all correct.
- Assert `new_compressed <= old_compressed` for typical compressible HTML.
- Assert `saved_bytes == old_compressed - new_compressed` exactly.
- Edge case: incompressible data → no crash, savings_pct near 0.

#### Group B — `measure_object` with mocked boto3
- Mock `client.get_object` to return known compressed bytes.
- Assert `MeasurementResult` fields are correct.
- Assert `client.put_object` is never called.
- Assert `client.delete_object` is never called.
- Simulate `ClientError` from `get_object` → `result.error` set, no raise.
- Simulate corrupt zstd bytes → `result.error` set, no raise.

#### Group C — `build_prefixes` and prefix construction
- `--prefix html/year=2026/month=5/artifact_type=detail_page/` → exact passthrough.
- `--year 2026 --month 5 --artifact-type detail_page` → correct prefix string.
- `--year 2026` without `--month`: mock `discover_months_for_year` to return
  `[(2026, 3), (2026, 4)]` → two prefixes returned.
- `--month` without `--year` → argparse exits non-zero.

#### Group D — sampler logic
- Systematic, rate=0.5, 10 objects → exactly objects 0, 2, 4, 6, 8 sampled.
- Systematic, rate=0.1, 100 objects → exactly 10 sampled.
- Bernoulli, rate=0.0 → none sampled (mock `random.random` returns 0.5).
- Bernoulli, rate=1.0 → all sampled.

#### Group E — extrapolation math
- `listed_bytes=1_000_000`, `old_compressed_bytes=50_000`,
  `new_compressed_bytes=40_000` → savings_pct=20%, extrapolated_saved=200_000.
  Assert summary prints these values.

#### Group F — failure accumulation
- Call `measure_object` 10 times, 3 return `error != None`.
- Assert `stats.failed == 3`, `len(stats.failed_keys) <= 5`.
- Assert `stats.sampled == 10` (failed objects still count as sampled).

#### Group G — read-only contract (integration-style)
- Mock boto3 client and s3fs client.
- Run `main()` end-to-end with mocked listing and downloads.
- Assert that the full set of boto3 method calls contains only `get_object`
  and listing equivalents. Assert `put_object`, `delete_object`,
  `copy_object` are never called.

---

## Files

| Action | Path |
|--------|------|
| ADD | `scripts/estimate_recompression_savings.py` |
| ADD | `tests/scripts/test_estimate_recompression_savings.py` |

No existing files change. The script imports only from `shared.minio` (already
installed in every container that runs scripts). No new dependencies — `boto3`,
`zstandard`, and `s3fs` are already present.

**Container**: run from any container with `shared/` mounted and `MINIO_*` env
vars set. Processing is the natural choice. On the server:

```bash
docker exec -it processing python scripts/estimate_recompression_savings.py \
  --year 2026 --month 6 --sample-rate 0.05
```

---

## Open Questions

**Q1: Level-3 assumption for existing objects.**
The script assumes all `.html.zst` objects were written at level 3. If
`ZSTD_LEVEL` was ever different during development, the savings estimate would
reflect level 9 vs whatever they actually are — arguably more useful. No
corrective action needed.

**Q2: Memory per object.**
Peak memory per `measure_object` call is `old_compressed + raw + new_compressed`.
For a 50 KB compressed blob decompressing to 300 KB, peak is ~400 KB. Objects
are processed sequentially, so memory usage is constant. Safe.

**Q3: `results_page` vs `detail_page`.**
Results pages are larger and may contain more volatile regions. Run the script
separately for each artifact type using `--artifact-type`. The design supports
this natively.

**Q4: Savings weighting.**
Savings percent is weighted by bytes (`total_saved / total_old_compressed`),
not by count. This is the correct metric for storage planning.

**Q5: Systematic vs random sampling.**
For UUID-named objects in lexicographic listing order, both are effectively
equivalent. Systematic is the default because it is reproducible — re-running
with the same args always produces the same sample, making it easier to validate
results across runs.

---

## Out of Scope

- Actual recompression of existing objects (separate tool, post-evidence).
- Automatic retention or deletion. See Plan 114.
- Adaptive detail scheduling. See Plans 111–113.
