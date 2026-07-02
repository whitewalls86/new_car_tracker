# Plan 117: Storage + Adaptive Refresh Roadmap

## Goal

Coordinate Plans 110-114 into one implementation arc.

The individual plans are intentionally scoped, but the strategy only makes
sense when read as a sequence:

1. Normalize storage.
2. Make historical datasets snapshot-safe.
3. Build refresh features.
4. Backtest policies reproducibly.
5. Deploy a conservative production policy.
6. Separately test whether raw HTML can be retained longer through sectioned
   artifacts.

This roadmap is the required context document for Plans 110-114.

---

## Problem

Raw HTML storage growth is mostly driven by repeated detail fetches that produce
the same parsed business state.

The first instinct was whole-file deduplication, but evidence showed that
whole-file hashes are weak because Cars.com pages contain volatile regions:
tokens, timestamps, IDs, analytics metadata, Cloudflare fields, and JSON
ordering changes.

The better strategy is:

1. Clean up the physical storage layer.
2. Put reproducible snapshot/experiment tracking on top.
3. Use historical data to learn which listings actually need frequent detail
   refresh.
4. Reduce redundant future fetches.
5. Explore smarter raw HTML retention after the fetch policy is under control.

---

## Evidence

### Semantic Duplication

A DuckDB query over `silver/observations` grouped detail artifacts by a parsed
business-state fingerprint:

| Metric | Value |
|--------|-------|
| Total detail artifacts | 5,804,559 |
| Unique parsed listing states | 804,870 |
| Semantically duplicate artifacts | 4,999,689 |
| Semantic duplicate rate | 86.13% |

This means repeated detail fetches often capture no new business information.

### Whole-File HTML Hashing

A targeted audit sampled high-duplicate semantic groups and hashed stored HTML:

| Metric | Value |
|--------|-------|
| Sampled groups | 5 |
| Sampled artifacts | 25 |
| Groups with identical sampled HTML | 0 |
| Repeat compressed-hash matches | 0 |

The parsed vehicle state was identical, but full-page bytes differed.

### Compression

Plan 116 measured existing bronze HTML recompressed from zstd level 3 to level 9:

| Prefix | Savings |
|--------|---------|
| detail_page, June | 8.1% |
| results_page, June | 9.4% |
| detail_page, May | 8.0% |

Compression is worth standardizing, but it is not the primary storage strategy.

### Production Incident

Plan 115 showed that operational queue logic can accidentally create runaway
detail fetch loops. That bug is fixed, but it reinforces the need for adaptive
refresh logic to be observable, backtested, and guarded before production use.

---

## Implementation Arc

### Plan 110: Storage Layout Hygiene + Iceberg Readiness

Normalize the storage layer before experiments depend on it.

Buys:

- zstd level 9 as the new bronze HTML write standard
- write-time HTML storage metrics
- manual historical bronze recompression to bring old objects to the new
  standard
- Parquet lake audit across silver and ops event datasets
- canonical pre-Iceberg Parquet layout
- safe rewrite and verification path
- guarded/manual cleanup after validation

Does not buy:

- automatic raw HTML expiry
- whole-file HTML dedup
- Iceberg registration itself
- production refresh throttling

### Plan 111: Adaptive Refresh Feature Foundation

Build listing-state and volatility feature models.

Buys:

- parsed-state fingerprints
- contiguous state runs
- listing stability features
- dealer/model volatility priors
- first rule-based refresh score/tier

Does not buy:

- production scraping changes
- policy approval
- ML model training
- experiment tracking

### Plan 112: Iceberg + MLflow Refresh Policy Backtesting

Create the reproducible experiment layer and run policy simulations.

Buys:

- Iceberg snapshots for experiment inputs
- MLflow runs for params, metrics, artifacts, and snapshot IDs
- replay of candidate refresh policies against historical timelines
- quality gates for fetch reduction vs detection delay
- auditable evidence for any production policy

Does not buy:

- production claim-query changes
- online ML serving
- automatic policy promotion

### Plan 113: Production Adaptive Refresh Integration

Deploy the approved policy conservatively.

Buys:

- ops claim logic filters listings by `next_detail_fetch_after`
- policy version pinning
- escape hatches for newly discovered, SRP-recent, forced, and never-scraped
  listings
- shadow mode
- counters for throttled/fetched/escaped listings
- feature flag rollback

Does not buy:

- MLflow or Iceberg reads at claim time
- model serving
- unguarded production throttling

### Plan 114: Sectioned HTML Artifact Audit

Test whether raw HTML can be decomposed into reusable, recomposable sections.

Buys:

- evidence on whether section-level storage preserves parser replayability
- possible path to longer raw-page audit retention
- storage design that works around volatile full-page bytes

Does not buy:

- immediate production retention deletion
- adaptive refresh
- Iceberg/MLflow experiment tracking

---

## Dependency Graph

```text
Plan 110 -> Plan 111
Plan 110 -> Plan 112
Plan 111 -> Plan 112
Plan 112 -> Plan 113

Plan 114 is parallel, but it should use evidence from Plan 110 and the fetch
volume results from Plans 112-113.
```

Plan 110 comes first because Iceberg snapshots should not be built on a messy
or poorly understood Parquet layout.

Plan 111 can begin once the normalized source contract is clear. It does not
need production Iceberg to define feature logic, but its outputs should be
designed for Plan 112 snapshotting.

Plan 112 must precede Plan 113 because production throttling needs reproducible
evidence, not hand-tuned intuition.

---

## Non-Goals Across The Arc

- No automatic 30-day raw HTML deletion until Plan 114 proves a safer retention
  strategy.
- No whole-file HTML dedup as a primary strategy.
- No production adaptive refresh before backtesting.
- No ML serving in production.
- No Iceberg or MLflow calls in the hot ops claim path.
- No destructive cleanup before row counts, schema checks, and query checks pass.

---

## Success Criteria

### Storage

- Bronze HTML uses the new compression/metrics standard.
- Historical bronze recompression either completes for selected prefixes or is
  intentionally scoped with reports.
- Silver/ops Parquet layout is inventoried, normalized, and verified.
- Old layouts are cleaned only through guarded/manual operations.

### Experiment Reproducibility

- Iceberg snapshot IDs are available for backtest inputs.
- MLflow records policy params, dataset snapshot IDs, metrics, and artifacts.
- A backtest run can be reproduced from recorded metadata.

### Adaptive Refresh

- Candidate policy passes agreed quality gates.
- Production policy is pinned to an approved MLflow run/config.
- Shadow mode metrics match expected backtest behavior closely enough to enable.
- Rollout can be disabled without code changes.

### Raw HTML Retention

- Sectioned artifact audit proves or disproves parser-equivalent reconstruction.
- Retention decisions are based on measured replayability and storage savings,
  not a blunt age cutoff.

---

## Reading Order For Implementers

For any implementation in this arc, read:

1. `docs/ARCHITECTURE.md`
2. this roadmap
3. the specific plan being implemented
4. any referenced completed plans, especially Plan 109 and Plan 115 when
   touching compaction or refresh eligibility

Use graphify before implementation to orient around the relevant service and
data-flow boundaries.
