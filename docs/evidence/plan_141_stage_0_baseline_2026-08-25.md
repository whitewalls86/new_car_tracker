# Plan 141 Stage 0 production baseline — 2026-08-25

All observations in this document came from read-only queries against the
production host. No container was recreated and no production configuration or
data was changed.

## Window and queries

The Loki queries completed between 14:49 and 14:50 UTC on 2026-08-25 and used
a rolling 24-hour window:

```logql
sum by (service, source, level) (
  count_over_time({service=~".+"}[24h])
)

sum by (service, source, level) (
  bytes_over_time({service=~".+"}[24h])
)

sum(count_over_time({service=~".+", source=""}[24h]))

sum(count_over_time({service=~"airflow-.+|oauth2-proxy", level=""}[24h]))
```

`source` was absent from every returned stream before Plan 141. A blank source
cell below therefore means a missing label, not an intentionally empty value.

| service | source | level | records | line bytes |
|---|---|---:|---:|---:|
| archiver | missing | INFO | 325 | 50,478 |
| dbt_runner | missing | INFO | 48 | 9,936 |
| ops | missing | INFO | 6 | 762 |
| processing | missing | INFO | 801,758 | 162,585,188 |
| processing | missing | WARNING | 196 | 46,256 |
| scraper | missing | INFO | 352,234 | 91,559,473 |
| scraper | missing | WARNING | 850 | 178,088 |
| airflow-dag-processor | missing | missing | 2,841 | 340,929 |
| airflow-scheduler | missing | missing | 434 | 1,217,599 |
| oauth2-proxy | missing | missing | 9,311 | 1,355,184 |
| **Total** |  |  | **1,168,003** | **257,343,893** |

The explicit missing-label queries, executed a few seconds later against the
moving window, returned:

- missing `source`: 1,166,163 records;
- missing `level` on the selected severity-bearing stdout sources: 12,586
  records.

The small difference from the grouped total is expected because the queries
did not share an atomic end timestamp and the application streams were active.

## Capacity projection

- Current Loki volume size: 4,146,816,434 bytes (4.15 GB).
- `/mnt/data` available: 133,095,424,000 bytes (133.10 GB).
- Observed Loki line bytes/day: 257,343,893 bytes (257.34 MB).
- Straight-line 90-day projection: 23,160,950,370 bytes (23.16 GB, 21.57 GiB).

`bytes_over_time` measures uncompressed log-line bytes rather than physical
chunk, index, and compactor overhead. The projection is therefore a rate
baseline, not a disk guarantee. It is comfortably below current filesystem
headroom, and Stage 4 must repeat both the rate and physical-volume checks after
the corrected filters have soaked.

## `ct-403-log-spike` decision evidence

The old query was `count_over_time({service="scraper"} |= "403" [5m])`.
Over the sampled 24 hours its matches grouped as follows:

| level | logger | text matches |
|---|---|---:|
| INFO | scraper | 5,750 |
| INFO | scraper.processors.scrape_results | 3 |
| INFO | shared.minio | 1,342 |
| WARNING | scraper | 688 |

The corresponding Prometheus counter recorded approximately 49 actual detail
fetch outcomes with `outcome="403"`. Samples showed the INFO matches came from
UUID fragments, timestamps, byte counts, hashes, and object keys. Samples also
showed each real failed request emits one stable warning beginning
`detail fetch 403 listing_id=...`; the current `app.log*` glob duplicated some
warnings across the active and rotated files.

The two alerts are therefore retained with separate contracts. The existing
UID remains `ct-403-log-spike` so alert history and notification references do
not break, but it no longer reads logs:

- `ct-403-log-spike` is a metric-based early partial-degradation detector. It
  fires when more than 5% of at least 25 detail fetches return the exact
  `outcome="403"` counter value over five minutes.
- `ct-detail-fetch-failing` remains the metric-based sustained-failure detector.
  It requires more than twenty attempts and zero successes over twenty minutes.

The 24-hour aggregate 403 rate was about 0.18% (49 of 27,260 attempts), so 5%
is well above observed background noise while still detecting partial failure
long before the 100%-failure rule. At the 25-attempt floor, one 403 is 4% and
does not alert while two are 8% and do. This keeps useful early warning coverage without
allowing arbitrary text or `shared.minio` INFO lines to page.

## Fixture provenance and continuation policy

The versioned fixture corpus is
`tests/fixtures/observability/plan_141_log_contract.json`. It preserves the
observed application JSON, Airflow bracketed-severity, OAuth access, and OAuth
lifecycle shapes after replacing identifiers and user/request data. No Airflow
ERROR or CRITICAL appeared in the seven-day sample, so those two cases are
explicit shape-preserving variants rather than claimed observations.

Airflow traceback and table continuation lines have no bracketed severity.
They are explicitly dropped as `airflow_unclassified_control_plane`; the
initiating bracketed ERROR or CRITICAL record is retained. This prevents an
unclassified continuation from silently looking like a classified event.
