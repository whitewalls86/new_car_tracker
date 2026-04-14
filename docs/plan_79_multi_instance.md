# Plan 79: Multi-Instance Detail Scraping

**Status:** On hold — partial implementation done, not currently needed
**Priority:** Low

Cars.com flagged the home server IP after sustained 50K+/day scraping (cumulative IP reputation, not per-request rate limiting). ~55K active listings need refreshing. The fix is to distribute detail scraping across multiple Oracle Cloud instances so no single IP exceeds the safe threshold (~20K/day).

## Architecture decisions (2026-04-03)

- **Scraper writes artifacts directly to Postgres** — raw HTML stored as `bytea` in DB, no more passing HTML through n8n JSON
- **Job Poller workflow eliminated** — scraper handles DB insertion and run status updates itself
- **Single n8n "Scrape Detail Batch" workflow** with round-robin worker selection via `detail_worker_hosts` DB table
- **"Parse Detail Pages" folded into unified "Results Processing" workflow** — handles both `detail_page` and `results_page` artifact types
- **Slim `detail_scraper/` service** (~200MB, no Playwright) for remote worker VMs
- **Dual-write to disk + DB for now** — cut file writes later once Parquet archive (Plan 72) is in place

## Why this approach
IP reputation is cumulative over days, not burst-based. Historical safe ceiling was ~50K/day. With 2-3 IPs at ~20K each, we stay under the threshold per IP while maintaining or increasing total throughput.

## Implementation checklist

### New DB table: `detail_worker_hosts`
```sql
CREATE TABLE detail_worker_hosts (
    host        text PRIMARY KEY,
    label       text,
    active      boolean NOT NULL DEFAULT true,
    last_used_at timestamptz
);
```

### New service: `detail_scraper/`
- Slim FastAPI service — no Playwright, no SRP logic
- Endpoints: `POST /scrape/detail/batch`, `GET /health`
- Writes artifacts (including `raw_html` bytea) directly to Postgres
- Handles run status updates — no Job Poller needed
- ~200MB image (vs ~2GB for full scraper with Playwright)

### n8n changes
- New "Scrape Detail Batch" workflow — round-robins across hosts in `detail_worker_hosts`
- "Results Processing" workflow — unified handler for both detail and SRP artifact types
- Retire old "Scrape Detail Pages" and "Job Poller V2" workflows once new flow is stable

### SRP pipeline (same treatment)
- SRP scraper also gets DB-write treatment (no more file-based artifact passing through n8n)

## Relationship to Plan 72 (Parquet archival)
Plan 72 archives HTML from disk to MinIO Parquet before deletion. Once multi-instance is stable and writing to DB, the archival path changes: archive from DB bytea → Parquet rather than from disk. The dual-write period (disk + DB) bridges this transition.
