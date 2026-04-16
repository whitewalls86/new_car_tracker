# Plan 79: Multi-Instance Detail Scraping

**Status:** On hold — not currently needed. Resume if IP flagging returns.
**Priority:** Low — blocked on Plan 71 (Airflow migration) completing first.

Cars.com flagged the home server IP after sustained 50K+/day scraping (cumulative IP reputation, not per-request rate limiting). ~55K active listings need refreshing. The fix is to distribute detail scraping across multiple Oracle Cloud instances so no single IP exceeds the safe threshold (~20K/day).

---

## Dependency on Plan 71

Plan 71 (Airflow migration) is a prerequisite. It produces the two things this plan needs:

- **Slim scraper** — the scraper loses all parsing/observation logic and becomes a pure fetch machine. That is the deployable unit for remote VMs.
- **Ops coordination endpoints** — `POST /scrape/claims/claim-batch` and `POST /scrape/claims/release` move to the ops service. Multiple scraper instances on different VMs all call this central coordinator; the `FOR UPDATE SKIP LOCKED` DB-level pattern already handles concurrency correctly without any per-instance logic.

Do not implement Plan 79 before Plan 71 is complete. The partial implementation from 2026-04-03 (see below) made architectural decisions that have since been superseded.

---

## Architecture (revised)

### What was decided in the original plan (2026-04-03) that has changed

The original plan called for:
- Raw HTML stored as `bytea` in Postgres — **superseded.** DB blob storage adds row bloat, complicates the Parquet archiving path, and doesn't scale. See Plan 71 notes.
- Scraper handles run status updates itself — **superseded.** Run lifecycle and claim management belong in the ops service (Plan 71 coordination endpoints).
- n8n "Scrape Detail Batch" workflow with round-robin worker selection — **superseded.** n8n is decommissioned in Plan 71. Coordination moves to Airflow + ops service.
- Separate `detail_scraper/` service — **reconsidered.** The slim scraper produced by Plan 71 *is* this service. Deploy the same image on multiple VMs; don't maintain a separate service.

### Revised approach

**Scraper VMs:** Deploy the Plan 71 slim scraper image on 2–3 Oracle Cloud Free Tier instances (different IPs). Each instance is stateless — it claims work, fetches pages, writes artifacts, releases claims. No local state.

**File storage:** Raw artifact files are written to MinIO (shared object store, already in the stack). Each scraper VM writes to the same MinIO bucket via S3 API. The `filepath` column in `raw_artifacts` becomes an S3 URI (`s3://cartracker-artifacts/...`). Processing service on the main VM reads from MinIO by that URI — no change to the processing logic, only the read path changes from `open(filepath)` to `s3.get_object(uri)`.

This is the right path. It doesn't require a schema migration, doesn't change the cleanup/archiving architecture, and MinIO is already the archival destination.

**Coordination:** All scraper instances call the same ops service endpoints:
```
POST /scrape/claims/claim-batch   → returns N listing_ids; DB prevents overlap across instances
POST /scrape/claims/release       → releases claims after completion
```

No round-robin logic needed in the orchestrator. The DB-level `FOR UPDATE SKIP LOCKED` is the coordination mechanism. Each scraper gets a non-overlapping batch automatically.

**Airflow DAG change:** The `scrape_detail_pages` DAG fans out to N scraper instances instead of one. Worker URLs come from Airflow connections (one connection per VM). The DAG doesn't need to know which instance claimed which batch — the DB handles that.

**Worker registration:** Replace the `detail_worker_hosts` table with Airflow connections (`scraper_vm_1`, `scraper_vm_2`, etc.). Airflow is the source of truth for which VMs exist; the DB doesn't need to track this.

---

## Implementation checklist (when resumed)

### MinIO as primary artifact store
- [ ] Update scraper to write raw files to MinIO instead of local disk (`s3fs` or `boto3`)
- [ ] Update `raw_artifacts.filepath` writes to use S3 URI format
- [ ] Update processing service to read from MinIO by S3 URI
- [ ] Update cleanup workflow to delete from MinIO (already the archival store — simplifies cleanup)
- [ ] Remove local disk artifact writes from scraper

### Remote scraper VMs
- [ ] Provision 2–3 Oracle Cloud Free Tier ARM instances
- [ ] Confirm Plan 71 slim scraper image builds and runs without Playwright (SRP only needs Playwright; detail scraping uses curl_cffi)
- [ ] Add Airflow connections for each VM (`scraper_vm_1_url`, etc.)
- [ ] Update `scrape_detail_pages` DAG to fan out across VM connections

### Ops service
- [ ] Confirm `claim-batch` endpoint (from Plan 71) correctly handles concurrent claims from multiple instances
- [ ] Add `claimed_by_host` or similar to `detail_scrape_claims` if per-instance diagnostics are needed

### Validation
- [ ] Confirm no listing_id is claimed by two instances simultaneously (DB constraint test)
- [ ] Monitor per-IP request volume stays under ~20K/day threshold
- [ ] Confirm processing service handles MinIO reads correctly before disabling local file writes

---

## What does not change

- `detail_scrape_claims` table and `FOR UPDATE SKIP LOCKED` concurrency model
- `blocked_cooldown` / `stg_blocked_cooldown` logic and 403 backoff
- Processing service logic — same parsing, same observation writes, same `artifact_processing` status management
- dbt models downstream of observations
