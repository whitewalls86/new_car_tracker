# Plan 79: Multi-Instance Detail Scraping

**Status:** Unblocked — resume when needed
**Priority:** Low — IP flagging not currently a problem; can start as soon as `scrape_detail_pages` Airflow DAG exists

The MinIO artifact store work originally scoped here has been extracted to Plan 97 as a core architectural requirement. Plan 79 now covers only the multi-VM scraper deployment.

---

## Background

Cars.com flagged the home server IP after sustained 50K+/day scraping (cumulative IP reputation, not per-request rate limiting). ~55K active listings need refreshing. The fix is to distribute detail scraping across multiple Oracle Cloud instances so no single IP exceeds the safe threshold (~20K/day).

---

## Dependencies

**Plan 97 (MinIO artifact store)** — complete. Scraper writes directly to MinIO; no shared filesystem required on remote VMs. ✓

**Ops coordination endpoints** (`claim-batch`, `release`) — implemented. ✓

**`scrape_detail_pages` Airflow DAG** — the only remaining hard dependency. Once this DAG exists, Plan 79 can start immediately. The DAG fans out to N scraper connections; adding more VM connections requires no DAG logic changes.

The slim scraper (Plan 71 Phase 7) is not required. The current scraper image runs on remote VMs for detail scraping — it carries unused SRP parsing code but that does not affect correctness or concurrency.

---

## Architecture

### Scraper VMs
Deploy the Plan 71 slim scraper image on 2–3 Oracle Cloud Free Tier instances (different IPs). Each instance is stateless — claims work from the ops service, fetches pages, writes artifacts directly to MinIO (per Plan 97), releases claims. No local state, no shared filesystem.

### Coordination
All scraper instances call the same ops service endpoints:
```
POST /scrape/claims/claim-batch   → returns N listing_ids; DB prevents overlap across instances
POST /scrape/claims/release       → releases claims after completion
```

`FOR UPDATE SKIP LOCKED` at the DB level handles concurrency. No round-robin logic needed in the orchestrator — each instance gets a non-overlapping batch automatically.

### Airflow DAG change
The `scrape_detail_pages` DAG fans out to N scraper instances instead of one. Worker URLs come from Airflow connections (`scraper_vm_1`, `scraper_vm_2`, etc.). The DAG does not need to track which instance claimed which batch.

---

## Implementation Checklist (when resumed)

- [ ] Confirm `scrape_detail_pages` Airflow DAG is live
- [ ] Provision 2–3 Oracle Cloud Free Tier ARM instances
- [ ] Confirm slim scraper image builds and runs without Playwright (detail scraping uses curl_cffi; Playwright is only needed for SRP)
- [ ] Add Airflow connections for each VM (`scraper_vm_1_url`, etc.)
- [ ] Update `scrape_detail_pages` DAG to fan out across VM connections
- [ ] Confirm concurrent claims from multiple instances don't overlap (DB constraint test)
- [ ] Monitor per-IP request volume stays under ~20K/day

---

## What Does Not Change

- `detail_scrape_claims` table and `FOR UPDATE SKIP LOCKED` concurrency model
- `ops.blocked_cooldown` hot table and `staging.blocked_cooldown_events` logic and 403 backoff
- Processing service logic — same parsing, same observation writes
- MinIO bucket structure — already the artifact store per Plan 97
