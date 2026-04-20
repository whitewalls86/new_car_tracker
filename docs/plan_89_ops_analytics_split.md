# Plan 89: Operational/Analytics dbt Split

**Status:** SUPERSEDED — do not implement
**Superseded by:** Plans 93 and 97

---

## Why This Plan Is Superseded

Plan 89 defined three application-owned tables to replace operational dbt models:
- `listing_to_vin` — VIN↔listing mapping
- `price_observations` — append-only price log in Postgres
- `vin_state` — current per-VIN state including `listing_state`

The architecture has since evolved. The core insight of Plan 89 was correct and is preserved. The specific implementation is superseded for three reasons:

**1. `price_observations` as an append-only Postgres log is wrong.** Append-only observation logs belong in MinIO (Parquet), not Postgres. The full observation history lives in MinIO silver (Plan 96). Postgres holds only the current/latest observation per listing — a small, fast, deletable HOT table.

**2. `vin_state` is eliminated.** Listing state (active vs. unlisted) is represented implicitly by table presence: a row in `price_observations` means active; a DELETE means unlisted. No explicit `listing_state` column is needed operationally.

**3. `listing_to_vin` is replaced by `vin_to_listing` with a simpler write contract.** The concept is the same; the schema is simplified and integrated with the `price_observations` upsert logic via a pre-upsert VIN lookup pattern.

---

## What Was Preserved

- The philosophy: Postgres owns only HOT operational data; analytics come from MinIO via DuckDB
- The direct write path owned by the processing service, not dbt
- The ops queue reading from application-owned tables, not dbt materialized tables
- The SQL-in-files pattern for queries

---

## Where the Implementation Lives

| Concern | Plan |
|---|---|
| `artifacts_queue` work queue, scraper→MinIO write | Plan 97 |
| `price_observations` + `vin_to_listing` schemas and write paths | Plan 93 |
| MinIO silver as primary observation store | Plan 96 |
| dbt decommission | Plan 90 |
