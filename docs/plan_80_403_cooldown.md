# Plan 80: 403 Cooldown Blocking for Detail Scrape Queue

## Context
Detail page fetches occasionally return 403s from Cloudflare. Currently these listings are retried on the next run with no cooldown. The fix: record 403'd listing IDs in a `blocked_cooldown` table (raw counts only), let a dbt staging model own all retry/backoff logic (next_eligible_at, fully_blocked), filter them from `ops_detail_scrape_queue` via that staging model, and record 403s in the Job Poller workflow where they're already identified.

## Architecture
- **Raw table** (`blocked_cooldown`): just tracks counts and timestamps — no business logic
- **dbt** (`stg_blocked_cooldown`): computes `next_eligible_at`, `fully_blocked` — all backoff logic lives here, easy to tune
- **`ops_detail_scrape_queue`**: joins `stg_blocked_cooldown`, filters out cooling/blocked listings
- **Job Poller V2**: upserts into `blocked_cooldown` on the existing "403 Error" Switch branch, then calls Build DBT

---

## 1. New Table: `db/schema/blocked_cooldown.sql` (new file)

```sql
CREATE TABLE IF NOT EXISTS public.blocked_cooldown (
    listing_id         text        PRIMARY KEY,
    first_attempted_at timestamptz NOT NULL DEFAULT now(),
    last_attempted_at  timestamptz NOT NULL DEFAULT now(),
    num_of_attempts    integer     NOT NULL DEFAULT 1
);
```

No `fully_blocked` or `next_eligible_at` here — those are computed by dbt.

---

## 2. New dbt model: `dbt/models/staging/stg_blocked_cooldown.sql` (new file)

Owns all cooldown business logic. Tune the formula and threshold here.

```sql
-- stg_blocked_cooldown: computes cooldown state from raw 403 tracking data.
-- Backoff schedule (12h base, doubles each attempt):
--   attempt 1 → eligible again after 12h
--   attempt 2 → 24h
--   attempt 3 → 48h
--   attempt 4 → 96h
--   attempt 5+ → fully_blocked (never retried)

select
    listing_id,
    first_attempted_at,
    last_attempted_at,
    num_of_attempts,
    last_attempted_at
        + (interval '1 hour' * (12 * power(2, num_of_attempts::float - 1)))
        as next_eligible_at,
    num_of_attempts >= 5 as fully_blocked
from {{ source('public', 'blocked_cooldown') }}
```

Add a schema `.yml` entry for it alongside other staging models.

---

## 3. `dbt/models/sources.yml`

Add `blocked_cooldown` under the existing `public` source:

```yaml
- name: blocked_cooldown
  description: "Raw 403 cooldown tracking. One row per listing that returned a 403 on a detail scrape."
  columns:
    - name: listing_id
      data_tests:
        - not_null
        - unique
```

---

## 4. `dbt/models/ops/ops_detail_scrape_queue.sql`

Replace the final `select distinct on` with a join to `stg_blocked_cooldown`. No backoff logic here — just a clean eligibility filter:

```sql
select distinct on (c.listing_id)
    c.vin,
    c.current_listing_url,
    c.listing_id,
    c.seller_customer_id,
    c.stale_reason,
    c.priority
from combined c
left join {{ ref('stg_blocked_cooldown') }} bc
    on bc.listing_id = c.listing_id
where
    bc.listing_id is null                                          -- never 403'd
    or (bc.fully_blocked = false and bc.next_eligible_at < now()) -- cooldown elapsed
order by c.listing_id, c.priority
```

---

## 5. `n8n/workflows/Job Poller V2.json` — upsert node

### Current Switch node outputs (4 outputs, already updated):
- output 0: "Has Backoff Error" (error contains ERR_HTTP2) → Summarize Errors → Send Telegram Alert
- output 1: "403 Error" (error contains "4") → Summarize Errors1 → Send Telegram Alert1
- output 2: "No Error" (error is empty) → nothing
- output 3: "Other Error" (error not empty) → nothing

### Change: Add "Upsert Blocked Cooldown" Postgres node

Wire **Switch output 1 ("403 Error")** to both:
- (existing) Summarize Errors1
- (new) **"Upsert Blocked Cooldown"** Postgres node

The upsert runs per-artifact (before Summarize Errors1 collapses items), so parallel from the Switch is the right attachment point.

```sql
INSERT INTO blocked_cooldown (listing_id, first_attempted_at, last_attempted_at, num_of_attempts)
VALUES ('{{ $json.listing_id }}', now(), now(), 1)
ON CONFLICT (listing_id) DO UPDATE
  SET last_attempted_at = now(),
      num_of_attempts   = blocked_cooldown.num_of_attempts + 1;
```

"Upsert Blocked Cooldown" is terminal — no further wiring needed.

---

## 6. `db/seed/dbt_intents.sql` — new `after_403` intent

```sql
('after_403', ARRAY['stg_blocked_cooldown+'])
```

---

## 7. `n8n/workflows/Job Poller V2.json` — call Build DBT after 403 alert

**Placement:** After "Send Telegram Alert1". One dbt build per batch of 403s, not one per artifact.

Add a new **`executeWorkflow` node "Call 'Build DBT'"**:
- Workflow: Build DBT (id `qdURL8XbEkdApnjdAcFFL`)
- Input: `{"intent": "after_403"}`

Wire: Send Telegram Alert1 → Call 'Build DBT'

Reuses the Build DBT workflow's existing lock-handling and retry logic. No changes to Build DBT needed.

---

## Verification

1. Run a detail batch with listings that return 403s.
2. Confirm rows appear in `blocked_cooldown` with `num_of_attempts = 1`.
3. Run `dbt run --select stg_blocked_cooldown+ ops_detail_scrape_queue`. Confirm those listing_ids are **absent** from `ops.ops_detail_scrape_queue`.
4. `UPDATE blocked_cooldown SET last_attempted_at = now() - interval '13 hours' WHERE listing_id = '<id>';` then re-run dbt. Confirm listing **reappears** in queue.
5. Repeat until `num_of_attempts = 5`, confirm `stg_blocked_cooldown.fully_blocked = true` and listing stays absent regardless of `last_attempted_at`.
