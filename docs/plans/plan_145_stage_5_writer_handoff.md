# Plan 145 Stage 5, slice 2 — assignment and the historical write set (CAR-21)

Hand this to a fresh session **after slice 1 has produced an authoritative
`compare` run**. This is the first slice in Plan 145 that writes to Postgres.

Read `docs/plans/plan_145_april_cutover_reconciliation.md` — its **Stage 5**
section is the specification, and *The production lifecycle of a captured page*
plus *What "full treatment" means for a recovered April artifact* are the two
tables this slice implements. Then read
`docs/plans/plan_145_stage_5_compare_handoff.md` for the inputs, and
`processing/writers/detail_writer.py` and `processing/writers/silver_writer.py`
for the production shapes you are mirroring.

**Build and unit-test everything here. Run `--apply` on nothing but the canary
until slice 3 has closed the live-state proof.** The full apply is gated on
that, and on named maintainer approval.

---

## What this slice adds

Two modes on `scripts/reconcile_april_detail.py`, plus one Flyway migration.

| mode | reads | writes |
|---|---|---|
| `assign` | `compared/<run_id>/to_import/` | `recovery/plan145/assigned/<batch>.parquet`, and `nextval` on one sequence |
| `apply` | the assignment shards | three staging tables and the receipt table, one transaction per batch |

---

## Artifact identity

Identity is **one value per source HTML object**, shared by that object's
primary row and every carousel row it produced. Two rules, in order:

1. **Preserve** the queue-event `artifact_id` when the normalized object path
   has exactly one. The March–May artifact-event lake holds 4,906,595 detail
   event rows reducing to **1,536,055 distinct object paths with zero paths
   mapped to conflicting artifact IDs**, so this is a strict lookup by
   normalized `minio_path` — strip `^s3://[^/]+/`.
2. Otherwise **allocate** with `nextval('ops.artifacts_queue_artifact_id_seq')`.

Never compute one. **`max(artifact_id) + 1` races live inserts and is
forbidden**; `nextval` never returns a value twice and does not roll back on
abort. The sequence was at 7,732,177 during the 2026-08-27 audit; the largest
historical ID in the March–May window is 4,902,473, so preserved and allocated
IDs cannot collide. **Never read `legacy_artifact_id`** — `raw_artifacts` and
`ops.artifacts_queue` are separate sequences and the same integer names two
different artifacts across the cutover.

The 42,276 April pack members with no historical event ID may stay unattributed
**only if they contribute no row to `to_import`**. One with a row to import gets
a new sequence value like any other: silver's `artifact_id` is NOT NULL and the
Stage 6 repacker needs the attribution.

### Assignment is immutable and precedes the write

`assign` writes `recovery/plan145/assigned/<batch>.parquet` **create-if-absent,
before any database insertion**. Each row records the object key, the assigned
`artifact_id`, and whether it was `preserved_queue_event` or
`allocated_sequence`. A sequence value lost before that write is a harmless gap
in a `bigserial`. After the write, **every retry reuses the recorded value** —
that is what makes `apply` idempotent at the identity level.

---

## Batching

Ordered by `object_key`. **A batch never splits an artifact** — all of an
object's rows go in one batch, because they share one identity and one
transaction. Capped at **5,000 artifacts and 50,000 silver rows**, whichever
binds first.

## The transaction

One transaction per batch, writing exactly four things:

1. **`staging.silver_observations`** — only the `to_import` rows, enriched from
   the frozen VIN snapshot, carrying the assigned `artifact_id`.
2. **`staging.price_observation_events`** — one row per imported **`detail`**
   row: `event_type='upserted'` when `listing_state` is active,
   `'deleted'` when unlisted, with **`event_at = fetched_at`** (the legacy
   capture time, explicitly — the column defaults to `now()`).
3. **`staging.artifacts_queue_events`** — one row per artifact,
   `status='recovered'`, `minio_path='s3://bronze/<object_key>'`,
   `artifact_type='detail_page'`, the primary listing ID, and
   **`fetched_at` = the April capture time** while `event_at` is the recovery
   action time. The table has no FK and no status CHECK, so `'recovered'` with
   no hot queue row is valid.
4. **The receipt** (below).

**No carousel price events.** Production mints those only for carousel hints
passing the search configuration active at capture time; that April
configuration is not recoverable. Applying today's filter, or writing all
carousel rows, would manufacture history. Carousel rows remain first-class
silver coverage and participated fully in slice 1's comparison.

### What must not be touched

`ops.price_observations`, `ops.vin_to_listing`, `ops.blocked_cooldown`,
`ops.detail_scrape_claims`, and live message emission. **No row is inserted
into `ops.artifacts_queue`** — `processing/sql/claim_artifacts.sql` claims
anything `pending` or `retry`, so an enqueued row is picked up by live
processing within seconds and runs the entire hot-state path this plan forbids.

---

## Three deployed-contract details that bite

**1. You cannot reuse the production helpers.** `shared.db.db_cursor`
(`shared/db.py:49`) opens **its own connection and commits on exit**, so three
calls to it are three transactions, not one. And
`write_silver_observations_postgres` (`processing/writers/silver_writer.py:55`)
**catches every exception and returns 0** — a failure inside a batch would be
logged as a warning and the run would continue believing it succeeded. Open one
connection, do all four writes on it, commit once, and let exceptions
propagate. Reuse `_POSTGRES_COLS` and the column list; do not reuse the
function.

**2. `staging.price_observation_events.listing_id` is `uuid NOT NULL`.**
`staging.silver_observations.listing_id` is `text`. Validate every listing ID
against `_UUID_RE` (`scripts/reconcile_april_detail.py:149`) before minting a
price event; a non-UUID listing ID is a stop, not a skip.

**3. The role matters, and one grant is missing.** `cartracker-archiver`
connects as `scraper_user`, which V026/V027 grant `INSERT` on
`staging.silver_observations` and `staging.artifacts_queue_events` and
`USAGE, SELECT` on `ops.artifacts_queue_artifact_id_seq` — but **only
`SELECT, DELETE` on `staging.price_observation_events`**. There is no INSERT
grant for `scraper_user` on that table anywhere in `db/migrations/`.

Run this slice from the **`april-processor`** profile instead, which connects as
`cartracker` (`docker-compose.yml:557`) — the role production's own processing
service uses for exactly these three writes. It has no duckdb, which this slice
does not need. Verify the effective grants before the first `--apply` and
record what you found; if you choose to grant `scraper_user` INSERT instead,
that is a second migration and a separate decision.

```
docker compose run --rm april-processor python -m \
  scripts.reconcile_april_detail assign
docker compose run --rm april-processor python -m \
  scripts.reconcile_april_detail apply --batch <name>        # dry run
```

---

## The receipt table — a new Flyway migration

`V047__plan145_recovery_batch_receipts.sql`, following
`V046__coordination_completion_receipts.sql` as the house pattern for exactly
this problem.

```sql
CREATE TABLE public.plan145_recovery_batch_receipts (
    batch_name       text        NOT NULL,
    manifest_sha256  text        NOT NULL CHECK (length(manifest_sha256) = 64),
    artifact_count   integer     NOT NULL,
    silver_count     integer     NOT NULL,
    price_event_count integer    NOT NULL,
    queue_event_count integer    NOT NULL,
    committed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_name, manifest_sha256)
);
```

Grant `SELECT, INSERT` to whichever role runs the writer, and `SELECT` to
`viewer`, as V046 does.

**Why this table has to exist:** all three staging tables are asynchronously
flushed to the lake and then **deleted** — `flush_silver_observations.py:195`
deletes the rows it flushed. So after an ambiguous client response, querying
Postgres cannot distinguish "never committed" from "committed and already
flushed away". The receipt is the only durable evidence, and it is written in
the same transaction as the rows it describes.

Retry semantics, exactly:

- an existing receipt with the **same** `batch_name` and the same
  assignment-manifest SHA-256 → **skip the batch, write zero rows**;
- the same `batch_name` with a **different** digest → **stop**. Do not write,
  do not overwrite the receipt, surface both digests.

---

## Tests

Unit tests extend `tests/scripts/test_reconcile_april_detail.py`. The
transactional behaviour needs a **real Postgres** integration test —
`tests/integration/processing/` and `tests/integration/ops/` have the fixtures
and the `integration` marker.

```
LOG_PATH=/tmp/p145test.log .venv/bin/python -m pytest \
  tests/scripts/test_reconcile_april_detail.py -q -m "not integration"
LOG_PATH=/tmp/p145test.log .venv/bin/python -m pytest \
  tests/integration -q -m integration
.venv/bin/python -m ruff check .
```

Cover at least:

- a preserved ID is used when the path has exactly one queue event; a sequence
  value when it has none; **`max()` appears nowhere** — assert on the emitted
  SQL;
- an object's primary and every carousel row share one `artifact_id`;
- a batch never splits an artifact, and both caps bind;
- the assignment shard is written before any INSERT; a re-run after a crash
  between the shard write and the commit reuses the recorded ID;
- **real Postgres:** a committed batch re-run writes zero rows and touches no
  sequence;
- **real Postgres:** the same batch name with a different digest stops and
  writes nothing;
- **real Postgres:** an exception mid-batch rolls back all four writes — no
  silver row, no event, no receipt;
- **real Postgres:** zero rows land in `ops.artifacts_queue`, and
  `ops.price_observations`, `ops.vin_to_listing`, `ops.blocked_cooldown` and
  `ops.detail_scrape_claims` are byte-identical before and after;
- every silver `fetched_at`, every price-event `event_at`, and every
  queue-event `fetched_at` equals the legacy capture time — not `now()`;
- a carousel row mints **no** price event; a detail row mints exactly one;
  unlisted yields `deleted`, active yields `upserted`;
- a non-UUID listing ID stops the run before any write;
- the VIN snapshot enriches but is never written back.

---

## Non-negotiables

1. **No row in `ops.artifacts_queue`.** Ever.
2. **No mutation of `ops.price_observations`, `ops.vin_to_listing`,
   `ops.blocked_cooldown` or `ops.detail_scrape_claims`**, and no live event
   emission.
3. **Identity comes from the sequence or from a preserved queue event.** Never
   `max()`, never `legacy_artifact_id`, never a sidecar `listing_id`.
4. **One transaction per batch, receipt included.**
5. **Times are the legacy capture time**, written explicitly, never a default.
6. **No `--apply` beyond the canary** until slice 3 closes the live-state proof
   and the maintainer approves by name.
7. **Announce the blast radius** — batch count, row counts, tables — before any
   production command.

---

## When you are done

Report: the identity census (`preserved_queue_event` vs `allocated_sequence`,
and how many of the 42,276 unattributed pack members became import-bearing);
the batch count and the caps that bound; the receipt behaviour proven on real
Postgres; and the before/after equality of the four protected tables from the
integration test.

Then **stop**. Slice 3 needs a named maintenance window and quiesced writers,
which are manual, separately approved production actions. Do not merge the
branch — that is the maintainer's call.
