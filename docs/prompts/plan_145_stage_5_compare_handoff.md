# Plan 145 Stage 5, slice 1 — the `compare` mode (CAR-21)

Hand this to a fresh session. This slice writes **no Postgres row and no
production object**. Everything it produces lands under
`recovery/plan145/compared/`, `recovery/plan145/inventory/` and
`recovery/plan145/vin_snapshot/`.

Stage 5 is three slices. This is the first, and the only one that can be
built and run without production write access:

| slice | mode | writes |
|---|---|---|
| **1 — this document** | `compare` | recovery prefix only |
| 2 | `assign` + `apply` | Postgres, behind a Flyway receipt table |
| 3 | canary + live-state proof | a maintenance window, maintainer-run |

Read `docs/plans/plan_145_april_cutover_reconciliation.md` first — it is the
source of truth, and its **Stage 5** section is the specification. Read *The
trust boundary* before deciding anything about identity. Then read
`docs/plans/plan_145_stage_4_handoff.md`, which describes the inputs you are
consuming, and `scripts/reconcile_april_detail.py`, whose five existing modes
(`census`, `materialize`, `dedupe`, `unpack`, `parse`) establish the structure
you are extending.

---

## Preflight: is Stage 4 finished?

`compare` may be **written and unit-tested** at any time. The **authoritative
run refuses to start** until Stage 4 is complete. That refusal is a feature of
this slice, not an operational note — build it as a gate function alongside
`check_parse_apply_gate` (`scripts/reconcile_april_detail.py:2185`).

The gate reads `recovery/plan145/parsed/parse_report.json` and requires:

- `completed_units == planned_units == 1204` (1,172 materialized shards +
  32 unpacked shards);
- `totals["inputs"] == EXPECTED_FLATTENED_INPUTS` (**983,043**); and
- the observation total reproduced by summing the real
  `recovery/plan145/parsed/rows/*.parquet` row counts, not read back from the
  report.

A `--probe` flag may run against whatever units exist and writes to a
disposable prefix. Probe output is never promoted; the authoritative run is a
separate invocation.

> **Correction to the plan document.** The plan's *Population arithmetic* table
> still reads "materialized objects surviving step 2 ~436,702" and "flattened
> population ~993,767". Those were pre-Stage-3a projections. The reconciled
> figures are **425,978 + 557,065 = 983,043**, matching
> `EXPECTED_FLATTENED_INPUTS` and the Stage 4 handoff. Use the reconciled
> numbers; do not edit the plan document — report the discrepancy instead.

---

## What you are comparing

**Left side — parsed observations.** `recovery/plan145/parsed/rows/*.parquet`,
schema at `scripts/reconcile_april_detail.py:1651`. Roughly 5.8M rows: about
929k importable pages at the probe's measured 5.25 silver rows per artifact.
One `source='detail'` row per page plus one `source='carousel'` row per
qualifying hint. No `artifact_id` column — that is slice 2's job.

**Right side — deployed March–May silver.** `silver_normalized/observations/`,
partitioned `source=/obs_year=/obs_month=` by
`archiver/processors/flush_silver_observations.py:180`. Three sources × three
months = **nine compacted objects, 20,681,645 observations, 219,710,181 stored
bytes**. Read those nine named objects directly. **Do not read a dbt view** —
it moves under you and cannot be fingerprinted.

**The predicate, and nothing else:**

```text
same listing_id AND abs(silver.fetched_at - parsed.fetched_at) <= 300 seconds
```

`source`, `vin`, `artifact_id` and every parsed business value are **not** match
keys. Carousel rows count as coverage: a carousel row in silver represents a
listing just as a detail row does, and refusing to count it would re-import
work production already did.

**Classification is an existence test, not a join.** The 2026-08-27 probe found
614 of 3,374 observations with more than one silver candidate inside the
window; that is normal and not a defect. Never let a candidate supply identity
or a value. The evidence row records `match_count`, the nearest absolute
distance in seconds, and the distinct sources of the candidates — as
measurements, not as a chosen row.

---

## Outputs

Under `recovery/plan145/compared/<run_id>/`, sharded by input unit, immutable,
never overwritten:

| family | what it holds |
|---|---|
| `already_represented/` | one row per parsed observation with ≥1 candidate, plus `match_count`, `nearest_distance_s`, `match_sources`, and `reason` |
| `to_import/` | one row per parsed observation with zero candidates, carrying full parsed provenance |
| `unclassifiable/` | see the gap below |
| `compare_report.json` | inventory digests, per-family counts, the carousel fan-out, and every refusal |

Under `recovery/plan145/inventory/<run_id>.json`, the freeze — key, size and
**ETag** for each of:

1. every parsed `rows/` and `inputs/` object;
2. the nine March–May silver objects;
3. the March–May `ops_normalized/artifacts_queue_events` objects used for
   identity; and
4. the VIN-lookup snapshot this slice writes (below).

A changed inventory requires a **new `run_id` and a complete re-compare**. Never
patch a family in place. Existing objects are never overwritten; a re-run that
finds its own outputs present and its inventory unchanged is a no-op.

### The read-only VIN snapshot

The plan makes the VIN lookup part of the frozen inventory, so it is captured
here even though it is consumed in slice 2. One read-only query, via
`processing/sql/batch_lookup_vin_to_listing.sql`'s shape:

```sql
SELECT listing_id, vin FROM ops.vin_to_listing
WHERE listing_id = ANY(%(listing_ids)s::uuid[])
```

`ops.vin_to_listing` has an index on `listing_id`, so batching the distinct
parsed listing IDs is cheap. Write the result to
`recovery/plan145/vin_snapshot/<run_id>.parquet` and fingerprint it. **Read
only.** A parsed VIN that collides with current hot state is *reported* in the
report and never causes a delete, a remap, or an exclusion.

---

## The gap you have to close: rows with no capture time

**The plan's Stage 5 gate says "every parsed observation is classified exactly
once", and its comparison contract offers only two families. Those two
statements cannot both hold.** Stage 4 resolves identity in three tiers, and
tier 3 — `primary["listing_id"]` read off the page itself — yields a listing
but **no capture time**. Roughly 760 pages land there. Such a row cannot be
tested against a ±300 s window at all, and it cannot be imported: silver's
`fetched_at` is NOT NULL and the whole point of the recovery is the historical
timestamp.

Emit a **third family, `unclassifiable/`**, holding every parsed row where
`fetched_at_source == 'none'`, with that as its reason. It is neither
represented nor importable, and calling it either would be a lie the gate
cannot catch. Count it in the report and make the three families sum to the
parsed row total — that sum is what makes "classified exactly once"
enforceable.

Report the count. If it is materially larger than ~760, stop and say so rather
than proceeding.

---

## Duplicate resolution is global, not per shard

Stage 4's work unit is one manifest shard, so the same
`(listing_id, fetched_at)` can appear in two shards. Resolving duplicates
inside a shard would let a pair through and break the plan's
**zero duplicate `(listing_id, fetched_at)` writes** success criterion.

For an otherwise-unrepresented group sharing `(listing_id, fetched_at)`:

- compute a **business fingerprint** over the silver business columns
  (`SILVER_FIELDS` and `DEALER_FIELDS`, `scripts/reconcile_april_detail.py:1634`)
  — excluding provenance, `object_key`, `content_sha256` and `vin`, which the
  parse stage deliberately leaves NULL on carousel rows;
- if every member of the group has the same fingerprint, pick one deterministic
  canonical winner — `source='detail'` before `'carousel'`, then lowest
  `object_key`, then lowest `content_sha256` — and send the rest to
  `already_represented` with `reason='recovery_duplicate'`;
- if two rows share the key but differ in fingerprint, **stop the comparison**
  and write the conflicting pairs to the report. Do not pick a winner.

### The asymmetry to name, not to fix

Representation uses a ±300 s window; duplicate collapse uses an **exact**
`(listing_id, fetched_at)`. So two parsed captures of one listing 200 s apart,
both unrepresented in silver, both survive into `to_import`. That is correct —
they are two real captures — but it means the plan's zero-duplicate criterion is
an exact-key criterion, not a windowed one. **Measure the population of
unrepresented same-listing pairs within 300 s of each other and report it.** Do
not silently collapse them; that would discard real history. Report the number
so the maintainer can rule on it before slice 2 writes anything.

---

## Where it runs

`compare` needs DuckDB or pyarrow over Parquet and psycopg2 for the one VIN
read. It does **not** need bs4.

- `cartracker-archiver` has `duckdb>=1.0`, `pyarrow`, `psycopg2-binary`, `boto3`
  and `shared/`, and connects as `scraper_user` — which is read-only on `ops`.
  That is exactly the privilege this slice should have. **Run it there.**
- `cartracker-processing` and the `april-processor` profile have **no duckdb**.

```
docker exec -w /app cartracker-archiver python -m \
  scripts.reconcile_april_detail compare                 # plans and measures
docker exec -w /app cartracker-archiver python -m \
  scripts.reconcile_april_detail compare --apply
```

Bound DuckDB threads explicitly — the probe ran on one thread and finished the
20.7M-row scan in seconds, and the host has 4 cores that production also needs.
Run the long form under tmux on the VM. Announce the blast radius (object
counts, request counts, which prefix) before running anything against
production.

---

## Tests

Extend `tests/scripts/test_reconcile_april_detail.py` (1,383 lines, lettered
sections; add a new one at the end).

```
LOG_PATH=/tmp/p145test.log .venv/bin/python -m pytest \
  tests/scripts/test_reconcile_april_detail.py -q -m "not integration"
.venv/bin/python -m ruff check .
```

Cover at least:

- an observation with one candidate inside 300 s is represented; one at 301 s
  is not; the boundary at exactly 300 s is represented (state the choice in a
  test name, since the plan says `<=`);
- a candidate differing only in `source` still counts as coverage;
- differing `vin`, `price` or `artifact_id` never affect classification;
- multiple candidates yield one classification, `match_count > 1`, and no
  candidate's values on the evidence row;
- the three families partition the parsed rows exactly — no row in two, none in
  none, counts sum to the input total;
- a row with `fetched_at_source == 'none'` lands in `unclassifiable` and never
  in `to_import`;
- identical duplicates across **two different shards** collapse to one winner
  deterministically, independent of shard order;
- duplicates with differing business fingerprints stop the run;
- the run refuses to start when `parse_report.json` reports fewer than 1,204
  completed units, or a total that disagrees with the real row counts;
- a changed input ETag forces a new `run_id` rather than a patch;
- a re-run with unchanged inventory writes nothing;
- the VIN snapshot query is read-only — assert against a fake cursor that no
  INSERT/UPDATE/DELETE is issued;
- `compare` defaults to a dry run.

---

## Non-negotiables

1. **No Postgres write, of any kind.** The only database statement this slice
   issues is the VIN `SELECT`.
2. **No object written outside `recovery/plan145/`.** Nothing is overwritten
   inside it either.
3. **Never use the sidecar's `listing_id`**, including the copy in the unpack
   manifest. It is wrong for 313,701 of 457,084 named April members.
4. **Never read or compare `legacy_artifact_id`.** The two `bigserial`
   sequences collide across the cutover.
5. **No candidate silver row supplies identity or a value** — only existence,
   count and distance.
6. **Do not modify the parser**, and do not re-run `parse --apply`, `dedupe
   --apply` or `unpack --apply`.
7. **Announce the blast radius** of any production command before running it.

---

## When you are done

Report: the three family counts and their sum against 983,043 inputs; the
carousel fan-out measured over the real population (the probe's 5.25 average /
9 maximum came from one biased shard and is **not** a population estimate); the
`unclassifiable` count against the ~760 expectation; the multiple-candidate
share against the probe's 18%; the duplicate-collapse count and the
conflicting-fingerprint count; the near-duplicate window measurement; the
inventory digest; and the `run_id`.

Then **stop**. Slice 2 has production write access, its own Flyway migration
and its own gates, and the maintainer decides when it starts. Do not create a
Linear issue and do not merge the branch — both are the maintainer's call.
