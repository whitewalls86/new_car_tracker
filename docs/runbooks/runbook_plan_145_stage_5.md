# Run Sheet: Plan 145 Stage 5 — Compare, Assign, and the Canary

Operational companion to [Plan 145](../plans/plan_145_april_cutover_reconciliation.md),
whose **Stage 5** section is the specification. Covers **slice 1** (`compare`),
**slice 2** (`assign`; `apply` only as far as the dry run), **slice 3 Phase A**
(`control`, `canary-sample`) and **slice 3 Phase B** (`canary-remanifest`,
`canary-commit`, `canary-flush-verify`, and the V040 verifier). Follow it in
order. **§7 is Phase B and holds its four commands in the order they run.**

**§7 is the only step in this sheet that commits a row to Postgres**, and it
runs inside a maintainer-opened maintenance window. Everything before it writes
under `recovery/plan145/` or writes nothing.

> **Two things here cannot be undone by deleting an object.** `assign --apply`
> (§5.2) advances `ops.artifacts_queue_artifact_id_seq` by one value per
> artifact, permanently — sanctioned, since a `bigserial` gap is not a reuse.
> And `canary-commit --apply` (§7.3) commits 505 silver rows. Both are meant to
> happen; neither is reversible by an object delete.

## The run id

Everything below is scoped to one authoritative compare run. **Pin it on every
command; never rely on auto-discovery.**

```
RUN_ID = cmp-6c7c90d807bbdf13
```

Inventory digest
`6c7c90d807bbdf137bf9b96c94d2a54c2cb9d94706a6a29afa36c209a08ea60d`.

## Where things stand — measured 2026-08-29 06:40 UTC

| step | state |
|---|---|
| Stage 4 (`parse`) | **complete** 2026-08-28 — 1,204/1,204 units, 983,043 inputs, 5,738,532 rows |
| §0 deploy | **done** — VM at `c5c5ee2`, both images rebuilt 04:21:36 UTC |
| §2 slice 1 `compare --apply` | **done** 04:54 UTC — `cmp-6c7c90d807bbdf13` |
| §3 parser control | **run 2026-08-29 — `FINDINGS`, diagnosed as out-of-scope (see below)** |
| §4 rulings | near-dup decomposed and fan-out measured 2026-08-29; neither shows divergence. **No ruling recorded** — §5.2 was run without one |
| §5 slice 2 `assign` | **done** 2026-08-29 — 341,903 artifacts, 69 batches; sequence 8,054,031 → 8,383,887. `apply` dry run validated `b00001`, nothing committed |
| §6 `canary-sample` | **done** 2026-08-29 — 234 artifacts, 505 rows, **9/9 strata covered**, no split |
| §7 Phase B + V040 proof | **← you are here.** Four steps, all built and tested (unit + real-Postgres integration), **none ever run against production**. Steps 1–2 are runnable today; step 3 needs a named window |

| live state | |
|---|---|
| `compared/` · `inventory/` · `vin_snapshot/` | 1,758 · 1 · 1 objects |
| `assigned/` · `control/` · `canary/` | **70 · 1 · 1** objects |
| `ops.artifacts_queue_artifact_id_seq` | **8,383,887** (advanced by `assign --apply`) |
| `plan145_recovery_batch_receipts` | 0 rows |
| `staging.artifacts_queue_events WHERE status='recovered'` | 0 |
| Flyway head | V047, applied 2026-08-28 15:56:39 |

---

## 0. Preflight — verify, do not redo

The deploy step this sheet used to open with is **already done**. Confirm rather
than repeat:

```bash
ssh -i ssh-key-2026-04-08.key ubuntu@147.224.199.86
cd /opt/cartracker
git rev-parse --short HEAD          # expect c5c5ee2 or later
docker images --format '{{.Repository}}\t{{.CreatedAt}}' \
  | grep -E 'cartracker-(archiver|processing)'
```

Both images were built from `c5c5ee2` at 2026-08-29 04:21:36 UTC. If you pull
again, rebuild with `docker compose build archiver processing` — **build, do not
redeploy**. The running containers keep serving the old image until they
restart, and every command here is an ephemeral `docker compose run --rm` that
picks up the new one. There is no reason to restart a live service for this work.

### 0.1 Use `compose run`, not `docker exec`

The Stage 4 handoff uses `docker exec -w /app cartracker-archiver …`. **Do not
use that pattern.** It runs inside the long-lived container, which is not
guaranteed to be on the built image. Use:

```bash
docker compose run --rm archiver         python -m scripts.reconcile_april_detail …
docker compose run --rm april-processor  python -m scripts.reconcile_april_detail …
```

Verified 2026-08-28: `archiver` gives cwd `/app`, Python 3.13.15, duckdb 1.5.5,
`scraper_user`; `april-processor` gives cwd `/app`, `cartracker`, pyarrow and
bs4, **duckdb absent by design**. `april-processor` is profile-gated but naming
it explicitly is enough — no `--profile` flag needed. Each `compose run` also
starts `flyway`, which migrates and exits; that is idempotent noise, not an
action.

### 0.2 Which image for which mode

| mode | image | role | why |
|---|---|---|---|
| `compare` | `archiver` | `scraper_user` | duckdb for the silver scan; read-only on `ops` is the right privilege |
| `control` | `archiver` | `scraper_user` | duckdb for the silver read; touches no Postgres |
| `canary-sample` | `archiver` | — | pyarrow only, no Postgres, no duckdb |
| `assign` | `april-processor` | `cartracker` | `nextval` needs `USAGE`; pyarrow only |
| `apply` | `april-processor` | `cartracker` | `scraper_user` has no `INSERT` on `staging.price_observation_events` |
| `canary-remanifest` | `april-processor` | — | pyarrow only, no Postgres, no duckdb |
| `canary-commit` | `april-processor` | `cartracker` | same `INSERT` grants as `apply`; pyarrow only |
| `canary-flush-verify` | `archiver` | — | pyarrow only, no Postgres, no duckdb |

### 0.3 tmux

Four sessions hold Stage 4, probe and Stage 5 scrollback: `plan145-compare`,
`plan145-probe`, `plan145-stage4-dryrun`, `plan145-stage-5`. **Do not kill
them.** Continue in `plan145-stage-5` or open a new one.

---

## 1. The probe run is superseded — and the code now refuses it

`compared_probe/cmp-e37723ede49fad4f/` and its 59 `assigned_probe/` shards are
**not read by anything in this sheet**, and as of `7410016`/`5802cb5` they
cannot be: `assign` and `apply` refuse any run whose `compare_report.json` has
no `blocked_excluded` section, which is by construction a compare run predating
the block-page filter. The refusal is keyed on `apply`, so a missing or empty
report fails closed rather than skipping the gate.

Two independent reasons it is superseded, both now moot but worth knowing:

- **It is 22% short of the population, and short in the half that matters.**
  Taken at 1,186 of 1,204 units, missing 18 of the 32 unpacked shards — the
  pack-side cohort in which every never-fired check lives.
- **Its assignment shards predate the block-page filter**, so its `to_import`
  carries block pages that the authoritative run quarantines.

Leave the `*_probe/` prefixes in place or delete them — maintainer's call,
~0.25 GB. What matters is that they are never promoted, which the code enforces.

---

## 2. Slice 1 — the authoritative `compare` — **done**

Run 2026-08-29, dry run 04:30→04:38 and `--apply` 04:40→04:54 UTC, identical
counts, `refusals: []`, no drift flag.

```bash
# for the record — do not re-run
docker compose run --rm archiver python -m scripts.reconcile_april_detail \
  compare --apply --duckdb-threads 1 --duckdb-memory-limit 2GB
```

| family | rows | share |
|---|---:|---:|
| `already_represented` | 4,977,697 | 86.74% |
| `to_import` | **701,375** | 12.22% |
| `blocked_excluded` | 59,460 | 1.04% |
| `unclassifiable` | **0** | 0% |
| sum | 5,738,532 | matches the parsed total |

Wrote `compared/cmp-6c7c90d807bbdf13/`,
`inventory/cmp-6c7c90d807bbdf13.json`,
`vin_snapshot/cmp-6c7c90d807bbdf13.parquet` (61,117 rows).

**Do not re-run this.** A re-run with an unchanged inventory is a no-op; one
with a changed inventory mints a new run id and invalidates every step below.
`--force` exists but has no business here.

---

## 3. The parser control — the next thing to run

The check the whole plan rests on: that reprocessing reproduces what production
wrote. **Its failure would invalidate slice 1's classification and everything
built on it**, so it runs before the rulings and before a single sequence value
is allocated. It reads only slice 1's output and needs nothing from slice 2.

```bash
docker compose run --rm archiver python -m scripts.reconcile_april_detail \
  control --apply --run-id cmp-6c7c90d807bbdf13 \
  --sample-size 500 --seed 145 \
  --duckdb-threads 1 --duckdb-memory-limit 2GB 2>&1 \
  | tee /home/ubuntu/plan145-control-auth.log
```

**Blast radius:** reads the `already_represented/` shards (4,977,697 rows,
reservoir-sampled so memory is bounded at `--sample-size`) and the nine silver
objects. **No Postgres statement.** `--apply` writes one JSON report to
`recovery/plan145/control/`; drop it to print only. Minutes.

What you are reading:

- `exact same-source candidates` — rows with `nearest_distance_s == 0` matching
  their own source. The plan doc still cites 2,879 of 3,374 from a one-shard
  smoke test; **measure it here**.
- `field disagreements` and the per-field census. **Any non-zero count is a
  finding, not a tolerance.** The mode exits non-zero.
- `ignored by name` — exactly four: recovery provenance, `artifact_id`,
  `written_at`, carousel `vin`. Each drives a real branch in
  `_control_ignored_columns`, and the silver read pulls `artifact_id` and
  `written_at` deliberately so the skip is **by name, not by absence**. A fifth
  raises.
- `no silver row` / `multiple silver rows` — both findings. "Clean" requires
  `compared > 0` and all three counters zero; a run that compared nothing does
  not persist a green result.

You are comparing the **raw parsed row**, so the carousel `vin` gap is expected —
slice 2 fills it from the frozen VIN snapshot.

### 3.1 It reported FINDINGS on 2026-08-29 — and why that is not a stop

The first real run returned `FINDINGS`, 2,867 field disagreements over 498
compared rows (46.8%). **Diagnosed: the Plan 100 migration boundary, not a parse
defect.**

[Plan 100](../plans/plan_100_historical_data_migration.md) migrated the legacy
observation tables into MinIO silver for `fetched_at < 2026-04-21`, the date the
Airflow processing service went live. April silver is therefore a mix, and the
legacy schema carried only `dealer_name` / `dealer_zip` / `customer_id` — none
of the seven dealer-address columns the control flagged. Measured over 19,872
exact-distance rows:

| | rows | disagree |
|---|---:|---:|
| `fetched_at >= 2026-04-21` | 11,665 | **4 (0.03%)** |
| `fetched_at < 2026-04-21` | 8,404 | 8,404 (100.0%) |

Against rows production actually wrote from the same artifact, recovery
reproduces production at 0.03%. `different_object` is 0 in every bucket, so the
control is matching the right artifact throughout.

**So: this specific FINDINGS result does not block §4 or §5.** The reasoning is
recorded in the plan document under *Evidence — slice 3 Phase A, the parser
control run, 2026-08-29*.

**A future FINDINGS result still does.** Until the mode gains a scope predicate
(`fetched_at >= 2026-04-21`, or silver rows whose `artifact_id` resolves in the
queue-event lake), read its exit code as *out of scope*, and check the split
above before concluding anything. Do not widen the four ignored fields to make
it pass — the ignore list is a plan decision and a fifth entry raises by design.

**If a rerun disagrees on post-cutoff rows, stop.** That is the real signal, and
the compare output stays valid as data — you would be re-deciding what it means,
not re-running it.

---

## 4. The rulings — one recorded, one still open

### 4.1 Near-duplicate cohort — measured and recorded 2026-08-29

Decomposed by a read-only scan over the authoritative `to_import/`. **The
"burst re-scrape" explanation the plan carried is wrong** — that mechanism is
105 of 96,800 pairs, 0.11%. All 96,800 pairs span two different source objects,
and zero have `gap == 0`.

| pair type | pairs | identical values | what it is |
|---|---:|---:|---|
| carousel ↔ carousel | 82,280 | 82,249 (100.0%) | one listing in two pages' carousels, same pass |
| carousel ↔ detail | 14,415 | **0 (0.0%)** | card vs full page — different observations |
| detail ↔ detail | 105 | 105 (100.0%) | the actual re-scrape case |

**Recommendation on the record: import all of them.** Production writes this
shape today with no deduplication anywhere in the live path — no `ON CONFLICT`
in `_INSERT_SQL`, no uniqueness on `(listing_id, fetched_at)`, no dedup in the
flusher or `stg_observations.sql`. Collapsing would make April's silver uniquely
deduplicated against every other month. Full detail in the plan document,
*The near-duplicate cohort, decomposed*.

### 4.2 Carousel fan-out — measured 2026-08-29, ruling open

The gate requires it measured and reviewed, not asserted. The recovery figure is
**5.2 per object over 915,972 objects, max 8** — measured over importable
objects only, blocked objects out of both numerator and denominator, so it is a
sharper figure than the probe's 5.6332, not drift.

Against production's own monthly fan-out, read straight from
`silver_normalized/observations` (carousel rows ÷ artifacts producing a detail
row):

| month | fan-out | median | max |
|---|---:|---:|---:|
| 2026-01 | 5.07 | 6 | 8 |
| 2026-02 | 2.92 | 0 | 8 |
| 2026-03 | 2.53 | 0 | 8 |
| **2026-04 — production silver** | **5.38** | 7 | **40** |
| **2026-04 — Plan 145 recovery** | **5.2** | — | **8** |
| 2026-05 | 7.33 | 8 | 16 |
| 2026-06 | 7.39 | 8 | 8 |
| 2026-07 | 7.29 | 8 | 16 |
| 2026-08 | 7.15 | 8 | 8 |

Three readings:

- **April against April agrees to within 3%** — 5.2 recovered against 5.38 in
  production silver. The recovery reproduces its own month's rate. A figure near
  May's 7.33 would have been the alarming result, meaning recovery was emitting
  carousel rows the month did not have.
- **The month-to-month spread is 2.53–7.39, nearly 3×**, so "production is
  ~5.7" was never a constant and 5.2 sits well inside the range. The spread is
  the Plan 100 boundary again: Feb and Mar are fully migrated legacy months with
  a **median of 0**, April straddles the 04-21 cutoff, and May onward settle at
  7.15–7.39 with median 8.
- **April production silver has a max of 40**, out of family with every other
  month (8 or 16) and almost certainly a migrated legacy artifact, since the
  current pipeline caps hints at 8. The recovery's max is 8. On this axis the
  recovered rows are better behaved than what is already in silver.

Caveat: the two April rows cover overlapping but not identical populations —
production April silver includes migrated rows the recovery does not touch. The
3% agreement is a strong signal, not a like-for-like identity.

**The ruling has not been made.** Nothing here suggests divergence, but it
blocks §5.2 from being run, and this sheet's summary is not the approval.

---

## 5. Slice 2 — `assign`, and `apply` as far as the dry run

### 5.1 Assign, dry run

```bash
docker compose run --rm april-processor python -m scripts.reconcile_april_detail \
  assign --run-id cmp-6c7c90d807bbdf13 2>&1 \
  | tee /home/ubuntu/plan145-assign-dryrun.log
```

**Blast radius:** reads `compared/cmp-6c7c90d807bbdf13/to_import/`,
`parsed/inputs/` and the March–May artifact-event objects. **Writes nothing and
touches no sequence.**

Validation happens here, before anything is allocated. It scans the **whole**
population before stopping, so a cohort is reported with its size rather than
dying on the first row. **Four refusals**, the last two new since PR #272:

| refusal | trigger |
|---|---|
| non-UUID / NULL `listing_id` | any `to_import` row |
| conflicting identity | one object path mapped to two queue-event artifact ids |
| **stale compare run** | the run's `compare_report.json` has no `blocked_excluded` section — a pre-filter run. `--apply` stops, a dry run warns |
| **block signature** | a `to_import` row that is `active` with `price`/`vin`/`make` all NULL — defence in depth behind compare's object-level filter |

`cmp-6c7c90d807bbdf13` passes the stale-run check by construction. The block
signature check is row-level and detail-only where compare's filter is
object-level; on this run the gap it hedges is provably empty
(`objects_that_emitted_carousel_rows: 0`), but the check stays because that was
not knowable in advance.

**Neither of the first two has ever fired on real data** — earlier probes ran
where the cohorts are structurally empty. This is the first run with the
pack-side population in scope at full weight. Treat a stop as information.

Record: artifact count; the `preserved_queue_event` / `allocated_sequence`
split; how many of the 42,276 unattributed pack members became import-bearing;
and the batch count. Expect the order of ~400k artifacts and ~80 batches from
701,375 rows, but **take the real number from the dry run**.

### 5.2 Assign, apply — the irreversible step

**Requires §3 clean and §4.2 ruled.**

```bash
docker compose run --rm april-processor python -m scripts.reconcile_april_detail \
  assign --apply --run-id cmp-6c7c90d807bbdf13 2>&1 \
  | tee /home/ubuntu/plan145-assign-apply.log
```

**Blast radius:** writes the `assigned/cmp-6c7c90d807bbdf13-bNNNNN.parquet`
shards plus one assign report, and calls real `nextval` once per allocated
artifact — **advancing the sequence permanently** from 8,054,031. No table row
is written. Expect `artifact_id` to jump; a `bigserial` gap is not a reuse.

Do **not** change `--max-artifacts` / `--max-silver-rows` from 5,000 / 50,000.
The caps decide batch membership, so re-assigning under different caps is
refused — the names would not change but their contents would.

```bash
docker exec cartracker-postgres psql -U cartracker -tAc \
  "select last_value from ops.artifacts_queue_artifact_id_seq"
```

Confirm it moved by exactly the allocated count.

### 5.3 Apply, dry run only

```bash
docker compose run --rm april-processor python -m scripts.reconcile_april_detail \
  apply --run-id cmp-6c7c90d807bbdf13 --batch cmp-6c7c90d807bbdf13-b00001 2>&1 \
  | tee /home/ubuntu/plan145-apply-dryrun.log
```

**Blast radius:** builds, validates and prints the whole write set. **No
statement is issued.** It re-checks the listing-id invariant on every row —
`text NOT NULL` does not catch `str(None)`, the four-character string `"None"` —
and `refuse_stale_compare_run` runs here too, because `apply` re-reads the
shards independently and with an explicit `--run-id` would otherwise select
batches straight from the assigned prefix.

**Stop here.** `apply --apply` commits, and Plan 145 allows nothing beyond the
~500-row canary until the live-state proof closes. An authoritative `--apply`
over `--max-unapproved-rows` (default **1,000** silver rows) is refused without
`--maintainer-approval <name>`; one default-cap batch is up to 50,000 rows, so a
bare `apply --apply` will be refused, correctly. `--maintainer-approval` is a
record of a human decision, not a way past one.

> The probe already retired the question this dry run half-answers. On
> 2026-08-28 `apply --probe --apply` ran the full statement sequence for one
> batch against production Postgres and rolled it back; every `::uuid` cast,
> `NOT NULL` and CHECK held on real rows. Note that path is now closed for the
> old run — its shards are refused as stale.

---

## 6. The canary sample

```bash
docker compose run --rm archiver python -m scripts.reconcile_april_detail \
  canary-sample --apply --run-id cmp-6c7c90d807bbdf13 \
  --target-rows 500 --seed 145 2>&1 \
  | tee /home/ubuntu/plan145-canary-sample-auth.log
```

**Blast radius:** reads the authoritative `to_import/` and `assigned/` shards.
No Postgres, no duckdb. `--apply` writes one manifest plus a report to
`recovery/plan145/canary/`.

Requires §5.2. It joins the assignment shards back to `to_import` because
`listing_state` lives on the compared row while `id_source` and `input_kind`
live on the assignment — expected, not a slice 2 gap.

**Three stops**, cross-checks against slice 2's own per-object counts. `assign`
builds from `_scan_to_import`, which stops on violations rather than dropping
objects, so the two object sets are exactly 1:1 and both directions are checked:

| stop | meaning |
|---|---|
| `missing` | an object read here with no assignment row |
| `absent` | an assigned object with **no rows** in this read — a whole dropped shard |
| `split` | an object read **short** of its assigned count — a half artifact |

`absent` is new in `5a5cce7`: a zero-row object vanishes from the read entirely,
so neither the short-read check nor the coverage guard — computed from that same
truncated read — could see it. **Any of the three is a real defect in the
inputs, not a sampler bug.**

Confirm every non-empty stratum across `source` × `listing_state` ×
`input_kind` × `id_source` is covered and no artifact is split. This is the
first run with the pack-side strata at full weight.

The manifest — `recovery/plan145/canary/cmp-6c7c90d807bbdf13-canary_sample.parquet`
— **is Phase B's input**, and is what `verify_recovery_live_state.py
--canary-cmd` will commit. Record the seed with the result.

### 6.1 The manifest needs one migration before Phase B can use it

The manifest written on 2026-08-29 predates the `write_set_digest` column, and
`canary-commit` refuses one without it. Migrating it is **Phase B step 1** —
§7.1. Do not re-run `canary-sample`: it reselects, and it would delete the only
record of what the window's subject is. Reasoning in §7.5.

---

## 7. Slice 3 Phase B — the write canary

Four steps, in order. Two before the window, one inside it, one after.
**Everything is built and tested; none of it has ever run against production.**
No window is scheduled — opening one is the maintainer's action.

| # | step | when | command | writes |
|---|---|---|---|---|
| 1 | migrate the manifest | any time | `canary-remanifest --apply` | 2 objects under `recovery/plan145/canary/` |
| 2 | dry run, read the pin | any time | `canary-commit` | nothing |
| 3 | **the window** | you make it — §7.3 (a)–(c) | `verify_recovery_live_state.py --canary-cmd "… canary-commit --apply …"` | **505 silver + 140 price + 234 queue + 1 receipt**, one transaction |
| 4 | verify the flush | ≤1h after step 3 | `canary-flush-verify --apply` | 1 report object |

Pin `--run-id cmp-6c7c90d807bbdf13` on every one of them.

---

### 7.1 Step 1 — migrate the manifest

```bash
docker compose run --rm april-processor python -m scripts.reconcile_april_detail \
  canary-remanifest --run-id cmp-6c7c90d807bbdf13 2>&1 \
  | tee /home/ubuntu/plan145-canary-remanifest-dryrun.log

docker compose run --rm april-processor python -m scripts.reconcile_april_detail \
  canary-remanifest --apply --run-id cmp-6c7c90d807bbdf13 2>&1 \
  | tee /home/ubuntu/plan145-canary-remanifest.log
```

**Blast radius.** Reads the frozen manifest, the assignment shards it names,
their `to_import` units, and the VIN snapshot. No Postgres statement. `--apply`
writes two new objects: `…-canary_sample_digested.parquet` and its report. The
frozen `…-canary_sample.parquet` is **not** deleted and **not** overwritten.

**Check:** `identical: True` on the two object-set digests. That is the
promotion proof — not the aggregate counts.

**Stops if** any input moved under the frozen sample, or a migrated manifest
already exists.

### 7.2 Step 2 — dry run, and read the pin

```bash
docker compose run --rm april-processor python -m scripts.reconcile_april_detail \
  canary-commit --run-id cmp-6c7c90d807bbdf13 2>&1 \
  | tee /home/ubuntu/plan145-canary-commit-dryrun.log
```

**Blast radius.** Reads only; **no Postgres connection is opened at all** — the
dry run returns before `get_conn`.

**Check:** 234 artifacts, 505 silver rows, 140 price events, 234 queue events.
Take the real numbers from the run.

**Then copy the last two lines**, which print the pin step 3 needs verbatim:

```
--expect-manifest-sha256 <64 hex> --expect-rows 505
```

An over-budget manifest does not kill this run — it prints `OVER BUDGET` with
the overage and exits 0. Only `--apply` refuses.

### 7.3 Step 3 — the window

**There is no window to book.** `--window <name>` is a free-text string: the
script records it in the report and refuses to run without it, and that is the
whole mechanism. It exists so a run that quiesced production is named on the
record and cannot be fired off casually. Pick something like
`p145-canary-2026-08-29` and use the same string in the report filename.

The window is a thing *you make* by doing (a)–(c) below, and unmake with (g).

#### (a) Declare deploy intent — this holds every DAG

```bash
curl -sS -X POST http://localhost:8060/deploy/start \
  -H 'Content-Type: application/json' \
  -d '{"targets": ["scraper", "processing", "ops"], "pause_long_jobs": true}'
```

`targets` is validated against `ops/coordination_contract.py`'s
`SERVICE_CONTRACTS`; unknown or duplicate names are a 422, and an intent
already held is a 409. **Every mutating DAG's first task is
`deploy_intent_sensor`**, which blocks in `reschedule` mode while an intent is
pending — so this one call holds `scrape_detail_pages`, `scrape_listings`,
`results_processing`, `orphan_checker`, `compact_silver` **and
`hourly_analytics_refresh`**. You do not need to pause DAGs by hand, and
`POST /deploy/complete` releases them all at once.

Holding the hourly flush is deliberate, not incidental — see (f).

#### (b) Stop the three writers — intent does not do this

```bash
docker compose stop scraper processing ops
```

**Deploy intent is a cooperative signal, not a stop.** `shared/deploy_intent.py`
has exactly two consumers — `pack_bronze_html` and `delete_packed_source_html`
— and its own docstring says *"nothing here kills anything."* `scraper`,
`processing` and `ops` never read it. They write the protected tables directly:
`ops` writes `ops.detail_scrape_claims` from an HTTP route (`routers/scrape.py`)
that the scraper calls, with no DAG involved. Intent alone leaves them running.

#### (c) Confirm it is quiet

```bash
curl -s http://localhost:8060/deploy/status          # number_running -> 0
docker exec cartracker-postgres psql -U cartracker -tAc \
  "select count(*) from airflow.dag_run where state = 'running'"
docker compose ps --status running | grep -E 'scraper|processing|ops' || echo "writers down"
```

`number_running` is the live execution count; it was **400** when this sheet
was written, so give it time to settle rather than assuming.

#### (d) Run it

```bash
python scripts/verify_recovery_live_state.py --window p145-canary-2026-08-29 \
  --canary-cmd "docker compose run --rm april-processor python -m \
    scripts.reconcile_april_detail canary-commit --apply \
    --run-id cmp-6c7c90d807bbdf13 \
    --expect-manifest-sha256 <from step 2> --expect-rows 505" \
  --report /tmp/p145-v040-p145-canary-2026-08-29.json
```

**Blast radius.** One transaction against production Postgres: **505** rows
into `staging.silver_observations`, **140** into
`staging.price_observation_events`, **234** into
`staging.artifacts_queue_events`, **1** receipt into
`public.plan145_recovery_batch_receipts`, plus one report object. **Nothing** is
written to `ops.artifacts_queue`, `ops.price_observations`,
`ops.vin_to_listing`, `ops.blocked_cooldown` or `ops.detail_scrape_claims` —
which is the claim the verifier around it measures.

#### (e) Read the result

Exit **0** pass, **1** fail, **2** refused (no `--window`). A pass needs both
`single transaction: True` and every relation `unchanged`. A failure on
`single_transaction` invalidates the proof outright — the views are
time-dependent and two snapshots at two `now()` values differ for reasons that
have nothing to do with recovery.

#### (f) Keep it, or roll it back — while the flush is still held

This is why (a) holds `hourly_analytics_refresh`. Until it runs, the canary's
rows exist in **one** place — Postgres staging — and a rollback is one
transaction. Once flushed they are also Parquet in the lake, and unwinding
means editing objects in two systems.

Check first, then decide:

```bash
docker exec cartracker-postgres psql -U cartracker -c "
  SELECT 'silver' AS t, count(*) FROM staging.silver_observations
   WHERE fetched_at < '2026-05-01' AND artifact_id IN (
     SELECT artifact_id FROM staging.artifacts_queue_events
      WHERE run_id = 'cmp-6c7c90d807bbdf13-canary')
  UNION ALL SELECT 'price', count(*) FROM staging.price_observation_events
   WHERE event_at < '2026-05-01' AND artifact_id IN (
     SELECT artifact_id FROM staging.artifacts_queue_events
      WHERE run_id = 'cmp-6c7c90d807bbdf13-canary')
  UNION ALL SELECT 'queue', count(*) FROM staging.artifacts_queue_events
   WHERE run_id = 'cmp-6c7c90d807bbdf13-canary'
  UNION ALL SELECT 'receipt', count(*) FROM public.plan145_recovery_batch_receipts
   WHERE batch_name = 'cmp-6c7c90d807bbdf13-canary';"
```

Expect 505 / 140 / 234 / 1. To roll back, in one transaction:

```bash
docker exec cartracker-postgres psql -U cartracker -c "
BEGIN;
CREATE TEMP TABLE canary_ids ON COMMIT DROP AS
  SELECT artifact_id FROM staging.artifacts_queue_events
   WHERE run_id = 'cmp-6c7c90d807bbdf13-canary';
DELETE FROM staging.silver_observations
 WHERE fetched_at < '2026-05-01'
   AND artifact_id IN (SELECT artifact_id FROM canary_ids);
DELETE FROM staging.price_observation_events
 WHERE event_at < '2026-05-01'
   AND artifact_id IN (SELECT artifact_id FROM canary_ids);
DELETE FROM staging.artifacts_queue_events
 WHERE run_id = 'cmp-6c7c90d807bbdf13-canary';
DELETE FROM public.plan145_recovery_batch_receipts
 WHERE batch_name = 'cmp-6c7c90d807bbdf13-canary';
COMMIT;"
```

> **The `fetched_at` / `event_at` predicates are not decoration.** 13,253 of the
> run's artifacts carry *preserved* historical `artifact_id`s, so an id alone
> could match a live row that happened to be in staging. Every canary row is an
> April capture; no live row is. The queue events need no such guard — `run_id`
> is the canary's own batch name.
>
> Deleting the receipt is what makes the canary re-runnable. Leave it and the
> next `canary-commit --apply` skips, writing nothing.

#### (g) Restore

```bash
docker compose start scraper processing ops
curl -sS -X POST http://localhost:8060/deploy/complete
curl -s http://localhost:8060/deploy/status          # intent -> none
```

Only now does `hourly_analytics_refresh` resume, and with it the flush step 4
verifies.

#### Services and DAGs, for the record

The script does not and cannot quiesce anything. What writes the four
protected tables:

| table | written by |
|---|---|
| `ops.price_observations` | `processing` — `upsert_price_observation.sql`, `delete_price_observation.sql`, `delete_price_observation_by_vin.sql`, `srp_writer.py` |
| `ops.vin_to_listing` | `processing` — `upsert_vin_to_listing.sql` |
| `ops.blocked_cooldown` | `scraper` — `upsert_blocked_cooldown.sql`; `processing` — `clear_blocked_cooldown.sql`; `ops` — `evict_delisted_cooldowns.sql` |
| `ops.detail_scrape_claims` | `ops` — `routers/scrape.py`, `expire_orphan_detail_claims.sql`; `processing` — `release_detail_claims.sql` |

Both V040 views resolve to `ops.price_observations` alone —
`ops_vehicle_staleness` reads it directly and `ops_detail_scrape_queue` reads
that view — so no fifth table hides behind them, and `cleanup_queue`,
`cleanup_parquet` and `pack_bronze_html` cannot affect the assertion.

> **Run the canary exactly once.** It is idempotent on its receipt, so a second
> run writes zero rows and measures nothing. Committing it outside a window
> *spends* the window's subject — and (f) is the only cheap way back.

### 7.4 Step 4 — verify the flush round trip

**Only after §7.3 (g).** `hourly_analytics_refresh` owns the scheduled flush
(`0 * * * *`) and is held by the deploy intent for the whole window — on
purpose, so a rollback stays one transaction. Once intent is released it
resumes and the canary's rows reach the lake within the hour.

To flush on demand instead of waiting, call what the DAGs call. **The archiver
publishes no host port**, so go through a container on its network:

```bash
docker exec cartracker-airflow-scheduler \
  curl -sS -X POST http://archiver:8001/flush/silver/run
docker exec cartracker-airflow-scheduler \
  curl -sS -X POST http://archiver:8001/flush/staging/run
```

(The standalone `flush_silver_observations` / `flush_staging_events` DAGs are
`schedule=None` and exist for the same purpose.)

```bash
docker compose run --rm archiver python -m scripts.reconcile_april_detail \
  canary-flush-verify --apply --run-id cmp-6c7c90d807bbdf13 2>&1 \
  | tee /home/ubuntu/plan145-canary-flush.log
```

**Blast radius.** Reads only. No Postgres, no duckdb. `--apply` writes one JSON
report. Exit 0 pass, 1 if any row is absent.

**Check:** every row found, by key, in all three lake prefixes, and record the
keys it names.

**If a compaction has already folded the flushed parts into the month's
compacted object, add `--scan-all`** — correct, but it reads the whole
partition.

---

### 7.5 Why it is shaped this way — reference, not steps

Nothing below is a command. It is here so a reader can tell a deliberate
refusal from a bug.

**Why not `apply --batch`.** The slice-2 batch unit is not the canary unit:
`b00001` alone is 5,000 artifacts and 10,157 silver rows against a 1,000-row
budget, so `apply --apply --batch` is refused, and forcing it with
`--maintainer-approval` commits 5,000 artifacts where the plan sizes the canary
at 234. `canary-commit` is manifest-scoped, and reuses slice 2's real
`assigned/` shards, `build_recovery_*` builders and `write_import_batch`.

**Three flags it does not have.** No `--probe` — a probe rolls back, and the
flush round trip cannot be proven on rolled-back rows. No
`--maintainer-approval` and no ceiling flag — `CANARY_ROW_BUDGET` is fixed in
code at 1,000, because a widenable ceiling is the same escape hatch renamed. No
slice-2 batch name — the receipt is `cmp-6c7c90d807bbdf13-canary`, since
borrowing `b00001`'s name would mark all 5,000 of its artifacts committed on
the strength of 505 rows.

**One thing `--apply` requires.** `--expect-manifest-sha256` and
`--expect-rows`. Unlike a ceiling flag these can only refuse.

**`--bucket` is refused, not half-honoured.** Reads take the bucket they are
given, but bare-key `object_exists` and `write_bytes` use the configured one,
so an override splits a run's inputs, checks and outputs across two buckets.
Set `MINIO_BUCKET` instead. (The same seam exists in `compare`, `assign` and
`apply`; making the whole file bucket-aware is separate work.)

**What the commit binds before issuing a statement**, in widening order:

| bound | catches |
|---|---|
| every manifest field vs the assignment shard — `artifact_id`, `id_source`, `input_kind`, `batch_name`, `page_listing_id`, `page_fetched_at`, `silver_rows`, `detail_rows` | a manifest and a shard that no longer describe one population |
| `vin_snapshot_sha256` vs the live snapshot | a VIN snapshot that moved after sampling — it fills the carousel VINs this would commit |
| per-artifact row count | a truncated read — half an artifact |
| per-artifact `detail_rows` and stratum set | a carousel row flipped to detail: same count, an extra price event |
| per-artifact `write_set_digest` over the **built** silver rows, price events and queue event | anything else at all |

The last is over the built write set — the exact column tuples the three
INSERTs send — because two things happen between the `to_import` rows and the
write set that a raw-row digest cannot see: a missing carousel `vin` is filled
from the VIN snapshot, and the queue event's historical `fetched_at` comes from
the assignment. The named checks above exist so those two are *diagnosed*
rather than surfacing as an opaque mismatch across hundreds of artifacts.

**Why the manifest is migrated, not re-sampled.** `canary-sample` reselects.
Determinism reproduces the selection only while every input is unchanged —
exactly the assumption the digest exists to distrust — and the aggregates (234
artifacts, 505 rows, 9 strata) cannot tell one 234-object set from another. It
is also create-if-absent, so that route means deleting the only record of the
window's subject before equality is established.

**Existence is not trust.** The migrated manifest records the bytes it was
promoted from and the object set they held, and every consumer re-proves the
promotion from the two manifests alone — not from the migration's report, which
is a separate object written afterwards by the run whose correctness is in
question. Three things are proved:

| proved | catches |
|---|---|
| it names the frozen manifest's exact bytes, as they stand now | a sibling promoted from something else |
| its object set equals the frozen manifest's, and equals what it recorded | a sibling over different artifacts |
| **every field the frozen manifest carried survives, per object key** | a sibling over the *same* artifacts that changed `detail_rows`, `strata`, `artifact_id`, `batch_name`, `id_source`, `input_kind`, `page_listing_id` or `silver_rows` |

The third is not implied by the first two: a substituted sibling can hold the
identical object set and carry `write_set_digest` values agreeing with
equally-mutated inputs, and everything downstream compares against those
*current* inputs. A promotion may add `page_fetched_at`, `write_set_digest`,
`vin_snapshot_sha256` and the two source columns; nothing else may move. So
**the frozen manifest must stay in place** — a promotion that can no longer be
checked against it is refused, not assumed.

**What the flush check looks for**, by key:

| table | lake prefix | key |
|---|---|---|
| `staging.silver_observations` | `silver_normalized/observations/source=…/obs_year=2026/obs_month=4/` | `artifact_id`, `listing_id`, `source`, `fetched_at` |
| `staging.price_observation_events` | `ops_normalized/price_observation_events/year=2026/month=4/` | `artifact_id`, `listing_id`, `event_type`, `event_at` |
| `staging.artifacts_queue_events` | `ops_normalized/artifacts_queue_events/year=…/month=…` | `artifact_id`, `status`, `run_id`, `fetched_at` |

It rebuilds that expectation from the manifest, not from the commit report: a
check that read its expectation out of the writer's own record of what it wrote
would pass on a writer that recorded the wrong thing. A silver row's `source`
lives in the **hive path, not the file** (`write_to_dataset` drops partition
columns), and the queue events land in the month the canary **ran** — recovery
leaves their `event_at` to `now()` by design.

**The commit time** comes from the receipt's `committed_at`, which V047 sets
inside the writing transaction and `canary-commit` reads back with `RETURNING`,
never from a wall clock — so a report write that failed after the commit is
repaired with the time the batch actually landed. Across a month boundary that
decides which queue-event partition gets read. There is no fallback: a missing
receipt time is a refusal.

### 7.6 What Phase B does not resolve

The 234 canary artifacts also belong to slice-2 batches `b*`. Once the canary
commits, a later full `apply` of those batches **writes the same 505
observations again**, which the Stage 5 gate's *no duplicate
`(listing_id, fetched_at)`* clause forbids. `canary-commit` writes a durable
record of exactly what it committed —
`recovery/plan145/canary/<run>-canary_commit.json`, naming the manifest digest,
the artifact ids and the assignment batches — so the full apply has something
authoritative to exclude with. **`apply` itself is unchanged and does not yet
read it.** Resolving that is the full apply's problem and the maintainer's
ruling, not Phase B's.

---

## 8. Where this sheet stops

After §6 you have: the authoritative four-family classification with its
inventory digest; the parser control's per-field census; the assignment census
and the sequence advance; and the canary sample's stratum coverage and manifest
key.

**Unproven, and to be said so explicitly in any report:**

- the write canary as a **real commit**, and the flush round trip into
  `silver_normalized/observations/` and `ops_normalized/` — Phase B is **built
  and tested but has never run**, against production or otherwise;
- the V040 before/after equality — needs Phase B's canary and a window;
- the duplicate-write interaction between the committed canary and a later full
  `apply` of the same batches (§7.6);
- the full apply — gated on all of the above plus named maintainer approval.

Do not merge a branch, do not open a Linear issue, and do not declare the gate
closed.

---

## Appendix A — blast radius at a glance

| § | step | objects written | Postgres | reversible |
|---|---|---|---|---|
| 2 | `compare --apply` | 1,760, ~0.3 GB | one read-only VIN `SELECT` | **done** |
| 3 | `control --apply` | 1 report | none | yes |
| 5.1 | `assign` | none | none | yes |
| 5.2 | `assign --apply` | ~80 shards + report | **`nextval` per artifact** | **no — permanent** |
| 5.3 | `apply` (dry) | none | none | yes |
| 6 | `canary-sample --apply` | 1 manifest + 1 report | none | yes |
| 7.1 | `canary-remanifest --apply` | 2 (the frozen manifest is preserved) | none | yes |
| 7.2 | `canary-commit` (dry) | none | none — no connection opened | yes |
| 7.3 | `canary-commit --apply` | 1 report | **505 silver + 140 price + 234 queue + 1 receipt, one transaction** | **no — a commit** |
| 7.4 | `canary-flush-verify --apply` | 1 report | none | yes |

## Appendix B — flags that are gates

`--no-verify`, `--allow-drift`, `--allow-rate-drift`,
`--allow-silver-shape-drift`, `--allow-unclassifiable-drift`, `--force`,
`--maintainer-approval`.

Each exists so a human can overrule one specific measured refusal after looking
at it. **None was needed by the compare run, and none is expected below.**
Reaching for one to make a run finish is how a plan built on measurement starts
shipping on assumption. Raise a specific ceiling rather than a blanket drift
flag — `--allow-unclassifiable-drift` disarms both ceilings at once.

## Appendix C — things that will cost you a debugging pass

- **`docker exec` may run stale code.** §0.1.
- **Pin `--run-id cmp-6c7c90d807bbdf13` on every command.** Auto-discovery
  exists but a second run under either prefix breaks it silently.
- **Do not re-run `compare`.** §2. And never re-run `parse --apply`,
  `dedupe --apply` or `unpack --apply` — Stage 4 is complete and Stage 3a's
  deletions are irreversible.
- **The probe prefixes are refused, not merely stale.** §1.
- **Four families, not three.** Any doc or memory saying three predates the
  block-page filter (PR #272).
- **Never use the sidecar `listing_id`**, including the copy in the unpack
  manifest — wrong for 313,701 of 457,084 named April members. Never read or
  compare `legacy_artifact_id`: the two `bigserial` sequences collide across the
  cutover.
- **`--probe` and a real commit are mutually exclusive, with no override.**
- **`compose run` starts `flyway` each time.** Idempotent; V047 is the head.
- **Never kill the four tmux sessions.** §0.3.
