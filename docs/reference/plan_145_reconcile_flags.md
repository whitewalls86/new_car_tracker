# `reconcile_april_detail` — every mode and every flag

Reference for `scripts/reconcile_april_detail.py`, the one script carrying Plan
145 Stages 1–5. Generated against `master` at `d4df9cb`; re-derive with
`python -m scripts.reconcile_april_detail <mode> --help` if in doubt — **this
page is a convenience, the parser is the authority.**

## The two words that matter

**`--apply` means write.** Every mode except `census` defaults to a dry run
that plans, validates and reports, and writes nothing. A dry run is safe to run
against production at any time.

**`--probe` means disposable** (`compare`, `assign` and `apply`). It routes
every Stage-5 read and write to a parallel `*_probe` prefix
(`compared_probe/`, `inventory_probe/`, `assigned_probe/`, `vin_snapshot_probe/`)
and relaxes the gate that demands a complete Stage 4, so slice 2 can be run
against real data while Stage 4 is still parsing. Probe output is **never
promoted** to the authoritative prefixes, and an authoritative run never reads a
probe prefix. `apply --probe --apply` issues every statement against real
Postgres and then rolls the transaction back; **no probe run ever commits, and
there is no flag that lifts that** — a committed probe batch would be re-imported
by the authoritative run and written twice.

The two are orthogonal, which is the part people get wrong:

| | writes nothing | writes |
|---|---|---|
| **authoritative** | `compare` | `compare --apply` |
| **disposable** | `compare --probe` | `compare --probe --apply` |

The same orthogonality holds for `assign` and `apply`; the full matrix is below.

## Blast radius, by mode

| mode | stage | reads | writes | touches Postgres |
|---|---|---|---|---|
| `census` | 1 | legacy Parquet | local CSV/JSON only | no |
| `materialize` | 2 | legacy Parquet | `html/…` objects + `materialized/` manifests | no |
| `dedupe` | 3a | manifests + sidecars | **deletes** `html/…` objects, writes receipts | no |
| `unpack` | 3b | April packs | `html/…` objects + `unpacked/` manifests | no |
| `parse` | 4 | flattened `html/…` | `parsed/rows`, `parsed/inputs`, `parse_report.json` | no |
| `compare` | 5.1 | parsed + silver + events | `compared/`, `inventory/`, `vin_snapshot/` | one read-only `SELECT` |
| `assign` | 5.2 | `compared/<run>/to_import` | `assigned/` | `nextval` only |
| `apply` | 5.2 | `assigned/` | **three staging tables + receipt** | yes, writes |

`dedupe --apply` and `apply --apply` are the only two that are hard to undo.
`apply --probe --apply` touches Postgres but rolls back, so it undoes itself.

### `assign` / `apply` × `--probe` × `--apply`

| invocation | sequence | writes objects | Postgres |
|---|---|---|---|
| `assign` | untouched | none | none |
| `assign --probe` | untouched | none | none |
| `assign --probe --apply` | `nextval` | `assigned_probe/` | none |
| `assign --apply` | `nextval` | `assigned/` | none |
| `apply --probe` | — | none | none |
| `apply --probe --apply` | — | none | **full transaction, rolled back** |
| `apply --apply` | — | none | commits, budget-capped |

`--probe` and a real commit are mutually exclusive with no override:
`apply --probe --maintainer-approval …` is refused rather than run, and the
canary row budget does not apply to a probe (it caps a commit; a probe has
nothing to cap).

Because a probe has no row budget, `apply --probe --apply` **requires an
explicit `--batch`** — a bare run would replay the whole assigned population
against production Postgres (rolled back, but still WAL and dead tuples), and
one batch proves every constraint and coercion the probe exists to check.

`compute_run_id` hashes the frozen input inventory, so each `compare --probe
--apply` as Stage 4 advances mints a **new** `run_id`. Once a second probe
compare lands, `assign --probe` / `apply --probe` stop auto-selecting and need
`--run-id`; the "one complete run" default below is only reliable for the
authoritative prefix.

## Universal flags

| flag | default | meaning |
|---|---|---|
| `--bucket` | `MINIO_BUCKET` | override the bucket |
| `--log-level` | `INFO` | `DEBUG` \| `INFO` \| `WARNING` |
| `--progress-every` | mode-specific | log every N units (not on `assign`/`apply`) |

## Per-mode flags

### `census` (Stage 1) — the only mode with no `--apply`; it never writes
| flag | default | meaning |
|---|---|---|
| `--out-dir` | cwd | where the CSV/JSON report lands |
| `--prefix` | discovered | legacy Parquet prefix |
| `--max-objects` | all | stop after N source objects |
| `--max-examples` | 20 | sample rows kept per finding |
| `--allow-drift` | off | report a census that disagrees with the frozen baseline instead of stopping |

### `materialize` (Stage 2)
| flag | default | meaning |
|---|---|---|
| `--apply` | off | write the `.html.zst` objects and manifests |
| `--prefix` | discovered | legacy Parquet prefix |
| `--max-objects` | all | stop after N source objects |
| `--force` | off | re-process a source file whose manifest already exists |
| `--no-verify` | off | skip the read-back hash check. **Do not use** — the check is the stage's gate |

### `dedupe` (Stage 3a) — deletes objects
| flag | default | meaning |
|---|---|---|
| `--apply` | off | actually delete |
| `--pack-prefix` | April packs | where sidecar hashes are read from |
| `--max-shards` | all | stop after N manifest shards |
| `--batch-size` | 1000 | keys per `delete_objects` call (the S3 cap) |
| `--expect-rate` | 0.456 | expected share of candidates deleted |
| `--rate-tolerance` | 0.10 | how far off that may be before stopping |
| `--allow-rate-drift` | off | report an off-band rate instead of stopping |
| `--force` | off | re-run a shard that already has receipts |

### `unpack` (Stage 3b)
| flag | default | meaning |
|---|---|---|
| `--apply` | off | write members back as loose objects |
| `--pack-prefix` | April packs | packs to unpack |
| `--max-packs` | all | stop after N packs |
| `--force` | off | re-run a pack that already has a manifest |
| `--no-verify` | off | skip per-member `raw_sha256` verification. **Do not use** |

### `parse` (Stage 4)
| flag | default | meaning |
|---|---|---|
| `--apply` | off | write `parsed/rows`, `parsed/inputs`, `parse_report.json` |
| `--workers` | `cpu_count()-2` | process pool size; bs4/lxml is GIL-bound |
| `--max-units` | all | stop after N of the 1,204 units |
| `--force` | off | re-parse a unit whose outputs exist |

`--apply` refuses until Stage 3b is complete: 1,172 materialized shards,
32 unpack shards, 557,065 members, 983,043 flattened inputs.

### `compare` (Stage 5 slice 1)
| flag | default | meaning |
|---|---|---|
| `--apply` | off | write the compared families, inventory freeze and VIN snapshot |
| `--probe` | off | run against whatever Stage 4 units exist; `*_probe` prefixes; skips the completeness gate |
| `--silver-prefix` | `silver_normalized/observations` | override the silver root |
| `--allow-silver-shape-drift` | off | proceed under `--apply` when silver is not the frozen nine objects |
| `--max-units` | all | first N parsed row shards (lexical, **not a sample**) |
| `--force` | off | re-run a `run_id` whose outputs already exist |
| `--duckdb-threads` | 1 | thread cap for the silver scan |
| `--duckdb-memory-limit` | `2GB` | DuckDB ceiling; empty string disables |
| `--vin-batch` | 1000 | listing_ids per read-only `vin_to_listing` SELECT |
| `--max-unclassifiable` | 2000 | stop if more **`no_capture_time`** rows than this |
| `--max-no-listing-id` | 0 | stop if any **`no_listing_id`** rows; set to the measured number after a ruling |
| `--allow-unclassifiable-drift` | off | warn instead of stopping — **disarms both ceilings at once** |

Both ceilings only stop an `apply and not probe` run; a dry run or probe warns
and lets the report carry the counts. Measured 2026-08-28: neither is expected
to trip, because the 5,260 objects without a capture time are all block pages
and emit no rows.

**Four families, not three.** `compare` classifies into `already_represented`,
`to_import`, `unclassifiable` and — since Plan 145 Stage 5's block-page filter —
`blocked_excluded`. Stage 4's block-page classifier is structurally dead for any
object whose identity resolved, so a block page with a `legacy_manifest` /
`queue_events` listing id parsed to an `active` detail row with `price`, `vin`
and `make` all NULL and leaked into the other families. `compare` now
quarantines the **whole object** (its detail row and any carousel rows it
emitted) into `blocked_excluded` with `reason = blocked_page`, before
`classify_from_summary` is consulted. The signature is on the detail row and is
**independent of body size** by design. The four families sum to the parsed row
total — that is what makes "classified exactly once" enforceable — and the
`blocked_excluded` report section carries the row and object counts, the
detail/carousel split, `objects_that_emitted_carousel_rows`, and the
`size_band` / `input_kind` / `listing_id_source` cross-tabs of the excluded
objects.

There is **no flag and no magnitude ceiling** (the cohort size is the
measurement). Two things are surfaced for a maintainer:

- `objects_that_emitted_carousel_rows` — a 439-byte block body has no carousel,
  so a nonzero count is evidence the predicate caught a real page. This is
  **reported, never raised**: nobody has measured whether block pages emit
  carousel rows, and a hard stop on an unmeasured number would be a gate tuned
  to an assumption. Zero is the confirmation the plan never got.
- `detail_rows_carrying_a_business_value` — tautologically zero today (a
  quarantined detail row matched `price/vin/make` all NULL). It is a cheap
  predicate-integrity guard: if `is_block_signature` is ever loosened so a
  quarantined detail row can carry a value, an `apply and not probe` run
  **stops** (a dry run or probe warns).

`assign` guards this two ways. First, it **refuses any run whose
`compare_report.json` has no `blocked_excluded` section** — such a report is by
construction a compare run that predates the filter, and its `to_import` family
may carry block pages in a shape the per-row check cannot see (a block page's
detail row can sit in `already_represented` while only its junk carousel rows
reach `to_import`). Re-run `compare` first. Second, it re-checks the detail-row
signature on every `to_import` row as defence in depth and refuses the
population — reporting the whole cohort's size, capped examples — which catches
the plain case where the detail row itself is in `to_import`.

The carousel fan-out and multi-candidate share are now measured over importable
objects only (the `blocked_excluded` objects are out of both numerator and
denominator), which makes them sharper than the probe's figures, not drift.

### `assign` (Stage 5 slice 2)
| flag | default | meaning |
|---|---|---|
| `--apply` | off | allocate sequence values and write the assignment shards |
| `--probe` | off | assign a `compared_probe/` run; write shards + report to `assigned_probe/`. `--probe --apply` calls the real `nextval` (a lost value is a harmless `bigserial` gap — expect `artifact_id` to jump). Never promoted. |
| `--run-id` | the one complete run | which compare run to assign (under `compared_probe/` with `--probe`) |
| `--max-artifacts` | 5000 | artifacts per batch; changing it changes every batch's membership |
| `--max-silver-rows` | 50000 | rows per batch; an artifact is never split |

### `apply` (Stage 5 slice 2) — the only mode that writes to Postgres
| flag | default | meaning |
|---|---|---|
| `--apply` | off | actually write; without it the whole write set is built, validated and printed |
| `--probe` | off | apply a `compared_probe/` run from `assigned_probe/`. `--probe --apply` runs every statement (silver insert, price events, queue events, receipt) against real Postgres in one transaction, then `ROLLBACK`. Never commits; the canary budget does not apply; `--maintainer-approval` is refused alongside it; **`--batch` is required**. |
| `--run-id` | the one complete run | which run's assignment shards to apply |
| `--batch` | every batch (authoritative); **required** under `--probe --apply` | batch name, repeatable |
| `--max-unapproved-rows` | canary budget | silver rows an authoritative `--apply` may write without named approval (a probe writes nothing durable, so it is exempt) |
| `--maintainer-approval NAME` | none | named approval to exceed that budget; refused if combined with `--probe` |

Plan 145 allows nothing beyond the canary until slice 3 closes the live-state
proof. `--maintainer-approval` is a record of a human decision, not a way past
one.

## Flags that are gates, and must not be routine

`--no-verify`, `--allow-drift`, `--allow-rate-drift`, `--allow-silver-shape-drift`,
`--allow-unclassifiable-drift`, `--force`, `--maintainer-approval`.

Each exists so a human can overrule a specific measured refusal after looking at
it. Reaching for one to make a run finish is how a plan built on measurement
starts shipping on assumption. Prefer raising the specific ceiling — e.g.
`--max-no-listing-id 3` — over a blanket drift flag, which disarms checks you
did not mean to touch.

## Where each mode runs

| image | has | use for |
|---|---|---|
| `cartracker-archiver` (`scraper_user`) | duckdb, pyarrow, boto3, psycopg2 | `census`, `materialize`, `dedupe`, `unpack`, `compare` |
| `cartracker-processing` / `april-processor` (`cartracker`) | bs4, lxml, pyarrow, boto3 — **no duckdb** | `parse`, `assign`, `apply` |

`assign`/`apply` run as `cartracker` because `scraper_user` has no INSERT on
`staging.price_observation_events`. That image has no duckdb, and the import is
lazy — a duckdb dependency there fails *after* the I/O, not at startup.
