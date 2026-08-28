# Plan 145 Stage 5 slice 2 — probe mode, so slice 2 can be tested while Stage 4 runs

Hand this to a fresh session. It is a small, well-bounded change to
`scripts/reconcile_april_detail.py` and its tests. No production run is part of
it.

Read `docs/reference/plan_145_reconcile_flags.md` first — it is the current
flag contract and this change adds to it. Then
`docs/plans/plan_145_stage_5_writer_handoff.md` for what `assign` and `apply`
do, and the **Stage 5** section of
`docs/plans/plan_145_april_cutover_reconciliation.md` for why the constraints
below are not negotiable.

---

## The problem

Slice 2 is merged and its only evidence is fixtures and integration tests. It
has never met real data, and it cannot until Stage 4 finishes — `compare`'s
authoritative `--apply` refuses to start until all 1,204 parse units are
complete, so `recovery/plan145/compared/` is empty and will stay empty for
hours yet.

Meanwhile Stage 4 is producing real output continuously. `compare --probe
--apply` can already turn that into a complete, real compare run under
`recovery/plan145/compared_probe/<run_id>/` — real April captures, real parsed
rows, real identity resolved against the real event lake. Partial, and
materialized-only until the unpacked units land, but real.

**Slice 2 cannot read it.** `run_assign` hardcodes
`run_dir = f"{COMPARED_PREFIX}/{run_id}"` and `_discover_compare_run` only lists
`COMPARED_PREFIX`. `--run-id` does not escape the prefix. So the one source of
real input available today is unreachable.

Closing that is the difference between finding slice 2's bugs during the parse
and finding them on the run that matters.

---

## What to build

### 1. `--probe` on `assign` and `apply`

Mirror slice 1 exactly — it already has the pattern at
`run_compare`'s `compared_root` / `inventory_root` (search `PROBE_SUFFIX`).
In probe mode:

- read `compared_probe/<run_id>/` and `inventory_probe/<run_id>.json`;
- discover runs under `compared_probe/`;
- write assignment shards and the assign report to **`assigned_probe/`**.

The call sites are `_discover_compare_run`, both `run_dir` constructions, the
paired inventory keys, and the three `ASSIGNED_PREFIX` helpers. Thread the
prefix rather than branching at each use — a helper taking `probe: bool` and
returning the three roots is cleaner than eight conditionals and is what slice 1
would have done with hindsight.

`assign --probe --apply` calls real `nextval`. That is correct and safe: the
sequence never reuses, and the plan already records that a value lost before the
assignment write is a harmless gap. Say so in the mode's help text so nobody
panics at a jump in `artifact_id`.

### 2. `apply --probe --apply` runs the real transaction and rolls it back

This is the point of the change, so do not shortcut it.

Execute **every statement** the authoritative path would: the silver insert, the
price events, the queue events, the receipt insert — against real Postgres, with
the real write set built from real data. Then `ROLLBACK` instead of `COMMIT`.

Constraints fire at statement time, not at commit, so a rolled-back run proves
the uuid cast on `price_observation_events.listing_id`, the NOT NULL on
`silver_observations.listing_id`, both CHECK constraints, and every type
coercion in the write set. What it does not prove — durability and the receipt's
retry semantics — is already covered by the real-Postgres integration tests.

Report what *would* have been written, per table, exactly as the dry run does.

### 3. Committing probe-derived rows is forbidden, with no override

A probe run reads a partial compare. Rows written from it would be invisible to
the authoritative run later, which would classify the same observations as
`to_import` again and write them a second time — manufacturing exactly the
duplicate `(listing_id, fetched_at)` writes the plan's success criteria set to
zero. The receipt table does not save you: different run, different batch names,
different digest.

So `--probe` and a real commit are mutually exclusive. Not behind
`--maintainer-approval`, not behind a drift flag. A refusal a human can wave
through will eventually be waved through.

---

## The resulting matrix

Add it to `docs/reference/plan_145_reconcile_flags.md` as part of this change.

| invocation | sequence | writes objects | Postgres |
|---|---|---|---|
| `assign` | untouched | none | none |
| `assign --probe` | untouched | none | none |
| `assign --probe --apply` | `nextval` | `assigned_probe/` | none |
| `assign --apply` | `nextval` | `assigned/` | none |
| `apply --probe` | — | none | none |
| `apply --probe --apply` | — | none | **full transaction, rolled back** |
| `apply --apply` | — | none | commits, budget-capped |

---

## Tests

Extend `tests/scripts/test_reconcile_april_detail.py`.

- probe `assign` discovers a run under `compared_probe/` and ignores a
  same-named run under `compared/`, and the reverse;
- probe `assign --apply` writes only under `assigned_probe/`, and an
  authoritative `assign` does not see those shards;
- **`apply --probe --apply` issues the full statement sequence and ends in
  `ROLLBACK`, not `COMMIT`** — assert against a fake connection recording the
  statements, and assert the ordering;
- a constraint violation in probe-apply surfaces as a failure rather than being
  swallowed by the rollback;
- probe-apply followed by a real query shows zero rows in all three staging
  tables (real-Postgres integration test);
- `--probe` combined with anything that would commit is refused, and
  `--maintainer-approval` does not lift it;
- the existing authoritative paths are unchanged — run the full slice 2 suite.

Watch each new assertion fail before you make it pass. The rollback test is the
one that is easy to write so that it passes without proving anything.

---

## Non-negotiables

1. **No probe run ever commits to Postgres.**
2. **Probe output never lands in an authoritative prefix**, and an
   authoritative run never reads a probe prefix.
3. **Do not change the authoritative behaviour** while adding the branch. The
   existing tests passing unchanged is the evidence.
4. **Scope every refusal to the run that can actually cause harm.** Slice 1
   shipped a gate that fired on dry runs, so the run whose only job was to
   measure a cohort died with one sentence instead of reporting it.

---

## When you are done

Report the new matrix, the statement sequence the rollback test captured, and
confirmation that the slice 2 suite is untouched. Then stop — running it against
`compared_probe/` is a production action and the maintainer's call.
