# Plan 145 Stage 5 — exclude the block pages Stage 4 failed to classify

Hand this to a fresh session. It is one bounded change to
`scripts/reconcile_april_detail.py` plus tests. **No production run is part of
it**, and the authoritative `compare --apply` must not be run until this lands.

Read `docs/reference/plan_145_reconcile_flags.md` for the flag contract, then
*The block-page defect* and the **Stage 5** section of
`docs/plans/plan_145_april_cutover_reconciliation.md` for why a block page must
never reach silver.

---

## The defect, measured

Stage 4 is complete — 1,204/1,204 units, 983,043 inputs, 5,738,532 rows, zero
`failed`, zero `missing_object`, zero identity disagreements. But its
block-page classifier is **structurally dead for every object whose identity
resolved**, and the plan's `Block pages imported as observations: 0` success
criterion is currently unenforceable.

Two lines that cannot both be true:

- `scripts/reconcile_april_detail.py:2069-2073` builds the parser's `url`
  argument as `https://www.cars.com/vehicledetail/{listing_id}/` whenever
  identity came from `legacy_manifest` or `queue_events`. The parser falls back
  to that URL when the page carries no `initial-activity-data` blob — which is
  exactly what a block page is.
- `scripts/reconcile_april_detail.py:2084-2086` only records `blocked_other`
  when `primary["listing_id"] is None`.

So `blocked_other` can fire **only** when `url is None`, i.e. only for tier-3
objects with `listing_id_source == 'none'`. That is precisely what the data
shows.

Measured on the VM 2026-08-28, over the completed Stage 4 output:

| | |
|---|---:|
| `blocked_other` recorded by Stage 4 | **4,966** |
| plan's expectation for that cohort | ~54,341 |
| every one of those 4,966 has `listing_id_source` | `none` |
| objects < 1 KiB classified `parsed` instead | **59,458** |
| detail rows `active` with `price`, `vin`, `make` all NULL | **59,460** |
| of those, from a source object < 1 KiB | 59,458 |

The leaked cohort by identity tier — note it is the *resolved* ones that leak:

| size band | input kind | `listing_id_source` | objects | outcome |
|---|---|---|---:|---|
| 000000-000511 | unpacked | `none` | 4,966 | `blocked_other` ✅ |
| 000000-000511 | unpacked | `queue_events` | 44,369 | **`parsed`** ❌ |
| 000000-000511 | unpacked | `legacy_manifest` | 7,470 | **`parsed`** ❌ |
| 000000-000511 | materialized | `legacy_manifest` | 7,616 | **`parsed`** ❌ |

### Why this is not yet a catastrophe, and still must be fixed

Production wrote the same junk rows in April — the plan already records 87,003
all-NULL active April detail rows in silver, 6.84% of the month. So the
existence test routes most of these to `already_represented` rather than
`to_import`. In the partial probe compare `cmp-e37723ede49fad4f` (1,186 of
1,204 units):

| family | all-NULL-active detail rows |
|---|---:|
| `already_represented` | 8,177 |
| `to_import` | **2** |

Two is already more than zero, and that probe covered roughly 14 of the 32
unpacked shards while the leak is concentrated in unpacked objects. **The full
exposure is unmeasured.** Do not treat "probably small" as the ruling.

### Why the fix is not a re-parse

Re-parsing is ~17 hours and would re-derive 983,043 objects to correct a
classification the plan says belongs downstream anyway:

> The filter is applied in the recovery pipeline, not in the parser, so the
> "parser runs unmodified" property that makes the comparison meaningful is
> preserved.

The parsed output is correct as a record of what the parser returned. The
filter belongs in `compare`, where classification already lives.

---

## What to build

### 1. A fourth compare family: `blocked_excluded`

Mirror how slice 1 added `unclassifiable` — a counted family, not a silent
drop. A dropped row would break the one assertion that makes *classified
exactly once* enforceable.

**Filter at the object level, not the row level.** A block page's detail row is
the junk signal, but if such an object also emitted carousel rows those are
junk too — they would be carousel hints read off a 439-byte `Access Denied`
body. Quarantine the whole object.

The block signature, on the object's `source == 'detail'` row:

```text
listing_state == 'active'
AND price IS NULL AND vin IS NULL AND make IS NULL
```

This is the same predicate the plan uses to measure the cohort in *The
block-page defect*, and it is deliberately independent of body size — do not
add a size threshold, which would be a second, unproven rule.

**Implementation shape.** In `run_compare`'s classification loop
(`scripts/reconcile_april_detail.py:3329-3384`), take two passes over `prows`
for each unit:

1. collect `blocked_keys` — every `object_key` whose detail row matches the
   signature above;
2. classify as today, except that a row whose `object_key` is in
   `blocked_keys` is routed to `blocked_excluded` with
   `reason = 'blocked_page'` **before** `classify_from_summary` is consulted.

Per-unit is the correct scope: an object's rows are all written by the unit
that parsed it. An object key appearing in two units (the content-derived
duplicate case the comment at `:3389-3391` describes) has a complete copy of
its rows in each, so each unit quarantines its own copy independently.

`_compared_schema` (`:2908`) already generalises — a new family gets the base
parsed-row schema plus `reason`, with no branch needed.

### 2. Make the sum assertion four-way

`family_sum` at `:3481` must become
`already + to_import + unclassifiable + blocked_excluded`, and still equal the
parsed row total. Extend the report block at `:3553-3560` and the printed
summary the same way.

### 3. Report it as a measurement

`blocked_excluded` gets its own report section carrying, at minimum:

- total rows and total distinct objects excluded;
- the split by `source` (detail vs carousel — a nonzero carousel count is
  interesting and should be visible, not assumed away);
- the `size_band` cross-tab of the excluded objects;
- the split by `input_kind` and `listing_id_source`.

Print the object count and row count in the summary block next to the other
families.

### 4. A gate that the filter is precisely targeted

Fail-closed, in the style of the existing ceilings: **stop an
`apply and not probe` run if any excluded row carries a non-NULL `price`,
`vin` or `make`.** That cannot happen if the predicate is written correctly, so
if it fires the predicate is wrong and the run must not continue. A dry run or
probe warns and reports instead of stopping — slice 2's non-negotiable #4 (a
refusal must be scoped to the run that can actually cause harm) applies here.

Do **not** add a magnitude ceiling on the excluded count. The cohort size is
what this change exists to measure; a ceiling tuned to today's 59,460 would
just be re-asserting the number we already have.

### 5. Defence in depth in `assign`

`assign` re-validates `listing_id` on every `to_import` row because it is the
last thing standing before identity is allocated (see the plan's *Refusals*
paragraph). Add the block signature to that same check: **`assign` stops if any
`to_import` row it is about to assign carries the block signature**, having
first counted the whole cohort so the maintainer learns its size rather than
the first offending row. Cap only the printed examples, as the existing check
does.

This costs one predicate and closes the path where a compare run predating this
change is assigned by a build that has it.

---

## What this changes downstream, and what it does not

- **Family percentages move.** Rows currently in `already_represented` (8,177
  in the partial probe, more at full scale) move to `blocked_excluded`. That is
  the correct reading — a block page is not *represented*, it is *excluded* —
  but it means the probe's 81.1% / 18.9% split is superseded. Say so in the
  report rather than letting it read as drift.
- **Carousel fan-out and the near-duplicate cohort shift slightly.** Both are
  maintainer rulings taken off the authoritative `compare_report.json`, so they
  must be read after this change, not before.
- **Stage 4's output is not rewritten.** `parsed/rows/` and `parsed/inputs/`
  stay exactly as they are; `parse_report.json`'s `blocked_other: 4966` remains
  an accurate record of what Stage 4 classified. This change is about what
  `compare` does with those rows.
- **No re-parse, no `--force` on Stage 4, no parser change.**

---

## Tests

Extend `tests/scripts/test_reconcile_april_detail.py` (lettered sections; add a
new one at the end).

```
LOG_PATH=/tmp/p145test.log .venv/bin/python -m pytest \
  tests/scripts/test_reconcile_april_detail.py -q -m "not integration"
.venv/bin/python -m ruff check .
```

Cover at least:

- an object whose detail row is `active` with `price`/`vin`/`make` all NULL is
  excluded **whole** — its carousel rows go to `blocked_excluded` too, and none
  of its rows reach any other family;
- an object whose detail row is `active` with a price is untouched, **including
  when its own carousel rows have NULL price/vin/make** — this is the test that
  fails a row-level filter, so write it first and watch it fail;
- an `unlisted` detail row with NULL price/vin/make is **not** excluded — an
  unlisted page legitimately has no price, and excluding it would discard
  316,896 real observations;
- the four families sum to the parsed row total, asserted on a fixture with at
  least one row in each;
- a row reaching `blocked_excluded` with a non-NULL `make` stops an
  `apply and not probe` run and only warns on a dry run and on a probe;
- `assign` refuses a `to_import` population containing the block signature,
  reports the cohort size, and caps the printed examples;
- the existing three-family tests still pass unchanged.

The second test is the one that is easy to write so it passes without proving
anything. A row-level filter passes every other test in this list.

---

## Non-negotiables

1. **Do not run the authoritative `compare --apply` before this lands.** The
   run is minutes; a run whose `to_import` carries block pages is worth
   nothing and would have to be re-done under `--force`.
2. **Do not modify the parser**, and do not modify Stage 4's classifier at
   `:2084-2086`. Its blindness is documented, out of scope, and its own ticket.
   Correcting it now would invalidate the parsed output this change consumes.
3. **Do not re-parse.** `parsed/rows/` and `parsed/inputs/` are frozen inputs.
4. **No production write.** This change is built and unit-tested only; running
   it is the maintainer's call.
5. Everything in slices 1 and 2's non-negotiables still holds.

---

## When you are done

Report: the four-family split on your fixtures; the exact predicate you
implemented and where it sits in the loop; confirmation that the whole object
is quarantined and that the unlisted case is untouched; the new report fields;
and the `assign` refusal's message. Update
`docs/reference/plan_145_reconcile_flags.md` with the fourth family and any new
flag.

Then **stop**. Running the authoritative `compare --apply`, and the ruling on
how many rows the filter removed, are the maintainer's.
