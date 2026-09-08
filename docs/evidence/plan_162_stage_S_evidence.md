# Plan 162 Stage S — the instrument correction addendum, 2026-09-07

The stage closed on 2026-09-07 with 297 of 297 measurable branch points
covered both arms. The PR #379 review, run the same day before merge, found
the prober itself counting arms over rows the model discards, and the two
gate steps measuring a warehouse no single `dbt build` produced. Both were
corrected on the PR branch, and this file records what the stricter
instrument read: the stage's verdicts survive it, at **295 of 295**, with
every ledger still empty and no exit reopened.

## 1. What was wrong with the probe, and what changed

`probe_for()` built a branch's probe from the scope's FROM, its joins and the
model's full WITH chain — and never the scope's own WHERE. So a select-list
branch was counted over rows the scope itself discards: `mart_deal_scores`'
`scored` CTE filters `listing_state = 'active' and price is not null and
price > 0`, and a `case when v.msrp > 0 and v.price > 0` inside it read as
two-armed off rows with no price, which no row reaching the CASE can be. An
arm only discarded rows take is not an exercised arm, and every such branch
inflated the headline number.

The probe now carries the scope's WHERE for every expression it gates. Two
kinds are exempt because they are evaluated before the WHERE runs: the
WHERE's own conjuncts (restricting a conjunct's probe by the whole WHERE
would make its false arm unreachable by construction) and outer-join
conditions (match-or-miss happens at join time). With the filter applied, a
coalesce fallback the same scope's WHERE forecloses is now classified
unreachable with a named reason — the same verdict the prober already gave
fallbacks foreclosed by an upstream scope, one clause closer.

Two hardening repairs landed in the same commit and are part of what
"corrected instrument" means here, though neither changes a number today:
the base fixture seed writes a shift marker to MinIO and later-phase seeds
and the selector tests refuse a marker from a different UTC day (a run
straddling midnight now fails as a named reseed instead of asserting windows
a day off the data), and `compiled_model_paths()` reads the model list from
dbt's manifest rather than filename convention, matching the mutation gate.

## 2. What the stricter read named: two branches, both dead code

`mart_deal_scores.scored.coalesce_fallback.1` and `.3` —
`coalesce(pctl.national_price_percentile, 0.75)` on the display column and
in the scoring term — never took the NULL arm once probed under the scored
WHERE. Read the way this stage read its eleven, they are dead code, not a
coverage gap: `price_percentiles` is the same
`mart_vehicle_snapshot` relation under the same `price > 0` filter, `vin` is
non-null upstream (`int_latest_observation` filters `vin17 is not null` and
the contract's `not_null` asserts it), and `percent_rank()` cannot be NULL
over a partition a row belongs to — so every row surviving the WHERE joins
its own percentile row. Both coalesces were deleted per the `942caec`
precedent, with the relaxation consequence stated in place: an unmatched row
now surfaces as a NULL percentile and a NULL deal score rather than silently
scoring 0.75. No unit test pinned the fallback — every expectation is 0.0,
from single-row partitions that still match.

The measurable count is therefore **295, all 295 covered both arms**. The
295 is by construction — 297 at close, minus exactly the two deleted
coalesce_fallback branch points, with no other enumeration change in the
diff — because the CI log proves the gate passes with every ledger empty but
does not print the count.

## 3. What the gate ordering exposed, and the fixture repair

The two gate steps previously ran after `pytest tests/integration/dbt/`,
whose real-build tests seed later fixture phases and rebuild a subset of
models — so the gates measured a mixed warehouse, and their ledgers were
seeded from that state. They now run directly after `dbt build`, so they
describe exactly what the build produced, which is also the state the
documented repro (seed, build, run the gate alone) reproduces.

Against the clean base build, the mutation gate found two constraints no
longer load-bearing: `unique_int_listing_observation_fingerprints_observation_id`
and `unique_int_listing_state_fingerprints_artifact_id`. Both models
guarantee their key with a `row_number() = 1` dedupe, and the base fixture
held no partition with two rows — the colliding second waves lived only in
the incremental phases the gates no longer run after, so dropping either
dedupe produced no duplicate. That is a fact about the fixture, not the
constraints: production reparses an artifact and lands a second observation
row with the same `artifact_id` and `listing_id`.

The repair is data, not a ledger move: `_dedupe_collision_rows()` in
`scripts/seed_lake_snapshot_fixture.py` seeds one detail artifact reparsed
once — two rows, same artifact and listing, distinct `fetched_at` and price.
Unlisted, with no price events and a stale `fetched_at`, so the pair stays
out of the deal-score and freshness populations and every other scenario's
assertions read exactly as before. Both constraints remain recorded
LOAD_BEARING and are again demonstrably so; the 15-of-161 split is
unchanged.

## 4. Recipe

CI run [34161288564] failed the branch gate naming the two coalesce
fallbacks; run [34161645297] failed the mutation gate naming the two unique
constraints; run **[34162171648]**, on `car-79-plan-162-stage-s` at
`5b96948` (2026-09-07), is the clean re-measurement — job
"dbt model tests (real build)":

- `pytest tests/integration/dbt/test_branch_coverage.py -v -m integration`
  → 7 passed, directly after `dbt build`, with `UNIT_TEST_WAIVERS`,
  `BRANCH_COVERAGE_WAIVERS`, `UNPROBEABLE_BRANCHES` and
  `UNREACHABLE_BRANCHES` all `frozenset({})`;
- `pytest tests/integration/dbt/test_constraint_mutation.py -v -m
  integration` → 10 passed, LOAD_BEARING and DECORATIVE ledgers unedited.

Instrument commits, all on the PR #379 branch: `3da7c5f` (WHERE fidelity,
gate ordering, the fixture shift marker, manifest-based model enumeration),
`48b2a02` (the two deletions), `5b96948` (the reparse pair).

[34161288564]: https://github.com/whitewalls86/new_car_tracker/actions/runs/34161288564
[34161645297]: https://github.com/whitewalls86/new_car_tracker/actions/runs/34161645297
[34162171648]: https://github.com/whitewalls86/new_car_tracker/actions/runs/34162171648
