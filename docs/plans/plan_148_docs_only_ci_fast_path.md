# Plan 148: Docs-Only CI Fast Path

## Status

CLOSEOUT, written 2026-08-23 after a documentation-only Plan 135 closeout ran the
same unit, Docker-build, dbt, and integration jobs as an application change.

Proposed priority **68 (medium)**. Effort **XS**.

## Problem

The single workflow in [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
runs four validation jobs for every pull request to `master`:

- `lint` scans the whole repository;
- `unit-tests` installs every service's Python dependencies and runs every
  non-integration test;
- `docker-build` builds every service image;
- `dbt` starts Postgres, MinIO, and Loki, builds dbt, and runs six integration
  slices.

That is the right contract when code, configuration, migrations, tests, or CI
itself changes. It adds no useful evidence when every changed path is under
`docs/`. The documentation tree already has a focused executable contract in
[`tests/test_planning_docs.py`](../../tests/test_planning_docs.py): plan-state
uniqueness, archive counts, required table structure, dangling links, recap
shape, and the planning system's other invariants.

[Plan 139](plan_139_test_suite_maintenance.md) measured the full CI critical
path at roughly 333 seconds. This plan does not make that path faster. It avoids
starting it when the diff cannot affect the application.

## Decision

Add a docs-only path through the existing workflow. A change qualifies only
when the changed-file set is non-empty and **every** path starts with `docs/`.

For that case, run:

```bash
pytest tests/test_planning_docs.py -q
```

and skip `lint`, `unit-tests`, `docker-build`, and `dbt` at the **job level**.
Do not use workflow-level `paths-ignore`: a workflow that never starts may
leave a required check pending, while a job that starts and is conditionally
skipped still produces an inspectable conclusion.

Any mixed diff runs full CI. These all deliberately disqualify the fast path:

- `.github/**`, including edits to the classifier itself;
- `tests/**`, including edits to `test_planning_docs.py`;
- root configuration and dependency files;
- application, migration, dashboard, Airflow, or Compose changes;
- a rename or deletion whose old or new path is outside `docs/`.

## Design

### 1. Classify the changed paths once

Add a small first job that checks out full history and compares the event's
base SHA with its head SHA. It exports one boolean, `docs_only`.

- Pull requests compare `pull_request.base.sha` to the checked-out head.
- Pushes compare `github.event.before` to the pushed head.
- An empty change set, an all-zero/missing base SHA, or any classifier error
  produces `docs_only=false` and therefore runs full CI.
- Read paths in a NUL-delimited form so spaces and unusual filenames cannot
  change the classification.

The classifier is a gate, so it fails closed. A false negative costs CI time; a
false positive suppresses tests that may matter.

### 2. Add the focused documentation job

The `docs-tests` job needs only checkout, Python 3.13, `pytest`, and
`tests/test_planning_docs.py`. It runs only when `docs_only=true`.

The job name is stable and explicit so branch-protection and PR output say what
was actually validated rather than presenting a generic green CI result.

### 3. Guard the existing jobs

Make each heavy job depend on the classifier and run only when
`docs_only=false`. Preserve the existing `lint` dependency for `unit-tests` and
`dbt` on the full path. A docs-only run should show:

- classifier: success;
- docs tests: success;
- lint, unit, Docker build, dbt/integration: skipped.

Do not duplicate the existing jobs into a second workflow. One workflow keeps
job names, branch-protection behavior, and dependency edges reviewable in one
place.

## Scope

### In scope

- changed-path classification for pull-request and `master` push events;
- focused execution of `tests/test_planning_docs.py` for docs-only diffs;
- job-level skipping of the four existing heavy jobs;
- a deterministic check of the classifier's boundary cases;
- verification on one docs-only PR and one mixed-path PR.

### Out of scope

- selecting smaller subsets for application changes;
- splitting or accelerating the full test suite (Plan 139 owns that);
- changing coverage thresholds or branch-protection policy;
- treating root Markdown files as docs-only;
- skipping CI for generated files or dependency-only changes;
- changing what `tests/test_planning_docs.py` asserts.

## Implementation

1. Add the changed-file classifier and expose `docs_only` as a job output.
2. Add `docs-tests` with the focused pytest invocation.
3. Add classifier dependencies and job-level conditions to `lint`,
   `unit-tests`, `docker-build`, and `dbt` without changing their steps.
4. Exercise the classifier against: docs-only, mixed docs/code, workflow-only,
   deleted non-doc file, empty range, and unavailable base SHA.
5. Open a docs-only PR and confirm the focused job passes while every heavy job
   reports skipped rather than pending.
6. Add a temporary non-doc change on the implementation branch (or use a
   dedicated test branch), confirm full CI is selected, then remove it before
   merge.

## Success criteria

1. A non-empty PR diff wholly under `docs/` runs
   `tests/test_planning_docs.py` and starts none of the four heavy jobs.
2. Any changed path outside `docs/` selects the existing full workflow.
3. Classification failure selects full CI.
4. The docs-only PR reaches a mergeable required-check state; no skipped or
   absent check remains pending.
5. A mixed-path verification run executes all existing jobs with their current
   dependency order and commands unchanged.

## Closeout evidence

### Mixed-fileset gate met, 2026-08-24

[PR #234](https://github.com/whitewalls86/new_car_tracker/pull/234) merged as
`9b171b2` after the changed-path classifier selected full CI. Lint passed in
11s, unit tests in 1m19s, Docker build in 1m23s, and dbt plus integration tests
in 4m10s. `Documentation tests` was skipped. This closes the mixed-fileset half
of the closeout gate; the docs-only half remains open.

## Rollback

Revert the workflow commit. Until this plan has passed both verification PRs,
the conservative fallback is the current behavior: run all jobs for every
change.
