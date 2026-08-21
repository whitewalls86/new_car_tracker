# Plan 73: Scraper Code Review & Refactor

**Status:** Deferred — review after Plan 72
**Priority:** Low (reassess after linting pass)

`scraper/app.py` has accumulated significant scope: scrape logic, async job management, background threading, rotation guards, processing endpoints, and FastAPI wiring all in one file. This plan is a full code review and refactor — structural split plus logic quality pass.

## Scope
- Full read-through of `scraper/app.py` — identify logic that belongs in separate modules, complex functions that should be simplified, error handling gaps
- Structural split along natural seams (similar to Plan 50's dashboard split):
  - Job management (in-memory store, ThreadPoolExecutor, job lifecycle)
  - Rotation logic (`advance_search_rotation`, slot guards)
  - Processing endpoints (results pages, detail pages, cleanup)
  - FastAPI app wiring and lifespan
- Code quality pass:
  - Identify any functions doing too many things
  - Improve error handling and logging where gaps exist
  - Surface any other logic bugs found during review
- Add type annotations to refactored code (coordinates with Plan 70)

## Done
- Shared DB connection resources refactored
- Processors split into separate modules
- Unit test suite added (184 tests, 70% coverage) via Plan 61 work

## Remaining
- Verify full structural split is complete — job management, rotation logic, and FastAPI wiring all in separate modules?
- Any remaining code quality / error handling gaps
- Type annotations on refactored code

## Decision point (after Plan 72)
The linting pass required for CI/CD (Plan 62) will force a full read-through of scraper files. Use that pass to assess whether the full structural split is worth the investment, or whether the current state (processors split + shared DB resources) is sufficient. The scraper container has scope creep concerns that may or may not warrant a full restructure.

## Notes
- Behavior changes (bug fixes) and structural changes (refactor) should be committed separately so regressions are easy to bisect
- This plan does not touch `dbt_runner/` or `dashboard/` — scraper only
