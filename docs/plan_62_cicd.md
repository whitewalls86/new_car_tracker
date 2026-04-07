# Plan 62: CI/CD — GitHub Actions

**Status:** Not started
**Priority:** High

Add a GitHub Actions workflow that automatically validates every PR before it can merge. CD (automated deployment) is out of scope for now — the deployment target is a single home server and the deploy step (`git pull + docker compose build + up`) requires a remote connection that adds complexity without enough payoff yet.

## Gates (in execution order)

| Step | Tool | What it catches |
|------|------|----------------|
| Lint | `ruff` | Syntax errors, unused imports, formatting drift |
| Type check | `mypy` | Type mismatches between functions |
| Unit tests | `pytest` | Business logic regressions |
| Docker build | `docker compose build` | Broken Dockerfiles, missing dependencies |
| dbt build + test | `dbt build` | Model compilation errors, data quality test failures |

## Workflow trigger
- On every pull request to `master`
- On every push to `master`

## Notes
- Workflow config lives at `.github/workflows/ci.yml` — travels with the code, versioned in git
- GitHub provides 2,000 free minutes/month for private repos; this pipeline will use ~3-5 min per run
- Fast gates (lint, type check) run first so failures surface quickly without waiting for slower steps
- dbt build step needs a test database — either a lightweight Postgres service container spun up in the GitHub Actions VM, or skipped initially and added once unit tests are in place
- Integration tests (if added) run separately from unit tests so the fast suite stays fast
