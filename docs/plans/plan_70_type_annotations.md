# Plan 70: Type Annotations

**Status:** Not started
**Priority:** Low — enforced going forward by CI (Plan 62); this plan addresses existing code

Plan 62 (CI/CD) adds `mypy` as a gate on new code, but existing functions throughout `scraper/` and `dbt_runner/` lack type annotations. This plan is a systematic pass to add them.

## Scope
- `scraper/app.py` — all endpoint functions and helpers
- `scraper/processors/` — scraping and parsing functions
- `scraper/routers/admin.py` — route handlers and helpers
- `dbt_runner/app.py` — all endpoint functions and helpers

## Notes
- Don't need to annotate everything perfectly — `Any` is acceptable for complex asyncpg return types initially
- Focus on function signatures (parameters + return types) first; internal variables second
- Dashboard excluded — display layer, lower value for the effort
