# Plan 66: SQL Injection Audit

**Status:** Not started
**Priority:** Medium (required before any public deployment)

SQL injection is a separate concern from authentication — it's about how queries are constructed, not who's allowed to make them. An authenticated user could still exploit an injection vulnerability.

## Current state
asyncpg and psycopg2 both use parameterized queries by default (`$1` placeholders and `%s` respectively), which are injection-safe. The risk is likely low but unverified — no systematic audit has been done.

## Scope
- Audit every database call in `scraper/`, `dbt_runner/`, and `dashboard/` for string-formatted SQL
- Audit n8n Postgres nodes — SQL entered directly in the n8n UI is a common injection surface
- Verify all form inputs that flow into queries go through parameterization, not f-strings or `.format()`
- Check any dynamic query construction (e.g. ORDER BY clauses, table name interpolation) — these can't be parameterized and need explicit allowlists

## Output
A short audit report noting any findings and the fix applied. If nothing is found, that's the output — documented confidence rather than assumed safety.
