# Plan 63: Schema Migration Management — Flyway

**Status:** Not started
**Priority:** Medium

Currently `schema_new.sql` is a pg_dump snapshot of the current state — it answers "what does the schema look like now" but not "how do I get an existing database from an older state to the current one." Every schema change is applied manually to production with no rollback capability and no audit trail.

## The problem this solves
- Schema changes applied by hand with no rollback if something breaks
- No ordered history of what changed and when
- A fresh install runs the full dump fine, but an existing database that's one version behind has to figure out what changed manually
- CI/CD (Plan 62) needs a reliable way to stand up a test database from scratch for SQL query tests (Plan 77)

## Tool: Flyway
SQL-first migration tool — just numbered `.sql` files in `db/migrations/`. Flyway tracks applied migrations in a `flyway_schema_history` table and applies any unapplied ones in order. No Python required, no ORM dependency, no autogeneration complexity. Matches how we already write migrations.

## Implementation
- Add a `flyway` container to `docker-compose.yml` (or run as a one-shot job on deploy)
- Rename existing ad-hoc migration scripts in `db/schema/` to Flyway naming convention: `V001__initial_schema.sql`, `V002__add_customer_id.sql`, etc.
- All future schema changes go in `db/migrations/` as new versioned files — never edit existing ones
- `schema_new.sql` retained as a reference for fresh installs and documentation

## CI/CD integration (Plan 62)
Flyway runs as a step in the GitHub Actions pipeline against the ephemeral test Postgres container, applying all migrations from scratch before SQL tests (Plan 77) and dbt tests run. This validates that migrations are correct and complete before any merge.

## Notes
- Flyway community edition is free and sufficient for this project
- `db/schema/` and `db/seed/` structure maps naturally to the migrations + seed convention with minimal reorganization
