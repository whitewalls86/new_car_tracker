# Plan 64: Connection Pooling — PgBouncer

**Status:** Not started
**Priority:** Medium

The project has a hard `max_connections=50` limit on Postgres, shared across 8 services. Under concurrent load (dbt build + detail scrape + dashboard + Orphan Checker + n8n firing simultaneously) the project can realistically spike toward that ceiling. When Postgres hits the limit it refuses new connections entirely — everything fails at once.

The deeper problem is that services use the connection layer inconsistently:
- `scraper` — asyncpg with a connection pool (correct)
- `dbt_runner` — psycopg2, new connection per function call, unbounded
- `dashboard` — Streamlit, opens connections per query
- `n8n` — direct Postgres node connections, unmanaged
- `dbt` build process — opens connections per model thread

Fixing `dbt_runner` in isolation only solves one service. The right fix is a coordination layer in front of Postgres that all services talk to.

## Solution: PgBouncer

PgBouncer is a lightweight connection pooler purpose-built for Postgres. It sits as a separate container between all application services and Postgres. Services connect to PgBouncer (which looks exactly like Postgres to them), and PgBouncer maintains a small real pool of Postgres connections, multiplexing them across all callers.

- Written in C, battle-tested at scale, 15+ years of production use
- Transparent to applications — just a connection string change
- Enforces a hard cap on real Postgres connections regardless of how many services connect simultaneously
- Standard pattern on any self-hosted Postgres data platform

## Implementation
- New `pgbouncer` container in `docker-compose.yml`
- All services update `DATABASE_URL` to point at PgBouncer port (5432 on PgBouncer, which forwards to Postgres internally)
- PgBouncer config: transaction pooling mode, ~10-15 real Postgres connections, up to 40 client connections

## Known issue to address: dbt + transaction pooling
dbt uses `SET` statements that don't survive transaction-level pooling (state is lost when the connection is returned to the pool). Two options:
- Run PgBouncer in **session mode** for dbt specifically (one real connection per session, less efficient but compatible)
- Set `SET search_path` in the dbt profile and use the `pgbouncer: true` flag in `profiles.yml` which disables the problematic statements

This is a known, documented issue with a known fix — just needs to be handled during implementation.

## Notes
- Application-level pooling (what asyncpg already does inside the scraper) only helps within a single service. It doesn't solve competition between 8 independent services.
- Depends on Plan 65 (auth) being aligned — PgBouncer sits between services and Postgres, which is also where role-based connections live
