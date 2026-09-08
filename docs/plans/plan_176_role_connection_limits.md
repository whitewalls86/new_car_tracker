# Plan 176: Postgres Role Connection Limits

## What this plan is for

Right-sizes the one Postgres role that carries a connection limit, and makes a
role hitting its cap visible. That role is the monitoring account, capped low
enough that it has already exhausted its limit in production and stopped
collecting database metrics for sixteen minutes.

## The case

Raised from a measurement taken during [Plan 154](plan_154_container_log_coverage.md)
Stage A, which sampled every container's logs to decide which deserve ingestion.
Postgres's own log turned out to hold one genuine incident in twenty days, and
this is it.

`metrics_user` is `postgres-exporter`'s account. It was created by
[Plan 86](plan_86_grafana.md)'s `V033__metrics_user.sql` with
`CONNECTION LIMIT 3`, against a server `max_connections` of 100. On 2026-08-29
it hit that cap:

```
05:55:28  FATAL:  too many connections for role "metrics_user"
05:58:52  FATAL:  too many connections for role "metrics_user"
05:58:52  FATAL:  too many connections for role "metrics_user"
```

Prometheus confirms what that cost. `up{job="postgres"}` reads 1 through 05:56,
drops to 0 at 05:57, and does not read 1 again until 06:13 — a **sixteen-minute
window in which no Postgres metric was collected at all**.

Two things make this worth a plan rather than a one-line fix.

**The limit was chosen once and never revisited.** `3` was written at role
creation in April and has not been re-derived against what `postgres-exporter`
actually opens. It is also the *only* per-role cap on this host — of the seven
login roles, `metrics_user` reads 3 and the other six (`airflow_user`,
`airflow_app_user`, `cartracker`, `dbt_user`, `scraper_user`, `viewer`) all
read `-1` — so there is no convention it follows and no reason to believe 3 is
the right number rather than the first number. `airflow_user` currently holds
11 connections against the server's 100; `metrics_user` holds 1 of its 3. The
server is not the constrained resource — the role is.

**Nothing makes the cap visible as it is approached.** The only artefact the
exhaustion produced was a `FATAL` in a Postgres log that has no path into Loki,
which is precisely why it took a log-coverage audit to find it four months
after the role was created. A role at 2 of 3 connections and a role at 3 of 3
are one scrape apart and indistinguishable from outside.

The failure mode is also self-concealing in a way worth stating: the account
that goes blind is the *monitoring* account. When `metrics_user` is refused, the
instrument that would report a database problem is the thing that breaks, so
the outage removes its own evidence. `shared/log_ingestion_policy.py` currently
justifies excluding Postgres from Loki on the grounds that "database health and
capacity are covered by postgres-exporter metrics" — and this is the case where
that coverage is exactly what fails.

It is latent, not historical. The cap is unchanged, the exporter's connection
behaviour is unchanged, and a single slow connection release reproduces it.

### Not Plan 64

[Plan 64](plan_64_pgbouncer.md) sits in the backlog behind the trigger
"Postgres connection exhaustion becomes the measured constraint," and this
measurement does not fire it. Plan 64 is about *server-wide* contention across
services sharing `max_connections`; a connection pooler in front of Postgres
does not change a per-role `CONNECTION LIMIT`. The two are independent, and
resolving this one should not be read as evidence for or against that one.

(Noted while reading it: Plan 64's document states `max_connections=50`. The
live value and the Compose flag are both 100. Left alone here.)
