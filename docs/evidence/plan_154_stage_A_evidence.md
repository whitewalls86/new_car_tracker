# Plan 154 Stage A — evidence

Supporting detail for the Stage A entry in
[`plan_154_container_log_coverage.md`](../plans/plan_154_container_log_coverage.md)
`## Record`. Everything here was read from the production host on 2026-09-07;
nothing was deployed or changed.

## Method

Volume was sampled per container over a single 24-hour window ending
**2026-09-07T19:27Z**, rather than over a shorter window chosen to contain one
scrape and one dbt run. Every container had been up at least 26 hours and
Docker rotation is `50m × 3` (150 MB per container), so a full day was
available for all eighteen without truncation.

```bash
for svc in <the eighteen>; do
  c="cartracker-$svc"; [ "$svc" = caddy ] && c=caddy
  docker logs --since 24h "$c" > /tmp/m.log 2>&1
  printf '%s\t%s\t%s\n' "$svc" "$(wc -l < /tmp/m.log)" "$(wc -c < /tmp/m.log)"
done
```

Note the redirection order. `docker logs ... > file 2>&1` is required; the
intuitive `2>&1 > file` sends stdout to the file and leaves stderr on the
terminal, and most of these containers log to **stderr** — an early attempt
that way reported `caddy` as producing zero lines.

Window validity, measured the same way: `scraper` 139,747 lines and
`dbt_runner` 8,727 lines inside the window.

## Line-shape composition

Taken by normalising timestamps and digits out of each line and counting the
most repeated shapes:

```bash
sed -E 's/[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9:.,]+Z?//g; s/[0-9]+/N/g' \
  | sort | uniq -c | sort -rn | head -3
```

| Service | Dominant shapes |
|---|---|
| `container-health` | 5,760 `INFO: N.N.N.N:N - "GET /metrics HTTP/N.N" N OK`; 2,871 the same for `/health` |
| `pgadmin` | 2,876 `"GET /pgadmin/misc/ping HTTP/N.N" N N "-" "Wget"` |
| `airflow-triggerer` | 1,438 `N watchers currently running`; 1,438 `N triggers currently running` |
| `grafana` | 1,440 `ngalert.sender.router ... "Sending alerts to local notifier"`; 288 each of two `tsdb.loki queryData` shapes |
| `redis-trawl` | 24 each of `N changes in N seconds. Saving...`, `Background saving started`, `Background saving terminated with success` |
| `prometheus` | 12 `Head GC started`, 12 `Head GC completed`, 6 `WAL checkpoint complete` |
| `promtail` | 33/21/15 `received file watcher event ... op=CREATE` for processing, scraper, dbt_runner |
| `trawl` | 329 `[challenge] Turnstile shadow-DOM checkbox clicked`; 47 `Turnstile keyboard Tab+Space attempted`; 31 `cf_clearance set but still on challenge page` |
| `docker-socket-proxy` | HAProxy: `::ffff:172.19.0.3:N [...] dockerfrontend dockerbackend/dockersocket 0/0/0/0/0 200 ... "GET /v1.44/containers/<id>..."` |

`grafana`'s apparent 866 `error` matches are `error=null` fields. Filtering on
`level=error` gives **2** lines in 24 hours, both
`Failed to fetch usage stats from provider ... plugin grafana-amazonprometheus-datasource not found`.

## The seven boot-only services

`docker logs` with no `--since`, whole container lifetime:

| Service | lines all-time | last line |
|---|---:|---|
| `loki` | 1,393 | 2026-09-02 `level=error ... "closing iterator" ... context canceled` |
| `minio` | 159 | 2026-08-31 startup banner |
| `node-exporter` | 114 | 2026-08-31 `TLS is disabled.` |
| `flaresolverr` | 27 | 2026-08-31 `Serving on http://0.0.0.0:8191` |
| `postgres-exporter` | 15 | 2026-08-31 `Semantic version changed ... 16.14.0` |
| `statsd-exporter` | 9 | 2026-08-31 `Accepting Prometheus Requests` |
| `dashboard` | 8 | 2026-08-31 |

`loki` is the exception in kind but not in verdict: it does emit occasional
genuine errors, and stays excluded under principle 4 regardless.

## caddy

```
total 24h = 2,884 lines / 711,842 bytes
admin.api = 2,876   (99.7%)
remainder = 8       ACME renewal + TLS storage cleaning
```

The healthcheck fires every 30s → 2,880/day, which is the whole of the traffic.
`grep -nE "^\s*log\s*[\{\s]|access_log|log_file" Caddyfile` returns no matches:
there is no access-log directive in any handle block.

## postgres settings

`select name, setting from pg_settings where name like 'log%'`, the load-bearing rows:

```
log_min_duration_statement = -1        log_checkpoints            = on
log_lock_waits             = off       log_autovacuum_min_duration = 600000
log_connections            = off       log_destination            = stderr
log_disconnections         = off       logging_collector          = off
log_statement              = none      log_min_messages           = warning
```

## postgres error forensics

The container was restarted 7 days before the window but not recreated, so
`docker logs` reaches back to **2026-08-18** — twenty days of history.

Interactive-session tells, which account for the large majority of the ~290
error lines:

```
column " rows / " does not exist              pasted table output
column " receipts | " does not exist
column "recovered qevts " does not exist
syntax error at or near "\"                   psql backslash quoting
syntax error at or near "%"                   shell format string
unterminated quoted identifier at or near ""SELECT"
relation "ops.listings" / "ops.artifacts" / "ops.raw_artifacts" does not exist
role "postgres" / "root" / "airflow" does not exist
```

One line in that set — `2026-09-07 19:08:13 syntax error at or near "\"` — was
produced by this stage's own first query and is excluded from every count here.

The three that are not interactive:

```
2026-08-29 05:55:28  FATAL:  too many connections for role "metrics_user"
2026-08-29 05:58:52  FATAL:  too many connections for role "metrics_user"  (x2)

2026-08-26 23:00:18 → 2026-08-27 14:00:18
  staging.coordination_state_events      does not exist    2/hour, on the hour,
  staging.coordination_release_evidence  does not exist    16 consecutive hours

2026-09-02 02:15:43  new row for relation "coordination_state" violates
                     check constraint "coordination_state_check"
```

## The metrics_user exhaustion

`metrics_user` is postgres-exporter's account —
`db/migrations/V033__metrics_user.sql` (Plan 86) creates it with
`CONNECTION LIMIT 3`, and `docker-compose.yml` sets
`DATA_SOURCE_NAME: postgresql://metrics_user:...@postgres:5432/cartracker`.

Of the seven login roles, only `metrics_user` carries a limit:

```
airflow_app_user = -1   cartracker = -1   scraper_user = -1
airflow_user     = -1   dbt_user   = -1   viewer       = -1
metrics_user     =  3
```

Server `max_connections` is 100 (Compose sets `-c max_connections=100`);
current usage at sample time was `airflow_user` 11, `cartracker` 2,
`metrics_user` 1.

Prometheus, `up{job="postgres"}` at 60s resolution:

```
05:50 → 05:56   1
05:57 → 06:12   0     ← sixteen consecutive zero samples
06:13 → 06:25   1
```

Both relations named in the 16-hour failure now exist
(`staging.coordination_state_events`, `staging.coordination_release_evidence`),
so that finding is closed.

## What could not be verified

**Whether any alert fired during the 05:57–06:13 blind spot.** Prometheus's
`ALERTS` series is empty over 7 days and `/api/v1/rules` returns nothing,
because alerting is Grafana's ngalert rather than Prometheus's — consistent
with the `rule_uid=ct-container-health-unconfigured` and
`rule_uid=ct-log-error-spike` entries visible in `grafana`'s own logs. Grafana's
annotations API rejected the credential in `GF_SECURITY_ADMIN_PASSWORD` with
`401 password-auth.failed`, so the question is recorded as **unknown**, not as
"nothing fired".
