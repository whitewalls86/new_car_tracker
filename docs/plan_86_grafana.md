# Plan 86: Grafana Observability Stack

**Status:** COMPLETE (2026-04-29)
**Priority:** Medium — Airflow is live with real DAG metrics; now worth instrumenting
**Depends on:** Plan 71 (Airflow, COMPLETE), Plan 93 (processing service, COMPLETE)

## Completion Notes

Deployed to production 2026-04-29. All containers live, all three dashboards populate.

- V033 Flyway migration applied (created `metrics_user` with `pg_monitor`; V032 was already taken)
- `/grafana` route live behind admin-only Caddy auth
- All five metrics sources scraping: Airflow StatsD, Postgres, MinIO, ops/processing HTTP, node-exporter
- `METRICS_DB_PASSWORD`, `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` set in server env

---

## Overview

Add a Prometheus + Grafana observability stack. Airflow is live with 8 DAGs running on
real schedules — there are now meaningful metrics to observe. The stack covers four signal
sources: Airflow DAG/task metrics, Postgres database health, service HTTP latency, and
host infrastructure.

Everything is provisioned as code: Prometheus scrape config, Grafana datasource, and
dashboard JSON all live in git.

---

## Metrics Sources

### 1. Airflow → StatsD → Prometheus

Airflow emits rich operational metrics over StatsD. A `prom/statsd-exporter` container
bridges them to the Prometheus scrape format.

Airflow env vars added to `x-airflow-common`:
```yaml
AIRFLOW__METRICS__STATSD_ON: 'true'
AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
AIRFLOW__METRICS__STATSD_PORT: '9125'
AIRFLOW__METRICS__STATSD_PREFIX: 'airflow'
```

Key metrics exposed:
- `airflow_dagrun_duration_success_<dag_id>` — run duration by DAG
- `airflow_dagrun_duration_failed_<dag_id>` — failed run duration
- `airflow_dagrun_schedule_delay_<dag_id>` — scheduling latency (scheduled vs actual start)
- `airflow_scheduler_tasks_running` — tasks currently in flight
- `airflow_pool_running_slots` / `airflow_pool_open_slots`
- `airflow_ti_failures` / `airflow_ti_successes` — task instance outcomes

### 2. Postgres (`prometheuscommunity/postgres-exporter`)

Scrapes `pg_stat_*` system views. Requires a dedicated DB user (added via Flyway migration):
```sql
CREATE USER metrics_user WITH PASSWORD '...' CONNECTION LIMIT 3;
GRANT pg_monitor TO metrics_user;
```

Key metrics:
- `pg_stat_activity_count` — active connections by state
- `pg_stat_database_tup_fetched` / `tup_inserted` — row throughput
- `pg_stat_bgwriter_buffers_*` — buffer hit vs disk read ratio
- `pg_locks_count` — lock contention

### 3. MinIO (native Prometheus endpoint)

MinIO exposes `/minio/v2/metrics/cluster` natively. Set `MINIO_PROMETHEUS_AUTH_TYPE=public`
to allow unauthenticated scraping from inside the Docker network (Prometheus never exits
the network, so this is safe).

Key metrics:
- `minio_bucket_usage_object_total` — object count per bucket
- `minio_bucket_usage_total_bytes` — storage usage
- `minio_s3_requests_total` — request rate by API type
- `minio_s3_requests_errors_total` — error rate

### 4. FastAPI services (`prometheus-fastapi-instrumentator`)

Add `prometheus-fastapi-instrumentator` to `ops` and `processing`. Mount `/metrics` endpoint
on each. Covers:
- `http_requests_total` by method, endpoint, status code
- `http_request_duration_seconds` (histogram) by endpoint

No changes to `scraper` — it has minimal internal HTTP surface.

### 5. Host (`prom/node-exporter`)

VM-level metrics: CPU utilization, memory, disk I/O, network. Essential for correlating
pipeline slowdowns with resource contention.

---

## New Containers

| Container | Image | Internal Port | Purpose |
|---|---|---|---|
| `statsd-exporter` | `prom/statsd-exporter` | 9125 (UDP), 9102 (HTTP) | Airflow StatsD bridge |
| `prometheus` | `prom/prometheus` | 9090 | Metrics store |
| `grafana` | `grafana/grafana` | 3000 | Visualization |
| `postgres-exporter` | `prometheuscommunity/postgres-exporter` | 9187 | Postgres metrics |
| `node-exporter` | `prom/node-exporter` | 9100 | Host metrics |

Prometheus and node-exporter are internal only — not exposed externally, not behind Caddy.
Grafana is at `/grafana` (admin-only).

---

## Prometheus Scrape Config (`prometheus/prometheus.yml`)

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: airflow
    static_configs:
      - targets: ['statsd-exporter:9102']

  - job_name: postgres
    static_configs:
      - targets: ['postgres-exporter:9187']

  - job_name: minio
    metrics_path: /minio/v2/metrics/cluster
    static_configs:
      - targets: ['minio:9000']

  - job_name: ops
    static_configs:
      - targets: ['ops:8060']
    metrics_path: /metrics

  - job_name: processing
    static_configs:
      - targets: ['processing:8070']
    metrics_path: /metrics

  - job_name: node
    static_configs:
      - targets: ['node-exporter:9100']
```

---

## Grafana Provisioning

Grafana datasource and dashboards are provisioned from files — no manual UI setup required.

```
grafana/
  provisioning/
    datasources/
      prometheus.yml        ← points at http://prometheus:9090
    dashboards/
      dashboards.yml        ← loads from /var/lib/grafana/dashboards
  dashboards/
    pipeline_health.json
    infrastructure.json
    service_latency.json
```

### Dashboard 1: Pipeline Health

Panels:
- DAG run outcome matrix (success/failed/running counts per DAG, last 24h)
- DAG run duration trend by DAG (line chart, P50 + P95)
- Scheduling delay per DAG (time between scheduled and actual start)
- Task failure rate per DAG (failures / total attempts)
- `artifacts_queue` processed/hour (from postgres-exporter query or processing service metric)
- `price_observations` upsert rate (from postgres-exporter row throughput)

### Dashboard 2: Infrastructure

Panels:
- Host CPU utilization (all cores, stacked)
- Host memory used vs available
- Disk I/O (reads + writes bytes/sec)
- Postgres active connections by state (idle, active, idle-in-transaction)
- Postgres transaction rate (commits/sec)
- MinIO storage used (bytes, by bucket)
- MinIO request rate (by API type)
- MinIO error rate

### Dashboard 3: Service Latency

Panels:
- HTTP request rate by service + endpoint (ops, processing)
- HTTP p50/p95/p99 latency by endpoint
- HTTP error rate (4xx + 5xx) by service
- Processing service: batch size distribution
- Scraper: artifacts written per run (from processing metrics)

---

## Caddy Route (`/grafana`)

Add to Caddyfile after the `/airflow` block (admin-only):

```caddyfile
handle_path /grafana* {
    forward_auth oauth2-proxy:4180 {
        uri /oauth2/auth
        copy_headers X-Auth-Request-Email X-Auth-Request-User
        @error status 401
        handle_response @error {
            redir * /oauth2/sign_in?rd={scheme}://{host}{uri}
        }
    }
    forward_auth ops:8060 {
        uri /auth/check?require=admin
        copy_headers X-User-Role
        @unauth status 403
        handle_response @unauth {
            redir * /request-access
        }
    }
    reverse_proxy grafana:3000
}
```

Grafana config:
```yaml
GF_SERVER_ROOT_URL: https://cartracker.info/grafana
GF_SERVER_SERVE_FROM_SUB_PATH: 'true'
GF_AUTH_ANONYMOUS_ENABLED: 'false'
GF_SECURITY_ADMIN_USER: ${GRAFANA_ADMIN_USER}
GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD}
GF_AUTH_DISABLE_LOGIN_FORM: 'false'   # keep login — Caddy already gates it
```

---

## Flyway Migration

Add `metrics_user` for postgres-exporter. Note: V031 is taken; use V032:
```sql
-- V032__metrics_user.sql
CREATE USER metrics_user WITH PASSWORD '${metricsPassword}' CONNECTION LIMIT 3;
GRANT pg_monitor TO metrics_user;
```

Add `metricsPassword` placeholder to Flyway command in `docker-compose.yml`.

---

## Rollout Order

1. Flyway migration: add `metrics_user` (V032)
2. Add `statsd-exporter` container; add StatsD env vars to `x-airflow-common`
3. Add `postgres-exporter` container
4. Add `MINIO_PROMETHEUS_AUTH_TYPE=public` to minio; verify scrape at `/minio/v2/metrics/cluster`
5. Add `prometheus-fastapi-instrumentator` to `ops` and `processing`; rebuild both images
6. Add `node-exporter` container
7. Write `prometheus/prometheus.yml`; add `prometheus` container
8. Write `grafana/provisioning/` + `grafana/dashboards/` JSON; add `grafana` container
9. Add `/grafana` block to Caddyfile
10. Add `/grafana` link to ops sidebar nav
11. Verify all three dashboards populate in production

---

## Out of Scope

- Alerting (Grafana Alert Manager or PagerDuty) — observability first, alerting later
- Log aggregation (Loki) — structured logs are in the existing log volumes; not needed yet
- `dbt_runner` instrumentation — dbt run duration is already visible in Airflow DAG metrics
- `scraper` HTTP metrics — scraper has no meaningful internal API surface to instrument
