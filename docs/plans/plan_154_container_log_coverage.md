# Plan 154: Container Log Coverage

## Status

**BACKLOG, written 2026-08-25.** Priority **70 (medium)**. Effort **S plus a
7-day volume observation**.

Split out of the Plan 141 logging health check. Depends on Plan 141, which
creates the source-policy registry this plan revises.

## Problem

[`docker-compose.yml`](../../docker-compose.yml) declares 33 services. 28 are in
the default profile; 26 remain after one-shots (`flyway`, `airflow-init`). Of
those 26, **10 have any path into Loki**:

- Six by application file — `ops`, `scraper`, `processing`, `dbt_runner`,
  `archiver`, `pack-worker`.
- Four by `promtail.enable=true` stdout — `oauth2-proxy`,
  `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`.

**Sixteen have none:** `postgres`, `caddy`, `minio`, `dashboard`, `pgadmin`,
`flaresolverr`, `airflow-triggerer`, `statsd-exporter`, `postgres-exporter`,
`node-exporter`, `prometheus`, `grafana`, `loki`, `promtail`,
`docker-socket-proxy`, `container-health`.

Most of those absences are correct, and Plan 141's source-policy table exists
precisely to say so in writing. The problem is not that 16 services are
excluded. It is that **their exclusion was never a decision** — it is the
residue of which services happened to get a label during the Plan 135 Stage 5
rollout. A correct exclusion and an unconsidered one currently look identical.

Two absences are probably wrong on their merits:

- **`caddy`** is the only edge. Every 4xx and 5xx a user actually receives
  originates there, and none of it is durable. An outage visible to users is
  currently invisible in Loki unless it also broke an instrumented backend.
- **`postgres`** holds every failure mode Plan 123 is still verifying —
  slow queries, connection exhaustion, lock waits. Plan 140's Airflow connection
  budget is the instrument that would show exhaustion; Postgres's own log is
  what would explain it.

**`container-health`** is a weaker third: it is the producer behind Plan 140's
alerts, and a silent producer is a bad failure mode.

The 2026-08-18 evidence in Plan 141 is the counterweight and must stay in view:
a ten-minute unfiltered sample of three Airflow containers produced 486 lines.
Adding services without measuring them is how that happened. This plan
therefore measures before it admits.

## Principles

1. **Exclusion is a decision with a reason.** Plan 141 makes every service have
   an answer; this plan revisits the answers that were never really chosen.
2. **Measure before admitting.** No service enters Loki without an observed
   lines/day and bytes/day figure and a projected 90-day footprint.
3. **Volume is a filter design problem, not an accept/reject one.** A chatty
   service is a candidate for a drop policy, not automatic exclusion.
4. **The observability stack does not observe itself into a loop.** `loki` and
   `promtail` stay out; local Docker rotation is their diagnostic path.
5. **Coverage is not a target number.** "All 26 services ingested" is a failure
   mode, not a goal.
6. **Privacy travels with the edge.** Caddy access logs carry client IPs and
   full request paths. A retention and redaction policy is a precondition for
   admitting it, not a follow-up.

## Non-goals

- The source-policy registry format, the completeness test, and parsing — all
  Plan 141. This plan supplies revised *entries*, not the mechanism.
- Structured application log fields — Plan 141 Stage 1.
- Dashboards over the resulting streams — Plan 155.
- Loki retention, disk caps, and Docker rotation limits — Plan 135, complete.
- Healthcheck coverage. Plan 140 owns liveness, and its inclusion policy is
  deliberately different; a service can be health-checked and correctly
  unlogged.
- Adding an ingestion path for Airflow DAG/task files, which remain local under
  Plan 135's 30-day policy.

## Stage 0 — Decide every exclusion on its merits

Read-only, and the substance of this plan.

1. For each of the 16 uncovered services, record: what it logs, at what volume,
   what question its logs would answer, and whether another signal already
   answers it.
2. Classify each as **admit**, **admit with a drop policy**, or **exclude with a
   written reason**. A reason of "not currently labeled" is not a reason.
3. Sample volume directly from `docker logs` over a representative window
   including a scrape cycle and a dbt run. Do not estimate from service type.
4. For `caddy`, additionally determine the access-log format, what PII it
   carries, and whether errors can be separated from routine 2xx traffic at
   ingestion.
5. For `postgres`, determine current `log_min_duration_statement` and related
   settings — the useful content may not be emitted at all today, which would
   make this a Postgres configuration change before it is a Promtail one.

### Stage 0 gate

Every one of the 16 has a written classification with a measured volume figure.
Services whose useful content is not currently emitted are recorded as
configuration work, not as ingestion work.

## Stage 1 — Admit the accepted services

1. Add `promtail.enable=true` only to services classified **admit** in Stage 0.
2. Give each an ingestion policy in Plan 141's registry — parsing, severity
   mapping, and any drop rule — with its Stage 0 reason attached.
3. Apply drop policies at ingestion so Promtail's drop counters attribute each
   policy separately, following the pattern Plan 141 establishes.
4. Deploy by recreating Promtail unless a Compose label changed, in which case
   recreate the labeled service too.

## Stage 2 — Observe for seven days

1. Measured lines/day and bytes/day per newly admitted service against the
   Stage 0 projection.
2. Recomputed 90-day Loki footprint and disk headroom against Plan 135's bounds.
3. Confirmation that each new stream's drop policy is doing the expected amount
   of work and no more.
4. One representative error per admitted service proven to reach the error
   view.

### Stage 2 decision

Per admitted service: **keep**, **narrow the filter**, or **remove**. A service
whose seven-day volume exceeds its Stage 0 projection by a stated margin is
narrowed or removed rather than absorbed.

## Test and verification contract

- The Plan 141 completeness test continues to pass with the revised entries; no
  second registry is created.
- Every exclusion entry carries a non-empty reason, enforced by test, following
  the pattern already used by Plan 140's healthcheck deny list in
  [`tests/test_observability_config.py`](../../tests/test_observability_config.py).
- Fixtures for each newly admitted format assert parsed labels or explicit drop.
- A test asserts `loki` and `promtail` are never admitted, with the feedback-loop
  reason recorded inline.
- A test asserts admitted-service labels in Compose and registry entries agree,
  so a label cannot be added without a policy.

## Relationship to other plans

- **Plan 141 is a hard prerequisite.** It builds the registry, the completeness
  test, and the parsing contract. This plan is the first substantive revision of
  its contents and should not start before it lands.
- **Plan 135 set the bounds** this plan spends against. Its retention and disk
  caps are the budget; Stage 2 verifies the spend fits.
- **Plan 140 owns healthchecks.** Its service set and this one differ on
  purpose; neither test may be rewritten to match the other.
- **Plan 141 Stage 1 supplies application log structure.** That is a separate
  concern from this plan, which is about which containers are heard at all, not
  what they say.
- **Plan 155 presents the result.** More streams make its per-service breakdown
  more necessary, not less.

## Success criteria

1. All 26 expected-running services have a written, reasoned classification;
   none is excluded merely by omission.
2. Every admitted service has a measured volume figure predating its admission.
3. `caddy` and `postgres` are resolved explicitly — admitted with a policy, or
   excluded with a reason that survives review.
4. Seven days of evidence support a keep/narrow/remove decision per admitted
   service.
5. The 90-day Loki footprint after admission stays within Plan 135's bounds with
   recorded headroom.
6. No second coverage registry exists.

## Rollback and safe stopping points

- **After Stage 0:** classifications recorded, no runtime change. This is a
  legitimate finish if every answer is "exclude".
- **After Stage 1:** remove the labels and registry entries; Promtail returns to
  its Plan 141 state.
- **After Stage 2:** the per-service decision is the durable output.
