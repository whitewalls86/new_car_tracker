# Plan 126: Basic Event Streaming Foundation

## Status

**Draft / future work.** This plan should start after Plan 125 has made the
analytics substrate more stable, and after the Plan 112/113 adaptive-refresh
path is clear enough that event semantics can be designed once.

This supersedes the old placeholder "Plan 87: Kafka event-driven layer" by
framing the work as a Kafka-compatible event streaming foundation, not as a
Kafka-for-Kafka's-sake infrastructure task.

## Goal

Introduce a small, reliable streaming layer that turns the existing
Postgres-staging-event pattern into a Kafka-compatible event stream.

The purpose is not to replace every batch job immediately. The purpose is to
create a durable, replayable event backbone for selected operational/domain
events, with clear parity against the existing staging-table plus Airflow flush
path.

## Why This Fits The Project

Today, the project already has a batch-mode event log:

```text
service transaction
  -> update hot Postgres table
  -> insert staging event row
  -> Airflow flushes staging rows to MinIO Parquet
  -> dbt/Iceberg analytics consume flushed history
```

The natural streaming shape is:

```text
service transaction
  -> update hot Postgres table
  -> insert durable event/outbox row
  -> publisher sends committed events to Redpanda/Kafka-compatible topics
  -> consumers update metrics, sinks, or downstream stores
```

The existing staging event tables are the seam. They should evolve toward a
transactional outbox or feed a parallel outbox, rather than adding direct
"write database then publish Kafka" calls inside request logic.

## Initial Event Families

Candidate event streams:

- `search_configs` / `tracked_models` reference-table changes
- price observation events
- VIN-to-listing mapping events
- blocked cooldown events
- detail scrape claim events
- artifact queue/status events

These should use stable event IDs, versioned payloads, and entity keys that
preserve useful ordering:

- `listing_id` for listing-scoped events
- `vin` or `vin17` for vehicle-scoped events
- `artifact_id` for artifact-processing events

Recommended first streaming job: publish `search_configs` and `tracked_models`
changes from the ops routers that already own those mutations, then have a small
consumer update the MinIO/Iceberg reference copy introduced by Plan 125. These
tables are low-volume, low-change, and easy to reconcile against Postgres, so
they exercise the core streaming shape without immediately taking on the much
larger silver observation/event-firehose problem. Keep the hourly Plan 125
snapshot as the repair/reconciliation path until the stream has proven itself.

## Likely Technology Choice

Start with **Redpanda** as the local/VM broker because it speaks the Kafka API
while keeping single-node operations simple. The architecture should remain
Kafka-compatible so the broker could be swapped for Apache Kafka, MSK, or
Confluent later if deployment requirements change.

## High-Level Gates

### Gate A: Broker Scaffold

- Add a Redpanda service for local/VM development.
- Define topic naming conventions, retention defaults, and persistent volume
  behavior.
- Add health checks and basic metrics.
- Do not change production event flow yet.

### Gate B: Outbox Publisher

- Publish a small subset of existing staging/outbox events to topics.
- Use at-least-once delivery with idempotent event IDs.
- Track publish state separately from long-term analytical history.
- Alert on unpublished backlog age/count.

### Gate C: Shadow Stream Consumers

- Add one low-risk consumer, likely for Prometheus-facing operational counters.
- Keep Airflow flushes and dbt/Iceberg analytics unchanged.
- Compare streaming-derived metrics against current DuckDB/Iceberg-derived
  metrics.

### Gate D: Stream Sink Parity

- Add an append-only sink from selected topics to MinIO/Iceberg.
- Dual-run against the existing Airflow flush path.
- Compare row counts, event IDs, duplicates, freshness, and replay behavior.

### Gate E: Cutover Decision

- Decide whether any staging-table flushes should be retired.
- Preserve rollback to Airflow flush until parity has held through a validation
  window.

## Non-Goals

- No adaptive scrape-control policy engine. Plan 127 owns that.
- No raw log ingestion into Redpanda by default; Loki/Promtail already handle
  logs.
- No replacing Prometheus/Grafana. Streaming may feed metrics, but Prometheus
  remains the metric store.
- No cluster-level Kafka operations requirement. Single-node Redpanda is enough
  for the first implementation.

## Challenges To Design Explicitly

- idempotent consumers
- duplicate delivery
- partition keys and per-entity ordering
- topic retention vs. Iceberg/MinIO long-term history
- outbox pruning and unpublished backlog alerts
- replay from broker offsets vs. replay from source tables
- schema versioning
- dead-letter or quarantine handling for malformed events

## Exit Criteria

- At least one existing staging-event family is published to a Kafka-compatible
  topic.
- At least one consumer runs from the stream and exposes useful operational
  output.
- Airflow/batch output remains canonical until parity is proven.
- The stream can be replayed over a recent retention window without corrupting
  downstream state.
