# Plan 151: Distributed Tracing and Runtime Topology Audit

## What this plan is for

Runs a bounded experiment in watching requests move between services, to answer
two questions with evidence: whether seeing the live dependency graph reveals
anything the hand-written one misses, and whether it costs less than it is
worth. The experiment is allowed to conclude no.

## The case

A research-gated observability plan. It deliberately does not commit the project
to Tempo, full trace retention, or broad instrumentation before a small
metrics-first experiment proves their value and operating cost.


[Plan 142](plan_142_planned_host_maintenance.md) is introducing a checked-in
service-to-surface registry so deploy and maintenance coordination can pause
only the work affected by a target. That registry must be explicit because it
is safety policy: the absence of recent traffic cannot prove that a dependency
does not exist.

Static policy can still drift. A new HTTP call, task handoff, database access,
or queue boundary can create a runtime dependency without updating the
registry. Unit tests can prove that known services and declarations are
internally consistent, but they cannot infer every semantic effect from source
code or Compose configuration.

The current Grafana stack provides metrics and logs, but not request traces or
an observed service graph. Grafana's service-graph model can derive
client-to-server edges and request/error/duration metrics from distributed
traces. That makes it a useful independent audit signal for Plan 142 and a
small, honest introduction to streaming-shaped telemetry. It does not make the
observed graph safe enough to become maintenance policy.

## Design

A bounded OpenTelemetry pipeline, metrics-first, over a few high-value
cross-service paths -- compared against Plan 142's declared operational graph,
and sized so that stopping after any stage leaves a coherent system. Tempo is
not adopted up front; it is a stage that only exists if an earlier stage's
decision calls for it.

### Objective

Introduce a bounded OpenTelemetry pipeline that observes a few high-value
cross-service paths, compares their runtime edges with Plan 142's declared
operational graph, and measures whether broader distributed tracing is useful
and affordable for this project.

The result should answer two questions with evidence:

1. Does runtime topology expose meaningful undeclared dependencies or improve
   incident diagnosis beyond the existing metrics and logs?
2. Is metrics-only trace processing sufficient, or does retaining and querying
   individual traces justify operating Tempo?

### Principles

1. **Declared policy remains authoritative.** Runtime observation audits the
   service-to-surface registry; it never decides what is safe to stop.
2. **Presence is stronger than absence.** An observed undeclared edge is useful
   evidence. An unobserved declared edge may merely be idle, rare, sampled, or
   uninstrumented.
3. **Start with metrics, not a new database.** Generate topology and RED
   metrics before accepting persistent trace storage as an operating cost.
4. **Instrument paths, not everything.** Begin with a small set of boundaries
   whose behavior matters to Plan 142 and incident response.
5. **Bound cardinality and sensitive data at ingress.** Raw URLs, VINs, search
   terms, object keys, SQL text, credentials, and request bodies do not become
   trace attributes.
6. **Telemetry may be lossy; control state may not be.** The pipeline can drop
   samples under pressure. Plan 142's Postgres coordination state and checked-in
   registry cannot.
7. **Keep/change/remove is a valid outcome.** The experiment may retain a
   metrics-only collector, add Tempo, narrow the instrumentation, or remove the
   pipeline.

### Non-goals

- Replacing Plan 142's service-to-surface registry, DAG admission declarations,
  drain counters, or release gates.
- Automatically adding or deleting declared dependencies from observed traffic.
- Treating a quiet service graph as proof that a service is safe to stop.
- Replacing Prometheus, Loki, structured logs, or Plan 141's log contract.
- Sending domain events through OpenTelemetry or using traces as a durable
  replay log.
- Replacing [Plan 126](plan_126_basic_event_streaming.md), which owns durable
  business-event transport, replay, and consumer semantics.
- Implementing Plan 127's closed-loop adaptive scrape control.
- A repository-wide tracing rollout or an unconditional Tempo deployment.
- Introducing tail-based sampling, a multi-component observability cluster, or
  long trace retention before the bounded experiment shows a need.

### Architecture boundary

The candidate flow is:

```text
instrumented application boundary
    -> OTLP
    -> Grafana Alloy / OpenTelemetry-compatible collector
    -> service-graph and span metrics
    -> Prometheus
    -> Grafana dashboards and alerts

optional only after the Stage C decision:
    collector -> Tempo -> trace lookup from Grafana
```

Plan 142 separately exports its declared graph in a stable, low-cardinality
form. A comparator reports:

- **observed and declared:** corroborating evidence;
- **observed but undeclared:** warning requiring registry or instrumentation
  review;
- **declared but not observed:** informational coverage evidence only;
- **neither declared nor observed:** no claim.

The comparator must distinguish service identity, operational surface, and
transport edge. A network call proves communication, not that both endpoints
mutate the same state or require the same drain behavior. Human review remains
the step that translates runtime evidence into safety policy.

### Test and verification contract

- Unit tests validate trace attribute normalization, route templating, status
  mapping, context propagation helpers, and sensitive/high-cardinality field
  rejection.
- Configuration tests validate collector syntax, bounded queues/retries, health
  telemetry, and the absence of prohibited exporters or attributes.
- Integration fixtures send sampled traces across each selected boundary and
  assert the expected service-graph/span metrics appear.
- Contract tests derive the declared graph from Plan 142's registry and reject
  a separately maintained copy.
- A negative integration fixture creates an observed undeclared edge and proves
  the comparator reports it without changing coordination state.
- Failure tests make the collector unavailable and prove application work
  continues while telemetry loss becomes visible.
- Dashboard and alert tests follow the repository's existing provisioned
  Grafana validation pattern.

## Stages

Sequenced before [`docs/PLAN_DOCUMENT.md`](../PLAN_DOCUMENT.md) landed; adopted
stage letters on 2026-09-07. No Linear issue carries a legacy number yet, so the
mapping serves inbound prose -- including
[Plan 155](plan_155_log_dashboards.md), which cites this plan's Stage C by its
old number and was updated with it:

| Legacy | Stage | | Legacy | Stage |
|:---:|:---:|---|:---:|:---:|
| 0 | **A** | | 3 | **D** |
| 1 | **B** | | 4 | **E** |
| 2 | **C** | | | |

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a--contract-and-experiment-design) | The telemetry contract, budgets, and the experiment's shape | `next` | -- |
| 2 | [**B**](#stage-b--metrics-first-telemetry-pipeline) | The metrics-first pipeline, no trace retention | `--` | -- |
| 3 | [**C**](#stage-c--declared-versus-observed-audit) | Runtime edges compared with Plan 142's declared graph, and the retention decision | `--` | -- |
| 4 | [**D**](#stage-d--optional-bounded-tempo-proof) | Tempo, only on an affirmative Stage C decision | `--` | -- |
| 5 | [**E**](#stage-e--observation-and-decision) | Seven days observed, and the keep/change/remove decision | `--` | -- |

### Stage A — Contract and experiment design

Stage A is the next executable slice once Plan 142's Stage 1 registry schema is
stable. It produces a written experiment contract before adding a collector.

1. Inventory the existing Prometheus, Loki, Grafana, and application metrics
   paths, including current host resource use and retention costs.
2. Select two or three cross-service paths that exercise distinct boundaries.
   Prefer paths important to scoped maintenance, such as Airflow to ops,
   Airflow to scraper/processing, and an application to Postgres or MinIO.
3. Define canonical `service.name`, environment, operation, status, and peer
   attributes. Prohibit raw identifiers and unbounded route values.
4. Define trace-context propagation across the selected HTTP, task, or process
   boundaries and document where propagation cannot yet be continuous.
5. Define the Plan 142 declared-graph metric or generated artifact and its
   stable identifiers. It must come from the registry rather than duplicate it.
6. Set explicit CPU, memory, disk, network, series-cardinality, and ingestion
   budgets. Record abort thresholds before deployment.
7. Choose an initial sampling policy and document what conclusions sampling
   makes unsafe.
8. Write the Stage B dashboard, alert, verification, and rollback contract.

#### Stage A gate

Proceed only if the selected paths can be instrumented without sensitive or
unbounded attributes, the declared graph has a stable export, and the
collector fits a written single-host resource budget. Otherwise record the gap
and stop without deploying another service.

### Stage B — Metrics-first telemetry pipeline

1. Instrument only the approved paths with OpenTelemetry SDKs or narrowly
   scoped automatic instrumentation.
2. Deploy one collector boundary, preferring Grafana Alloy if it integrates
   cleanly with the existing Grafana configuration and deployment model.
3. Generate service-graph and span metrics for Prometheus without retaining
   traces in Tempo.
4. Add readiness, refusal/drop, export-failure, queue, resource-use, and
   cardinality metrics for the telemetry pipeline itself.
5. Add a small Grafana view showing observed edges, request rate, error rate,
   duration, instrumentation coverage, and collector health.
6. Verify that stopping or removing the collector does not block application
   work. Telemetry export must fail open and remain outside operational control
   paths.

#### Stage B gate

The metrics-only pipeline must survive representative traffic without breaching
its resource or cardinality budget, leaking prohibited attributes, or changing
application success behavior. Failure rolls back instrumentation export and
the collector; it does not weaken Plan 142 policy.

### Stage C — Declared-versus-observed audit

1. Publish Plan 142's declared service/surface relationships using the contract
   designed in Stage A.
2. Compare declared and observed edges with explicit instrumentation-coverage
   and last-observed timestamps.
3. Alert only on observed-but-undeclared edges after a bounded persistence
   window prevents one-off startup or healthcheck noise from paging.
4. Keep declared-but-unobserved edges informational. They may identify missing
   instrumentation, sampling gaps, rare paths, or stale declarations, but they
   cannot automatically narrow maintenance scope.
5. Exercise at least one fixture that deliberately creates a known undeclared
   edge and prove the audit catches it.
6. Review every mismatch and record whether it found registry drift,
   instrumentation noise, or a modeling limitation.

#### Stage C decision — Is trace retention justified?

Add Tempo only if the metrics-first experiment produces a concrete diagnostic
question that aggregate metrics cannot answer and individual trace lookup is
expected to answer it. The decision must include measured ingestion volume,
projected storage/retention, memory headroom, operational ownership, and a
removal path.

If that evidence is absent, keep the metrics-only pipeline and do not deploy
Tempo merely to complete the conventional stack.

### Stage D — Optional bounded Tempo proof

This stage exists only after an affirmative Stage C decision.

1. Deploy the smallest supported single-host Tempo topology with bounded local
   or object-store retention.
2. Retain only the approved services and sampling rate.
3. Link metrics and logs to traces where stable identifiers permit it without
   raising cardinality or disclosure risk.
4. Demonstrate the specific incident or latency investigation named in the
   Stage C decision.
5. Measure query usefulness, ingest failures, storage growth, compaction, CPU,
   memory, and operator burden.

Tempo is removed if it does not answer the named question reliably within the
resource budget. Metrics-only operation remains an acceptable finish.

**Exit:** either Tempo has answered the specific investigation named in the
Stage C decision, within the resource budget and with its measured cost written
down, or it has been removed and the pipeline is back to metrics-only. Both are
valid exits; the stage is not finished while Tempo is running unevaluated.

### Stage E — Observation and decision

Observe the approved pipeline for seven days including scheduled DAG activity,
ordinary idle periods, a targeted service deployment, and at least one scoped
Plan 142 dry run when safe.

Record:

- declared, observed, and mismatched edge counts;
- mismatch causes and time to resolution;
- instrumented traffic and sampling coverage;
- collector drops, refusals, queue pressure, and export failures;
- added Prometheus series and storage growth;
- application latency/error changes;
- host CPU and memory cost;
- incidents or investigations materially improved by topology or traces;
- false confidence risks found during review.

End with one explicit decision:

- **keep metrics-only** and define the small supported instrumentation set;
- **keep Tempo** with measured retention and ownership limits;
- **change** sampling, coverage, or topology and repeat a bounded observation;
- **remove** the pipeline because its value does not justify its cost.

**Exit:** seven days observed across the named conditions, every item in the
record list above filled in, and exactly one of the four decisions written down
with its reasoning. A decision of **remove** is a successful exit, not a failed
one -- this plan is allowed to conclude no, and the exit is the record, not the
pipeline surviving.

Broader instrumentation is a later decision, not an implicit Stage E task.

## Relationship to other plans

- **Plan 142 is the source of safety policy.** Plan 151 supplies independent
  runtime audit evidence and may propose reviewed registry corrections.
- **Plan 141 owns structured log semantics.** Trace/log correlation may use its
  stable fields after they exist; Plan 151 does not redefine log parsing.
- **Plan 140 owns service health.** Collector health follows that contract, but
  an observed trace edge is not a healthcheck.
- **Plan 139 Stage E may later use the declared dependency model for advisory CI
  impact selection.** Runtime evidence can reveal missing declarations, but CI
  selection must consume reviewed declarations rather than live telemetry.
- **Plan 126 remains the durable streaming plan.** OTLP telemetry is
  observational, sampled, and disposable; Plan 126's domain events must be
  durable and replayable. Plan 151 provides lower-risk operational experience
  with producers, backpressure, cardinality, and consumers without pretending
  those semantics are interchangeable.
- **Plan 127 remains downstream of Plan 126.** No trace or service-graph signal
  enters automated scrape control under this plan.

## Success criteria

1. At least two meaningful cross-service paths emit bounded, sanitized trace
   telemetry through a collector that can fail without affecting application
   work.
2. Prometheus and Grafana show useful service-graph and RED metrics within the
   written resource and cardinality budgets.
3. Plan 142's declared graph is exported from its authoritative registry, not
   copied into telemetry configuration.
4. A deliberate observed-but-undeclared fixture is detected while
   declared-but-unobserved edges remain non-authoritative.
5. Seven days of evidence support a written keep/change/remove decision.
6. Tempo is present only if the project records a diagnostic need that metrics
   cannot answer and verifies it within bounded retention and resource cost.
7. Plan 126 remains independently justified by durable domain-event and replay
   requirements rather than by the existence of an OTLP pipeline.

## Rollback and safe stopping points

- **After Stage A:** stop with a written contract and no runtime change.
- **After Stage B:** remove application exporters and the collector; existing
  Prometheus, Loki, Grafana, and application behavior remain intact.
- **After Stage C:** keep or remove metrics-only auditing without changing Plan
  142's registry or coordination behavior.
- **After Stage D:** remove Tempo while retaining metrics-only processing, or
  remove the entire telemetry path.
- **After Stage E:** the recorded keep/change/remove decision is the plan's
  durable output; no broader rollout is owed implicitly.
