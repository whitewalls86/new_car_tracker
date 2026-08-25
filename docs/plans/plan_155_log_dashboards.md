# Plan 155: Log Dashboards and Aggregate Triage

## Status

**BACKLOG, written 2026-08-25.** Priority **62 (medium)**. Effort **S**.

Trigger: Plan 141 lands its label contract and formatter change. Presentation
work before then would be built on labels that are about to change.

Split out of the Plan 141 logging health check.

## Problem

[`grafana/dashboards/logs.json`](../../grafana/dashboards/logs.json) has three
panels for 26 services:

| Panel | Query |
|---|---|
| All Service Logs | `{service=~".+"}` |
| Error / Warning Logs | `{service=~".+", level=~"ERROR\|WARNING"}` |
| Log Volume by Level | `sum by (level) (count_over_time({service=~".+"} [5m]))` |

That is a text firehose, a severity filter over the same firehose, and one
stacked count. It answers "is something wrong right now" and essentially nothing
else. There is no way to see which service is responsible, to follow one unit of
work across the services that handled it, or to tell a quiet service from an
absent one.

Three structural gaps sit underneath the panel count:

1. **No per-service dimension.** With 26 services and one combined stream, a
   service that stops logging entirely looks the same as one that has nothing to
   say.
2. **No join key.** Nothing in a log record identifies the unit of work it
   belongs to, so "show me everything about this scrape run" is not expressible
   as a query. Plan 141 Stage 1's formatter change is what creates the room for
   such a field.
3. **No triage path.** The error panel is a flat list. It does not rank, group,
   or distinguish a new failure from the hundredth repeat of a known one — which
   is how a real false positive like `ct-403-log-spike` produced 49 of 51
   annotations over the Plan 140 soak without anyone's view making that obvious.

## Boundary with Plan 141

This is the thinnest boundary in the set and needs stating precisely.

**Plan 141 Stage 2 owns the correctness of the existing panels** against its own
contract: adding `CRITICAL`, splitting volume by `service`/`source`/`level`, and
adding the coverage panel whose healthy value is zero. Those changes are
required for Plan 141's success criteria and must not be deferred into this
plan.

**Plan 155 owns the views that do not exist at all** — navigation, triage, and
cross-service correlation. If a change is needed for Plan 141's contract to be
verifiable, it belongs to Plan 141. If it exists to make 26 services'
worth of logs *usable by a person*, it belongs here.

## Principles

1. **Build on labels, never on text search.** Every panel selects on `service`,
   `source`, and `level`. A panel that greps message bodies is a contract gap
   being papered over.
2. **Absence must be visible.** A service that stops logging is a signal. Views
   are built so zero is a value, not a missing series.
3. **Triage before browsing.** The default view answers "what is broken and is
   it new", not "show me everything".
4. **One dashboard per question.** Panels are not added to the existing board
   until the board has a stated question it answers.
5. **Correlation is opportunistic.** Cross-service following is built only for
   the identifiers that actually exist after Plan 141; it does not invent a
   tracing system, which is Plan 151's decision to make.
6. **Provisioned, tested, and reviewable.** Dashboards stay checked in and
   validated by the repository's existing Grafana test pattern; none is authored
   in the UI.

## Non-goals

- Label semantics, parsing, and drop policy — Plan 141.
- Application log fields and the formatter — Plan 141.
- Coordination transition history and narration — Plan 142 Stage 2.
- Which services are ingested — Plan 154.
- Distributed tracing, span metrics, and the service graph — Plan 151. If
  cross-service correlation needs real trace context rather than a shared
  identifier, that is Plan 151's Stage 2 decision and not a dashboard change.
- Alert rules. Existing alerts are revalidated against new labels where they
  overlap, but new alerting design is not in scope.
- Business-data dashboards. `mart_vehicle_snapshot` and the price lineage are
  analytics surfaces, not log surfaces, and Plan 150 owns their presentation.

## Stage 0 — Name the questions

1. Write the operational questions the dashboards must answer — at minimum:
   what is failing now, is it new, which service owns it, has any service gone
   silent, and what did one unit of work do across services.
2. For each, record whether the labels and fields to answer it exist after
   Plans 141/142/154, or whether the question is blocked.
3. Discard questions that a Prometheus metric already answers better. Logs are
   not the right instrument for rates that a counter already tracks.

### Stage 0 gate

Every retained question maps to labels that exist. A question needing a label
nobody produces is referred to the owning plan rather than answered with a text
search.

## Stage 1 — Service-dimensioned overview

1. Volume and error rate broken out by `service` and `source`, with zero
   rendered as zero.
2. A silence panel: services with a Loki path but no records in the window,
   derived from the same expected-service set Plan 154 maintains rather than a
   hand-listed copy.
3. Retain All Service Logs as the broad diagnostic view, with service and source
   filters once the combined stream is unwieldy.

## Stage 2 — Triage view

1. Errors and warnings grouped by `service` and `logger`, ranked by volume, so a
   repeat failure collapses into one row.
2. A first-seen indicator distinguishing a new failure from a known one.
3. Drill-through from a grouped row to its underlying records.

## Stage 3 — Correlation, if the fields exist

Conditional on Plan 141's formatter change landing a usable identifier.

1. A view that follows one unit of work across the services that touched it.
2. For coordination windows specifically, a view keyed on `generation` showing
   the phase transitions and every record emitted during that window.
3. If no adequate identifier exists, record that and stop. This stage is
   explicitly droppable, and dropping it is the honest outcome rather than
   approximating correlation with timestamps.

## Test and verification contract

- Dashboard JSON parses and every `datasourceUid` resolves, following the
  existing provisioned-Grafana test pattern.
- Every panel query selects on labels; a test rejects a `|=` text match in a
  panel that claims to be label-driven.
- Panel selectors are exercised against Plan 141's fixture corpus — warning,
  error, and critical fixtures match; INFO fixtures do not.
- The silence panel's service set derives from the shared expected-service set,
  enforced by test, not duplicated.
- A test asserts no panel depends on a label that no ingestion policy produces.

## Relationship to other plans

- **Plan 141 supplies the labels** and owns its own Stage 2 panel corrections.
  This plan starts after those land and does not re-litigate them.
- **Plan 141 Stage 1 supplies the fields** that make Stage 3 possible. Without
  the formatter change, Stage 3 is dropped rather than approximated.
- **Plan 142 Stage 2 supplies coordination history and narration.** Its
  `generation` is the identifier the maintenance-window view in Stage 3 keys on.
- **Plan 154 supplies the streams.** Its added services make Stage 1's per-service
  breakdown necessary; the two are complementary but neither blocks the other
  once Plan 141 is in.
- **Plan 151 owns tracing.** If Stage 3 concludes that a shared log identifier
  is insufficient for real correlation, that finding is input to Plan 151's
  Stage 2 decision and is not solved here.
- **Plan 150 owns analytics presentation.** Business questions about listings
  and prices are answered from the marts, not from logs.

## Success criteria

1. Every retained Stage 0 question is answerable from a checked-in panel, or
   recorded as blocked with its owning plan named.
2. Volume and errors are visible per service and per source, with silence
   distinguishable from quiet.
3. Triage groups repeated failures instead of listing them, and distinguishes
   new from known.
4. No panel depends on a text match where a label exists.
5. Dashboards are provisioned, checked in, and covered by selector tests against
   Plan 141's fixtures.
6. Stage 3 either delivers a working correlation view or records why it cannot,
   with the finding routed to Plan 151.

## Rollback and safe stopping points

- **After Stage 0:** a written question list, no dashboard change.
- **After Stage 1:** dashboards revert by reverting the JSON; provisioning is
  declarative.
- **After Stage 2:** triage view is additive and removable on its own.
- **After Stage 3:** either the correlation view or the written finding is the
  durable output.
