# Plan 127: Streaming Adaptive Scrape Control

## Status

**Draft / future work.** This plan depends on Plan 126's basic event streaming
foundation and should follow the Plan 125, Plan 112, and Plan 113 work rather
than lead it.

## Goal

Use streaming operational feedback to adjust scraping cadence, batch size, and
claim pressure safely.

The goal is a closed-loop control system:

```text
scrape/process events
  -> rolling metrics and backlog/freshness state
  -> policy decision
  -> bounded scrape-control config
  -> scraper/ops claim behavior adapts
```

## Why This Is Different From Basic Streaming

Plan 126 is about event transport and low-risk consumers. Plan 127 is about
changing production behavior based on live signals. That makes it higher risk
and requires more guardrails.

This should only start once:

- key event streams are stable and replayable
- freshness/backlog/block-rate semantics are understood
- Plan 112/113 have clarified the policy/backtest path
- the scraper has a safe runtime control surface

## Candidate Inputs

- block rate over recent windows
- scrape success/failure counts
- artifact processing backlog
- detail claim backlog
- cooldown backlog
- freshness by model/make/listing group
- oldest unprocessed artifact age
- processing lag

## Candidate Outputs

The policy engine should write a small operational config, not force every
scraper to consume the event stream directly.

Possible target:

```text
ops.scrape_control_policy
```

Example fields:

- scope (`detail`, `srp`, `carousel`, model group, etc.)
- max concurrency
- batch size
- minimum delay
- cooldown multiplier
- policy version
- reason
- computed_at
- manual override flag

## High-Level Gates

### Gate A: Read-Only Policy Simulation

- Consume streaming or replayed events.
- Compute rolling facts and proposed decisions.
- Do not apply decisions to production scraping.
- Compare proposed behavior to current static settings.

### Gate B: Policy Store

- Add an operational policy table or config store.
- Record inputs, previous policy, new policy, reason, and timestamp.
- Include manual override and safety bounds.

### Gate C: Scraper Integration Behind A Flag

- Let scraper/ops claim logic read the policy.
- Apply only bounded changes.
- Keep a feature flag and immediate rollback path.

### Gate D: Backtest And Replay

- Replay historical event windows to test whether rules would have reduced
  blocking without starving freshness.
- Validate against Plan 112/113 policy assumptions.

### Gate E: Production Trial

- Start with narrow scope and conservative bounds.
- Monitor block rate, freshness, backlog, and processing lag.
- Record every policy decision for auditability.

## Control Risks

- oscillation: policy reacts too quickly and swings between too much and too
  little scraping
- stale inputs: processing lag makes the controller react to old information
- bad attribution: block-rate changes may come from target-site behavior,
  scraper bugs, or processor lag
- runaway policy: model/rules must never exceed hard safety bounds
- opaque decisions: every applied change needs a reason and input snapshot

## Non-Goals

- No fully autonomous ML policy on day one.
- No replacing Plan 113's approved/pinned production policy model.
- No direct stream dependency inside every scraper worker.
- No removal of manual controls or deploy-intent gates.

## Exit Criteria

- A read-only policy simulator can replay recent events and produce explainable
  scrape-control recommendations.
- A bounded policy store exists.
- A feature-flagged scraper integration can apply conservative policy changes.
- Manual override and rollback are documented and tested.
