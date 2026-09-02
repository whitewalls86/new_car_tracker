# Plan 169: Public Analytics Agent

## Status

**Backlog, written 2026-09-01.** Priority **46**. Effort **L**.

**Stages 1 and later** are blocked on
[Plan 168](plan_168_generated_knowledge_substrate.md) Stage 3, which supplies
the substrate they read. **Stage 0 is not blocked** — it is the threat model,
the budget and the go/no-go, it needs nothing that does not exist today, and a
"no" there ends this plan without any of Plan 168's work being wasted, since
Plan 168 is scheduled on Plan 150's account rather than this one.

An agent on the public `/info` page that answers questions about this project —
its architecture, its data model, its methodology — from
[Plan 168](plan_168_generated_knowledge_substrate.md)'s generated public
projection. Its audience is recruiters and hiring managers, and its purpose is
to make the depth of the platform legible to someone who will not read a repo.

Querying the data is **Stage 4**, gated behind Stage 2 shipping and proving out.
It is deliberately not in the first release.

## Problem

[Plan 150](plan_150_analytics_product_and_bi_serving_layer.md) already records
the gap this plan attacks from a different angle:

> Cartracker's internal analytical capability is much larger than its public
> analytical surface. […] A visitor can see an application, but cannot readily
> see the depth of the dataset, the semantics encoded in the marts, or the
> market questions the platform can answer.

Plan 150's answer is a BI product, and it sits behind four large plans. This
plan's answer is cheaper and available sooner: let the visitor ask.

A hiring manager evaluating this project has a specific problem — the evidence
of judgment is in 23 dbt models, 96 plan documents and a test suite they will
never open. An agent that can answer *"how does the deal score actually work?"*
or *"what happens when Cloudflare blocks a scrape?"* converts that buried
evidence into something reachable in one question.

## The measurement

Taken 2026-09-01 on `3f40f55`.

| Fact | Value |
|---|---|
| `ops/routers/info.py` | 40 lines, one route — `@router.get("/info")` |
| `ops/templates/info.html` | 1,526 lines of Jinja2 |
| Access | anonymous, no auth |
| LLM provider in first-party code | none — this is a new dependency |

## Principles

1. **The guardrail is capability, not instruction.** A prompt telling the agent
   not to reveal something is not a control. What the agent cannot reach, it
   cannot leak.
2. **Anonymous means adversarial.** Recruiters are the intended audience. The
   internet is the actual one. Design for the second.
3. **Bounded cost, enforced.** A public LLM endpoint with no ceiling is an
   unbounded liability on a solo-funded project.
4. **Refuse rather than guess.** For this audience a wrong confident answer is
   worse than "I don't know" — it discredits the work it was built to showcase.
5. **The substrate is generated.** Every fact the agent states traces to
   [Plan 168](plan_168_generated_knowledge_substrate.md)'s artifact, which
   cannot go stale without failing CI.

## Stage 0 — Threat model and budget

The stage that decides whether to build this at all. It ends in a written
decision, and a "no" here is a legitimate outcome.

**Record before any code:**

- the monthly spend ceiling, and what happens at it — hard stop, or degrade to
  a static FAQ;
- expected and worst-case question volume;
- the abuse cases and the control for each: scripted flooding, prompt
  extraction, using the endpoint as a free general-purpose LLM, attempts to
  reach data the agent should not have;
- whether an anonymous endpoint is acceptable at all, or whether a low-friction
  gate (per-IP rate limit, proof-of-work, a soft email ask) is the price of
  admission;
- what is logged, given that visitor questions are third-party input landing in
  this project's logs.

### Cost shape

The dominant lever is prompt caching. Plan 168's catalog is byte-identical on
every request, so it belongs in a cached prefix with the volatile question after
the last breakpoint. Cached reads bill at ~0.1x.

### Prefix budget — measured corpus

Measured 2026-09-01 on `3f40f55`. Byte counts are real; token figures are
`bytes ÷ 3.5–4.0` and **must be replaced by `client.messages.count_tokens`
before Stage 1 ships** — no Anthropic CLI or key was available in the
measuring session.

| Candidate prefix source | Bytes | ~Tokens |
|---|---|---|
| Mart `.schema.yml` only — 9 models, 63 columns | 8,715 | ~2.2–2.5K |
| All `.schema.yml` — 23 models, 210 columns | 40,290 | ~10–11.5K |
| `docs/ARCHITECTURAL_OVERVIEW.md` | 39,302 | ~9.8–11.2K |
| `README.md` | 23,760 | ~5.9–6.8K |
| `graphify-out/GRAPH_REPORT.md` | 220,875 | ~55–63K |
| `graphify-out/graph.json` | 12,831,489 | ~3.2–3.7M |

Two findings change the design.

**The data-model half is far smaller than assumed.** Plan 168's *public*
catalog is mart-facing — intermediates are internal plumbing — so it starts
from 8,715 bytes, roughly **2.5K tokens**, not the 10K first sketched. dbt
`unit_tests.yml` accounts for 128,243 of the 178,287 total yml bytes and is not
catalog material; excluding it is most of the difference.

**The codebase half is the one with a budget problem.** Plan 169 promises
answers about how the project works, not only about its data model, and that
corpus is a different order of magnitude — graphify reports 676 files and
~765,576 words across the repository. It cannot be a prefix. The workable
substitute is the curated pair already written for humans:
`ARCHITECTURAL_OVERVIEW.md` + `README.md`, ~16–18K tokens.

So Stage 1's realistic prefix is **~20K tokens** (public catalog + both
documents), not 10K.

### Cost shape

The dominant lever is prompt caching. The prefix is byte-identical on every
request, so it belongs in a cached block with the volatile question after the
last breakpoint. Cached reads bill at ~0.1x; writes at ~1.25x.

On `claude-opus-5` ($5/$25 per MTok), 500-token answer:

| Prefix | Warm question | Cold question | Cold/warm |
|---|---|---|---|
| 2.5K — catalog only | ~$0.014 | ~$0.028 | 2.0x |
| **20K — Stage 1 realistic** | **~$0.023** | **~$0.138** | **6.1x** |
| 75K — with graphify report | ~$0.051 | ~$0.482 | 9.5x |

At 1,000 questions/month the 20K prefix costs ~$23 warm and ~$138 if every
question is a cold start. The spread, not the average, is the number Stage 0's
ceiling has to survive.

**Prefix size is what makes cold starts hurt.** The write/read ratio is 12.5x
while the output cost is a fixed floor, so as the prefix grows the write term
dominates and the cold/warm gap widens — 2x at 2.5K, 6x at 20K. That matters
because this page's traffic is sporadic bursts against a 5-minute default TTL,
which is the regime where cold starts are the common case. **Keeping the prefix
small is a cost control, not an aesthetic preference**, and it is a stronger
lever than model choice.

It also settles the graphify question. `graph.json` exceeds even Opus 5's 1M
context; `GRAPH_REPORT.md` would triple the prefix and 622 of its 2,879 lines
are community navigation links — traversal scaffolding, not answer material.
Graphify is a good tool for *choosing* which files matter and as a retrieval
index; it is not prefix material. Retrieval, in turn, varies the prefix per
question and forfeits the cache, so it is a Stage 3 question at the earliest.

The per-question price is not the risk. Unbounded volume is. Rate limiting is
therefore a Stage 1 requirement, not a Stage 3 refinement.

### Model choice

Default to **`claude-opus-5`**. Two reasons beyond capability:

- mid-conversation system messages — appending `{"role": "system", ...}` to
  `messages[]` rather than editing top-level `system` — are supported on Opus 5
  and are the injection-resistant operator channel; they are not available on
  Sonnet 5;
- prefix caching makes the capability difference cheap, since the expensive part
  of each request is cached.

`claude-sonnet-5` ($2/$10) and `claude-haiku-4-5` ($1/$5) are the named
alternatives if Stage 0's budget says otherwise. **That is a cost decision for
the operator to make explicitly in Stage 0**, not one to make silently at
implementation time.

### One model, not two — and why

The obvious cost design is two-tier routing: a cheap model for simple lookups,
Opus for questions that need reasoning about the project. It is the standard
pattern, and it is **rejected as the Stage 1 baseline**. It is reconsidered at
[Stage 3](#stage-3--the-gate) with observed data.

**Caches are per-model, and the cache is where this plan's cost model lives.**
Two models means two prefixes, two sets of cache writes at ~1.25x, two TTLs
expiring independently. Routing splits the traffic, each model's cache runs
colder, and both write more often. At the measured 20K prefix, 500-token answer:

| | Opus 5 | Haiku 4.5 |
|---|---|---|
| Cache write (~1.25x) | $0.125 | $0.025 |
| Cache read (~0.1x) | $0.010 | $0.002 |
| Output, 500 tokens | $0.0125 | $0.0025 |
| **Warm question** | **$0.0225** | **$0.0045** |
| **Cold question** | **$0.1375** | **$0.0275** |

Routing a question to Haiku saves ~$0.018. One extra Opus cold start costs
~$0.115. **Every additional Opus cache miss caused by splitting traffic eats
the savings from roughly six correctly-routed Haiku questions.**

That ratio was ~4 against the original 10K estimate. Measuring the corpus moved
it to ~6, in the predicted direction: a larger prefix raises the cold-start
penalty faster than it raises the per-question saving, so the case against
routing strengthened rather than weakened when real numbers arrived.

That interacts badly with this specific traffic profile. A portfolio page gets
sporadic bursts — a visitor asks three questions and leaves, then nothing for
hours — against a 5-minute default TTL. Most questions are cold starts already,
and halving the traffic per model makes it worse. **Buy the 1-hour cache TTL
before buying routing**; it is a larger lever on the same problem and costs no
architecture.

Two further objections:

- **Classification is not free and not easy.** "Simple" is not a property of the
  question but of what answering it well requires. *"What is a deal score?"* is
  a lookup; *"why is the deal score computed that way?"* is nearly the same
  string and needs the reasoning. Routing needs a classifier call — latency plus
  a request — and a cheap classifier drawing that line is performing exactly the
  judgment the cheap model was not trusted with.
- **The failure asymmetry runs the wrong way.** A subtly shallow answer about
  methodology is the precise failure that discredits the work this page exists
  to showcase, bought for $0.014.

### The levers to use instead

1. **`effort` before a second model.** `output_config: {"effort": "medium"}` or
   `"low"` on Opus 5 cuts thinking tokens without splitting the cache, adding a
   classifier, or risking a quality cliff. Default is `high`; these are Q&A
   turns over supplied context, so `medium` is the likely resting place and
   `low` is worth testing. This is the first knob, not the last.
2. **Haiku as the pre-flight guard, not the answer.** Before the Opus call: is
   this in scope, is it an injection attempt, which catalog sections are
   relevant. Those are genuine classification tasks — short outputs, small or no
   prefix, no cache to fight. That is a real fit and a different question from
   "which model answers."
3. **Revisit routing at Stage 3.** If observed volume keeps both caches warm,
   the math flips and routing becomes correct. Deciding it now means guessing at
   a volume that has not been measured.

Every figure above rests on the measured byte counts in
[Prefix budget](#prefix-budget--measured-corpus) converted at 3.5–4.0 bytes per
token. The bytes are real; the conversion is not. `count_tokens` against the
built Stage 1 prefix is what replaces it, and if that lands materially above
20K the cold-start penalty grows again and this conclusion holds harder.

## Stage 1 — Q&A agent, no data access

The agent answers from Plan 168's public projection and selected repository
prose. It has no database connection, no tools, and no network access.

**Request shape:**

- `client.messages.create` on the `anthropic` Python SDK — first-party SDK, not
  raw HTTP;
- system prompt and catalog as a cached prefix
  (`cache_control: {"type": "ephemeral"}`), question after the last breakpoint;
- `thinking: {"type": "adaptive"}` with `output_config: {"effort": ...}` tuned
  down — this is Q&A over supplied context, not a reasoning task;
- `max_tokens` bounded deliberately, as a cost control and because long answers
  are worse answers for this audience;
- streaming, so the page shows progress rather than a long pause.

**Required handling:**

- check `stop_reason` before reading `content`; `"refusal"` returns HTTP 200
  with `stop_details`, not an exception;
- a most-specific-first exception chain — `RateLimitError`, then
  `APIStatusError`, then `APIConnectionError` — not one broad catch, so
  retryable and non-retryable failures stay distinguishable;
- verify caching is live via `usage.cache_read_input_tokens`; if it is zero
  across requests, a silent invalidator is in the prefix and the cost model is
  wrong.

**Injection posture at this stage** rests on the agent having nothing to leak
beyond what is already public: no tools, no DB, and a system prompt whose
disclosure costs nothing. That is the whole defense, and it is sufficient
*because* of the capability boundary, not because of instructions in the prompt.

## Stage 2 — Abuse, cost and observability

Ships with or immediately after Stage 1; Stage 1 is not public without it.

- per-IP and global rate limits, with the global limit set from Stage 0's
  ceiling;
- a hard monthly spend cap and the decided behavior on hitting it;
- metrics into the existing Grafana stack ([Plan 86](plan_86_grafana.md)):
  question volume, token spend, cache hit rate, refusal rate, error rate;
- an alert on spend rate, not just on total, so a flood is caught in hours;
- a documented kill switch that returns `/info` to its current static form.

## Stage 3 — The gate

An explicit decision point, not a formality. Stage 4 does not start until
Stage 1 and 2 have run in production long enough to record:

- observed volume and spend against Stage 0's estimate;
- observed abuse, if any, and whether the controls held;
- answer quality — specifically, whether the agent's failures were missing
  facts (which Stage 4 would fix) or bad reasoning over facts it had (which
  Stage 4 would amplify);
- the measured cache hit rate, and with it the two-tier routing question
  deferred from [Model choice](#one-model-not-two--and-why). Routing becomes
  correct once volume keeps both caches warm; the reading that settles it is
  hit rate, not spend. Record the decision either way — including that the
  `effort` setting Stage 1 landed on is the one still in use.

If quality is limited by reasoning rather than by data access, Stage 4 makes the
product worse. Record that finding and stop.

## Stage 4 — Querying, over a public view set

Only after Stage 3 passes.

**The agent never queries the marts.** It queries a small purpose-built set of
public aggregate views, defined and tested in dbt, whose exposure classification
is enforced by the view definition. The guardrail is schema-level: the
credential the agent uses can reach nothing else, so a successful prompt
injection reaches nothing else either.

This is the design constraint that keeps Stage 4 from pre-empting
[Plan 150 §0e](plan_150_analytics_product_and_bi_serving_layer.md). Plan 150
decides the general public/private contract; this stage decides one bounded
view set and does not generalize from it.

Required before any query runs:

- the view set, its grain, and its suppression thresholds;
- a read-only credential scoped to those views alone, verified by attempting to
  reach a mart and failing;
- query cost and row-count ceilings, enforced by the database, not the prompt;
- tool definitions with `strict: true` and `additionalProperties: false`, so
  inputs validate exactly;
- the answer showing the generated SQL, so a reader can check the claim — the
  article's own trade of query-time guarantees for inspectability.

Plan 168's generated catalog is extended here to describe the public view set,
which is what makes the agent able to write correct SQL against it.

## Out of scope

- any access to marts, silver, bronze, or the operational database;
- authenticated or operator-facing agent surfaces;
- replacing `/info`'s existing content — this is added to the page, not
  substituted for it;
- deciding Plan 150's BI or serving architecture;
- an agent that can take any action, or write anything anywhere.

## Success criteria

- a visitor can ask a substantive question about the platform's architecture,
  data model or methodology and get a correct, inspectable answer;
- every factual claim traces to Plan 168's generated artifact;
- spend stays under Stage 0's ceiling, with the cap enforced rather than
  monitored;
- a prompt-injection attempt reaches nothing the visitor could not already see,
  demonstrated by test rather than asserted;
- `/info` can be returned to its current static form in one action.

## Failure and stopping rules

- If Stage 0 cannot name a spend ceiling and its enforcement, do not build.
- If the agent cannot beat the existing static `/info` page for the target
  audience, stop — a worse answer delivered conversationally is still worse.
- If cache hit rate is near zero and the cost model does not hold, fix the
  prefix or stop; do not absorb a 10x cost.
- If Stage 3 shows quality is reasoning-limited rather than data-limited, do
  not build Stage 4.
- If any Stage 4 injection test reaches a non-public view, roll back to Stage 2
  and treat the view set as unsound.

## Relationship to other plans

- **[Plan 168](plan_168_generated_knowledge_substrate.md)** supplies the
  substrate. This plan cannot start before its Stage 3, and its staleness gate
  is what keeps this agent from confidently stating things that stopped being
  true.
- **[Plan 150](plan_150_analytics_product_and_bi_serving_layer.md)** — Stage 4's
  bounded public view set is a narrow instance of §0e, deliberately scoped so it
  does not settle the general contract. This plan is not a substitute for
  Plan 150's BI product; it is a cheaper, earlier answer to the same visibility
  problem.
- **[Plan 138](plan_138_public_surface_refresh.md)** owns the public surface
  `/info` lives on. Sequence behind its in-flight stages rather than editing the
  same template concurrently.
- **[Plan 86](plan_86_grafana.md)** supplies the metrics and alerting stack
  Stage 2 reports into.

## Safe stopping point

After Stage 2. A Q&A agent with enforced cost controls and a kill switch is a
complete product; Stage 4 is a separate decision with its own gate.
