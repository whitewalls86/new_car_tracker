# Plan 168: Generated Knowledge Substrate

## Status

**Build order, written 2026-09-01.** Priority **70**. Effort **M**.

Written after reading
[The semantic layer nobody maintains](https://www.angellist.com/blog/the-semantic-layer-nobody-maintains)
(AngelList, Beau Rothrock). The article's architecture does not transfer to this
repository at face value — see [Where the article does not
apply](#where-the-article-does-not-apply) — but its central discipline does.

**Why it is scheduled rather than triggered.**
[Plan 150](plan_150_analytics_product_and_bi_serving_layer.md) Stage 0a is a
hand-written inventory of every mart's grain, keys, materialization, consumers,
tests and exposure class. This plan generates that inventory from the dbt
manifest. Doing 0a by hand first is both wasted work and stale on arrival, so
this plan must precede it — which is where its priority of 70 comes from: it
inherits its rank from what it unblocks, one above Plan 150's 68, not from being
independently more valuable than the plans it now outranks.

[Plan 169](plan_169_public_analytics_agent.md) is a second consumer — a public
agent that reads this plan's output — and is a stronger near-term motivation
than Plan 150, which sits behind four large plans. It is not a dependency in
either direction: this plan is worth building for Plan 150 alone, and nothing
here waits on Plan 169's go/no-go.

## Problem

There is no machine-readable, current description of what this project's data
models mean.

The information exists. It is spread across `.sql` files, `.schema.yml` files,
the dbt manifest, plan documents and the operator's memory. A reader who wants
the grain of `mart_deal_scores` reads the SQL. That is a workable cost for the
one person who wrote it, and an unworkable one for any other consumer.

The article names the failure mode that matters here:

> Documentation and metadata that require manual upkeep go stale, and stale
> semantics are worse than none. They're confidently wrong.

For a maintainer, confidently wrong is friction. For an agent answering a
stranger on the public site — Plan 169's entire purpose — confidently wrong is
the product failing in front of the audience it exists to reach. That raises
the cost of staleness enough to justify generating the artifact instead of
writing it.

## The measurement

Taken 2026-09-01 on `3f40f55`.

| Fact | Value |
|---|---|
| dbt models, total | 23 |
| — marts | 9 |
| — intermediate | 9 |
| — staging | 5 |
| Marts with a checked-in `.schema.yml` | 9 of 9 |
| First-party code reading the dbt manifest for documentation | none |
| Procedural markdown already in the repo (`.claude/skills/`) | 8 skills |

The manifest is already produced by every dbt run and is already consumed by
this repository for other purposes — the lake-snapshot and provenance paths in
`archiver/processors/` and `scripts/`. Nothing reads it for documentation.

### Size of the source material

| Source | Bytes | Columns |
|---|---|---|
| Mart `.schema.yml` — the public projection's input | 8,715 | 63 |
| All `.schema.yml` — the internal catalog's input | 40,290 | 210 |
| All model `.sql` | 77,363 | — |
| `unit_tests.yml` — **not** catalog material | 128,243 | — |

Two things follow. The unit-test files are 128,243 of the 178,287 total yml
bytes, so a generator that globs `*.yml` would emit a catalog that is mostly
test fixtures; it must match `*.schema.yml`. And the public projection's entire
input is **8,715 bytes across 9 models** — which is the final argument against
the article's three-tier domain index, and the reason
[Plan 169](plan_169_public_analytics_agent.md) can afford to hold the whole
catalog in a cached prompt prefix rather than retrieve against it.

## Where the article does not apply

AngelList describes 1,400 mart models across 59 domains. This repository has 9
marts, all authored by one person, each with a schema file beside it. The
article's three-tier index with domain grouping is scaffolding for a discovery
problem that does not exist here, and building it would be the exact error
[Plan 150 §0c](plan_150_analytics_product_and_bi_serving_layer.md) already
warns against — a textbook structure where the questions do not need one.

Two further parts of the article are already answered here or should be dropped:

| Article component | Disposition |
|---|---|
| **Skills** (procedural markdown) | Already present — `.claude/skills/` holds 12, including `close-out`, `stage-close`, `plans`, `testing-contract`. Not in scope. |
| **Model catalog** (generated) | In scope. This is the missing half. |
| **Business glossary** (hand-written) | Deferred to Stage 4 and gated. It is hand-maintained, which is the staleness failure the article opens by condemning, reintroduced inside its own solution. |
| **Analytics guidance** (hand-written) | Same gate as the glossary. |

What survives is one claim, and it is scale-independent:

> The documentation cannot go stale because it's governed by the same trigger
> discipline as tests.

## Objective

Generate a current, machine-readable description of this project's data models
from the systems of record, in two projections — one internal, one public-safe —
regenerated on merge and gated in CI so it cannot drift from the models it
describes.

## Principles

1. **Generated, or not written.** Anything derivable from the manifest or the
   schema files is generated. Prose is reserved for what cannot be derived.
2. **The classification lives with the model.** Exposure class is declared in
   the model's own `schema.yml` `meta`, not in a separate list that can fall out
   of step with it.
3. **Public is a projection, not a redaction pass.** The public artifact is
   built by including what is marked publishable, never by removing what is not.
   A new unclassified column is absent from the public artifact by default.
4. **Staleness is a build failure.** A generated artifact that does not match a
   fresh generation fails CI, the same way a failing test does.
5. **This plan describes; it does not serve.** No endpoint, no LLM, no query
   path. Those are Plan 169.

## Stage 0 — Exposure vocabulary

Decide the classification vocabulary and where it is declared, before any
generator exists. This is the one stage whose output cannot be regenerated.

Define the class set and its default. The starting proposal, narrowed from
[Plan 150 §0e](plan_150_analytics_product_and_bi_serving_layer.md)'s five
classes to what a catalog actually needs:

| Class | Meaning | In public artifact |
|---|---|---|
| `public` | Safe for anonymous readers | Yes — name, description, type |
| `methodology` | The measure's definition is publishable even where its values are not | Yes — description only, no value ranges |
| `private` | Proprietary detail | No |
| unset | Not yet classified | No — absence is the default |

Declare it in `meta` on the model and on the column in the existing
`.schema.yml` files, so the classification travels with the thing it classifies
and is reviewed in the same diff.

**Stage 0 must record**, before Stage 1 starts:

- the class set and the meaning of each;
- that unset means private, and why the default is closed;
- whether model-level class and column-level class can disagree, and which
  wins;
- what a reviewer checks when a new column is added.

Stage 0 does **not** classify all 23 models. That is Stage 3, and it needs the
generator to enumerate what needs classifying.

## Stage 1 — The generator

One script, reading the dbt manifest plus the `.schema.yml` files, emitting a
generated markdown model catalog.

Per model, emit what is derivable:

- name, layer, materialization;
- description, from the schema file;
- columns with types and descriptions;
- declared tests;
- upstream `ref`/`source` dependencies and downstream dependents;
- exposure class, from Stage 0's `meta`.

Deliberately excluded from v1: row counts, freshness timestamps, sample values,
and anything else requiring a live warehouse connection. Those make the
generator depend on a running database and turn a build-time artifact into a
runtime one. If Plan 169 needs freshness, it reads it live rather than baking a
stale number into a document.

Follow [Plan 138](plan_138_public_surface_refresh.md)'s build-time projection
pattern rather than inventing a second one.

## Stage 2 — The staleness gate

The stage that makes the rest of it true.

CI regenerates the catalog and fails if the result differs from what is
committed. Same discipline as a test, same trigger. Without this stage the
generated file is just a file someone remembered to update once, and the article's
only load-bearing claim does not hold.

Sequence this behind [Plan 162](plan_162_testing_census_and_restructure.md)'s CI
restructure if the two would collide over job layout; check before adding a job.

## Stage 3 — Classify the models

Walk all 23 models and set the Stage 0 `meta` classification on each, using the
Stage 1 generator to enumerate what is unclassified.

This is the stage where the public projection first has content. Expect the
honest answer to be that most operational marts are `methodology` at model level
— the fact that `mart_block_rate` exists and what it measures is portfolio
evidence; its values are not.

## Stage 4 — Glossary and guidance, gated

Only after Stages 0–3 are in production, and only if Plan 169's Q&A quality
measurably needs it.

The glossary and analytics-guidance documents are hand-maintained by
construction. Before either is written, record:

- the specific questions the generated catalog answered badly;
- who updates it and on what trigger;
- the rule that retires it if it goes stale.

If those cannot be answered, do not write them. A missing glossary is a known
gap; a stale one is confidently wrong, which is the thing this plan exists to
prevent.

## Out of scope

- any LLM, endpoint, or query path — that is [Plan 169](plan_169_public_analytics_agent.md);
- a three-tier or domain-grouped index, at 9 marts;
- mining upstream application code for column metadata (the article's
  `enrich_staging_columns.py`) — this repository's staging models come from its
  own parser, and the schema files are already checked in;
- deciding [Plan 150](plan_150_analytics_product_and_bi_serving_layer.md)'s
  serving or BI architecture;
- a query-time semantic layer or metric-definition service.

## Success criteria

- the catalog is generated from the manifest and schema files, never
  hand-edited;
- CI fails when it is stale;
- an unclassified column is absent from the public projection without anyone
  remembering to remove it;
- [Plan 150 §0a](plan_150_analytics_product_and_bi_serving_layer.md)'s model
  inventory can be read out of the generated artifact rather than written by
  hand;
- Plan 169 has a stable artifact to load as context.

## Failure and stopping rules

- If the generated catalog is not materially more useful than reading the
  `.schema.yml` files directly, stop after Stage 2 and keep the staleness gate —
  the gate is worth more than the document.
- If Stage 0's vocabulary cannot be applied unambiguously in Stage 3, fix the
  vocabulary before classifying the remainder; do not accumulate judgment calls
  in commit messages.
- If Plan 169 is abandoned, this plan's value drops to the Plan 150 §0a
  inventory alone. That is still positive, but it would not justify Stage 4.

## Relationship to other plans

- **[Plan 169](plan_169_public_analytics_agent.md)** consumes this plan's public
  projection. It is the reason this plan is scoped for an agent reader rather
  than a human one, and it must not begin before Stage 3.
- **[Plan 150](plan_150_analytics_product_and_bi_serving_layer.md)** — §0a's
  model inventory becomes a generated artifact instead of a hand-written one.
  This plan does **not** answer §0c's semantic-layer question: the article
  explicitly declines to build a query planner, so nothing here forecloses that
  decision. Stage 0's vocabulary is a narrowing of §0e for catalog purposes and
  does not settle the wider exposure boundary.
- **[Plan 138](plan_138_public_surface_refresh.md)** established the build-time
  projection pattern. Reuse it.
- **[Plan 162](plan_162_testing_census_and_restructure.md)** owns CI job layout;
  Stage 2 adds a job and should not collide with its restructure.

## Safe stopping point

After Stage 2. A generated catalog with a staleness gate is independently
useful and is the input Plan 150 §0a needs, whether or not Plan 169 is ever
built.
