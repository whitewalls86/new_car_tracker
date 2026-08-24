# Plan 150: Analytics Product and BI Serving Layer

## Status

**BUILD ORDER, written 2026-08-24.** Priority **68 (medium)**. Effort
**XL, research-gated**.

This plan intentionally begins with research and ideation. It records the
direction and its rationale without prematurely choosing Snowflake, a BI
product, a public/private data boundary, or a deployment topology.

## Problem

Cartracker's internal analytical capability is much larger than its public
analytical surface.

The repository already operates a production-shaped data platform over a
large, proprietary, longitudinal vehicle-listing dataset. Bronze artifacts are
retained, silver data is normalized for analysis, and dbt builds tested marts.
The public product, however, presents only a small Streamlit dashboard with
basic graphs. A visitor can see an application, but cannot readily see the
depth of the dataset, the semantics encoded in the marts, or the market
questions the platform can answer.

That mismatch limits the portfolio value of work that already exists. Plans
125, 126 and 127 extend the engineering story through Iceberg/Spark-compatible
execution and streaming. Beyond that arc, more platform machinery has
diminishing value unless the data is turned into visible, trustworthy
analytical products.

The next question is therefore not "how do we create a gold layer?" The gold
layer already exists in dbt. The question is:

> Which analytical products should Cartracker expose, to whom, through which
> serving boundary and BI tools, without making proprietary detail public or
> creating an expensive second platform to operate?

## Existing foundation

The current dbt marts are the starting point, not migration inventory to be
discarded.

### Consumer-facing gold

- `mart_vehicle_snapshot` provides one current-state row per VIN, combining
  listing state, vehicle attributes, dealer linkage, price history and days on
  market.
- `mart_deal_scores` turns current listings into a reusable analytical product
  with market percentiles, discount measures, inventory depth, price history,
  a composite score and a business-readable tier.

### Operational and data-quality gold

- `mart_inventory_coverage` and `mart_price_freshness_trend` make enrichment
  and freshness measurable by make/model.
- `mart_scrape_volume`, `mart_block_rate`, `mart_cooldown_cohorts` and
  `mart_detail_batch_outcomes` support operational decisions about acquisition
  health and data quality.

These models are materialized, documented, tested and consumed by the current
dashboard. They satisfy the logical role of a gold layer regardless of whether
they run on DuckDB, Spark/Iceberg, Snowflake, or another analytical engine.

The repository also already has an ML foundation rather than a blank slate.
Plan 111 defines listing-state history, interpretable volatility features and
the proposed `mart_detail_refresh_priority` output. Plan 112 owns
Iceberg-backed adaptive-refresh backtesting and MLflow experiment provenance;
Plan 113 owns promotion into production scrape control. Plan 150's role is the
publishable analytical record of that chain: a durable gold output and product
surface that can explain the features, approved policy/model version,
recommendations or predictions, aggregate performance and observed outcomes.
It must not stand up a second MLflow system or confuse experiment tracking with
delivery of an analytical product.

The likely modeling gap is historical market intelligence rather than current
state or platform health. Candidate additions include listing-lifecycle facts,
price-change facts, periodic inventory snapshots, stable dimensions, and
market/model/dealer aggregates. Stage 0 must validate that claim against actual
questions and existing model grains before any of those models are committed.

## Objective

Turn Cartracker's existing dbt gold layer into a legible analytics product and
analytics-engineering portfolio surface.

The finished system should demonstrate an end-to-end path from proprietary
production data to governed, documented and decision-oriented analytics while
keeping the high-volume raw history in its cost-effective platform and
publishing only the data each consumer needs.

## Principles

1. **Gold is a semantic role, not a vendor.** Moving a table to Snowflake does
   not make it gold; stable business meaning and consumption do.
2. **Research precedes procurement and migration.** No product is selected
   because it is familiar, fashionable, or named in an interview rubric.
3. **Expose decisions, not database inventory.** Dashboards begin with useful
   questions and audiences, then select models and measures.
4. **Keep proprietary detail private by default.** Public exposure is an
   explicit contract using aggregates, suppression or sanitization where
   required.
5. **Publish bounded products.** Do not replicate bronze or the full silver
   history merely to attach a BI tool.
6. **One metric, one definition.** Metric logic lives in governed dbt models or
   an intentionally selected semantic layer, not independently in every chart.
7. **Cost and operational burden are architecture inputs.** A solo-maintained
   portfolio must remain inexpensive, observable and easy to suspend.
8. **The public surface should make scale legible without disclosing the
   dataset.** Counts, coverage, distributions, lineage, freshness and methods
   can demonstrate scope without publishing row-level proprietary data.

## Stage 0 — Research and architecture decision

Stage 0 is the next executable slice. It ends in a written recommendation, not
an implementation or vendor account.

### 0a. Inventory the existing analytical contract

For every current mart and relevant intermediate model, record:

- grain, keys and time semantics;
- materialization and refresh cadence;
- source history retained or collapsed;
- business or operational question served;
- current consumers;
- tests, freshness checks and documented assumptions;
- portability state after Plan 125;
- fields that are safe, unsafe or ambiguous for public exposure.

This inventory must distinguish business analytics, operational analytics and
data-quality analytics. It must not label operational marts as deficient merely
because they do not answer market questions.

### 0b. Define audiences and questions

Evaluate at least these audiences:

- a vehicle shopper comparing listings and market position;
- an analyst exploring supply, pricing, turnover and inventory age;
- a dealer or market observer comparing segments without row-level access;
- a recruiter or interviewer evaluating data and analytics engineering work;
- the operator diagnosing freshness, coverage and acquisition quality.

For each candidate product, write:

- the decision or question it supports;
- the required grain and history;
- canonical dimensions and measures;
- acceptable freshness;
- public, authenticated or private classification;
- minimum useful visualization or interaction;
- evidence that the product reveals more than a basic descriptive graph.

Candidate questions include, but are not commitments:

- Where is new-vehicle inventory growing or tightening?
- Which makes and models turn over fastest?
- How do listing age and pricing distributions differ across markets?
- What price-change behavior precedes a listing becoming unlisted?
- Which dealers or segments reprice inventory most often?
- Can a reproducible model estimate listing turnover, expected time to removal,
  unusual pricing, or another outcome that is useful and honestly measurable?
- How complete and fresh is the dataset behind each conclusion?

### 0c. Evaluate the modeling gap

Map the approved questions onto the existing marts and classify each need as:

- already served;
- served with a presentation-only change;
- served by extending an existing mart;
- requires a new fact, dimension, snapshot or aggregate;
- requires a reproducible feature, trained model or prediction-output contract;
- cannot be supported reliably by the collected data.

Explicitly evaluate—but do not assume—the need for:

- `fct_listing_lifecycle`;
- `fct_price_change`;
- `fct_inventory_snapshot_daily`;
- vehicle, dealer, market and calendar dimensions;
- weekly/monthly market, model and dealer aggregates;
- governed metric definitions or a semantic layer.

Prefer the smallest coherent analytical model set that supports the selected
products. Do not create a textbook star schema where the questions do not
need one.

### 0d. Evaluate the Plan 111 analytical artifact

Treat Plan 111's adaptive-refresh feature foundation as the first concrete
candidate for an ML-backed analytical product. Determine what durable gold
record and public or portfolio presentation can make that work inspectable
without exposing proprietary listing-level data. This is not permission to
train a novel model before Plan 112 proves the objective, labels and quality
gates.

The candidate artifact should connect:

```text
Plan 111 feature definitions
    -> pinned input snapshot
    -> Plan 112 MLflow run and baseline comparison
    -> approved policy/model version
    -> versioned gold recommendations or predictions
    -> later observed outcomes and aggregate evaluation
    -> BI model/policy card and analytical views
```

Research whether the durable output should extend
`mart_detail_refresh_priority`, become a versioned scoring fact, or pair a
current recommendation mart with an append-only evaluation fact. At minimum,
the published record should make it possible to answer:

- Which feature and policy/model version produced this recommendation?
- Which input snapshot and MLflow run can reproduce it?
- How did it compare with the simple rule-based baseline?
- How does performance vary by refresh tier, make/model, listing age or another
  approved aggregate segment?
- What happened later, and was the predicted material change actually observed?
- What freshness, coverage, uncertainty and known limitations apply?

Then determine whether machine learning adds decision value beyond the
interpretable Plan 111 policy and governed descriptive metrics. Begin with a
measurable product question, not an algorithm or an MLflow deployment.

For each credible candidate, define:

- prediction target, unit of prediction and decision served;
- observation window, label timing and leakage controls;
- training/validation split appropriate to longitudinal market data;
- simple heuristic or statistical baseline that the model must beat;
- features sourced from versioned dbt outputs;
- treatment of drift, missingness, sparse segments and late-arriving data;
- evaluation metrics tied to the product rather than model novelty;
- explanation, uncertainty and user-facing limitations;
- retraining cadence and the condition that retires a model.

MLflow should record parameters, metrics, artifacts, code/model identity and
the Iceberg snapshots or dbt dataset versions used for a run. A reviewed run may
produce a batch scoring job, but BI must never query an experiment run as if it
were a governed table.

Recommendations or predictions that become product inputs return through a
tested gold contract, for example one row per scored listing and policy/model
version with `scored_at`, recommended tier or prediction,
confidence/uncertainty, feature and dataset version, and `mlflow_run_id`.
Downstream marts should join those records to later observed state changes for
monitoring and analysis. Whether that contract is
`mart_detail_refresh_priority`, a new fact table, or a separate scored dataset
is a Stage 0 decision.

Reject an ML candidate when labels are not defensible, leakage cannot be
controlled, the baseline performs comparably, the output cannot be explained
to its audience, or operating it would exceed the product value. A well-tested
market metric is preferable to decorative machine learning.

### 0e. Define the exposure boundary

Classify fields and outputs before selecting a destination:

| Class | Default treatment |
|---|---|
| Public aggregate | May be published with documented grain and freshness |
| Public row-level | Requires an explicit disclosure and source-policy review |
| Portfolio evidence | Publish metadata, lineage, aggregate profiles and methods |
| Proprietary detail | Remains in the private platform |
| Operational security | Never enters a public BI dataset |

Research suppression thresholds, dealer identification, VIN/listing exposure,
downloadability, cached extracts and the consequences of embedding. "Public
dashboard" often also means "public underlying data"; the decision must state
that behavior for the selected tool.

### 0f. Compare serving architectures

Compare at least these shapes:

1. BI reads the existing analytical platform directly.
2. Existing dbt gold models publish a bounded subset to a separate serving
   warehouse.
3. Stable silver interfaces are published and gold transformations run in the
   serving warehouse.
4. Static or scheduled extracts serve the public product while private BI uses
   a live warehouse.

For each, evaluate:

- whether model logic is duplicated;
- incremental publication and reconciliation behavior;
- schema-change and failure handling;
- refresh latency;
- role and credential boundaries;
- egress, storage, compute and idle costs;
- suspend/resume and budget controls;
- compatibility with Plan 125's final reader shape;
- operational burden for one maintainer;
- what skill or architecture evidence it adds to the portfolio.

Snowflake is one candidate serving warehouse, not the predetermined answer.
The comparison may include other managed warehouses, query engines, extracts,
or retaining the existing platform if they better satisfy the product.

### 0g. Compare BI and public-presentation tools

Evaluate Streamlit's future role alongside recognizable BI products and, where
useful, a custom public frontend. The comparison must cover:

- industry recognition and interview value;
- live-query and extract support;
- public embedding and anonymous access;
- whether viewers can download or inspect source rows;
- semantic modeling and reusable measures;
- accessibility and mobile behavior;
- refresh limits;
- source-control and deployment workflow;
- licensing after trials expire;
- ongoing cost and maintenance.

Also compare how each option consumes versioned prediction outputs, presents
uncertainty and model limitations, and keeps reusable metric or scoring logic
outside individual visualizations.

Streamlit may remain the operational or exploratory interface even if another
tool becomes the public analytical surface. Replacement is an outcome to
justify, not an assumption.

### 0h. Produce the decision record

Stage 0 closes only when the plan records:

- selected audiences and first analytical products;
- existing marts reused and new models required;
- public/private data contract;
- chosen serving topology and rejected alternatives;
- chosen BI/presentation tool and rejected alternatives;
- selected ML-backed product, its baseline and delivery contract—or an explicit
  decision that the first release should remain non-ML;
- the durable and publishable record of the Plan 111 -> Plan 112 -> Plan 113
  lineage, including how later outcomes reconcile to scored recommendations;
- estimated recurring cost with hard budget controls;
- refresh, reconciliation, observability and rollback contracts;
- a staged implementation plan with one thin vertical slice first;
- evidence that implementation waits behind Plans 126, 127, 69 and 121 unless
  the build order is explicitly changed.

If no option provides enough portfolio or product value for its cost and
maintenance burden, Stage 0 may recommend improving the existing dashboard and
models without adding a warehouse or BI vendor.

## Provisional implementation shape

This section is a hypothesis for Stage 0 to test, not an approved architecture.

```text
private production platform
bronze -> silver -> dbt features -> MLflow-tracked experiment
                    |                         |
                    v                         v
                 dbt gold <---- governed prediction outputs
                    |
                    | bounded, tested publication contract
                    v
optional analytics serving layer -> semantic/BI layer -> public analytics
```

A likely first vertical slice would take one existing gold product and one new
historical product through the complete path:

1. define their grains, measures and exposure classifications;
2. test them in dbt;
3. publish the approved Plan 111/112 policy evidence through a versioned gold
   contract and, if justified, compare an ML candidate with its rule baseline;
4. publish or expose the products through the selected serving boundary;
5. reconcile row counts and measures at the boundary;
6. build one decision-oriented BI experience;
7. publish freshness, coverage, methodology and any model limitations alongside
   it;
8. measure cost, refresh reliability and maintenance effort before expanding.

## Provisional later stages

Stage 0 may rewrite these stages. They exist only to make the likely delivery
shape visible.

### Stage 1 — Analytical product contracts

Freeze the audience, questions, dimensions, measures, grains, freshness and
exposure policy for the first vertical slice.

### Stage 2 — Gold-layer extension

Add only the facts, dimensions, snapshots or aggregates the selected products
require. Preserve existing mart consumers and add migration contracts where a
model changes grain or meaning. If Stage 0 approves an ML-backed product, add
its versioned feature, prediction and observed-outcome contracts here, reusing
Plan 111's feature definitions and Plan 112's MLflow provenance boundary.

### Stage 3 — Serving boundary

Implement the selected publication path, reconciliation checks, schema-change
behavior, credentials, budgets, refresh schedule and rollback.

### Stage 4 — BI product

Build and document the first decision-oriented experience. Include visible
freshness, coverage and methodology so the conclusions remain inspectable.

### Stage 5 — Public integration and evidence

Integrate the selected experience into the public surface, publish architecture
and lineage evidence, measure usage/cost/reliability, and decide whether to
expand, revise or stop.

## Out of scope until Stage 0 approves it

- copying bronze or full silver history into a second warehouse;
- rewriting working marts solely to use a new vendor;
- selecting Snowflake, Tableau, Power BI or another product by default;
- adding MLflow independently of Plan 112 or selecting a model before a
  product question and baseline exist;
- presenting predictions without version, provenance, uncertainty and measured
  outcomes;
- publishing VIN-level, listing-level or dealer-level proprietary data;
- replacing Streamlit before its remaining role is defined;
- building dashboards before audiences, questions and metric contracts exist;
- adding infrastructure whose only justification is a résumé keyword;
- treating chart count as analytical-product completeness;
- beginning implementation ahead of Plans 126, 127, 69 and 121 without an
  explicit build-order decision.

## Success criteria

This plan succeeds when:

- the existing dbt gold layer is accurately represented and extended rather
  than unnecessarily rebuilt;
- at least one useful historical market product complements the current-state
  and operational marts, if the source data supports it;
- public outputs have explicit grains, metric definitions, freshness and
  exposure classifications;
- the serving and BI choices are justified against alternatives and remain
  within a recorded operating budget;
- a visitor can understand the dataset's scale, lineage, quality and analytical
  value without access to proprietary raw data;
- the public experience demonstrates analytics-engineering judgment, not just
  visualization mechanics;
- any ML-backed product beats a declared baseline, has reproducible MLflow and
  dataset provenance, and enters BI through a tested gold output;
- every publication path is tested, reconcilable, observable and reversible.

## Failure and stopping rules

- If Stage 0 cannot identify a decision-oriented product stronger than the
  current dashboard, stop without adding a warehouse or BI vendor.
- If a free trial is the only thing making an architecture affordable, cost it
  at post-trial rates before approval.
- If a tool requires public row-level disclosure beyond the approved contract,
  reject the tool or use a sanitized extract.
- If metric logic starts diverging between dbt and BI, stop expansion and name
  one authoritative definition.
- If an ML candidate cannot beat its declared baseline or defend its labels,
  do not ship it; retain the experiment evidence and serve the governed metric.
- If prediction outputs cannot be joined to observed outcomes by model version,
  stop before public presentation.
- If publication duplicates substantial transformation logic, reconsider the
  silver/gold boundary before adding more models.
- If the first vertical slice cannot be reconciled or operated within budget,
  roll it back while preserving the existing dbt marts and dashboard.

## Relationship to other plans

- **Plan 125** determines the durable analytical storage and execution shape
  on which this plan should build.
- **Plan 111** owns the listing-volatility feature definitions and interpretable
  refresh-priority design. Plan 150 publishes an analytical record of those
  outputs and their later outcomes; it does not redefine the features.
- **Plan 112** owns MLflow-backed experiment provenance and adaptive-refresh
  backtesting. Plan 150 may reuse that foundation for a customer-facing
  analytical model, but does not duplicate or broaden Plan 112 silently.
- **Plan 113** owns production policy promotion and scrape-control integration.
  Plan 150 may visualize the approved version and its measured outcomes, but BI
  does not become part of the hot control path.
- **Plans 126 and 127** complete the currently planned streaming arc. Plan 150
  follows them so the engineering milestone is coherent before the public
  analytics expansion begins.
- **Plan 138** refreshes the present public surface and can improve how existing
  project evidence is presented without pre-deciding Plan 150's BI stack.
- **Plan 69 and Plan 121** establish infrastructure-as-code and staging before
  this plan begins. Plan 150 may use that foundation, but must not silently
  expand their scope.
- **Plan 149** may track the eventual execution slices, while this document and
  `PLANS.md` remain authoritative.

## Safe stopping point

Stage 0 ends with a written tool and architecture decision. Plan 150 remains
behind Plans 126, 127, 69 and 121, and no warehouse or BI commitment is made
before that decision.
