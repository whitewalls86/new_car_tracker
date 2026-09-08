# Plan 179: Derived service call graph

## What this plan is for

Derives the service-to-service call graph from the files that already define it
— Compose environment blocks, service URLs in source, proxy and datasource
config — and checks it in CI against the declared coordination contract, so a
new cross-service dependency cannot appear without being noticed.

## The case

Raised by [Plan 151](plan_151_distributed_tracing_and_runtime_topology_audit.md)
Stage A on 2026-09-08, which set out to measure the runtime service graph with a
distributed-tracing pipeline and instead found that most of the answer is
sitting in checked-in files. Stage A's contract
([`docs/reference/plan_151_telemetry_contract.md`](../reference/plan_151_telemetry_contract.md))
declined to build the pipeline; this plan is the cheap half it recommended
building instead.

**The measurement.** Plan 142's `ops/coordination_contract.py` is the
authoritative service-to-surface registry, and its `compose_dependencies` field
is the only edge relation the repository declares. It models **container startup
order, not runtime call topology**, and the two turn out to barely overlap. A
prototype deriver reading Compose `environment:` blocks and literal service URLs
in each service's own source produced **77 call edges** against the registry's
37, of which **62 are undeclared** — `ops → prometheus`, `ops → loki`,
`ops → grafana`, `ops → container-health`,
`ops → {scraper, processing, archiver, dbt_runner, pack-worker}`,
`dashboard → postgres`, `scraper → postgres`, and all four Airflow services to
six application services. Asked directly, the registry says `scraper` and
`processing` are depended on by **nobody**, while Airflow calls them roughly
1,961 times a day.

Of the 22 declared edges the prototype did *not* derive, 11 are correctly not
call edges — `X → flyway` and `X → airflow-init` are waiting on a migration, not
calling it. The other 11 are real calls it missed for one reason: their
configuration lives in a Caddyfile, a Grafana datasource YAML, a promtail YAML,
or Flyway command arguments rather than in Python or Compose env. All 11 are in
checked-in files. That bounds the work — the deriver's coverage is a function of
how many config formats it reads, not of what is knowable without running the
system.

**Why static rather than runtime.** Plan 151 concedes in its own principles that
an unobserved declared edge "may merely be idle, rare, sampled, or
uninstrumented," which makes absence weak evidence in a runtime pipeline. A
derived graph has no such weakness: coverage is complete regardless of traffic,
the result is deterministic, it runs at CI time on the pull request that
introduces the drift rather than seven days later, and it costs nothing to
operate. On the signal Plan 151 cared most about, the cheaper method is also the
more trustworthy one.

**Why it does not simply extend Plan 142.** A hand-maintained `runtime_calls`
field was the first idea and was rejected: it would be a second copy of
something the code already states, and a second copy is how the first one goes
stale — the argument `maintenance-running-set.txt` already makes about itself
when it refuses to list all 26 default services. Deriving the relation instead
means it cannot drift from its sources, and it needs no change to
`ops/coordination_contract.py`, which keeps Plan 142's closeout undisturbed.

**Neighbours.** [Plan 139](plan_139_test_suite_maintenance.md) Stage E builds a
path-to-CI-impact graph for advisory test selection and explicitly consumes
Plan 142's stable service names rather than deriving edges; it is a different
artifact answering a different question, and it says so. It does note that CI
selection "must consume reviewed declarations rather than live telemetry" — a
derived, CI-checked call graph is exactly the kind of reviewed declaration that
argument asks for. Plan 151 remains the record of why the runtime pipeline was
declined, and is where the trigger for revisiting that decision lives.

## Design

A generated, checked-in call-graph artifact, plus a CI check that regenerating
it produces no diff. A new cross-service dependency then appears as a snapshot
diff in the pull request that introduces it, and is reviewed once, at
introduction.

This is the pattern the repository already uses for its three generated public
surfaces — a source of truth, a generator, and a `--check` mode that fails when
the committed output no longer matches. Plan 179 inherits that convention rather
than inventing a mechanism.

### What the check gates on, and what it does not

The prototype in Plan 151 Stage A compared derived edges against
`compose_dependencies`, and **that comparison is the thing not to ship as a
gate.** The two model different relations: one is container startup order, the
other is runtime call topology. A gate that failed on "derived but not declared"
would fail on 62 real edges the day it landed, and keep failing, which trains a
reader to ignore it.

So the comparison still runs, but as an **informational report** rather than a
failure condition. It separates:

- edges that are startup ordering only — `X → flyway`, `X → airflow-init`;
- edges that are calls only — the 62;
- edges that are both.

The gate is the snapshot diff. The report is the audit.

### Rejected alternatives

- **A hand-maintained `runtime_calls` field on `ServiceContract`.** A second
  copy of what the code already states, and second copies go stale — the
  argument `maintenance-running-set.txt` makes about itself. It would also edit
  a file Plan 142 owns while that plan is in closeout.
- **Deriving the graph at runtime, from traces.** Plan 151 Stage A costed this
  and declined it; see
  [`docs/reference/plan_151_telemetry_contract.md`](../reference/plan_151_telemetry_contract.md).
- **Hard-failing on any edge absent from `compose_dependencies`.** Compares two
  different relations and fails 62 times on day one.

## Stages

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a--the-deriver-and-its-snapshot) | The deriver over Compose env and source literals, its checked-in snapshot, and `--check` | `next` | -- |
| 2 | [**B**](#stage-b--the-remaining-config-formats) | The four config formats the prototype could not read | `--` | -- |
| 3 | [**C**](#stage-c--ci-gate-and-the-declared-versus-derived-report) | The CI gate and the informational report | `--` | -- |

### Stage A — The deriver and its snapshot

1. Harden the Plan 151 Stage A prototype into a script: read Compose
   `environment:` blocks and literal `<scheme>://<service>` constants in each
   service's own source, resolving hosts against the Compose service set.
2. Emit the derived graph as a checked-in artifact with a stable sort order, so
   a diff is reviewable and does not churn.
3. Add `--check`, which regenerates and exits non-zero on any difference.
4. Unit-test the derivation: host extraction, the service-name join, the
   source-directory mapping for services that share an image.

**Exit:** the snapshot is checked in, `--check` passes on a clean tree and fails
on an injected edge, and the derivation is covered by unit tests.

### Stage B — The remaining config formats

The prototype missed 11 real call edges, all for the same reason: their
configuration is not Python and not Compose env.

1. Caddyfile — `caddy → {ops, dashboard, grafana, airflow-apiserver, oauth2-proxy}`.
2. Grafana datasource YAML — `grafana → {loki, prometheus}`.
3. Promtail YAML — `promtail → loki`.
4. Flyway command arguments — `flyway → postgres`.

**Exit:** all 11 known-missed real call edges appear in the snapshot, and a test
asserts each of the four formats contributes at least one edge, so a parser that
silently stops matching fails rather than quietly shrinking the graph.

### Stage C — CI gate and the declared-versus-derived report

1. Run `--check` in `ci.yml`.
2. Emit the declared-versus-derived report described in the design, classifying
   every edge as startup-ordering-only, call-only, or both.

**Exit:** `ci.yml` runs `--check`; a pull request that adds a cross-service URL
without regenerating the snapshot fails it; and the report separates the three
edge classes.
