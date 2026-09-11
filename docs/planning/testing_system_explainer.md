# The testing system: what it is for, how it is shaped, and how Plans 180, 181 and 182 fit

Written 2026-09-11, out of the conversation that produced
[the artifact obligation index](artifact_obligation_index.md) and
[Plan 182](../plans/plan_182_what_a_unit_owes_itself.md). This is the
explainer, not the contract: [`docs/TESTING.md`](../TESTING.md) is the
standard and is asserted by the suite; this document is where the shape of
the whole program is written down once so that a reader, or a session that
starts cold, does not have to re-derive it from fifteen thousand lines of plan
records. Where this and the contract disagree, the contract wins.

---

## What we are trying to accomplish

**Code correctness that is hard for an agent to get wrong.** Not "well
tested" as a feeling, and not a coverage number. The goal is that the ways a
change can be wrong are each met by a mechanism, in a fixed order of
strength:

1. **Make the wrong thing unconstructable.** A refusal envelope a route
   declares its statuses through, a generated route registry a caller
   imports, a double builder that returns a real response object, a fixture
   factory that refuses a row the database would reject, a query loader that
   is the only way a statement reaches an engine. An agent cannot retype a
   status it never gets to write.
2. **Where it can still be written, make it fail by default.** Every rule
   derives its subject corpus from the repository rather than from a list, so
   a new thing is measured before anyone remembers to measure it. Both
   directions are asserted. Ledgers only shrink. An exemption costs a table
   row a reviewer can see. An agent cannot silence a rule by appending to a
   list.
3. **Where it can only be judged, hand the agent the judgement before CI
   does.** Skills, keyed on the thing being added rather than on the rule
   that will fail.

The first is the strongest and the third is the weakest, and the third is
what makes the other two usable rather than punitive. Everything in the plans
below is one of these three, and a proposal that is none of them is probably
a description that will drift.

## The doctrine, in the sentences the repository already uses

These are the rules that recur across Plans 161 and 162 and are cited by
name. They are why the system has the shape it has.

- **Coverage is asserted, not enumerated.** A test you can silence by
  appending to a list reproduces the defect it was written to catch.
- **A denominator that is listed, or scoped to what exists when it is
  written, will be wrong.** Every by-eye count in Plan 162 was an undercount.
  The rules that have never been wrong are the derived ones.
- **A floor may assert nothing, or everything, never a number between.** A
  guessed bound catches a corpus collapsing and misses it eroding.
- **Move the code, do not sharpen the checker.** Stage X struck a judgement
  rule by moving 505 SQL literals so a checker no longer had to tell a seed
  from a paraphrase. That is the precedent every drain in Plan 180 copies.
- **A rule nobody has watched fail is a rule nobody knows anything about.**
  Every rule owes a mutation, and writing the mutation is what found the six
  Stage Y rules that shipped wrong in the healthy-looking direction.
- **A check you must remember is weaker than one you cannot forget.** The
  contract is a test, the contracts are generated and diffed, the skips are
  declared, the anchors are asserted.
- **A document drifts; a mechanism does not.** `ARCHITECTURE.md:179` was
  accurate in April and false by August, and nothing could tell. The contract
  exists to make that impossible, and it has caught itself drifting three
  times since.

## How it got here, in four moves

**Plan 84** built three integration layers and left unit tests unnumbered
beside them. Accurate when written.

**Plan 161** turned the description into a contract: five layers numbered by
dependency cost, one convention per concern, a test that fails when the
document and the repository disagree, a skill that reviews against it, and a
gap list with owners. It found 73 tests that no CI job had ever run.

**Plan 162** was the census and the restructure, and grew from a waiver
drain into thirty-six stages, because every stage that closed found the next
class of defect by measuring. The pattern it discovered, stated once in
Stage W and instantiated a dozen times after: *a test may not supply both
halves of a contract*, so every truth needs one owner and everything else is
derived from it or compared against it in both directions. Stage S did that
for dbt models end to end. Stages W, Y, Z, AA, AB and AL did it for the
database vocabularies and the HTTP boundary. Stage AG made rules a directory
so an unregistered rule cannot exist. Stage AF made the mutation harness
prove itself. By Stage AL the repository had written the same statement four
times because the general form had no home.

**Plans 180, 181 and 182** are that general form, split into the two halves
it turns out to have, plus the plan that makes the checkers themselves
honest.

## The shape: two frames and one index

Every obligation in this system is one of two kinds. **What a thing owes
across a boundary**, and **what a thing owes inside itself.** Each has a
frame, and an index sits on top of both.

### Between parties: Plan 180, the seam program

A **seam** is two parties and the artifact that owns the truth between them.
The owner has a **provenance tier**: generated from our running code, such as
`contracts/*.json`; generated by interrogating an installed dependency, such
as Airflow or curl_cffi; or recorded empirically because nothing publishes
anything, which is cars.com alone. The tier picks the instrument and what a
diff means. Ours says fix the service; theirs says they changed, fix our
caller.

Every seam sits on a **maturity ladder**: unowned, censused, compared with
ledgers, standardized by derivation, guarded. The rules at a compared seam
are heuristic-heavy by necessity, because the reader has to recognise every
ad-hoc shape. The move that simplifies is never a cleverer reader. It is
standardizing the seam so the reader becomes a lookup and most of it deletes.
The SQL seam is the existence proof: statements in files, one loader, an
execution recorder, and what survives is a handful of cheap border guards.

[The seam census](seam_rule_census.md) sorted all 83 rule rows into eight
seams and graded them. [The ideal-state spec](seam_ideal_state_spec.md) names
the mechanisms each seam needs, the border guards that survive, and a
retirement map: every scaffolding rule names the ledger whose emptying
retires it, and a new meta-rule fails a scaffolding rule whose ledger has
emptied, so retirement is forced rather than remembered.

Plan 180 builds the mechanisms, lands the ideal rules with ledgers seeded at
everything unconverted, writes the skills against that corpus, restructures
the contract on the seam spine, then drains seam by seam. HTTP first, metrics
second, object storage behind its own design.

### Within a party: Plan 182, what a unit owes itself

The seam frame cannot hold this by definition, and the census said so. What
a module owes its own caller had no section, no grade and no plan. Its
obligations were scattered: the "enough" floor's judgement clause, a global
coverage number that cannot see a module with its parsing layer dead, Stage
Y's honest-answer rules filed under seams, the parsers nobody ran, the
fixtures that fabricate impossible rows, all three remaining judgement rules.

The frame already existed once, for dbt. Stage S derived every model's branch
list from its compiled SQL, exercised every branch in both arms, required
every model to materialise rows with no waiver list, and showed every
constraint load-bearing by mutation or recorded it as decorative. Plan 182 is
that design for Python: **branches exercised, declared outcomes produced,
tests load-bearing**, at module grain with a "not yet converted" ledger, no
percentage anywhere, mutation as the conversion check run by command rather
than in CI, and the judgement rules struck by moving code once the builder
and factory exist. It absorbed the parsers and fixtures stages from Plan 180
because they were never seams.

### The checkers themselves: Plan 181, the rule readers are code

Forty-three percent of `tests/rules/` is not tests. It is AST walkers,
classifiers and resolvers, living where coverage cannot see them, and every
floor in the directory exists because a reader with a dead branch is
invisible to every instrument the repository owns. Plan 181 moves that code
into a package coverage measures, gives it unit tests, and lands a rule that a
module-level function in a test module is a test or a fixture and nothing
else. It is the within-party frame applied to the testing infrastructure, and
it is kept as its own plan because it was already sequenced with its issues
filed when the frame was named.

Its sequencing matters to both others. Plan 180 Stage B writes a border guard
per seam, each a new reader, and where those land is decided by whether
181 Stage A exists. Plan 182 Stage B's reader lands in the same package.

### On top of both: the artifact obligation index

A person adding a route does not ask what the integration layer owes or what
seam 4 holds. They ask what they owe for a route. [The
index](artifact_obligation_index.md) answers that per kind of thing that can
be added: fifteen kinds, each with where it lives, what it must declare, what
evidence discharges it, the rules that catch you, and the skill. Every kind
is the set of rules sharing a corpus function, so once Plan 181 makes those
functions importable the index becomes a derived join rather than a document,
and the pre-flight version, pointed at a diff, prints the obligations before
anything fails.

Two findings from building it drive the plans: fourteen of fifteen kinds have
no skill, and the service kind could only state obligations that cross a
boundary. Any kind's full obligation is its between-party statements plus its
within-party ones, and until Plan 182 the index can state only half.

## What happened to the layers

Plan 161's five layers, numbered by dependency cost, still answer the
question they were built for: what a test needs in order to run, which CI job
runs it, what it may skip. They no longer do anything else. Their CI-ordering
rationale went when Stage E split the jobs and Stage R showed wall clock is
the longest job, not the sum. Stage AG rejected layer as the key for a rule
property, since "which layer this lives at is not a property of the file".
The SQL guarantee is held by a cross-job runtime gate, not by Layer 2.

The conversation that produced this document reached two conclusions about
them, and only one is decided.

**Decided by Plan 180 Stage G:** the contract's spine becomes the seams, and
layers become an attribute on an obligation. A seam statement says what kind
of evidence discharges it: a Layer 4 request, a Layer 2 execution, a runtime
gate. **Seams are what is held; layers are what kind of evidence counts.**
The runtime gates, meaning the execution recorder, the declared-skips hook,
the fidelity plugin, the contract generation job and the replay jobs, are a
sixth evidence kind that none of the five layers describes and should be
named as one.

**Proposed, not yet a stage anywhere:** re-index the layers by what they test
rather than by what they need. Conventions, configuration, correctness, data,
integration. The directory tree already encodes that split, Stage AG made the
first cut of it when it separated `tests/rules/` from the top-level config
tests, and dependency would become a per-directory attribute the skip rule
and CI placement read. The one design constraint is that `tests/rules/` would
span four subjects, so either a rule's subject is read from its table row or
the directory splits by subject, which is a move Plan 181 Stage C is about to
make anyway. Data would need to keep transform-code correctness and
production-data validity apart. "Quality" should be avoided as a name; those
rules are about code being shaped so the other subjects can see it. This is
recorded here so it is not re-derived, and it belongs to Plan 180 Stage G's
interview when that stage starts.

## How the three plans interlock

| This | Wants | Because |
|---|---|---|
| 180 Stage B, ideal rules and ledgers | 181 Stage A, the package | each border guard is a new reader, and where it lands is decided by whether the package exists |
| 182 Stage B, per-module branch coverage | 181 Stage A, the package | its reader lands there and is unit tested there |
| 182 Stage F, the judgement rules struck | 180 Stage A, the double builder; 182 Stage G, the fixture factory | the rules are struck by making their premise unwritable, not by a cleverer checker |
| 180 Stage C, skills against the ideal corpus | 180 Stage M, seam 7 final; the index's kinds | skills map to tasks, and a task is adding a kind |
| 180 Stage G, the contract on the seam spine | the census, the spec, 182 Stage A | the within-party section has to exist to be filed |
| 181 Stage B, reader and rule retire together | 180 Stage B's expiry meta-rule | the meta-rule knows only assertions; a reader left behind is dead code no ledger describes |

Plan 181 Stage A was already recorded in `docs/PLANS.md` as wanting to land
before 180 Stage B. Plan 180's own order table does not say so, and that is
the one sequencing contradiction between the three documents as they stand.

One collision waits at 181 Stage A. Two derivations of "a service" exist:
`service_packages()`, which reads top-level packages with an init file and
drives the "enough" table, coverage sources and the contract rules, and
Stage AL's compose-derived definition, which is what lets `airflow/dags`
count. Plan 181's package will be a top-level package no Compose service
builds, and that is the day they disagree. Reconciling them is that stage's
first work.

## What done looks like

- Every seam at grade 3 or 4, every ledger empty, and the HTTP seam's rules
  down from roughly twenty-four to ten, with every retirement fired by a
  ledger rather than remembered.
- Every production module held to every branch and every declared outcome,
  with the global coverage threshold retired because nothing needs it.
- The judgement rules down from three to at most one, with the residue a
  named number rather than an open class.
- Most floors gone, because the readers they guarded are unit tested.
- Every artifact kind names a skill, and a diff-keyed command prints what a
  change owes before CI runs.
- The contract shorter than it is today, with rationale single-homed in rule
  docstrings and history in plan records.

What does not become mechanical is stated rather than promised: whether an
assertion is meaningful is proxied by mutation, and a suite can be sensitive
to a module for the wrong reason.

## Decisions still owed

- Whether the layers are re-indexed by subject, and if so whether
  `tests/rules/` splits by subject. Plan 180 Stage G's interview.
- Whether the ten planning-document rules and the five README rules are a
  second contract the restructured document points at. The index says point.
- Which plan owns the pre-flight obligations command. It follows from the
  index becoming a derived join, which is Plan 181's package plus one rule.
- Plan 180's order table versus Plan 181's stated precedence.
- `query_constants()` walks into `.claude/worktrees/` and its floor fails on
  any machine with worktrees open. The exclusion tuple the SQL corpus already
  uses is the fix, plus a mutation entry. Found 2026-09-11, not yet made.

## Glossary

**Seam.** Two parties and the artifact that owns the truth between them.
**Tier.** Where the owner artifact comes from: ours, generated from theirs,
or recorded. **Grade.** How far a seam is along unowned, censused, compared,
standardized, guarded. **Border guard.** A rule keyed on one signature of
deviation that survives standardization. **Scaffolding.** A rule that exists
only while a ledger is non-empty, and names the ledger that retires it.
**Ledger.** A shrink-only tuple of the unconverted or unresolved, seeded at
the measured whole; an entry can be "not yet converted" rather than a
violation. **Floor.** The assertion that a rule's reader found anything,
required because a set difference over an empty corpus passes. **Mutation.**
One deliberate defect per rule, in `scripts/verify_testing_contract_mutations.py`,
watched failing. **Evidence kind.** What discharges an obligation: a layer,
by dependency cost, or a runtime gate. **Artifact kind.** A thing a person
adds, keyed by the corpus function whose rules it owes. **Within-party.**
What a unit owes itself. **Between-party.** What a unit owes across a seam.

## Where to read next

In this order, for a session starting cold: this document;
[`docs/TESTING.md`](../TESTING.md) for the standard; [the seam
census](seam_rule_census.md) and [the ideal-state spec](seam_ideal_state_spec.md)
for the between-party frame; [the index](artifact_obligation_index.md) for
what any kind owes; then the three plans,
[180](../plans/plan_180_seam_program.md),
[181](../plans/plan_181_rule_readers_are_code.md) and
[182](../plans/plan_182_what_a_unit_owes_itself.md), each of which keeps its
arguments in its own document and its history in its Record.
[Plan 162](../plans/plan_162_testing_census_and_restructure.md) is where the
stages those plans absorbed were argued, and
[Plan 161](../plans/plan_161_testing_contract.md) is where the nine questions
were first answered.
