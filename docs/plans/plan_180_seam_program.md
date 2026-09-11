# Plan 180: the seam program

## What this plan is for

Gives every boundary in the system — service-to-service HTTP, the database,
the object store, the metrics stack — one declared way across it, with rules
that make silent drift fail loudly and ledgers that measure the conversion
until every ad-hoc path is gone.

## The case

**Plan 162 built the seam-closing machinery one instance at a time, and the
thirty-first stage was the one that finally named the template.** Stage W
closed the database-vocabulary seam; Stage AA and AL closed the HTTP seam to
grade 2 — comparators and shrink-only ledgers holding a measured gap of some
three hundred entries. Along the way the repository wrote the same statement
four times ("a test double derives from the owner artifact of the seam it
stands on" — `test_no_mock_invents_a_shape.py` counts them itself) because
the general form had no home, and grew a contract document of 29,000 words
whose table rows restate what rule docstrings and plan records already say.

**The census sorted all 83 asserted-rule rows into seams and graded them**
([`seam_rule_census.md`](../planning/seam_rule_census.md)): four seams are
already at the ideal — SQL is the existence proof that heavy standardization
leaves only cheap border guards — two need one upgrade each, the HTTP and
mocking seams sit at grade 2 with heuristic readers holding the unconverted
majority, object storage has one governed artifact and an unowned channel,
and metrics sits at grade 0 despite having already cost eight silent hours
of a stale-gauge outage nothing alerted on.

**The case for the program over the status quo is the cost curve of the
heuristics.** A grade-2 seam's readers must recognise every ad-hoc shape,
which is why the HTTP seam alone carries hundreds of lines of AST analysis
that a declaration helper, a generated route registry and a real-response
double builder would delete — the same trade Stage X already made once,
moving 505 SQL literals so a judgement rule could be struck. Comparing
forever means maintaining that analysis forever; converting means each
reader retires the day its ledger empties, having verified every conversion
on the way out. The ideal-state specification
([`seam_ideal_state_spec.md`](../planning/seam_ideal_state_spec.md)) names
the mechanisms, the border guards that survive, the retirement map, and the
one new meta-rule that makes retirement forced rather than remembered.

**Origin, recorded:** split out of Plan 162 on 2026-09-10, from the Stage AL
session that closed the HTTP seam's rule surface and then, asked whether the
new rules made old ones redundant, found the question unanswerable without
the seam frame. It supersedes Stage AL's conversion tail, absorbs Stage AM's
exit shape, reshapes Stage AH (skills written against the ideal corpus
rather than the interim one — three interim drafts were paused for exactly
this reason), and is the intended home for Stages AD, AE and AJ pending a
check of each against the frame. The `docs/TESTING.md` restructure follows
the same spine — one section per seam — and lands here rather than in a
separate effort, because the census is what makes it mechanical.

## Design

**Two axes, one cross-cut, one ladder — the frame is
[`seam_ideal_state_spec.md`](../planning/seam_ideal_state_spec.md) and is not
restated here.** A seam is two parties and an owning artifact; the owner's
provenance tier (generated from ours, generated from theirs, recorded
empirically) picks the instrument and never the bucket; the mocking statement
instantiates per seam; maturity runs census → compared-with-ledgers →
standardized-by-derivation → border-guards-only. The program moves every seam
to grade 3: five thin mechanisms (`refusals()` and the body-kind helpers, a
generated route registry with a typed client and a generated Airflow copy,
`service_double()`, `metric_names.py`, and `lake_layout.py` behind Stage F's
design), ideal border-guard rules whose ledgers seed at everything
unconverted, skills written against that corpus, then per-seam drains.

**One suite, never two.** Ideal rules land beside the current heuristic
readers; the heuristics keep checking the ledgered majority and retire when
their named ledger empties, enforced by a new meta-rule — a scaffolding rule
whose expiry ledger has emptied fails until it is deleted, the
waiver-outlives-its-owner clause applied to rules themselves.

**Rejected:** a parallel second suite (two sources of truth through the
program's longest-lived state); sharpening the heuristic readers instead of
standardizing (the cost curve — Stage X's struck rule is the precedent that
moving the code beats teaching the checker); a big-bang conversion (the
shrink-only ledgers exist precisely so drains are incremental and every
conversion is verified by the scaffolding on its way out).

**Absorbed from Plan 162**, with each stage's argument staying in that
document and only the exit carried here: Stage AL's conversion tail (→ D),
AM (→ F), AH (→ C), AC (→ H), AD (→ J), AE (→ K), AJ (→ L). Their Linear
issues are re-pointed or superseded during 162's close-out, not duplicated.

## Stages

### Stage A: the mechanisms exist

**Issue:** CAR-120 · **State:** `next`

Delivers the five stubs — `refusals()` with the body-kind helpers,
`shared/service_routes.py` generated from `contracts/*.json` and gated like
them, the typed client with its generated `airflow/plugins` copy,
`tests/service_double.py`, `shared/metric_names.py` — each importable, unit
tested, adopted by nothing yet.

**Exit:** every mechanism importable with unit coverage; the registry
committed and diffed by CI; `generate_service_contracts.py --check` exit 0
with the artifact unmoved.

### Stage B: the ideal rules, seeded at everything

**Issue:** CAR-120 · **State:** `—`

The border guards from the spec's tables, each with floor, mutation,
`Asserted by` row and shrink-only ledger seeded at the measured whole; the
expiry meta-rule enforcing the retirement map.

**Exit:** every ideal guard green with its ledger seeded and the seeds
recorded in §Record as the change census; the expiry meta-rule fails a
scaffolding rule whose named ledger is empty; demonstrated by one mutation
per guard.

### Stage M: the config field carries named parts *(from 162 Stage AK)*

**Issue:** CAR-126 · **State:** `—`

**Seam 7's pattern asserts the pair and says nothing about the fields the
pair asserts on**, which is the whole of what is wrong with it. Three
artifacts pass through one parser — `load_health_exemptions` reads
`healthcheck-exemptions.txt`, `maintenance-running-set.txt` and
`deploy-followers.txt` — and each treats the same reason field differently:
`len(reason) > 40` in the first two, the identical constant written twice in
two files, and no reason check at all in the third. Across the suite the
idiom is six instances in five files with three constants, every one outside
`tests/rules/` and so invisible to the ledger that exists to forbid it.

**`deploy-followers.txt` already contains the answer, applied once and never
declared.** It asserts `"docker restart" in entry` — a required *part*,
not a length — with the reason recorded on the assertion: *"a warning
without a command is how this stayed unfixed for two days."* Generalising
that to the artifact and declaring it for the seam is this stage.

**Why it precedes C rather than following G.** G files sections for rules
that already exist; this stage produces rules, like A and B. And C writes
skills against the ideal corpus, so seam 7's ideal has to be final before C
runs — the sequencing that paused three interim drafts already.

**Exit:** the tightened pattern from
[`seam_ideal_state_spec.md`](../planning/seam_ideal_state_spec.md) §Seam 7
carried into the contract; the reason field of the three allowlist artifacts
carries named parts with the readers asserting on the parts; the six
prose-length bounds drained; `deploy-followers.txt`'s ad-hoc `"docker
restart"` check retired into the general form. Demonstrated by an entry that
loses a required part failing.

### Stage C: skills against the ideal corpus *(ex-162 Stage AH)*

**Issue:** CAR-120 · **State:** `—`

**Exit** (AH's, carried): every rule in `docs/TESTING.md`'s `Asserted by`
column names a skill that teaches meeting it before CI does; the three
paused interim drafts replaced by skills teaching the mechanisms.

### Stage G: the contract restructured on the seam spine

**Issue:** CAR-120 · **State:** `—`

One section per seam — parties, owner + tier, statements, rules per
statement with keep/expiry marked, ledgers, grade, skill — with rationale
single-homed in rule docstrings and history in plan Records.

**Exit:** the structure parsers and membership rules green against the
restructured document; every `Asserted by` cell reduced to statement plus
test names; the planning-document rows filed under their own contract or an
explicit pointer.

### Stage D: the HTTP drain *(ex-162 Stage AL's tail)*

**Issue:** CAR-121 · **State:** `—`

**Exit:** the HTTP seam's ledgers — unreadable exits, retyped statuses,
unproven meanings, bodyless responses, fabricated objects, hand-written
callers, uncalled endpoints, cross-imports, `Busy`-undeclared, `/admin 307`
— empty; scaffolding retired by the expiry rule; the `/ready`-as-`Busy` and
29-meaning repairs and `/admin` 308 **verified in production**; contracts
regenerated throughout.

### Stage E: the metrics seam *(production-verified)*

**Issue:** CAR-122 · **State:** `—`

**Exit:** every `cartracker_*` name declared once in `metric_names.py`;
declared == emitted == queried in both directions; the ledger drained; the
comparison **verified against the deployed Prometheus and Grafana**, not
only the config files.

### Stage H: the vocabulary columns become enums *(ex-162 Stage AC)*

**Issue:** CAR-123 · **State:** `—`

**Exit** (AC's, carried): the 18 constrained columns are enum-typed; Stage
W's corpus reader reads `CREATE TYPE … AS ENUM`; a stale literal in a
`.sql` file or a dbt model fails; demonstrated by the `retry_later`
mutation now failing where it passed — **with the migration verified
applied in production**.

### Stage J: a fixture cannot fabricate a forbidden row *(ex-162 Stage AD)*

**Issue:** CAR-124 · **State:** `canceled`

**Superseded 2026-09-11 by [Plan 182](plan_182_what_a_unit_owes_itself.md) Stage G (CAR-131).** A fixture that fabricates a forbidden row is a within-party obligation, not a seam; it sat here only because it fell out of Plan 162's tail while that plan was being split.

**Exit** (AD's, carried): a unit fixture cannot carry a value the owning
column forbids; `FABRICATED_ROW_WAIVERS` seeded at its measured count and
drained to 0; demonstrated by a fabricated row failing at construction.

### Stage K: configuration is what Compose delivers *(ex-162 Stage AE)*

**Issue:** CAR-124 · **State:** `—`

**Exit** (AE's, carried): no production module reads an environment
variable with an inline default; every variable read strictly is delivered
by its service's Compose block; each of the 39 moved to Compose or reduced
to a constant, reads made strict in the same change; the waiver tuple
drained and deleted; the twelve that moved verified by asking a deployed
container what it loaded. Demonstrated by an inline default failing.

### Stage L: the parsers nobody ran *(ex-162 Stage AJ)*

**Issue:** CAR-124 · **State:** `canceled`

**Superseded 2026-09-11 by [Plan 182](plan_182_what_a_unit_owes_itself.md) Stage H (CAR-131).** A parser no test executes is the first within-party drain target, and the plan that holds the frame is where its ledger belongs.

**Exit** (AJ's, carried): every production function that parses a response
from a system this repository does not own is exercised by a test or
waived with its reason; the ledger seeded from a re-measurement at this
stage's start and drained to 0; demonstrated by a parser whose only
exercising test is removed failing.

### Stage F: the object-storage channel *(ex-162 Stage AM)*

**Issue:** CAR-125 · **State:** `—`

Designs what AM's stub left open, toward the spec's stated invariant.

**Exit:** every address computed from one declaration (`lake_layout.py`);
every exchange through the store named with its writer, reader and
artifact; the bronze three-party exchange compared through an artifact all
three parties answer to; the S3 code vocabulary filed as this seam's
tier-2 member.

## The order

| # | Stage | Est. | Production-gated exit |
|---|---|---|---|
| 1 | A | 2 | no |
| 2 | B | 2 | no |
| 3 | M | 1 | no |
| 4 | C | 2 | no |
| 5 | G | 2 | no |
| 6 | D | 2 | **yes** |
| 7 | E | 2 | **yes** |
| 8 | H | 2 | **yes** |
| 9 | J | 2 | no |
| 10 | K | 2 | no |
| 11 | L | 2 | no |
| 12 | F | 2 | no |
