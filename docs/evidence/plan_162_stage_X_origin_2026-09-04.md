# Plan 162 Stage X — how the stage was arrived at, 2026-09-04

Nothing here is a production measurement. This records the reasoning that
produced [Stage X](../plans/plan_162_testing_census_and_restructure.md#stage-x-a-test-may-not-author-sql-either),
including the two positions argued and abandoned on the way, because the stage
was not found by an incident or a census sweep and would otherwise have no
provenance. The prior-art readings below were taken by web search on 2026-09-04
and are the first external citations in this repository's planning documents.

## The origin was a publishing question, not an engineering one

The session opened by asking whether the planning system and the testing
contract were worth writing about publicly, and whether a reader would find them
obvious. That is a question about how the work sits against the field, and
answering it required a comparison against outside practice that had never been
made.

**The comparison found that most of the corpus restates established practice.**
*(Too strong, and narrowed on 2026-09-06 — see [the
correction](#correction-2026-09-06-the-comparison-conceded-more-than-it-measured).
The storage convention is well-trodden; the enforcement is not.)*
Executable architecture rules are *fitness functions*, with a published book and
libraries (ArchUnit, import-linter). A waiver list that only shrinks is a
ratcheting baseline (`betterer`, SonarQube, mypy). SQL in files is
sqlc/jOOQ/MyBatis/aiosql. Testing SQL against a real engine is Testcontainers.
Opaque stage identifiers are surrogate keys. None of that makes the work wrong,
and several pieces are stronger than their off-the-shelf equivalents — the three
self-assertions on the waiver list are the clearest case, since most baseline
files rot silently and this one fails on a stale entry.

## The prior art, named here because it is named nowhere else

| Tool | What it is | Status against this repository |
|---|---|---|
| [aiosql](https://nackjicholson.github.io/aiosql/) | SQL in `.sql` files, named by `-- name:` comment, loaded as Python methods. PEP 249 and asyncio drivers, PostgreSQL **and** DuckDB | ~~Substantially `shared.query_loader` plus the `queries.py` exposure layer, as a maintained library~~ — **overstated; see the correction below.** It is the loader half only |
| [testcontainers-python](https://github.com/testcontainers/testcontainers-python) | Real service containers per test. v4.14.2, pytest-integrated, [PostgreSQL module](https://testcontainers.com/modules/postgresql/) | ~~Directly addresses Layer 2's premise and [Stage Q](../plans/plan_162_testing_census_and_restructure.md#stage-q-cis-services-are-productions-in-definition-and-in-contents)'s problem statement~~ — **wrong about Stage Q; see the correction below** |
| sqlc | Generates typed code from SQL, validated against a real schema at build time | Go-first; Python support less mature. A maybe |
| sqlfluff | A SQL linter | The honest answer to "is there a linter", which flake8 is not |
| pgTAP | Unit tests written in SQL, run inside Postgres | Unexamined |

**jOOQ, MyBatis and Yesql are other-language precedent, not options**, and are
recorded that way rather than padding the list. aiosql documents itself as
"YeSQL for Python", which is where that lineage lands.

### Correction, 2026-09-06: the comparison conceded more than it measured

**Two rows above are wrong and the headline is too strong.** They are struck
rather than rewritten, because this document's whole subject is a question
answered from recall instead of retrieval, and quietly correcting the answer
would repeat that in the other direction. Stage X has since been built, which
is what makes the gap visible: the comparison was made before the thing it was
comparing against existed.

**aiosql is the loader half and nothing above it.** Four things Stage X rests on
have no equivalent in it:

- **Provenance.** `SqlText` carries the set of files a statement was composed
  from and preserves it through `.format()`, unioning in the origins of any
  nested statement. A library that returns `str` cannot do this, and without it
  the recorder attributes nothing — this is the property that turned "fourteen
  selectors never execute" into "fourteen selectors run nested inside
  `wrap_candidate_query.sql`".
- **The execution record.** Merging twelve per-job artifacts across six CI jobs
  to answer *did this file's text reach a database client in this run*. That is
  a coverage instrument that happens to need a loader, not a loader feature.
- **Polarity.** aiosql is opt-in convenience: it cannot report that someone
  typed a statement inline, it simply does not load that one.
  `test_no_production_module_holds_a_sql_statement` fails the build. A library
  that is used and a rule that cannot be evaded are different mechanisms.
- **The both-ways assertions.** A waiver that no longer describes a violation
  fails; a declared execution route nothing used fails as stale.

The overlap is real and shallow: file-per-statement, and a function that reads
it. That is worth knowing and is not what this plan is about.

**Testcontainers does not address Stage Q, and would make it worse.** Q's
problem is not that CI lacks real services — it has them, and Layer 2 already
runs against a Flyway-migrated Postgres, DuckDB and MinIO. Q's problem is that
CI's service *definitions* have drifted from production's: four copies of
`postgres:16` omitting Compose's `shared_buffers`, `max_connections` and
`shm_size`, three of MinIO omitting the OIDC and Prometheus configuration, four
hand-maintained Flyway argument lists. Testcontainers moves that definition into
Python, making it a **fifth** transcription of what production runs. Q's answer
is the opposite move: a `docker-compose.ci.yml` override derived from the file
production actually uses, so there is one definition rather than five. The row
above named the right stage for the wrong reason.

**What the headline should have said.** Not *"most of the corpus restates
established practice"* but: **the storage convention is well-trodden and the
enforcement is not.** Fitness functions, ratcheting baselines and surrogate keys
are all genuine prior art and that half of the original reading stands. What has
no off-the-shelf equivalent is the combination this plan is actually made of —
a statement that carries its own origin, a record of what executed, and rules
that fail rather than assist. The three self-assertions on the waiver list were
already named above as stronger than their equivalents; that observation was
right and was too small.

**The rule this document states still holds, and now cuts both ways.** *"Does
this already exist"* is a retrieval question. So is *"and does it do what we
need"* — a tool found by search still has to be read before its coverage is
conceded, and this table conceded on the strength of a product page.

## The gap that made the comparison necessary

**Observed.** Five plan documents were read in full this session — 146, 161,
162, 172 and the decision log. Every citation in them is internal: a plan
number, a commit sha, a PR, or a tool already in the tree. There is no reference
to anything outside the project in any of them.

**Maintainer's account, corroborated by the above.** The question *"is this
solved, are there off-the-shelf options"* was asked directly and more than once
during the SQL testing work, and was answered with professional conventions and
a linter — flake8, which lints Python and has nothing to say about SQL. None of
the table above was surfaced.

**The rule that follows, recorded because it is cheap and was not being
applied:** *"does this already exist"* is a retrieval question and must be
answered by search, not recall. Answering it from memory mid-build is the
failure mode, and it is worse than volunteering nothing, because the question
was asked and came back answered.

## The distinction that resolved it, and is Stage X's whole basis

The prior art above is **affordance**. aiosql is a way to load SQL from a file;
it forbids nothing, and `cur.execute("SELECT ...")` on the next line stays green.
Testcontainers supplies a real engine; it does not report which statements ever
reached one. Adopting both leaves G14's 54-of-76 finding entirely intact.

**What this repository built is a negative property**, and no library supplies a
negative property:

> There is no way to write SQL in this repository that does not execute against
> a real engine in a test. Attempting it turns the suite red.

Stated as *"SQL should live in files"* the claim is answered by five libraries.
Stated as *"SQL not in a file cannot be green"* it is not answered by any of
them — and the second is what `docs/TESTING.md` actually asserts. The first
form is [`ARCHITECTURE.md:179`](../ARCHITECTURE.md); the second is the contract.
Restating one as the other is the failure [Plan
161](../plans/plan_161_testing_contract.md) exists to end, and it happened three
times in this session before the maintainer corrected it.

**Enforcement is achievable because the check has no environment.**
`tests/test_testing_contract.py` is Layer 0 — 15 assertions, ~4s, no
dependencies. A rule enforced at Layer 4 only bites when Docker is up and can
therefore always be not-run. A rule with no environment cannot decline, which is
why the property belongs to the repository rather than to a CI job.

## The consequence: an exemption whose premise had expired

If the property is negative and total, `tests/` is a hole in it.
[Plan 161 question 3](../plans/plan_161_testing_contract.md#3-what-must-never-be-mocked)
exempted test SQL for one stated reason — fixture seeds are SQL in test files
too, and a checker that cannot tell a seed from a paraphrase fails on correct
code. That reasoning is sound and load-bearing for the whole judgement/mechanical
split.

**It stops applying if no SQL literal appears under `tests/` at all.** The
ambiguity has nothing left to live in, and the rule becomes static. That is why
Stage X removes a judgement rule rather than adding a mechanical one, taking
the contract's split from 7/4 to 8/3.

[Stage W](../plans/plan_162_testing_census_and_restructure.md#stage-w-a-test-may-not-supply-both-halves-of-a-contract)
was already circling this. It names the class — production authors both halves
of a contract and a test restates one — and cites the `.sql` convention as
having solved that class for statements. It solved it for *production*
statements, which is the hole.

## Two positions argued and abandoned

**That it should be a new plan (proposed number 174).** Argued on two grounds,
both wrong.

The first was that Plan 162's non-goal — *"Deciding the standard. That was Plan
161, and it is archived"* — put a contract change outside the plan. Misread. The
paired clause is *"rewriting the contract to match the repository"*, whose
example is arguing about which mock library is correct; it forbids **weakening a
rule to fit the code**. Stage X strengthens a rule because an exemption's
premise turned out false, which is the opposite direction, and the same clause
names the required form — an explicit decision in `docs/TESTING.md` with the
reasoning recorded.

The second was that seeding ~96–160 new waivers makes success criterion 1 ("the
waiver list is empty") unreachable. Answered before it was raised: Stage L took
its own column from 56 to 66 + 23 across two new rules, and the plan's own words
are that *"a stage discovering more than it was scoped for is the instrument
working, not the arithmetic failing."* The list has already grown mid-plan and
drained to 37.

**The maintainer's counter, which is the correct reading:** Plan 162 owns the
census, so it has to be allowed to grow, and Plan 161 was written blind and
adapts to what implementing 162 discovers. The record supports it. G14 did not
exist when Plan 161 answered its nine questions — it was created by mechanising
what 161 had measured by eye (G6 4→12, G4 20→34, G14 unmeasured→54 of 76, later
56). And Plan 161's *"The question this document did not ask"* section was added
on 2026-09-01 **from Plan 162 Stage L, after Plan 161 had archived**. The
contract has been following the census throughout.

**That the stage would be numbered 15.** It was proposed as 15, then 16, from a
checkout two commits behind `origin/master`. Stage W had landed the same day in
`71f6b1d` / PR #361. Minor, and recorded because it is an instance of the class
this plan is about: reasoning from a record that had moved, with nothing in the
reading able to notice. The fix was `git fetch` before proposing a number, which
is the same shape as every derived-rather-than-listed rule this plan has built.

## What was decided

- **Stage X**, in Plan 162, appending rather than inserting, so a plain integer
  needs no letter.
- **Stage T scoped to its Python half** — 55 module-local seed helpers, `_seed`
  in three archiver modules, `_make_tar_zst` in three script modules. Its *"these
  must not become `.sql` files"* paragraph is struck rather than deleted: the
  mechanism it names is correct and only the conclusion was wrong, since a
  separate root under `tests/sql/` answers the circularity without keeping the
  exemption. Its 96 `INSERT`s and 161 read-backs become Stage X's denominator.
- **The ordering collision resolved by scoping rather than renumbering.** Stage
  numbers carry order in this plan, so 16 runs after 12 and would otherwise build
  shared helpers that 16 converts to files — the Stage F/G collision again.
- **Stage W gains a pointer** at its precedent sentence. Its own repair is
  unaffected and does not wait.
- **`PREPARE` recorded as the likely instrument**, with its limits stated: it
  parses and plans against a Flyway-migrated catalogue with no rows written, but
  covers no DDL and catches no constraint violation. Its value is that it
  schema-checks every test statement whether or not the test consuming it runs,
  where today a seed is checked only if its own test executes.

## What is still open

- Whether anything off the shelf enforces the negative property. Not searched.
  The instinct is that it is a custom rule or a Semgrep pattern rather than a
  library, and that instinct is a recall, which is the thing this document says
  not to trust.
- Test DDL — temp tables and scaffolding created inside tests — has no position.
- Whether `tests/sql/` wants one root or a split mirroring the test layers.
