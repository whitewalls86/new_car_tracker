# Plan 162 Stage M — the assertionless suite and the scraper's write path

**Legacy:** Stage 8 · **Issue:** CAR-52 · **Closed:** 2026-09-02

The record entry this belongs to is [`plan_162` §Record](../plans/plan_162_testing_census_and_restructure.md#record), under Stage M. It carries the summary; the sections below are the detail.

---

#### The rule found four violations no reading of the suite would have

G7 was written as *"the only Layer 2 suite with none"*, and that was true and
also not the whole denominator. `test_dashboard_queries.py` had 25 assertionless
tests; the rule the stage owed found **four more**, one each in
`test_airflow_dag_queries.py` and `test_archiver_queries.py` and two in
`test_ops_queries.py` — every one of them sitting directly above a *sibling*
that asserts, with a comment explaining why. A suite with zero assertions is
visible to anyone who opens it. Four tests inside suites averaging sixty
assertions are not, and that is the entire argument for a derived rule over a
read-through. This is the plan's own recurring lesson landing on the plan:
**a denominator scoped to what exists when it is written will be wrong.**

The rule checks that an assertion **exists**, never that it is meaningful.
`assert True` passes it. That distinction is in the contract's row on purpose,
because *"whether an assertion is meaningful"* is one of the four judgements
`docs/TESTING.md` says are not mechanically checkable, and a rule that implied
otherwise would be the instrument problem this plan keeps naming.

#### Writing the contract down found five dead columns, not three

[The narrowing](../plans/plan_162_testing_census_and_restructure.md#stage-m-narrowed-and-g7-now-names-a-different-gap) predicted
that naming the columns each page reads would surface the ones it does not, and
put the figure at three, all in `data_health_block_rate.sql`. The measured
answer is **five**: those three — `total_block_events`, `max_attempts_seen`, and
the SQL's own `block_rate_pct`, which `data_health.py` discards and recomputes
in pandas because percentages do not average — plus
`data_health_scrape_volume.sql`'s `unique_listings` and `vin_extraction_pct`.

**Both files are on the same page, and the reason is structural.** Data Health
is the one page that renders no frame wholesale: everywhere else
`st.dataframe(df)` displays every column it is handed, so a column that no line
of Python names is still shown to a person. A first pass that counted
never-named columns reported 22 and was wrong about 17 of them for exactly that
reason. Recorded rather than deleted; Plan 150 Stage 0c owns the modeling half.

#### G8 was not the file count

The row said *"one integration file, and still the whole of the service's
coverage above Layer 1"*, which framed it as a quantity. Reading the service
settled that the quantity was a symptom. `tests/scraper/conftest.py` patches
`shared.db.get_conn` and `shared.minio.write_html` **autouse, for every test in
the directory**, and the route rule attributes a test by its directory — so all
eight routes counted as reached while the half of the service that *writes* had
never executed in any layer: the MinIO object, the `ops.artifacts_queue` row,
its `staging.artifacts_queue_events` twin, and the blocked-cooldown pair on a
403. The single integration file, meanwhile, executed SQL constants against a
cursor and touched no route at all — Layer 2's shape in a Layer 4 directory,
the same misfiling G9 was.

Both fetch paths now run against a loopback origin serving real captures, to
real MinIO and real Postgres, with nothing mocked: **12 tests in 2.38s**.

**The two paths needed different work, and the asymmetry is the finding.**
`scrape_detail` takes `payload.url` from its caller, so Layer 4 could point it
anywhere and no production change was required. `scrape_results` *composes* its
URL from a module constant, so it needed one — `SCRAPER_RESULTS_BASE_URL`,
unset in production. A service is testable to the exact extent that its inputs
are inputs.

#### The pacing seam is keyed to the origin, and the direction was the decision

`human_delay` sleeps 13–35s before page 1, up to ~80s when its 10% distraction
branch fires. No CI job can absorb that, so it needed a seam, and the seam's
failure mode mattered more than its shape. A misconfigured `FLARESOLVERR_URL`
breaks loudly — no solver, 403s, cooldowns, alerts. A misconfigured *delay*
switch breaks **silently**: scraping works perfectly, at machine speed, until
the site notices, which is the 2026-08-14 shape where 22 days of apparent health
preceded a 0% solve rate.

So pacing keys off the origin already made configurable rather than taking a
switch of its own, and **the list names the origins that are exempt rather than
the real sites**. An allowlist of paced origins inverts the failure: add a second
scrape target, forget the list, and it scrapes un-paced. Written this way a new
target is paced from its first request and only a loopback double is declared.
`.env.example` documents the variable and says to leave it unset, because unset
is the only value that cannot be wrong.

#### The fixture had to be page 1, and the code was right

The first capture pulled from production MinIO was **page 7 of 236**. Against
it, `scrape_results` fetched, refused to save, and returned zero artifacts —
because `_fetch_page` sets `_break_no_save` when the page's own
`result_page_number` disagrees with the page requested, cars.com clamping a
request being duplicate territory. Nothing was wrong with the code and the
failure looked nothing like its cause. Replaced with a page-1 capture (24
listings, page 1 of 19); the conftest records the trap so the next replacement
does not repeat it.

#### The mutation harness had been aborting for two stages

`scripts/verify_testing_contract_mutations.py` was anchored on the `| G6 |` row
that **Stage H deleted**, so `_edit`'s staleness guard raised and every mutation
after it stopped running — 17 of 24, unnoticed, across Stages H, J, K and L,
including guards for rules those stages shipped. This is precisely what Stage B
hit on G1 and G2, and it recurred within a fortnight, which says the anchor
convention is not enough on its own: a mutation anchored on a gap row is
anchored on the thing the plan is trying to delete. Three anchors were moved to
live gaps and the full run is now **24 mutations, all caught**.

#### Two production changes, and the deploy that carried them

`scraper/app.py` imported `from db import close_pool, get_pool`, which resolved
only because the Dockerfile ran `cp scraper/db.py db.py` — so the module existed
twice under two names, with a `_pool` global in each, and `import scraper.app`
failed outright outside the conftest that put `scraper/` on `sys.path`. That is
[G18](../TESTING.md#the-gap-list)'s dual-identity defect, in a second service,
papered over rather than absent. Both removed; the app imports `scraper.db` like
every other module in its package.

**This needed a scraper image rebuild**, which the section below records.

#### Deployed 2026-09-02, and confirmed

Merged as `ff690e0`; the VM pulled it and `scripts/redeploy.sh scraper` rebuilt
the image. Coordination drain confirmed in 1s, container recreated, healthy
after 5s.

**What the container loaded, asked of the container rather than the checkout.**
A `git pull` is not a deploy and a healthy container is not evidence the new
code is running, so all four were read out of the running process:

| Check | Result |
|---|---|
| `/app/db.py`, the `cp` layer | gone — `No such file or directory` |
| `app.py:15` | `from scraper.db import close_pool, get_pool` |
| `app.get_pool.__module__` | `scraper.db` |
| bare `db` in `sys.modules` | `False` |

**The pacing seam was checked in production, because it is the change that
fails silently.** In the running container `BASE_URL` is
`https://www.cars.com/shopping/results/`, `SCRAPER_RESULTS_BASE_URL` is unset,
and **`PACING_APPLIES` is `True`** — so `_pace()` calls `time.sleep` exactly as
the code it replaced did. That is the whole risk of keying pacing to the origin,
answered against production rather than argued.

**The detail path is confirmed.** In the 25 minutes after the deploy: 400 rows
in `ops.artifacts_queue`, 3,780 rows in `staging.artifacts_queue_events`, and a
newest MinIO object at 18:45:20 — against a last pre-deploy artifact of
18:15:47.

**The SRP path is confirmed, two rotation slots later.** `search_configs`
rotates on a **four-hour** cycle at `:30`, so the `*/30` DAG mostly
short-circuits on `advance_rotation` returning `configs=[]` — its runs take
10–19s when nothing is due. The two slots that were due ran under the new code:
**21:30 UTC for 521s producing 45 objects**, and **01:30 for 463s producing
26**. The three newest parse to 24, 24 and 8 listings at pages 22/26, 16/26 and
26/26, no challenge pages — a complete paginated walk to the last page.

**The pacing is visible in the artifacts themselves**, which is better evidence
than the config read this section opened with. Those three objects are stamped
`01:36:57`, `01:37:15` and `01:37:32` — **18 and 17 seconds apart**, inside
`human_delay`'s 13–35s band. The gaps between stored objects *are* the sleeps,
so `_pace()` is demonstrably still sleeping against cars.com. Keying pacing to
the origin was the change with the silent failure mode, and this is that
question answered from production rather than argued.

**`ops.artifacts_queue` cannot evidence the SRP path at all**, which is worth
recording because it misleads on first reading: the table holds **zero**
`results_page` rows *all time*, since they are consumed and deleted downstream.
Only MinIO object timestamps answer the question.

#### Cost

Estimate 2 points, actual **1**. `In Progress` ran 16:34 to 18:20 UTC on
2026-09-02 — 1h45m, ending at the merge — across 22 files.

**The first draft of this line said the stage "ran well past" its estimate, and
that was wrong in a way worth keeping.** It reasoned from how many things turned
up rather than from what they cost: the mutation-harness repair, the production
import fix, two `scrape_results` seams and a capture pulled from production
MinIO all sound like overrun and were minutes each — two lines, fifteen lines,
three SSH round-trips. Scope surprise is not effort, and this plan has spent
nine stages arguing that an unmeasured claim is worth less than a measured one
whichever direction it points. [Stage L](../plans/plan_162_testing_census_and_restructure.md#stage-l--sql-execution-from-both-directions)
is the calibration: estimate 2, actual 1, and it was the plan's largest stage by
file count at 132 files gaining tests.

What genuinely cost time was not the building but the *reading* — settling that
G8 was a mocked-write defect rather than a file count, and that the SRP path
composes its URL where the detail path receives one. Both were decided before
any test was written, and both changed what got built.
