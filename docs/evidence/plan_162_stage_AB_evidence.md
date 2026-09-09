# Plan 162 Stage AB — evidence

**Issue:** CAR-106 · **Measured and demonstrated:** 2026-09-09

Everything bulky about Stage AB: the census as measured rather than inherited,
the four numbers this stage's own scoping got wrong, the production recording
and what it cost to take, **the three live defects the recording exposed once
it was read against the code**, the fourteen mutations watched failing, and
the two findings that are not vocabularies at all.

---

## 1. The census, measured

Ownership is Stage AA's compose-derived split, consumed rather than re-derived:
*ours* is a service with `build.dockerfile: <pkg>/Dockerfile` **and** a
`contracts/<pkg>.json`; *third-party infra* is a compose service with an
`image:` and no `build:`; *external* is not a compose host at all. AA keeps the
first bucket, this stage keeps the other two.

Read off `docker-compose.yml`, `docker-compose.lakehouse.yml` and
`docker-compose.mlflow.yml` on 2026-09-09 at `4711fb5`: six packages build and
have a contract (`ops`, `scraper`, `dbt_runner`, `archiver`, `processing`,
`container_health`), which is exactly the six files in `contracts/`.

**A fourth bucket exists and the split as given does not name it.** `dashboard`,
`dbt`, `dbt_test`, the five `airflow-*` services, `lakehouse-worker` and
`mlflow` all carry a `build:` and have no contract. `dashboard` is already
accounted for — it is Streamlit, serves no schema, and owes the "enough" table
as G7 — but the others are not, and they are recorded here rather than silently
sorted into a bucket they do not fit.

**14 vocabularies: 9 replayed, 5 declared out of scope.** The register is
[`tests/external_vocabulary_census.py`](../../tests/external_vocabulary_census.py)
and it is the population; this table is a reading of it, not a second copy.
**Both of those numbers were 10 and 4 in the first draft of this sentence**,
while the table below it was already right -- caught by importing the tuple
and counting. That is the fourth instance of this plan's own recurring
defect happening inside this plan, and it is left on the record for the
reason the others were: read the tuples, do not read the paragraph.

| Owner | Bucket | Verdict | Where the replay runs |
|---|---|---|---|
| cars.com — HTTP statuses | external | replayed | `unit-tests`, against a corpus recorded from production |
| cars.com/Cloudflare — interstitial titles | external | replayed | `unit-tests`, against a captured interstitial |
| Airflow — `TaskInstanceState` | infra | replayed | the isolated `apache-airflow==3.2.0` venv |
| Airflow — `TriggerRule` | infra | replayed | already, by the real `DagBag` build |
| curl_cffi — `BrowserType` | external | replayed | `unit-tests` |
| MinIO / S3 — not-found codes | infra | replayed | the shared integration step's MinIO |
| Docker Engine API | infra | replayed | already, `verify_container_health_docker_contract.py` |
| Promtail — pipeline semantics | infra | replayed | already, `verify_promtail_contract.py` |
| FlareSolverr — `/v1` solve body | infra | replayed | `flaresolverr-contract`, against the pinned image |
| api.telegram.org | external | out of scope | replaying it means sending a message |
| Lakekeeper | infra | out of scope | nothing in production reaches it |
| Prometheus / Loki — query envelope | infra | out of scope | **see §5 — a different defect** |
| git — config keys | external | out of scope | a stale one fails loudly |
| markdown-it-py — token types | external | out of scope | **see §4 — right verdict, wrong reason** |

---

## 2. Four numbers this stage inherited, and what they actually are

Every one of them moved, which is this plan's own recurring finding happening
to this plan again. The method is stated so the next reading can disagree with
this one on the same terms.

**"9 fabricated `cars.com` responses" counted only the literal 403s.** Reading
`<mock>.status_code = <int>` out of the three cars.com test modules with `ast`,
and resolving the three `_mock_http(..., status=...)` helpers through their
defaults and call sites:

| | Sites |
|---|---|
| `tests/scraper/conftest.py` | 1 |
| `tests/scraper/processors/test_scrape_detail.py` | 20 |
| `tests/scraper/processors/test_scrape_results.py` | 7 |
| **Total** | **28** |

**But the count of sites is the less interesting number.** Those 28 sites carry
**two** distinct statuses — 16 × `200`, 9 × `403`, and 3 parametrised, every one
of which resolves to `200`. Production has returned **seven**.

**The scope was one system too narrow.** `curl_cffi` is a third instance nobody
had named: `scraper/processors/cf_session.py` restates 16 impersonation targets.
All 16 were correct on 2026-09-09 and the real set is exactly those 16, so this
is a hole rather than a defect — but it is the *silent* kind, and in the
direction nobody would look. `cffi_target_for_ua` falls back to the nearest
*lower* entry, so a target curl_cffi drops raises at the session; a target
curl_cffi **adds** and the list does not simply never gets picked, and the
scraper keeps impersonating an older Chrome while presenting the newer
user-agent FlareSolverr reported. That is a TLS-fingerprint mismatch, which is
the shape of the 2026-08-14 outage.

**And the suite could not have caught it, for a reason that is this stage's own
subject matter.** `tests/scraper/conftest.py` puts a `MagicMock` into
`sys.modules["curl_cffi"]` at module scope, commenting that the package is
"only present inside Docker". That is no longer true — `curl_cffi` is line 4 of
`scraper/requirements.txt`, which the `unit-tests` job installs. The stub is
never torn down, so from the moment pytest collects `tests/scraper/` every
later test in the process sees the mock, and **a mock iterates as empty**: the
first version of the curl_cffi rule passed alone and failed in the full suite,
reporting that curl_cffi had none of its own targets. `_real_browser_types()`
lifts the stub, reads the package, asserts what it got is not empty, and puts
the stub back — and that emptiness assertion is the load-bearing line.

---

## 3. The production recording, and the asymmetry it forces

**A live fetch from CI is not available at any price**, so the corpus is
recorded from the only vantage point that can reach cars.com. `_fetch_url` is
behind Cloudflare and the 2026-08-14 outage established that even a bare
request from the known-good production IP comes back 1020 — a TLS fingerprint
artifact, not an IP ban, and not something a GitHub runner does better at.

**Postgres was the wrong place to look and the search is worth recording.**
`public.raw_artifacts.http_status` exists in `V001` and is the obvious
candidate; it does not exist in production. Nothing in the live schema carries
an HTTP status at all — checked by asking `information_schema.columns` for the
column name across every schema, which returned zero rows. The status reaches
the artifact dict, the DAG log and a Prometheus label, and stops there.

**So two instruments, and they disagree by construction.** Loki holds 90 days
and logs a status; Prometheus holds 30 and counts an outcome.
`record_detail_fetch` buckets everything that is not 200 or 403 into `error`,
so Prometheus alone cannot name a member — it could only say that 30 fetches in
30 days were *something else*. Loki is what resolved that into five statuses.

`scripts/record_cars_com_status_corpus.py --record`, 2026-09-09:

| Status | detail_page | results_page | Instrument |
|---|---|---|---|
| 200 | 584,995 (30d) | 82,144 | Prometheus / Loki |
| 302 | 1,172 | — | Loki |
| 403 | 30,981 | — | Loki |
| 500 | 36 | — | Loki |
| 502 | 271 | 4 | Loki |
| 503 | 1,604 | — | Loki |
| 504 | 12 | 4 | Loki |

**Counts are per instrument and deliberately not reconciled.** Loki counts log
lines and one fetch logs twice — `_fetch_url` warns and `scrape_detail_fetch`
warns again — so its 403 count is roughly double Prometheus's. Reconciling two
instruments that disagree by construction would be inventing a third number
nobody measured. **Only membership is asserted.**

**Loki's default `max_query_length` is 721h**, so a single `[90d]` range
selector is refused by dropping the connection rather than returning an error
body — the first pass reported `RemoteDisconnected` for every query and looked
like a network fault. The recorder walks 13 weekly windows instead.

**The first pass also missed every results-page status**, because the two
scrapers log differently: `detail fetch HTTP <n>` against `page <n>:
search_key=... status=<n>`. A query written for one silently returns nothing
for the other, which is the same shape as every other finding in this plan.

**The asymmetry this forces is the one thing Stage AB does not share with the
pattern it copies.** `verify_promtail_contract.py` and
`verify_container_health_docker_contract.py` both record *and* verify in CI,
because the real thing is an image CI can start. Here the recording runs off
the runner and the verifying runs on it. Same split — one corpus, two
consumers, neither importing the other — with the recorder moved because the
runner cannot see the subject.

**The five statuses nothing exercised, and what they turned out to cost.**
302, 500, 502, 503 and 504 are answers cars.com demonstrably gives — 3,103 of
them across the window — and when this stage opened, no test produced any of
them and no branch decided about any of them. §3b is what that was doing.


---

## 3b. What the unhandled statuses were actually doing

The corpus was recorded to check fabrications. Reading it against the code
found three live defects, each of which had been running for as long as the
window.

**Every non-403 recovered the adaptive delay.** `_update_detail_delay` took a
bare `is_403` boolean, so 500/502/503/504 took the `else` arm — the *recovery*
arm — and multiplied the delay by 0.85. 1,911 measured 5xx in 91 days, **1,600
of them in a single outage on 2026-08-20**, each one making the scraper press
harder on an origin that was failing. `scrape_results._update_srp_penalty` had
the identical defect, and one of its call sites is worse: it recovered the
penalty on the line immediately after logging that the fetch had *raised
twice*.

**Every non-200 body was enqueued for parsing.** `scrape_detail_fetch` writes
the artifact and enqueues it before it looks at the status, and
`ops.artifacts_queue` **has no status column** — the status is known at fetch
time and thrown away. So `processing.parse_detail_page` classifies blind, and
its decision is `listing_state = (unlisted or {}).get("listing_state") or
"active"`. A cars.com 502 page has no unlisted marker, so it published as an
active listing; and because that observation carries `source='detail'`,
`mart_vehicle_snapshot` trusts it over every other signal.

**The 302s are the sharpest case and their provenance was measured, not
guessed.** Exact equality over 91 days: 1,172 detail fetches reported 302 and
1,172 *bootstraps* reported 302. Every one came through the FlareSolverr
bootstrap, which runs whenever the 25-minute credential cache expires, against
whichever listing triggered the miss. When cars.com redirects a listing that
has been removed, `get_cf_credentials` hands back **the redirect target's
body**, `_fetch_url` returns it as that listing's artifact with `final_url`
still reported as the URL that was asked for, and the parser publishes a
removed listing as active. Not a burst: 36–52 a day, every day, about 6% of
bootstraps. The 503s by contrast were one incident — 1,600 on 2026-08-20 and 4
on 2026-06-22.

### What now happens instead

[`scraper/fetch_outcomes.py`](../../scraper/fetch_outcomes.py) is one
classification read by the metric, the adaptive delay and the enqueue, where
there used to be three opinions and one omission.

| Outcome | Statuses | Backs off | Reaches the parser |
|---|---|---|---|
| `OK` | 200 | no | yes |
| `BLOCKED` | 403 | yes | **yes** |
| `TRANSIENT` | 500, 502, 503, 504 | yes | no |
| `REDIRECTED` | 302 | no | no |
| `UNKNOWN` | anything else, and a raised fetch | yes | no |

`BLOCKED` still enqueues, deliberately: a challenge page is a fact about
availability that `_detect_challenge` records as `listing_state='blocked'`, and
Plan 128 exists because those pages were once counted as successful scrapes.
`REDIRECTED` does not back off, because a redirect is a prompt and correct
answer about somewhere else. Non-enqueued bodies **are still written to
MinIO** — that is the only copy of what the site said — they just do not become
observations.

`UNKNOWN` is the catchall the rule requires, and it is the conservative one.
`classify(None)` — a fetch that raised — lands there too, which used to read as
"not 403" and *speed the scraper up* after a total failure.

**The metric's label set widened and the two alert selectors did not move.**
`cartracker_detail_fetch_total`'s catch-all `error` split into `transient`,
`redirected` and a residual `error`. `ct-detail-fetch-failing` keys on
`outcome="ok"` and the block-rate rule on `outcome="403"`; both are unchanged
and pinned by `test_the_labels_the_alerts_key_on_are_unchanged`. That alert's
own description told an operator *"mixed `error` points at the fetch path or
the site"* and gave them no way to tell which — it now names all four.

**Demonstrated by reverting.** Putting `should_back_off` back to `outcome is
BLOCKED` and `should_enqueue_for_parsing` back to `True` fails **17 tests**
across three modules. The two pre-existing guards in
`tests/scraper/test_metrics.py` that pinned the old label set also fired when
the set widened, which is them working; both were updated with the reasoning
rather than relaxed.
---

## 4. The markdown-it exclusion: right verdict, wrong reason

Plan 162 says `git` and markdown-it are excluded because "a stale one there
makes a script error out". That is exactly true of `git` — `git config --get`
on a renamed key returns empty, `audit_git_refs.py` reads that as "not
configured" and fails.

**It is not true of markdown-it.** A renamed token type makes `_render`'s walk
match nothing: no heading gets an `id`, no link gets rewritten, and
`build_public_recaps.py` exits 0 having produced a subtly broken page. Nothing
errors out.

What actually saves it is an assertion on the rendered output —
`tests/scripts/test_build_public_recaps.py` asserts `'<h2 id="merges">'` is in
the HTML, which goes red the moment the token type stops matching. The verdict
is unchanged and the entry is still out of scope; the reason recorded against
it was not the reason it is safe.

---

## 4b. Two false claims this stage made about itself

**The census landed asserting a replay that did not exist.** The FlareSolverr
row named
`test_every_flaresolverr_field_the_scraper_reads_is_in_the_recorded_body` as
its check. Nobody had written that function.
`test_every_replayed_census_entry_names_something_that_exists` passed anyway,
because it checked that the *file* existed and not that the *function* did —
and the file did, because it holds a dozen other rules. **A check on the
containing file is not a check on the thing named, and the gap is exactly the
width of the thing being claimed.** The rule now reads the function out of the
file, and the first thing it did was fail on this entry.

**And the reason that row gave for settling for a recording was wrong too.** It
said standing FlareSolverr up in CI would leave nothing legitimate to point it
at, because the only URL that exercises it is cars.com. That is false:
FlareSolverr solves any URL, and a page with no challenge on it returns the
same envelope as one with. Measured 2026-09-09 against
`ghcr.io/flaresolverr/flaresolverr:v3.4.6` with a four-line page served from a
thread in the checker's own process — `status='ok'`, `solution.status=200`, one
cookie, a 101-character user-agent. So the row went from a recording to a real
replay in CI, which is what `scripts/verify_flaresolverr_contract.py` and the
`flaresolverr-contract` job now are.

**Why it matters more than the other rows.** `get_cf_credentials` reads six
keys out of that envelope and **four of them with `.get` and a default**: a
renamed key does not raise, it makes `html` empty, defaults the status to 200,
and returns an empty cookie jar while caching the credentials as though the
bootstrap worked. Demonstrated by pointing the scraper at
`solution["userAgentString"]` and watching the job name the field and list what
FlareSolverr actually sends.

The page sets a cookie deliberately. Without one, `solution.cookies` comes back
`[]`, which satisfies "is a list" and proves nothing about the `{name, value}`
shape the scraper's dict comprehension depends on.

---

## 5. Prometheus and Loki: no fabrications because no coverage

This is a **different defect** from the rest of the census and is recorded
separately so that "no fabrications" cannot read as "no gap".

`ops/coordination_release.py` reaches Prometheus and Loki over HTTP at three
sites. `tests/ops/test_coordination_release.py` never exercises that seam: it
patches `_prometheus_scalar` (7 sites), `_container_health_values` (9) and
`_loki_has_recent_ingestion` (5) — one level **above** `requests.get`. So
`payload["data"]["result"]`, `result[0]["value"][1]` and the
`payload.get("status") != "success"` guard are asserted by nothing at all.

Everywhere else in this census a test fabricates a response that may be wrong.
Here there are no fabrications because there are no responses. A corpus would
buy **first** coverage, not corrected coverage, and writing the tests that
consume it is a larger job than this stage. Declared out of scope with that as
the reason.

---

## 6. The fourteen mutations, watched failing

`python scripts/verify_testing_contract_mutations.py`, 2026-09-09 — exit 0,
**all 106 entries `CAUGHT`** (14 of them this stage's), baseline and
restored tree both `90 passed`.

| Mutation | Rule that noticed |
|---|---|
| a census entry is left with neither a replay nor a reason | `test_every_census_entry_is_replayed_or_carries_a_reason` |
| a census entry names a replay whose file has been renamed away | `test_every_replayed_census_entry_names_something_that_exists` |
| a census entry goes on naming a site that has moved | `test_every_census_site_still_exists` |
| a members tuple is emptied | `test_no_declared_vocabulary_is_empty` |
| a test fabricates a status the corpus does not hold | `test_no_test_fabricates_a_cars_com_status_production_has_never_seen` |
| the recorded corpus drifts away from what the suite fabricates | same |
| the corpus is emptied | `test_the_cars_com_corpus_is_not_empty` |
| an observation loses its instrument | `test_the_corpus_records_which_instrument_saw_each_status` |
| the census names a curl_cffi target that does not exist | `test_every_restated_curl_cffi_target_is_a_real_browser_type` |
| the scraper's target list drifts from the census | `test_the_declared_curl_cffi_targets_are_the_ones_the_scraper_holds` |
| curl_cffi offers a target the scraper never added | `test_no_chrome_target_curl_cffi_offers_is_missing_from_the_scraper` |
| a DAG compares against a word the census does not declare | `test_every_airflow_state_the_dags_compare_against_is_declared` |
| a status the corpus observed loses its handling | `test_every_observed_cars_com_status_is_handled` |
| the catchall stops refusing the parser | `test_the_unknown_catchall_is_reachable` |

**The exit clause's demonstration, in full.** Removing `403` from
`statuses_observed` failed the rule and named all nine 403 sites by file and
line, with the message pointing at `--record` rather than at the tests. That is
"a recorded corpus that has drifted failing".

**Three anchors had to be re-anchored before they were unique**, which is Stage
Y's anchor rule doing exactly what it was built for: `"verdict":
OUT_OF_SCOPE,` matched 5 times, `mock_resp.status_code = 403` matched 6, and
`"instrument": "loki",` matched 9. Each now rides a neighbouring line that is
unique today and fails loudly when it stops being.


---

## 6b. The two replays CI runs and this machine had not

Both integration modules were written and committed before either had executed
anywhere. Both were then run against the real thing rather than left for CI to
discover.

**MinIO**, started the way CI starts it — `docker compose -f docker-compose.yml
-f docker-compose.ci.yml --env-file .env.ci up -d --wait minio`, so production's
definition rather than a `services:` block. 3 passed. Not vacuous: a missing
object and a missing bucket both come back `'404'`, read off a real
`ClientError`. Worth recording *why* it is `404` and not `NoSuchKey` — both
calls are `HEAD`, which has no response body for botocore to parse an S3 error
code out of, so it falls back to the HTTP status. That is the same call shape
`shared/minio.py`'s `object_exists` and `object_size` use, which is what makes
the assertion the right one.

**Airflow**, inside the pinned `apache/airflow:3.2.0` image with the repository
mounted. 3 passed, and the real member set came back non-empty:
`TaskInstanceState` has 12 members and `failed` is one of them; `TriggerRule`
has `one_failed` and `all_done`.

**Demonstrated failing, both halves.** Renaming the declared member to
`"failure"` — Airflow's rename, simulated — failed
`test_every_restated_airflow_task_state_is_a_real_member` *and*
`test_every_airflow_state_the_dags_compare_against_is_declared`. That is the
"neither rule works alone" property observed rather than asserted: the first
catches Airflow moving, the second catches the DAG and the register moving
apart.

**And running it found a defect in this stage's own code.**
`airflow.utils.trigger_rule.TriggerRule` still resolves in 3.2.0 and emits
`DeprecatedImportWarning: Please use 'airflow.task.trigger_rule.TriggerRule'`.
The test had imported the deprecated path, which would have made the rule
guarding against an Airflow upgrade the thing that broke on one. Both the
import and the census's `what` moved to `airflow.task.trigger_rule`.

**Still unverified, and named rather than implied:** the `flaresolverr-contract`
job has run on this Windows Docker host and not on a Linux runner. `_host_ip()`
and the published port are the parts that could differ.
---

## 7. What the `Asserted by` column deliberately does not name

Two of this stage's replays need something the mutation harness cannot
provide — `test_every_restated_airflow_task_state_is_a_real_member` needs the
isolated Airflow venv, `test_a_missing_object_still_reports_a_code_this_repository_knows`
needs a running MinIO. Naming them in `docs/TESTING.md`'s `Asserted by` column
would oblige a mutation nobody can run.

**The precedent for leaving them out is two rows of this same class.**
`verify_promtail_contract.py` and `verify_container_health_docker_contract.py`
are the repository's other two live replays, and neither appears in that column
either. What the column keeps is the half a mutation *can* reach: that the
register still describes the code — which is the end that drifts silently. The
other end is asked of the owning system on every CI run, by the system itself.

---

## 8. What this stage does not claim

**Replay proves shape, not meaning.** Every entry in the census inherits the
sentence `docs/TESTING.md` already carries about `container_health`: cars.com
can keep returning 403 and start meaning something else by it, Airflow can keep
`failed` and change when it is set, and nothing here would notice either.

**The corpus is as current as its last recording.** It carries `captured_at`
and the windows its instruments cover, and nothing fails when it ages. A
staleness gate would fail CI on a schedule rather than on a defect, which is
the trade this stage declines — but it means a status cars.com started
returning after 2026-09-09 reads as invented until somebody re-records.
