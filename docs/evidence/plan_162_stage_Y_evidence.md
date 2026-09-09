# Plan 162 Stage Y — measurements

The readings behind [Stage Y](../plans/plan_162_testing_census_and_restructure.md#stage-y-grew-its-rule-passes-a-route-that-reports-work-it-did-not-do),
which grew from *a route declares the statuses it can return* into that plus
G27 and G28 while it was being written.

**The plan document holds what the numbers mean; this file holds the numbers and
the recipe for each.** All three steps are recorded: handlers observing their
effects, the declarations, and the coverage rule.

The short version: **writing the contract down found routes with nothing true to
write**, and the stage's own original rule passes every one of them.

## The census that opened the stage

**Recipe.** Two scans over `d6e3a6d`, on this machine. Declared codes come from
`app.openapi()` per service in a subprocess — the same enumerator
`test_no_route_is_hidden_from_the_schema_this_rule_reads` uses. Produced codes
come from an AST walk of each handler body for `HTTPException`, the redirect
classes and the `Response` subclasses, resolving `status.HTTP_*` names and
module-level integer constants.

**100 routes across the six importable services; 46 produce a status code they
never declare; not one route declares a single code today.**

| Service | Routes | Declares `200` only | Declares `200, 422` |
|---|---:|---:|---:|
| `ops` | 64 | 37 | 27 |
| `archiver` | 12 | 6 | 6 |
| `scraper` | 9 | 5 | 4 |
| `dbt_runner` | 6 | 5 | 1 |
| `processing` | 5 | 3 | 2 |
| `container_health` | 4 | 3 | 1 |

`dashboard` contributes nothing: it still cannot be imported ([G18](../TESTING.md#the-gap-list)).

The census taken 2026-09-07 said 85 routes and 42, and **the difference is grain
rather than drift** — six `ops` handlers are `api_route(..., methods=["GET",
"HEAD"])` and serve two routes each, which the earlier count counted once.

Produced-but-undeclared codes, by frequency: **503 (15), 303 (13), 409 (11),
500 (10), 404 (7), 400 (5), 422 (3), 403, 307, 308.**

### What made it cheaper than the census predicted

**No `Depends()` and no `@app.exception_handler` anywhere in the six services** —
authentication is Caddy's `forward_auth`, not a FastAPI dependency. A handler
body is therefore the whole of what a route can produce, with no dependency
graph to walk. The census priced 42 judgement calls; most are not judgement,
because the 500s argue for themselves in docstrings their own callers branch on
(`archiver/app.py:241`, `dbt_runner/app.py:155`).

### The phantom 422

**Recipe.** `fastapi/openapi/utils.py`, read at both versions; and a scan of each
operation's `parameters` and `requestBody` in the emitted schema for a parameter
that can actually fail validation — anything other than an unconstrained string.

FastAPI injects `422` on the **presence** of a parameter, never its fallibility:

```python
http422 = "422"
if (all_route_params or route.body_field) and not any(
    status in operation["responses"] for status in [http422, "4XX", "default"]
):
```

That text is **byte-identical in 0.128.0 and 0.141.1**, so no upgrade removes it;
the only difference between versions is where `all_route_params` is computed. Of
the **41 routes that declare 422, 33 can produce it and 8 cannot**, their only
parameters being unconstrained strings. Six of the eight are repaired by G27 or
resolved by Stage AA. The two that remain are `GET` and `HEAD /recaps/{slug}`,
where the guard already exists as `_SLUG_RE` in the handler body and moving it
into `Path(pattern=...)` would turn today's 404 into a 422 on a public route.

Confirmed empirically at 0.128.0: a route with no parameters emits no 422; a
route declaring `responses={404: ...}` still gets one; declaring `422` yourself
replaces FastAPI's description rather than adding a second.

## G27 — the blind mutations

**Recipe.** Every `.sql` file outside `dbt/`, `db/`, `tests/` and `lakehouse/`
whose statement is a `WHERE`-bearing `UPDATE`/`DELETE`, mapped to the constants
each service's `queries.py` binds, then matched against the production functions
naming those constants and filtered to those reading neither `RETURNING` nor
`rowcount`.

**13 functions.** Four needed no change — the rule's third clause, a preceding
read in the same function that gates the mutation — and one was restructured
rather than repaired.

| Disposition | Count | Sites |
|---|---:|---|
| Repaired: observe `rowcount` | 8 | `update`/`toggle`/`delete_search`, `change_user_role`, `revoke_user`, `deny_access_request`, `release_claims`, `_set_status`, `write_detail_unlisted` |
| Already gated by a preceding read | 3 | `advance_rotation` (both statements), `_reap_stuck_processing`, `approve_access_request` |
| Already observing `RETURNING` | — | `_evict_delisted_cooldowns`, `expire_orphan_detail_claims` |
| Restructured so the guard is local | 1 | `_record_last_used` |

`coordination.py` and `deploy.py` are not in the set: every transition there
returns `RETURNING generation` and reads it.

### The swallowed half

**34 sites** swallow an exception and fall through to the success path. **11
cover a mutation**; the other 23 cover a read or a parse and degrade honestly.
Five of the eleven are the dead `dbt_runner` calls and belong to Stage AA.

`scraper/processors/scrape_detail.py:196` is the **control case** and stays: it
binds `minio_write_error` into the artifact it returns, so the caller can tell.
`revoke_user` bound nothing and returned the identical redirect — for an
access-control operation, whether the row matched, did not exist, or the
database raised.

### Why `_record_last_used` is not an exemption

Its statement carries an identity clause **and** a throttle:

```sql
WHERE token_sha256 = %s
  AND (last_used_at IS NULL OR last_used_at < now() - %s::interval)
```

so a `rowcount` of 0 is the designed outcome whenever the window has not
elapsed. It is nonetheless not an exemption: it is called at exactly one site,
after `SELECT_MACHINE_TOKEN` fetched that digest and three guards returned early.
The read gates it. **It was inlined into `_resolve_machine_token`** because that
gate lived one frame up with nothing connecting the two — a second caller would
have got the write with no guard, and neither half's tests would have noticed.

## G28 — declared codes nothing exercises

**Recipe.** Per `(METHOD, path)`, the produced set above against every status
code any test asserts in a function that requests that path, matching literal
path segments against the route template.

**68 `(route, code)` pairs; 46 asserted by some test; at most 22 by none.**

**That ceiling is soft, in the direction this plan has been caught by before.**
`tests/ops/routers/test_coordination.py:156` asserts a `parametrize`-injected
code, which a literal scan reads as no assertion at all — the same blindness
Stage H fixed for paths. The true figure is lower, and the rule re-measures with
`_parametrized_strings`.

## Step 1 in production

**Deployed 2026-09-08** (timestamps UTC), both through `scripts/redeploy.sh`,
drain confirmed at 0s, every pollable service healthy, deploy intent released.

| Service | Image built | Container recreated |
|---|---|---|
| `processing` | 22:37:30Z | 22:37:50Z |
| `ops` | — | 00:27:34Z (following UTC day) |

**Loaded code verified inside each container**, which a `git pull` does not
establish: `docker exec cartracker-processing python -c "from
processing.routers.batch import StatusWriteFailed, _set_status"` imports, and
`inspect.getsource` confirms `rowcount` is read; `ops` carries five
`_not_found_response` call sites in `users.py`, four in `admin.py`, and
`claims_released` in `release_claims`.

### The soak

**Recipe**, on the VM — one `docker exec` into the scheduler, globbing the task
logs written since the deploy and pulling the response dict out of each:

```
docker exec cartracker-airflow-scheduler sh -c
  for f in $(find /opt/airflow/logs/dag_id=results_processing -name "*.log"
             -newermt "2026-09-08 22:38" | sort); do
    grep -o "{.srp_count.*status_write_failures[^}]*}" $f | tail -1
  done
```

Eleven consecutive `results_processing` runs (`*/5`), 22:40Z–23:30Z:

| Run (UTC) | `detail_count` | `status_write_failures` | `silver_write_failures` |
|---|---:|---:|---:|
| 22:40 | 0 | 0 | 0 |
| 22:45 | 0 | 0 | 0 |
| **22:50** | **400** | 0 | 0 |
| 22:55 | 0 | 0 | 0 |
| 23:00 | 0 | 0 | 0 |
| **23:05** | **400** | 0 | 0 |
| 23:10 | 0 | 0 | 0 |
| 23:15 | 0 | 0 | 0 |
| **23:20** | **400** | 0 | 0 |
| 23:25 | 0 | 0 | 0 |
| 23:30 | 0 | 0 | 0 |

**1,200 detail artifacts, zero status-write failures.** Every artifact takes at
least one `_set_status`, so that is upwards of 1,200 `MARK_ARTIFACT_STATUS`
executions each finding its queue row. Zero ERROR lines and zero tracebacks in
the container across the window.

**What the zero means, both ways.** The stale-claim case `StatusWriteFailed` was
built for does not occur under normal operation at this cadence — the
`cleanup_queue`-overlapping-a-batch scenario it was aimed at is not happening.
The counterpart is that its handling path has therefore only ever run in tests:
the `continue`, the counter and the log line are unproven in production.

### The guards

**Recipe.** `curl` from the VM **host** against `http://localhost:8060` — there
is no `curl` binary inside `cartracker-ops`, and the published port bypasses
Caddy's `forward_auth`. 2026-09-09T00:28:21Z.

| Request | Before | Now |
|---|---:|---:|
| `POST /admin/searches/{no-such-key}/toggle` | 303 | **404** |
| `POST /admin/searches/{no-such-key}/delete` | 303 | **404** |
| `POST /admin/users/999999/revoke` | 303 | **404** |
| `POST /admin/access-requests/999999/approve` | 303 | **404** |
| `POST /admin/access-requests/999999/deny` | 303 | **404** |
| `POST /admin/users/999999/role` (`role=superadmin`) | 303 | **400** |
| `GET /health` (control) | 200 | 200 |

Non-mutating by construction: every target is an identity matching no row, so
`rowcount` is 0 and nothing is written.

### Plan 147's loop guard, measured

**Recipe.** The `release_claims` task log of the first `scrape_detail_pages` run
(`*/15`) after the `ops` deploy, run `scheduled__2026-09-09T00:30:00+00:00`.

```
release_claims: run_id=86b1e39d-9e2a-4abd-a228-dec0015ea98e status=None total=400 errors=0
'total': 400   'claims_released': 400   'fetches_recorded': 400
```

**400 released, 400 claims deleted, 400 fetches recorded — the three agree.** So
on this run Plan 147's guard is not under-recording: every listing the batch
released had an `ops.price_observations` row for `RECORD_DETAIL_FETCHES` to
match, and the counts the endpoint now reports are the database's rather than
the request body's.

**What it does not establish.** One run of 400. The failure mode the change
exposes — a listing with no observation row, so `fetches_recorded < total` — did
not occur here, and cannot be shown to be impossible from a single agreeing
reading. What is established is that the counts are now capable of disagreeing:
before this step `fetches_recorded` was `len(fetched_ids)` and would have read
400 whatever the database did.

**`status=None` in that same line is Stage AA's second recorded instance**,
observed in production rather than inferred: `airflow/dags/scrape_detail_pages.py`
logs `result.get("status")` and `release_claims` has never returned a `status`
key.


## The rules, and what writing them cost

Six rules landed across the stage's three gaps, each proved by a mutation rather
than by argument. **Every one of them had a bug that made the repository look
healthier than it was**, and none of those bugs failed a test — which is the
argument for the mutations and for the floors, stated as measurements rather
than as a principle.

| Rule | The bug it shipped with | What it hid |
|---|---|---|
| a mutation observes its effect | keyed on the `execute` call's arguments | `admin.py` binds the statement first, so all three search handlers were unseen — **it passed on a tree with a rowcount check deleted** |
| the same | read only `matched = cur.rowcount`, not `if not cur.rowcount:` | reported six compliant functions as violations |
| the same | fed a bare expression to a statement walker | the gate check answered `False` for every gated mutation in the tree |
| every produced code is asserted | a template-only decorator path is a suffix of everything | 99 of 100 requested paths matched more than one handler |
| the same | unioned the implicit 200 in unconditionally | demanded a 200 test from every route that only redirects — 10 of the first 16 findings |
| the same | enumerated response *classes* | never saw `templates.TemplateResponse(..., status_code=404)`, which is how both admin routers answer |
| the same | let a wildcard match a literal segment | **38 of 89 handlers left scope while the failure list still read four** |
| a response has its status read | nearly shipped an escape clause crediting a shape check | an inference one step from crediting a 200 that happens to parse |

The last of those was not repaired in the rule. **The conforming code was changed
instead**, on the argument that six explicit `raise_for_status()` calls in
working code buy a rule that states one thing and cannot be argued around. It
was free: `HTTPError` subclasses `RequestException`, which those gates already
catch and already turn into an `unknown` verdict.

## What the rules found that no census had

**Two dead call sites in the DAGs.** Both detail-scrape DAGs `POST` to
`mark_job_fetched`, which answers **404** when the job is already gone — and a
404 is not a `RequestException`, so the `except` beneath never fired and the job
stayed in the scraper's memory unreported.

**Five dead endpoints.** `ops` calls `GET /dbt/lock`, `GET` and `POST
/dbt/intents`, `DELETE /dbt/intents/{intent_name}` and `GET /logs` on
`dbt_runner`. All five were deleted by `9f08336` (2026-04-28) and `d88a41e`
(2026-05-05) and every caller was left standing, each wrapped in
`except Exception: pass` and followed by an unconditional 303. The admin dbt
panel and log viewer have done nothing since April.

**A five-valued result thrown away.** `_set_intent` returns `ok`, `locked`,
`invalid`, `unavailable` and `error`; the admin deploy buttons discard it and
redirect. `_intent_release`'s own docstring records the near miss — it returned a
bare `bool` until Stage K *"collapsing five outcomes into False"*. Stage K
widened the signal and the caller never started reading it.

## The declarations

**Recipe.** The declaration rule's own failure list, seeded at 52 from its first
run and drained by declaring each route, file by file. Entries were deleted only,
never regenerated: a drain that rewrites the ledger to match reality absorbs
anything broken on the way, and this one reports it instead. Nothing was
reported.

| Service | Declared before | Declared after |
|---|---|---|
| `ops` | `200 422` | `200 303 307 308 400 401 403 404 409 422 500 503` |
| `archiver` | `200 422` | `200 400 409 422 500 503` |
| `dbt_runner` | `200 422` | `200 400 409 422 500 503` |
| `scraper` | `200 422` | `200 400 404 422 503` |
| `processing` | `200 422` | `200 404 422 503` |
| `container_health` | `200 422` | `200 422` — unchanged, and correctly: its four routes produce nothing else |

## The coverage gaps, and two of them were ours

**Recipe.** The coverage rule's failure list, seeded at 11 and drained by writing
sixteen tests across five files.

Nine were pre-existing: `/recaps` answering 404 before the first generator run,
the snapshot archive download's entire refusal surface, archiver's pack and prune
400s, coordination's status and host-evidence arms.

**Two were introduced by this stage's own repairs, hours earlier.**
`approve_access_request` and `deny_access_request` gained a 503 when their
swallowed database errors were fixed, and neither got a test. The rule written
after them is what said so.

Writing those tests produced two findings in the tests themselves. One asserted
`status_code in (400, 404)`, which is the vague shape this plan exists to remove;
it is a definite 400 now. And four of them were **invisible to the coverage
rule** — they built their path from a class attribute, which the resolver cannot
read, so four tests existed and credited nothing while the file showed green.

## The phantom 422

**Recipe.** Each operation's `parameters` and `requestBody` in the emitted
schema, scanned for a parameter that can actually fail validation — anything
other than an unconstrained string.

41 routes declare `422`; **33 can produce it and 8 cannot**. Six are the dead
admin routes and are waived to Stage AA. Two are permanent and carry no owner:
`GET` and `HEAD /recaps/{slug}` guard the slug in the handler and answer 404, and
moving that into `Path(pattern=...)` would turn a public page's *no such recap*
into a validation error.

## What the stage could not close

Every live waiver across all four G27 clauses and the phantom-422 ledger is a
call into the dead `dbt_runner` admin panel. **Fourteen entries across five
rules, and one decision drains all of them** — whether that panel is deleted or
the endpoints return, which is Stage AA's and not this stage's to force.
