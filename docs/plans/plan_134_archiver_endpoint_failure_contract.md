# Plan 134: The Archiver Endpoints Do Not Signal Failure

## Status

**Build order — ready to start.** Split out of [Plan 131](plan_131_packed_cold_storage.md)
Stage 5 decision D5 on 2026-08-14, which fixed the two Plan 131 endpoints and
deliberately left the rest alone.

This is correct to fix and should not be fixed casually: every endpoint below
has been failing silently for as long as it has existed, so the change converts
long-standing quiet into sudden DAG failures and pages. That is the whole
content of this ticket — the code change is a few lines per endpoint.

---

## The defect

Every archiver processor returns a **summary dict** rather than raising.
Partial results are still results, which is the right shape for a job you run
by hand and read. The CLI then translates that summary into an exit code:

```python
# delete_packed_source_html.py — the face that got this right
return 1 if result["error"] or result["objects_refused"] else 0
```

**The HTTP side never got the same translation.** `archiver/app.py` returns
whatever the processor returned, with a 200. So `resp.raise_for_status()` — the
entire check a DAG performs — passes on a run that did nothing, failed, or
never started.

| endpoint | summary carries | consequence of the 200 |
|---|---|---|
| `POST /flush/silver/run` | `{"flushed": 0, "error": ...}` | `hourly_analytics_refresh` proceeds to build dbt on stale data |
| `POST /flush/staging/run` | same shape | staging events are not flushed; the DAG is green |
| `POST /compact/silver/run` | same shape | compaction silently stops happening |

No DAG in `airflow/dags/` inspects an `error` key.

## The fix, which already exists in two places

`dbt_runner/app.py:214` raises `HTTPException(status_code=500, detail=result)`
on a failed build, and `sensors.post_json` was built for exactly that —
`JsonPostError` carries the parsed body so a notify task can quote the stderr.

Plan 131 Stage 5 applied that pattern to `/pack/bronze/run` and
`/pack/bronze/prune` as `_pack_failure_reason` / `_prune_failure_reason` in
`archiver/app.py`. **Those two functions are the template**: a predicate per
job, mirroring that job's own CLI exit code, unit-tested directly against
summary dicts, raising a 500 whose `detail` is the summary plus a
`failure_reason`.

## Why it was scoped out of Plan 131

Not because it is wrong. Because the blast radius is unrelated to packing:

- Each of these runs **hourly**, not monthly. A predicate that is wrong pages
  every hour.
- Nobody knows the current failure rate, because the failures have never been
  visible. Turning three silent endpoints loud at once, on a plan about cold
  storage, is how a packing deploy gets blamed for an unrelated pager storm.
- `flush_silver_observations` returning `flushed: 0` is *normal* when there is
  nothing to flush. The predicate is `error`, not `flushed == 0` — and the same
  trap exists for each of the three, so each needs reading before it is wired.

## Suggested approach

1. **Measure first.** Add the predicate as a WARNING log only, deploy, and read
   a week of logs. That answers "how often does this actually fail" without
   paging anyone, and it is the number that decides whether step 2 is a
   one-line change or a week of fixing real breakage.
2. Then flip each endpoint to a 500, one deploy per endpoint, so an unexpected
   pager storm names its own cause.
3. The DAGs need no change: they already call `raise_for_status()` or
   `post_json`.

## Files

| File | Change |
|---|---|
| `archiver/app.py` | `_failure_reason` per job on `/flush/silver/run`, `/flush/staging/run`, `/compact/silver/run` |
| `tests/archiver/test_app.py` | Predicates unit-tested against summary dicts, per endpoint |

## Not yet surveyed

`/cleanup/parquet/run`, `/cleanup/queue/run` and `/cleanup/artifacts` return a
`failed` count, and their DAGs call `raise_for_status()` and **discard the
body** — so a run where every deletion failed is green there too. Whether that
is the same defect depends on each processor's summary shape, which has not
been read. Check them during step 1 rather than assuming either way.

## Out of scope

The two Plan 131 endpoints. Already done — see D5 in
[`docs/prompts/claude_prompt_plan_131_stage_5.md`](../prompts/claude_prompt_plan_131_stage_5.md).
