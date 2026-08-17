# Run Sheet: Plan 131 Stages 3-5 — Pack Read Path, Source Deletion, Then The Schedule

Operational companion to [Plan 131](plan_131_packed_cold_storage.md). Follow it
in order. **Stage 3 is deployed and verified before Stage 4 deletes anything** —
that ordering is the entire safety argument, not a preference. Sections 1-7 are
the manual procedure and remain how a backlog month gets done;
[section 8](#8-stage-5--the-lifecycle-dag) covers the DAG that now runs the
steady-state month on a schedule.

> **The bucket is un-versioned** (verified 2026-08-10). A delete is immediate
> and there is no undo. What makes that acceptable is that the bytes are
> already inside a pack that was verified, stored, and re-read from storage —
> and that every deletion re-proves it for that artifact first.

**Two PRs, two deploys.** Do not merge them into one.

| | PR | Deploys | Safe to run? |
|---|---|---|---|
| Stage 3 | read path | `processing`, `archiver` | Yes — additive, deletes nothing |
| Stage 4 | source deletion | `archiver` | **Only after Stage 3 is verified below** |

---

## 0. Before you start

```bash
ssh <the VM>
cd ~/new_car_tracker    # wherever the checkout lives
```

Record the starting position. Everything after this is measured against it:

```bash
df -i /mnt/data          # inodes — the constraint this plan exists for
df -h /mnt/data          # bytes
```

Write the `IUsed`/`IFree` numbers down. At ~65,500 inodes/day the ceiling was
~mid-October 2026 from a 2026-08-13 reading of 4,005,530 free.

---

## 1. Deploy Stage 3

```bash
git pull
bash scripts/redeploy.sh processing archiver
```

Three things that will cost you a debugging pass if skipped:

- **`bash` prefix is required.** `scripts/redeploy.sh` is tracked `100644` and
  is not executable on a fresh checkout.
- **`archiver` as well as `processing`.** The archiver reads HTML now. A stale
  archiver image was exactly the Stage 2 trap: its `shared/minio.py` had no
  idea dictionaries existed.
- **`docker compose build` runs inside `redeploy.sh`**, which matters because
  this PR adds new files.

Confirm both came up:

```bash
docker compose ps processing archiver
docker logs --tail 20 cartracker-archiver
```

### Sanity check: an artifact that still exists reads from its object

Nothing has been deleted, so every read should still be answered by the object
path. This is only checking that the deploy did not break today's behaviour.

```bash
docker exec -w /app cartracker-processing python -c "
from shared.minio import read_html
key = 'html/year=2026/month=8/artifact_type=detail_page/<any-recent-uuid>.html.zst'
print(len(read_html(key)), 'bytes')
"
```

Pick any recent key from `ops_normalized/artifacts_queue_events`, or skip
straight to step 2 — it covers this and more.

---

## 2. Verify the read path against real April packs — **the gate**

This is the step Stage 4 depends on. It reads sampled members through **both**
the object path and the pack path and asserts they are byte-identical, while
every source object still exists.

```bash
docker exec -w /app cartracker-archiver \
    python -m archiver.processors.verify_pack_read_path \
    --year 2026 --month 4 --per-pack 5 --json-out /tmp/verify_april.json
```

`--per-pack 5` over 32 packs is 160 artifacts and takes a few minutes. Note the
app root is **`/app`**, not `/usr/app` — `/usr/app` holds only the logs volume,
and `import shared` fails from there.

### What good looks like

```json
{
  "sidecars": 32,
  "sampled": 160,
  "verified": 160,
  "failed": 0,
  "latency_ms": {
    "object":    {"p50": ..., "p95": ...},
    "pack_cold": {"p50": ..., "p95": ...},
    "pack_warm": {"p50": ..., "p95": ...}
  }
}
```

**`failed` must be 0 and `verified` must equal `sampled`.** Anything else stops
the run — do not proceed to Stage 4, and read `failures[]`, which names the
artifact and what disagreed.

**Record the three latency figures in the plan doc.** Plan 131's success
criteria require single-artifact extraction latency *measured and accepted*,
not assumed, and this is the only place that number comes from. `pack_cold` is
the honest worst case (every cache dropped per read); `pack_warm` is what
reprocessing actually sees.

### If it fails

`PACK_READ_FALLBACK=0` on the processing/archiver services disables the pack
fallback entirely and restores exactly the previous read behaviour. Nothing has
been deleted at this point, so every artifact is still readable from its
object — a failure here is a latency regression at worst.

### Then widen it once

```bash
docker exec -w /app cartracker-archiver \
    python -m archiver.processors.verify_pack_read_path --year 2026 --month 5 \
    --per-pack 5 --json-out /tmp/verify_may.json
```

May has complete metadata coverage where April does not, so it is the cleaner
of the two and should be at least as good.

---

## 3. Deploy Stage 4

Only after step 2 came back clean.

```bash
git pull
bash scripts/redeploy.sh archiver
```

Nothing runs on deploy. The job is dry-run by default, requires `--apply`,
requires an explicit `--year/--month`, and is capped.

---

## 4. First delete run — dry run

```bash
docker exec -w /app cartracker-archiver \
    python -m archiver.processors.delete_packed_source_html \
    --year 2026 --month 4 --max-objects 100 --max-packs 1 \
    --json-out /tmp/prune_dry.json
```

A dry run performs **every** verification and read, and deletes nothing.

Check, in this order:

| field | expect |
|---|---|
| `objects_refused` | **0.** Anything else: read `failures[]` before going further. |
| `objects_verified` | equals the objects it considered |
| `objects_deleted` | **0** — it is a dry run |
| `orphan_packs` | `[]`. A pack listed here has no sidecar and nothing will be deleted for it. |
| `by_status` | a plausible spread. `ok` is **success**; `no_event_row` is April's 42,276 no-provenance captures and is expected here. |
| `objects_surviving_before` | ~557,065 for April on the first run |

---

## 5. First delete run — apply, small

```bash
docker exec -w /app cartracker-archiver \
    python -m archiver.processors.delete_packed_source_html \
    --year 2026 --month 4 --max-objects 100 --max-packs 1 --apply \
    --json-out /tmp/prune_apply_100.json
```

100 objects is small enough to inspect by hand. Then confirm the artifacts are
still readable **through the pack**, which is the whole point:

```bash
docker exec -w /app cartracker-archiver python -c "
import json
from shared.minio import read_html
run = json.load(open('/tmp/prune_apply_100.json'))
print('deleted:', run['objects_deleted'], 'bytes:', run['bytes_freed'])
print('inodes  est:', run['inodes_freed_estimated'], ' measured:', run['inodes_freed_measured'])
"
```

**Do not hand-pick keys for this.** The run's JSON does not list what it
deleted, and an earlier revision of this step left `<deleted-key-1>`
placeholders that have no obvious source — pasted literally, they raise
`NoSuchKey` from a key that was never real. Derive them from the sidecar
instead, which also lets the check compare against `raw_sha256` rather than
merely counting bytes:

```bash
docker exec -w /app cartracker-archiver python -c "
import hashlib
from shared.minio import BUCKET, get_boto3_client, object_exists, read_html
from shared.packfile import read_index_parquet

c = get_boto3_client()
body = c.get_object(Bucket=BUCKET,
    Key='html_packs/detail_page/2026/04/pack-00000.idx.parquet')['Body'].read()

# The deleter walks members in frame order, so the first survivors it saw are
# the ones it deleted.
entries = sorted(read_index_parquet(body), key=lambda e: (e.frame_ordinal, e.offset_in_frame))
gone = [e for e in entries[:150] if not object_exists(e.source_key)]
print(f'{len(gone)} of the first 150 members have no source object left')

for e in gone[:5]:
    data = read_html(e.source_key)
    match = hashlib.sha256(data).hexdigest() == e.raw_sha256
    print(f'  {e.source_key.rsplit(chr(47),1)[-1]}  {len(data):>8} bytes  sha256_match={match}')
"
```

Every line must print `sha256_match=True`, and `gone` must be non-zero — if it
is zero, the apply run did not delete anything and there is nothing to verify
yet. If any read 404s or mismatches, **stop**: that is the failure the whole
plan is built to prevent, and the remaining sources are still intact.

Then check the constraint moved:

```bash
df -i /mnt/data
```

---

## 6. Scale up

One pack at a time, then a month at a time. Long runs must be detached —
**two foreground attempts died with their SSH connection mid-listing**, and
never run bulk object work over an SSH tunnel (~8x slower than in-container).

**A detached run must redirect its own output to a file.** `docker logs` shows
only the container's *main* process — the uvicorn app — so a `docker exec -d`
process never appears there. The CLI calls `logging.basicConfig`, which writes
to stderr and installs no file handler, so with `-d` that output is simply
lost. Detaching and watching `docker logs` are mutually exclusive, and an
earlier revision of this run sheet told you to do both.

```bash
# the whole month, one listing, output captured
docker exec -d -w /app cartracker-archiver sh -c \
  'python -m archiver.processors.delete_packed_source_html \
     --year 2026 --month 4 --max-objects 600000 --max-packs 0 --apply \
     --json-out /tmp/prune_april.json \
     > /usr/app/logs/prune_april.log 2>&1'
```

`/usr/app/logs` is the archiver's own named volume, so the log outlives both
the exec and a container restart.

These operator-named `prune_*.log` files are **not** scraped by Promtail. The
structured app stream is `app.log*`; detached CLI output remains a local
run-sheet artifact until the separate manual-run logging convention is built.

Watch it:

```bash
docker exec cartracker-archiver tail -f /usr/app/logs/prune_april.log
watch -n 300 'df -i /mnt/data'
```

`PACK_PRUNE_PROGRESS_EVERY` (default 1,000) drives both the listing tick and
the deletion counter. At 557,065 objects that is ~557 listing lines; pass
`-e PACK_PRUNE_PROGRESS_EVERY=25000` to the `docker exec` for a calmer log.

**If a run is already detached without redirection**, it is working, just
blind: it writes its JSON at the end, and `df -i /mnt/data` shows inodes
falling meanwhile. Confirm it is alive with

```bash
docker exec cartracker-archiver sh -c \
  'for p in /proc/[0-9]*; do tr "\0" " " < $p/cmdline 2>/dev/null | grep -q delete_packed && echo "RUNNING $p"; done'
```

Do not kill and restart it for the sake of logging — resume is safe, but the
listing costs 12 minutes each time.

> **A hand-run month and a scheduled one must not overlap.** Stage 5 adds a
> single-flight guard that returns 409 while a run is in flight, but it is
> in-process: it sees the HTTP endpoints on one service and **cannot see a
> `docker exec` CLI run**, which cannot see it either. Two packers on the same
> month would compute the same `next_seq` and overwrite each other's packs.
> Nothing is lost — the sidecar/pack disagreement check refuses to delete from
> the result — but that pack is then blocked and the work is wasted. The
> scheduled DAG only ever takes the **oldest eligible closed month**, so while
> you are working the backlog by hand, either stay ahead of it or pause it.
>
> **A deploy now stops these jobs cleanly.** `POST /deploy/start` sets
> `pause_long_jobs` (default true), and a pack or prune returns at its next
> boundary with everything so far durable. Resume is a re-run. Pass
> `{"pause_long_jobs": false}` for a deploy that touches nothing they depend
> on. A **manual CLI run is stopped by this too** — that is the same flag, read
> from the same table.
>
> **Every production deploy declares intent before the build starts.** This is
> broader than pack/prune: it stops new DAG work at the common sensor while the
> operator waits for in-flight queues and service work to drain before Flyway,
> image replacement, or container recreation. `scripts/redeploy.sh` sends
> `/deploy/complete` on exit but does not send `/deploy/start`, so call
> `curl -sf -X POST http://localhost:8060/deploy/start` by hand, require
> `/deploy/status` to report `intent: pending` and `number_running: 0`, and
> confirm the affected services are ready/idle before running the script. If
> start/status fails or omits the running count, do not deploy. The script's
> exit trap releases intent even when the deploy fails.
>
> Keep the window short: the current Airflow deploy-intent sensor times out
> after 600 seconds, so a DAG run waiting through a longer deploy can fail. That
> is a sensor defect to fix separately, not a reason to skip deploy intent.

**Resume is free and safe.** The surviving-object listing is the checkpoint —
an object that is gone is skipped with no request, each object is deleted at
most once, and a fully drained month costs one listing and nothing else. Re-run
the same command after an interruption.

Expected relief, from the plan's measurements:

| month | objects | ~inodes | ~headroom |
|---|---|---|---|
| April | 557,065 | ~1.248M | ~19 days |
| April+May+June | — | ~6.05M | **~92 days** |

May and June need their own runs (`--month 5`, `--month 6`) and are only
eligible once packed — check `html_packs/detail_page/2026/{05,06}/` has
sidecars before running.

---

## 7. July and later months

July is packed only after its calendar month closed plus `PACK_SETTLE_DAYS`,
which is what keeps a month that might still be moving out of scope. Deletion
inherits that automatically: a month with no packs has nothing deletable.

There is **no grace period** — `PACK_DELETE_GRACE_DAYS` defaults to 0, by
decision (see the plan doc). If something ever does surface that a waiting
period would have caught, `--grace-days N` is the lever, measured from the
sidecar's write time.

---

## Abort and recovery

| situation | do this |
|---|---|
| Read path misbehaving, nothing deleted yet | Set `PACK_READ_FALLBACK=0`, redeploy. Previous behaviour exactly. |
| A delete run reports `objects_refused > 0` | It already refused those objects — nothing was lost. Read `failures[]`; each names the artifact and the disagreement. |
| A delete run died mid-way | Re-run the same command. Resume is idempotent. |
| An artifact 404s after deletion | Stop all delete runs. The bytes are in the pack; recover with `read_packed_html`, and check `html_packs/.../pack-*.idx.parquet` still exists. |
| Disk full | Deletes still work — a DELETE is not a PutObject, and MinIO's minimum-free-drive threshold only blocks writes. This job is the recovery lever. |

## Afterwards

- Record the measured latency p50/p95 and the actual inodes freed in
  [Plan 131](plan_131_packed_cold_storage.md). Both are success criteria, and
  the plan has been wrong three times and corrected by measurement every time.
- [Plan 132](plan_132_unrecorded_artifact_recovery.md)'s reparse of the 42,276
  unrecorded April captures reads through the pack path once sources are gone.
  That is unblocked by Stage 3, not by anything in Stage 4.

---

## 8. Stage 5 — the lifecycle DAG

Built 2026-08-17. `pack_bronze_html` runs **`0 6 3 * *`** (day 3, 06:00 UTC) and
does pack → prune → verify against the oldest eligible closed month, on the
`pack-worker` service. Everything above still works by hand and is still how a
backlog month gets done.

**The archiver no longer accepts pack jobs.** `ARCHIVER_ALLOW_PACK_JOBS` is set
only on `pack-worker`; `cartracker-archiver` returns **409** for
`/pack/bronze/*`. The `docker exec cartracker-archiver` commands earlier in this
sheet target the CLI directly, which is unaffected — but any `curl` at the
archiver's pack endpoints must now go to the worker.

### Trigger a specific month

The schedule takes one month per run, so July and the rest of the backlog stay
manual. Trigger with explicit params rather than waiting:

```bash
# Airflow UI: DAGs -> pack_bronze_html -> Trigger DAG w/ config
{"artifact_type": "detail_page", "apply": true, "max_buckets": 1,
 "max_packs": 0, "prune": true, "prune_max_objects": 0, "prune_max_packs": 0}
```

`apply: false` is the dry run — it packs and prunes nothing and returns the
summaries it would have produced.

### Pause it

Two levers, in order of bluntness:

| want | do |
|---|---|
| Stop a run in flight, cleanly | `curl -X POST http://localhost:8060/deploy/start` — both processors stop at their next boundary and return `stopped_for_deploy` |
| Let it resume | `curl -X POST http://localhost:8060/deploy/complete` |
| Stop a deploy from pausing long jobs | `curl -X POST http://localhost:8060/deploy/start -H 'Content-Type: application/json' -d '{"pause_long_jobs": false}'` |
| Stop it for good | Pause the DAG in the Airflow UI |

The stop is **cooperative, not a kill**. The packer finishes the pack it is
writing, stores the sidecar, and skips the tail flush; the deleter stops at a
pack boundary having deleted only verified objects. Everything completed so far
is already durable, so there is nothing to clean up.

A paused run is an ordinary task retry — `retries=6` at 15 minutes gives 90
minutes for a deploy to land. It does **not** page. Exhausting the retries does.

> **A forgotten deploy intent keeps jobs paused.** `long_jobs_paused()` has no
> staleness clause on purpose: the DAG exhausts its retries and pages someone,
> rather than a ten-hour job papering over it. If the pack task fails with
> `deploy intent remained pending through all retries`, check
> `GET /deploy/status` first.

### Re-test the alert

`ct-pack-verification-refused` stays silent until a real failure, so it can rot
unnoticed. To prove it still works, query Loki directly on port 3100 (Grafana is
not port-published) and check both directions — the selector must match live
streams, and the filter must reject them:

```bash
# 1. streams are live -- expect non-empty
curl -sG http://localhost:3100/loki/api/v1/query \
  --data-urlencode 'query=sum by (service) (count_over_time({service=~"archiver|pack-worker"}[10m]))'

# 2. filter is discriminating -- expect [] with totalPostFilterLines: 0
curl -sG http://localhost:3100/loki/api/v1/query \
  --data-urlencode 'query=sum by (service) (count_over_time({service=~"archiver|pack-worker"} |= "REFUSED" [10m]))'
```

For an end-to-end test including Telegram, push a synthetic line — it fires
within ~5 min and auto-resolves ~10-15 min later when the window rolls past:

```bash
curl -sS -X POST http://localhost:3100/loki/api/v1/push \
  -H 'Content-Type: application/json' \
  --data-raw "{\"streams\":[{\"stream\":{\"service\":\"pack-worker\",\"level\":\"ERROR\",\"synthetic\":\"alert-test\"},\"values\":[[\"$(date +%s%N)\",\"delete_packed_source_html: REFUSED SYNTHETIC ALERT TEST $(date -Is) - not a real verification failure\"]]}]}"
```

**Mark every synthetic line.** Loki has no retention yet, so each one is
permanent and will surface in any future `REFUSED` search. See
[Plan 131](plan_131_packed_cold_storage.md#the-alert-validated-in-production--2026-08-17).

### Do not hand-run a month the schedule might also take

The single-flight guard is **in-process on the worker**. A `docker exec` CLI run
is invisible to it and it to the CLI. Two packers on one month overwrite each
other's packs, which the sidecar/pack disagreement check then refuses to delete
from — wasted work and a blocked pack, not lost data, but avoid it. Day 3 of the
month is the window to stay clear of.

### Back it out

| situation | do this |
|---|---|
| The DAG is misbehaving, nothing packed yet | Pause the DAG in the Airflow UI. Nothing else is required — it holds no state. |
| A prune inside a DAG run refused objects | `ct-pack-verification-refused` fires on the first one. Nothing was lost — it refused rather than deleted. Read `failures[]` from the task's XCom `result`, and pause the DAG before pruning again. |
| The canary reports failures | Stop pruning — pause the DAG. The pack read path is the thing Stage 4's safety rests on. Investigate before any further deletion. |
| A run died mid-way | Let the retry run, or re-trigger. Both jobs resume idempotently. |
| The worker itself is the problem | `docker compose stop pack-worker`. The `pack_worker_up` health sensor then holds the DAG at the gate instead of failing tasks. |

### What it does not do yet

- **No scheduled run has completed.** The first is 2026-09-03. Until then the
  measured numbers in [Plan 131](plan_131_packed_cold_storage.md) come from
  manual runs.
- **No inode alert.** That moved to
  [Plan 135](plan_135_storage_observability.md), which must first fix
  node-exporter — it has never reported `/mnt/data` at all. `watch -n 300 'df -i
  /mnt/data'` is still the only inode signal.
