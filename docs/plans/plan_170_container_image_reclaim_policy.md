# Plan 170: Container Image Reclaim Policy

## Status

**Build order, ahead of [Plan 125](plan_125_duckdb_to_iceberg_migration.md).**
Written 2026-09-01 from a read-only pass over production taken while answering a
question about which prune command was safe to run. Image content accumulates in
`/var/lib/containerd` with no retention rule, and the one command that would
reclaim it — `docker image prune -a` — cannot tell an image that is paused by
decision from one that is garbage.

Priority **82 (high)**. Effort **S** — the keep-set derivation is a reading
exercise over a manifest that already exists, and the scheduled job copies a
shape the fleet already runs.

The reclaim policy is not a new idea. `docs/runbooks/runbook_storage_maintenance.md`
already names it as unowned:

> Reclaim policy is undecided and wants its own slice: rollback depends on
> previous images being present, so `docker system prune -a` stays on the §3
> list below.

This plan is that slice. It is sequenced ahead of Plan 125 because Plan 125 is
what productionizes the largest images the fleet has ever carried, and a reclaim
policy written after they land is a policy written under pressure.

## The measurement

Taken 2026-09-01 from the production host, read-only.

```
=== docker info ===
Storage Driver: overlayfs
  driver-type: io.containerd.snapshotter.v1
containerd namespaces: moby, moby_history

=== /var/lib/containerd ===
20G   total
15G   io.containerd.snapshotter.v1.overlayfs/snapshots
4.8G  io.containerd.content.v1.content/blobs
11M   io.containerd.metadata.v1.bolt

=== docker system df ===
TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
Images          35        25        21.03GB   14.72GB (69%)
Containers      30        28        469.8MB   40.33MB (8%)
Local Volumes   29        21        51.78GB   69.28MB (0%)
Build Cache     35        0         2.037GB   530.1MB

=== df ===
/dev/sda1   49G   26G   23G  53%  /
/dev/sdb   196G   55G  131G  30%  /mnt/data
```

**Docker on this host keeps image content in the containerd store.** That is why
`/var/lib/docker` reads 714 MB while `/var/lib/containerd` holds the whole 20 GB,
and why `docker image prune` has always reported "~0 dangling" truthfully. The
storage runbook recorded this on 2026-08-29; it is restated here because it is
the fact that makes every naive reclaim command wrong.

### The ten images no container references

These are exactly what `docker image prune -a` would delete today.

| Size | Image | Classification |
|---:|---|---|
| 2.07 GB | `cartracker-lakehouse:latest` | **`aux-paused`** — Plan 125 |
| 1.31 GB | `cartracker-mlflow:latest` | **`aux-paused`** — Plan 112 Gate B |
| 1.18 GB | `ghcr.io/germondai/trawl:latest` | tag only; the running solver is a different, untagged image |
| 1.09 GB | `ghcr.io/flaresolverr/flaresolverr:latest` | superseded by the running `v3.4.6` |
| 459 MB | `cartracker-dbt_test:latest` | **`on-demand`** — profile-gated `compose run` |
| 275 MB | `grafana/promtail:2.9.8` | superseded by the running `3.5.8` |
| 235 MB | `quay.io/lakekeeper/catalog:v0.13.1` | **`aux-paused`** — Plan 125 |
| 209 MB | `python:3.13-slim-bookworm` | build base |
| 37 MB | `curlimages/curl:latest` | unreferenced |
| 35 MB | `curlimages/curl:8.10.1` | unreferenced |

**Four of the ten are deliberately not running**, and `maintenance-running-set.txt`
already says so in as many words — `cartracker-lakehouse/lakekeeper` and
`cartracker-mlflow/mlflow` are `aux-paused`, `dbt_test` is `on-demand`. Docker
has no access to that distinction. It sees ten images with no container and
treats all ten identically.

That is the whole defect: **the safety information exists, and the tool that does
the deleting cannot read it.**

## Why now, and not after Plan 125

`cartracker-lakehouse:latest` is 2.07 GB today. The deployed copy was built
2026-07-15 and predates Plan 125 Gate A — it carries neither `hadoop-aws` nor
`aws-java-sdk-bundle`, and `/app/dbt/dbt_packages` does not exist, so `dbt deps`
never ran. Rebuilding it from current `master` adds
`aws-java-sdk-bundle-1.12.262.jar` at 280,645,251 bytes, taking the image to
roughly **2.34 GB**.

Plan 125 Gates C and D are what put that image into regular production use, and
Plan 112 Gate B does the same for the 1.31 GB MLflow image. A fleet that gains
two multi-gigabyte images and rebuilds them on an ARM64 host — where rebuilds are
slow — wants its reclaim rule decided beforehand. Deciding it afterwards means
deciding it while `/` is filling.

## The open question Stage 0 must resolve

The storage runbook recorded `/` at **72% with `/var/lib/containerd` at 29 GB** on
2026-08-29. The reading above, three days later, is **53% and 20 GB**. Roughly
9 GB was reclaimed in between and this plan does not know by what. The 2026-08-31
maintenance window is the obvious candidate and has not been checked.

This matters more than the number. If containerd growth is **monotonic**, the
policy is a retention rule. If it is a **sawtooth** that maintenance windows
already flatten, the policy is a smaller thing — a documented step in an existing
procedure — and this plan should shrink accordingly. Writing a retention rule
against an unexplained 9 GB drop is how a plan solves the wrong problem.

## Design

### The keep-set is derived, never enumerated

The reclaim job must not carry its own list of images to protect. A second copy
of the running set is how the first one goes stale — `maintenance-running-set.txt`
makes that argument for itself and this plan inherits it.

The keep-set is therefore: every image referenced by a container, **plus** every
image belonging to a service the manifest classifies `aux-paused`, `on-demand`,
`profile-running` or `aux-foreign`. Anything a new Compose service adds is
protected by default, and has to be named to become reclaimable — the safe
direction for this particular rule to fail.

### Rollback depends on previous images being present

The runbook's reason for leaving `docker system prune -a` on the do-not-run list
is that reverting a bad deploy means starting the previous image, and an image
that has been pruned has to be rebuilt first. Any retention rule this plan writes
has to keep enough history to roll back, and has to say how much and why. "Keep N
per tag" and "keep anything younger than D days" are both defensible; picking one
without naming the rollback window it buys is not.

### `docker system df` is not available as an instrument

It exceeded 120 s in this session's foreground, and the runbook records 5+ minutes
before being killed. Whatever measures the reclaim must not depend on it.
`du -x -d1 /var/lib` and the per-path panels Plan 135 Stage 4 already publishes
are the instruments that work.

## Stages

### Stage 0 — Resolve the trend before writing a rule

Establish what actually happened between 2026-08-29 and 2026-09-01, and whether
`/var/lib/containerd` growth is monotonic or window-sawtooth. Check the
2026-08-31 window's records first; it is the cheapest candidate and it either
explains the 9 GB or eliminates itself.

Then decide the retention rule and the rollback window it buys, and record both
with the measurement they came from.

**Exit:** a written trend verdict, a retention rule, and — if the trend turns out
to be a sawtooth that windows already flatten — an explicit decision to stop here
and fold the remainder into the maintenance procedure instead.

### Stage 1 — Derive the keep-set, and report without deleting

Build the keep-set from container references plus the manifest's non-running
classes. Run it against production in report-only mode across at least one deploy
and one `hourly_analytics_refresh` cycle.

The output that matters is the disagreement: every image `docker image prune -a`
would delete that the keep-set protects, and vice versa. Today that set has four
known members and the job should find exactly those four.

**Exit:** report-only output matching the manifest's classification with no
unexplained entries, across at least one deploy.

### Stage 2 — Schedule it

`prune_task_logs` (Plan 135 Stage 5d) is the shape: a `PythonOperator` behind
`deploy_intent_sensor`, `max_active_runs=1`, tagged `maintenance`/`storage`, on a
weekly off-peak cron. Reuse it rather than inventing a second maintenance idiom.

Schedule clear of `disk_usage`'s Sunday slow walk, which already runs 20+ minutes
against the high-inode volumes.

**Exit:** the job runs on schedule, reclaims what Stage 1 predicted, and
`/var/lib/containerd` is visible in the Plan 135 per-path panels so the effect is
legible without an SSH session.

## Files

- `airflow/dags/` — the scheduled job, in the `prune_task_logs` shape
- `maintenance-running-set.txt` — read, not modified; the classes are the input
- `docs/runbooks/runbook_storage_maintenance.md` — §2's table currently measures
  `/var/lib/docker`; it needs the containerd path and the policy this plan sets
- `archiver/processors/disk_usage.py` — only if `/var/lib/containerd` is not
  already on the watchlist

## Out of scope

- **Making any image smaller.** `cartracker-lakehouse` is 2 GB because PySpark
  bundles 322 MB of jars and Spark needs a 224 MB JRE. Two reductions were
  identified while measuring — dropping `aws-java-sdk-bundle` once Plan 125
  Gate D's reader inventory confirms nothing reads plain `s3a://` Parquet
  (−268 MB), and replacing the lakehouse target's `COPY . .` with the targeted
  copies its own `mlflow` target already uses (−105 MB). **Both belong to
  Plan 125**, not here. This plan governs what is kept, not what is built.
- **Rebuilding the stale lakehouse image.** It predates Gate A and must be
  rebuilt before Gate C/D, which is Plan 125's business.
- **`docker volume prune`, in any form.** `/var/lib/docker/volumes` is a symlink
  to `/mnt/data/docker-volumes` (Plan 105) and volumes are 51.78 GB with 69 MB
  reclaimable. The runbook's prohibition stands and this plan does not touch it.
- **Build cache policy.** 530 MB reclaimable with zero active is a real but
  separate pool with different rollback consequences; fold it in only if Stage 0
  shows it growing.

## Success criteria

1. The retention rule is written down with the rollback window it buys and the
   measurement it came from.
2. The keep-set is derived from `maintenance-running-set.txt`, and adding a
   Compose service protects its image without editing the reclaim job.
3. A scheduled run reclaims image content without deleting any image the manifest
   classifies as `aux-paused` or `on-demand`.
4. `/var/lib/containerd` is readable from the Plan 135 panels, so the next person
   does not need `du` over `/var/lib` to see the trend.

## Intersections

### Plan 142 — host maintenance

Two ways, and they point in opposite directions. `maintenance-running-set.txt` is
Plan 142's artifact and is this plan's central input — without it there is no safe
automated prune. But Plan 142 is in closeout and owes no code, so this plan must
consume the manifest without modifying it. If Stage 0 finds that windows already
flatten the growth, the right outcome is a step in Plan 142's procedure rather
than a new scheduled job, and this plan should say so and stop.

Plan 142's `validate-host` carries a 10 GiB `disk_headroom` floor. `/` had 14 GB
free on 2026-08-31 against a 29 GB containerd store — close enough that the
storage question was recorded as a window finding rather than an observation.

### Plan 125 — the reason for the sequencing

Plan 125 Gates C and D productionize `cartracker-lakehouse`, which is the single
largest image in the fleet and grows by 268 MB on its next rebuild. Plan 125 also
owns the two size reductions listed under Out of scope. This plan does not block
Plan 125's gates; it lands first so that the reclaim rule is decided before the
images it governs become routine.

### Plan 135 — storage observability

Complete, archived 2026-08-23. This plan inherits two of its artifacts: the
`prune_task_logs` DAG as the shape to copy, and the Stage 4 per-path disk panels
as where the result becomes visible. Plan 135's own scope was making storage
*legible*; reclaiming it was never in it.

### Plan 152 — scheduled worker lifecycle

Plan 152 owns one-shot execution and the narrow launch authority. If it lands
first, Stage 2 should use that mechanism rather than a `PythonOperator`. It is not
a blocker in either direction — the job is small enough to port.
