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

## The trend, resolved 2026-09-01

The storage runbook recorded `/` at **72% with `/var/lib/containerd` at 29 GB** on
2026-08-29. The reading above, three days later, is **53% and 20 GB**. The ~9 GB
reclaimed in between was **a manual `docker builder prune`**, confirmed by the
maintainer and corroborated in `~/.bash_history`:

```
cd /opt/cartracker
sudo docker builder prune          # after `tmux attach -t plan145-stage-6`
sudo docker builder prune -a       # near the plan158-decoy work, before `tmux new -s plan147`
```

The history file carries no timestamps, but Plan 147 landed 2026-08-30 and the
decoy work is the 2026-08-29 deploy hang, which brackets the `-a` to 2026-08-29
or 08-30. **No `docker image prune` or `docker system prune` appears anywhere in
the file.**

Two conclusions, and the second was not anticipated when this plan was drafted.

**The trend is monotonic, not sawtooth.** Nothing scheduled reclaims this space.
The apparent drop was a person intervening by hand, which is precisely the toil a
scheduled policy replaces — and it means the Stage 0 escape hatch that would have
shrunk this plan into a step in Plan 142's procedure is **closed**.

**There are two monotonic pools and build cache is the faster one.** BuildKit
cache lives in the same containerd snapshotter, which is why `/var/lib/containerd`
fell 9 GB without a single image being deleted. It has regrown from approximately
zero to **2.04 GB in about two days** — roughly 1 GB/day, against images that only
shrink when somebody removes them explicitly. Build cache was scoped out of this
plan's first draft on the assumption it was small; the measurement says otherwise
and it is in scope.

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

### Stage 0 — Decide the two retention rules

**The trend half of this stage is already answered** — see "The trend, resolved
2026-09-01" above. Growth is monotonic in both pools, nothing scheduled reclaims
either, and the plan does not shrink.

What remains is the rule, and there are two of them because the pools have
different rollback consequences:

- **Images.** Keep enough history to roll back a bad deploy without a rebuild,
  which on this ARM64 host is slow. Name the rollback window the rule buys.
- **Build cache.** Nothing rolls back to a build cache entry; the cost of
  discarding it is a slower next build, not a blocked recovery. It can therefore
  be far more aggressive than the image rule, and the two should not share a
  threshold merely because they share a directory.

Measure the build-cache regrowth curve over at least one week before fixing its
threshold. The ~1 GB/day figure is derived from two endpoints and an undated
history entry; it is enough to put build cache in scope and not enough to size a
rule.

**Exit:** two retention rules, each with the measurement it came from, and — for
the image rule — the rollback window it buys.

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
- **Rebuilding the build cache deliberately.** This plan discards cache; warming
  it, or deciding a build should be cached differently, is not its business.

## Success criteria

1. Both retention rules — images and build cache — are written down with the
   measurement each came from, and the image rule names the rollback window it
   buys.
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
