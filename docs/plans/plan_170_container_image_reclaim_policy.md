# Plan 170: Container Image Reclaim Policy

## What this plan is for

Docker build cache grows about 780 MB a day on the production host and nothing
reclaims it. Makes each deploy discard the cache it just produced, rather than
scheduling a job. Images need no retention rule; what protects the deliberately
paused ones needs testing and a written home.

## The case

Written 2026-09-01 from a read-only pass over production taken while answering a
question about which prune command was safe to run. Image content accumulates in
`/var/lib/containerd` with no retention rule, and `docker image prune -a` cannot
tell an image that is paused by decision from one that is garbage.

The reclaim policy is not a new idea.
`docs/runbooks/runbook_storage_maintenance.md` already names it as unowned:

> Reclaim policy is undecided and wants its own slice: rollback depends on
> previous images being present, so `docker system prune -a` stays on the §3
> list below.

This plan is that slice. It is sequenced ahead of
[Plan 125](plan_125_duckdb_to_iceberg_migration.md) because Plan 125 is what
productionizes the largest images the fleet has ever carried, and a reclaim
policy written after they land is a policy written under pressure.

### The measurement

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
| 2.07 GB | `cartracker-lakehouse:latest` | **`on-demand`** — its Compose label says `lakehouse-worker`, not one of the `aux-paused` lakekeeper entries. Protected either way; corrected 2026-09-08 |
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

### Why now, and not after Plan 125

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

### The trend, resolved 2026-09-01

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
scheduled policy replaces — and it means the Stage A escape hatch that would have
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

### ~~Rollback depends on previous images being present~~ — it does not, today

**Corrected by measurement 2026-09-08.** The premise was that reverting a bad
deploy means starting the previous image, so a retention rule has to keep enough
history to do it. `docker image ls -a -f dangling=true` returns **zero images**:
Docker 29's containerd store drops the old manifest when a tag moves, so there
are no untagged predecessors and there never have been.
`cartracker-archiver:latest` was rebuilt 2026-09-07 19:25 and its predecessor is
gone, not dangling.

The rollback window is therefore **zero, and no retention rule can raise it** —
retention has no input until something creates one. Buying a rollback window
means tagging in `scripts/deploy.sh` first, which is a separate decision and
deliberately not this plan's. What the runbook's prohibition on
`docker system prune -a` actually protects is the `aux-paused`/`on-demand` set,
not a rollback history that does not exist.

### `docker system df` is not an instrument — but `?type=` is

**Corrected by measurement 2026-09-08.** Bare `docker system df` exceeded 120 s
here and the runbook records 5+ minutes; that cost is the *volume* walk
(`/mnt/data`, ~4M inodes). Scoped by type against the daemon socket it is fast
enough to call on any schedule:

| Endpoint | Elapsed | Returns |
|---|---:|---|
| `GET /system/df?type=build-cache` | **0 s** | per-entry `Size`, `CreatedAt`, `LastUsedAt`, `UsageCount` |
| `GET /system/df?type=image` | **1 s** | 35 images with sizes and container refcounts |

This plan does not end up needing either — the rules it sets are self-derived
(see Stage A) — but the note is recorded because "measuring Docker is too
expensive here" was load-bearing in the original design and is false as stated.

**For seeing whether the policy works, the instrument already exists.**
`/var/lib/containerd` is on the `disk_usage` watchlist
(`archiver/processors/disk_usage.py:55`, Plan 135 Stage 4), walked daily and
published to a per-path panel. It cannot separate the two pools — image layers
and BuildKit cache share `io.containerd.snapshotter.v1.overlayfs/snapshots`, so
there is no directory boundary for `du` — but it shows the total stepping down,
which is what success looks like.

### Files

- `scripts/deploy.sh`, `scripts/redeploy.sh` — the post-build cache prune. These
  two lines are the **entire** set of things that build on this host; CI builds
  on GitHub runners and the `compose run` profiles use already-built images
- `maintenance-running-set.txt` — read, not modified; the classes are the input
- `docs/runbooks/runbook_storage_maintenance.md` — §2's table currently measures
  `/var/lib/docker`; it needs the containerd path, the policy this plan sets, and
  the derived keep-set block
- `tests/` — the static keep-set derivation and the assertion that the runbook's
  rendered list matches it
- `archiver/processors/disk_usage.py` — **read only.** `/var/lib/containerd` is
  already on the watchlist at line 55; this plan consumes that series and adds
  nothing to it

### Out of scope

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

## Stages

Sequenced before [`docs/PLAN_DOCUMENT.md`](../PLAN_DOCUMENT.md) landed; adopted
stage letters on 2026-09-07. No Linear issue carries a legacy number yet:

| Legacy | Stage | | Legacy | Stage |
|:---:|:---:|---|:---:|:---:|
| 0 | **A** | | 2 | **C** |
| 1 | **B** | | | |

**B and C were redefined on 2026-09-08** by Stage A's outcome — the scheduled
job the legacy stage 2 described does not get built. The letters are kept
because Linear issues already carry them.

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a--decide-the-two-retention-rules) | Two retention rules, one per pool, each with its measurement | `done` | CAR-94 |
| 2 | [**B**](#stage-b--make-the-deploy-clean-up-after-itself) | The post-build cache prune, in both build paths | `done` | CAR-103 |
| 3 | [**C**](#stage-c--make-the-keep-set-legible-and-do-the-one-time-image-sweep) | The tested keep-set, the runbook block, and the one-time image sweep | `done` | CAR-112 |

### Stage A — Decide the two retention rules

**Decided 2026-09-08.** Both rules, their measurements and the reasoning are in
[`## Record`](#stage-a--decide-the-two-retention-rules-2026-09-08). In short:
build cache is bounded by a 4 GB size cap pruned by the deploy that produced it;
images get no automated retention at all, because they do not accumulate and the
rollback window a retention rule would protect does not exist.

**Exit:** two retention rules, each with the measurement it came from, and — for
the image rule — the rollback window it buys. **Met**, with the image half
answering "zero, and not purchasable by retention".

### Stage B — Make the deploy clean up after itself

Build cache is ~90% of the growth and is produced only by builds, and the build
paths on this host are exactly two lines: `scripts/deploy.sh:25` and
`scripts/redeploy.sh:617`. So the reclaim belongs to the producer, not to a
clock.

~~Add `docker builder prune --keep-storage 4GB -f` after each~~ — **the size cap
was removed on 2026-09-08 after two production runs showed it enforcing
nothing.** What ships is `docker builder prune -a -f`: the cache is discarded
whole, positioned **after health verification** and made non-fatal, so a prune
failure can never fail a deploy. The measurements are in
[`## Record`](#stage-b--make-the-deploy-clean-up-after-itself-2026-09-08); the
short version is that nothing in production reads build cache, so there is
nothing for a cap to protect.

Stage B also grew a **pre-flight disk guard**, which the stage as written did
not anticipate. A build that would run `/` below the reviewed floor does not
start. It is scoped here rather than in its own plan because it is the same two
scripts, the same session and the same measurements.

**Exit** — restated 2026-09-08, because the original named a quantity this plan
no longer sets:

1. A deploy leaves the build cache at **zero**.
2. The next deploy builds cold without meaningfully lengthening the deploy —
   measured as operator wall-clock, since the build runs *before*
   `_prepare_coordination` and so is not paid in parked DAGs.
3. `/var/lib/containerd` turns over from a ramp to a sawtooth on the panel.

~~a deploy leaves the build cache at or under 4 GB~~ and ~~the next deploy still
hits cache for the layers it did not change~~ are struck: the first names the
cap that was removed, and the second asks whether a cache we now discard on
purpose was retained.

### Stage C — Make the keep-set legible, and do the one-time image sweep

The keep-set needs no runtime job. Compose already stamps
`com.docker.compose.project` and `com.docker.compose.service` on all 12 images
this repo builds, and `maintenance-running-set.txt` is keyed on exactly
`project/service` — so the join is static, computable from
`docker-compose*.yml` plus the manifest, and needs no Docker and no socket.

Two things it cannot derive, both handled by naming:

- **Shared images.** `cartracker-archiver` is built by three services and
  `cartracker-airflow` by four; the label records whichever built it last, so the
  join must run image → all services, never label → class.
- **Third-party images carry no labels and cannot be given any.** Exactly one
  matters: `quay.io/lakekeeper/catalog:v0.13.1`, `aux-paused` for Plan 125 and
  unprotectable by any derivation.

Render the derived keep-set into `runbook_storage_maintenance.md` as a fenced
block and **have the test assert the block matches the derivation**. An
unchecked list would be the second copy this plan's own design section forbids,
and the Plan 138 projection is the precedent for one going stale in silence.

Then the one-time sweep of the genuinely unreferenced images — roughly 2.8 GB
across `flaresolverr:latest`, `promtail:2.9.8`, both `curl` tags,
`trawl:latest`, `alpine:latest`, and `python:3.13-slim-bookworm` if the re-pull
is acceptable. Reviewed and manual, not scheduled: it is a one-off, because
images have not accumulated.

**Exit:** the test fails when a new Compose service or manifest entry desyncs the
runbook block, and the sweep has run without touching anything the manifest
classifies `aux-paused` or `on-demand`.

## Success criteria

1. Both retention rules — images and build cache — are written down with the
   measurement each came from, and the image rule names the rollback window it
   buys. **Met 2026-09-08.**
2. The keep-set is derived from `maintenance-running-set.txt` and the Compose
   sources, and adding a Compose service changes the derived set without anyone
   editing a list.
3. The runbook's rendered keep-set cannot go stale without CI going red.
4. The `/var/lib/containerd` panel shows the store stepping down on each deploy
   instead of climbing to the next manual intervention.

## Intersections

### Plan 142 — host maintenance

Two ways, and they point in opposite directions. `maintenance-running-set.txt` is
Plan 142's artifact and is this plan's central input — without it there is no safe
automated prune. But Plan 142 is in closeout and owes no code, so this plan must
consume the manifest without modifying it. If Stage A finds that windows already
flatten the growth, the right outcome is a step in Plan 142's procedure rather
than a new scheduled job, and this plan should say so and stop.

**A variant of that escape hatch fired on 2026-09-08.** Not the one anticipated
— maintenance windows do not flatten the growth — but the same shrink: the
producer flattens it, so no scheduled job gets built. The manifest stays a
read-only input and Plan 142 is still owed nothing.

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

Complete, archived 2026-08-23. This plan inherits its Stage 4 per-path disk
panels as where the result becomes visible. It no longer inherits the
`prune_task_logs` DAG shape — Stage A's outcome is that no scheduled job gets
built. Plan 135's own scope was making storage *legible*; reclaiming it was
never in it.

### Plan 152 — scheduled worker lifecycle

~~Plan 152 owns one-shot execution and the narrow launch authority.~~ **Moot as
of 2026-09-08.** Stage A's outcome is a line in the deploy scripts, not a
scheduled job, so there is nothing here for Plan 152's launch mechanism to
carry. No intersection in either direction.

## Record

### Stage A — Decide the two retention rules (2026-09-08)

Both rules decided from a read-only pass over the production host, taken
2026-09-08 04:05–04:40 UTC. The stage's own method — a week of build-cache
regrowth curve — was not used: a size cap is chosen from disk headroom rather
than from a growth rate, so the curve it needed was not the measurement the
rule wanted.

**Rule 1 — build cache: a 4 GB cap, pruned by the deploy that produced it.**
`docker builder prune --keep-storage 4GB -f`, after health verification in both
build paths, non-fatal.

Build cache grew 2.04 → **7.52 GB in 8 days** (~783 MB/day), against images that
stayed flat, so it is ~90% of all growth. It is produced only by builds, and
`scripts/deploy.sh:25` and `scripts/redeploy.sh:617` are the *entire* set of
things that build on this host — CI builds on GitHub runners and the `compose
run` profiles use already-built images. Producer and reclaim are therefore the
same event, and a weekly job would run against a pool that only changes when
someone deploys.

Post-build is the correct moment because **`LastUsedAt` refreshes on cache hit,
not just creation**: 28 of 108 entries carry a gap over an hour between
`CreatedAt` and `LastUsedAt`, up to 191 hours, with `UsageCount` reaching 34 —
including a 442 MB pip layer created 5.7 days before its last use. Everything a
finished build touched is stamped *now*, so the live working set survives and
the superseded tail does not.

A size cap rather than `--filter until=Nh` because the age distribution is
deploy-shaped, not smooth — `until=72h` would reclaim 6.83 GB today and 0 GB
four days ago, and the 3d→5d cliff is a single build burst on 09-02/04. The cap
is picked from headroom (19 GB free on `/`), which is the quantity that
actually matters, and self-tunes against a cadence no day-count can anticipate.

**Rule 2 — images: no automated retention.** They do not accumulate: 21.03 →
20.97 GB across the same 8 days, and `docker image ls -a -f dangling=true`
returns **zero**. Docker 29's containerd store drops the old manifest when a tag
moves, so there are no untagged predecessors — `cartracker-archiver:latest` was
rebuilt 2026-09-07 19:25 and its predecessor is gone, not dangling.

**The rollback window the image rule buys is zero, and no retention rule can
raise it.** Retention has no input until something creates a predecessor;
buying a window means tagging in `scripts/deploy.sh` first, which is a separate
decision and deliberately not this plan's. The unreferenced set is ~2.8 GB and a
one-time reviewed sweep, not a recurring flow.

**Three corrections to this plan's premises.**

| Premise as written | What the measurement says |
|---|---|
| "Rollback depends on previous images being present" | Zero dangling images; the window is already zero and unbuyable by retention |
| "`docker system df` is not available as an instrument" | True bare (5+ min, the volume walk), false scoped: `?type=build-cache` returns in 0 s and `?type=image` in 1 s |
| "the safety information exists, and the tool that does the deleting cannot read it" | Half wrong. Compose already stamps `com.docker.compose.project`/`service` on all 12 images this repo builds, and `maintenance-running-set.txt` is keyed on exactly `project/service`. The join key exists on both sides and nothing had used it |

The third is why Stage C needs no runtime job and no socket grant. It survives
for third-party images only, which carry no labels and cannot be given any —
exactly one matters, `quay.io/lakekeeper/catalog:v0.13.1`, `aux-paused` for
Plan 125 and unprotectable by any derivation.

Also corrected: the plan's ten-image table classified
`cartracker-lakehouse:latest` as `aux-paused`; its Compose label says
`lakehouse-worker`, which the manifest classifies `on-demand`. Protected either
way.

**Reproduction.** All read-only, from the production host:

```
curl -s --unix-socket /var/run/docker.sock \
  'http://localhost/v1.52/system/df?type=build-cache'   # sizes + LastUsedAt
docker image ls -a -f dangling=true                      # the rollback set
docker image inspect <img> --format \
  '{{index .Config.Labels "com.docker.compose.project"}}' # the join key
```

Reading the published `.prom` pulled `alpine:latest` (12 MB) onto the host as a
side effect; it is unreferenced and folded into Stage C's sweep.

**Cost:** estimate 2 → actual 1.

**Public surfaces: yes, and small.** This plan's `## What this plan is for` is
published — Plan 154's move to closeout removed build-order row 3 on 2026-09-07,
renumbering rows 4–22 and promoting Plan 170 into the top four that
`project-updates.json` carries. The summary as it stood promised to "report what
it would delete before deleting anything" and to set retention rules for images,
both of which Stage A eliminated. Rewritten to 299 characters against the
320-char cap that applies inside the published window, and the projection
regenerated to match.

That promotion is also why this entry first recorded "no": it was verified
against this branch's copy of `project-updates.json`, which predated the
renumbering. The published set can change without this plan's row being edited,
so the check has to read a freshly fetched `master`, not the working tree.


### Stage B — Make the deploy clean up after itself (2026-09-08)

Shipped, deployed and verified the same day, across four PRs — #390, #391,
#392 and #397 — because the rule Stage A decided did not work and each failure
had to be measured before the next attempt.

**What ships.** `docker builder prune -a -f` in both build paths, after health
verification, guarded by `|| echo`. Plus a pre-flight check that refuses to
start a build when `/` is below `HOST_DISK_FLOORS["bytes_available"]`.

**Stage A's size cap is superseded. It never enforced anything, at any value.**

| Run | Command | Result |
|---|---|---|
| 1 | `--keep-storage 4GB -f` | 7.52 → 5.72 GB, **cap never reached** |
| 2 | `-a --keep-storage 4GB -f` | 6.00 GB resident, **0 B reclaimed** |
| 3 | `-a -f` | 6.00 GB → **0**, 6.381 GB reclaimed |

Run 1 is explicable: without `all`, BuildKit's sweep skips `internal`,
`frontend` and *shared* records, and 1.46 GB of the survivors were
`Shared=True`, so the eligible set ran out above the cap. **Run 2 is not.**
`all` should have made the whole 6.00 GB eligible against a 4 GB cap.
`cache/manager.go` says `keepBytes` binds (`if opt.keepBytes != 0 &&
opt.totalSize < opt.keepBytes { return }`) and that eviction is least-recently-
used first, which predicts ~2 GB reclaimed. Nothing was.

No model fits both runs. Rather than keep changing flags against production,
the policy stopped depending on the flag. `test_no_size_cap_came_back` carries
both numbers in its failure message, because a cap reads as obviously prudent
and is exactly what someone reintroduces in good faith.

**The cache was worth about two seconds.** Cold build 23.7s against 21.7s warm,
for `ops`. The fastest `ops` build ever observed was 5.9s, but that was a
redeploy with no code change; on any deploy that ships something, `COPY . .`
invalidates and the difference is ~2s. Notably `pip install` did not hit cache
in the warm run either — it precedes `COPY . .` and should have — which,
alongside `WARN: Docker Compose is configured to build using Bake, but buildx
isn't installed`, suggests the classic builder's cache reuse on this host is
poor generally. **The build cache here was buying very little under any
policy**, which is the finding underneath the finding.

**Result on the host.** `/` 63% → **50%**, free 19 → **25 GB**,
`/var/lib/containerd` 24,926 → **18,814 MiB**. Free space now returns to the
same floor after every deploy instead of declining; the sawtooth is visible
without waiting for the panel.

**The disk floor is measured, not inherited.**

| | Consumed |
|---|---:|
| CI, x86, no images at all | 4.48 GiB (ceiling) |
| Production, ARM64, bases held | **4.22 GiB** (2m13s, 13 services) |

The runner over-estimates by ~6%, so it bounds the host tightly enough to watch
per-PR. `HOST_DISK_FLOORS["bytes_available"]` is 2.4x the measured worst case,
~1.6x once Plan 125 returns `cartracker-lakehouse`.

**That measurement corrected the premise the guard was argued from.** The
stated risk was images doubling — new layers written while the old ones stay
resident. That is not what happens: of the host's 4.22 GiB, **3.65 GiB was
build cache** and new image layers added ~0.6 GB, because most rebuilt layers
deduplicated against ones already present. The guard is still right; the reason
given for it was wrong.

**Two things this stage got wrong and had to fix.**

The CI footprint job shipped claiming it would catch `cartracker-lakehouse`
growing under Plan 125. It cannot: `docker compose build` reads
`docker-compose.yml`, and lakehouse and mlflow live in their own compose files,
so the largest image in the fleet is exactly what that instrument cannot see.
Now recorded as a named blind spot for Gate C to inherit.

`test_falling_below_the_floor_refuses_rather_than_warns` was **blind when
written** — a file-wide search for `exit 1|return 1`, which both scripts carry
elsewhere, so a guard changed to return success still passed. Mutation testing
caught it; reading would not have.

**Reproduction.** All read-only except the deploys themselves:

```
curl -s --unix-socket /var/run/docker.sock \
  'http://localhost/v1.52/system/df?type=build-cache'    # size, Shared, LastUsedAt
df -B1 --output=avail /                                   # sampled every 2s during a build
sudo du -s -x --block-size=1M /var/lib/containerd
```

**A side effect Stage C inherits.** A full-fleet `docker compose build` was run
on the host to measure the ARM64 footprint. It moved every `:latest` tag, so
the previous generation of all 13 images was orphaned on top of the 11
unreferenced images already there (6.92 GB, of which ~4.07 GB is deliberate
`aux-paused`/`on-demand`). Stage C's sweep set is therefore larger than the
~2.8 GB this plan estimated, and should be re-measured rather than trusted.

**Cost:** estimate 1 → actual 1.

**Public surfaces: yes.** First recorded here as "no", on the reasoning that the
summary described the policy above the level the cap change reached. That was
wrong — the published sentence said the deploy prunes the cache it produced
*"under a size cap"*, and Stage B removed the cap, so the roadmap was carrying a
promise this stage had just falsified. Caught while writing this entry, by
reading `project-updates.json` rather than trusting the reasoning.

`under a size cap` is struck and the projection regenerated. This is the second
consecutive Stage of this plan to get its public-surface answer wrong on the
first pass, in opposite directions: Stage A said "no" against a stale copy of
the projection, and Stage B said "no" against a stale reading of its own
summary. **The check is to open the published file and read the sentence**, not
to reason about whether the change was big enough to reach it.

### Stage C — Make the keep-set legible, and do the one-time image sweep (2026-09-08)

**The sweep ran on the production host at 19:04 UTC. 36 → 29 images, `/` 50% →
45%, and `/var/lib/containerd` 18,798 → 16,285 MiB — 2,513 MiB reclaimed.**
Seven images removed by name with `docker image rm`, never a prune, so nothing
unnamed could be caught: `ghcr.io/germondai/trawl:latest` (1.18 GB),
`ghcr.io/flaresolverr/flaresolverr:latest` (1.09 GB), `grafana/promtail:2.9.8`
(275 MB), `python:3.13-slim-bookworm` (209 MB), both `curlimages/curl` tags
(37.4 + 35.4 MB) and `alpine:latest` (13.6 MB).

**The reclaim is 195 MiB short of the images' reported sizes** — 2,513 against
2,708 MiB — because `docker image ls` bills a shared layer to every image
holding it. The two `curl` tags overlap almost entirely and the two `promtail`
versions partly; the sum was never going to be the reclaim.

**Nothing the manifest protects was touched, verified after rather than
assumed:** `cartracker-lakehouse`, `cartracker-mlflow`, `cartracker-dbt_test`
and `quay.io/lakekeeper/catalog:v0.13.1` are all still present, and the fleet is
28 running plus the two `oneshot` containers exited 0.

**Stage B's warning about a larger sweep set did not hold, and re-measuring is
why it is known.** It expected 11 unreferenced images at 6.92 GB plus a whole
orphaned generation from the full-fleet build. The host carried **13**
unreferenced images and the orphaned generation was not among them: the
containerd store had already dropped those manifests when the tags moved, which
is Stage A's own finding arriving from the other direction. The garbage half
came back to ~2.84 GB, the figure this plan first estimated.

**Two of the 13 were not garbage, and no static derivation can see why.**
`cartracker-airflow:latest` (3.07 GB) and `cartracker-container-health:latest`
(264 MB) were built by Stage B's footprint measurement and never started, so
their containers still pin the *previous* image IDs — which are themselves
untagged. `docker image prune -a` today would have deleted the newest airflow
image and kept the superseded one. Both were excluded from the sweep, and the
case is written into the runbook rather than the derivation, because it is a
fact about container state and not about Compose.

**Reproduction.** Before and after, from the production host:

```
docker ps -a -q | xargs -r docker inspect --format '{{.Name}} {{.Image}}'
docker image ls -a --no-trunc --format '{{.ID}} {{.Repository}}:{{.Tag}} {{.Size}}'
sudo du -s -x --block-size=1M /var/lib/containerd
df -h /
```

The unreferenced set is the second list minus every image ID appearing in the
first. The sweep itself:

```
docker image rm ghcr.io/germondai/trawl:latest \
  ghcr.io/flaresolverr/flaresolverr:latest grafana/promtail:2.9.8 \
  python:3.13-slim-bookworm curlimages/curl:latest curlimages/curl:8.10.1 \
  alpine:latest
```

**For the exit:** this discharges the second clause — the sweep has run without
touching anything the manifest classifies `aux-paused` or `on-demand`. The first
clause, the test failing when the runbook block desyncs, is the stage's other
half and landed in `21804a0`.

**The keep-set is derived, not typed, and the runbook block is asserted against
the derivation.** `tests/test_image_keep_set.py` joins `docker-compose*.yml` to
`maintenance-running-set.txt` on `project/service` — no Docker and no socket, as
Stage A measured — and renders the seven images no plain `docker compose up -d`
materialises a container for. The block lives in §2 of
`runbook_storage_maintenance.md`; the test fails when the two disagree.

**The join runs image → services, never service → image**, which is the hazard
Stage A named. `cartracker-archiver` is built by three services and
`cartracker-airflow` by five, so one running service protects the whole image
and one paused service protects it against every running sibling. Five, not the
four this plan named — `airflow-init` declares the same build, and a join
counting only the services that stay up would have got that image right for the
wrong reason.

**Three mutations, each noticed** — the exit's first clause demonstrated rather
than asserted:

| Mutation | |
|---|---|
| a new profile-gated Compose service | 3 failed |
| a manifest class flipped `on-demand` → `oneshot` | 2 failed |
| a line deleted from the rendered block | 1 failed |

The harness was ad hoc rather than added to
`scripts/verify_testing_contract_mutations.py`: that script's subject is
`tests/test_testing_contract.py`, and widening it is not this stage's business.

**Verified in CI on PR #400** — 12 jobs pass, 2 path-skipped. Locally 3,856 unit
tests pass and `ruff` is clean.

**Both exit clauses are therefore met**: the runbook block cannot desync without
CI going red, and the sweep ran without touching anything the manifest
classifies `aux-paused` or `on-demand`.

**Cost:** estimate 1 → actual 1.

**Public surfaces: no** mechanism, name or quantity either surface states was
changed by this work. `README.md:399` links the storage runbook but restates
nothing from it, and neither surface names an image, a prune policy or a
containerd quantity.
