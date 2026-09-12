# Plan 183: the MinIO image, held where we control it

## What this plan is for

Keeps this project's object store running after its vendor withdrew the public
MinIO image: the exact build production already runs, copied into this
repository's own container registry and pinned by digest, so production and CI
pull an image nothing outside the project can remove.

## The case

**On 2026-09-11 Docker Hub began returning 404 for `minio/minio` itself — the
repository, not a tag.** All three live references are unpinned `:latest`:
`docker-compose.yml` (production), `docker-compose.lakehouse.ci.yml` and
`docker-compose.lakehouse.local.yml`. Six heavy CI jobs died at image pull on
runs `34641922853` and `34647407605` — `dbt model tests`, `dbt build against a
production snapshot`, `SQL + Airflow metadata contracts`, `Lake integration
tests (MinIO)`, `SQL execution coverage` and `Test invocation coverage` — with
`pull access denied for minio/minio, repository does not exist`. The same
workflow was green on `8b50e4e` about ninety minutes earlier. Master's last CI
run (`34610142223`, 2026-09-11 14:26 UTC) predates the withdrawal, so master has
not shown it yet; its next run will. This plan was raised from
[CAR-134](https://linear.app/cartracker/issue/CAR-134), which was filed ahead of
it because the remediation could not wait for a document.

**It blocks more than itself.** Plans 180, 181 and 182 hold the top three
build-order positions, and every one of their stages lands through heavy CI.
None of them can merge green until the image resolves. Production is not at
risk today — the VM holds its image locally — but the next container recreate
that pulls, or any rebuild of the VM, fails the same way CI does.

**The build production runs is still obtainable, exactly.** Production runs
`minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
(`RELEASE.2025-09-07T16-13-09Z`; read from `docker inspect` on the VM,
2026-09-11). Three copies of MinIO were checked the same day:

| Source | Has production's digest | Notes |
|---|---|---|
| the VM's local store | yes | arm64 only — the VM is `aarch64`, CI runners are amd64 |
| `quay.io/minio/minio` | **yes** (HTTP 200, full multi-arch index) | no `latest`, nothing newer than 2025-09-07 |
| `mirror.gcr.io/minio/minio` | no (404) | holds only the `-cpuv1` variant of the same release, `sha256:13582eff…`, a different build |

Neither registry that still serves it is ours. quay.io's frozen tag list is
consistent with the community images having stopped rather than moved, and
mirror.gcr.io is a pull-through cache that makes no retention promise. Either
could disappear the way Docker Hub did, and a floating reference would learn
about it the same way this one did: from six red jobs.

**Copying it is the fix; pinning it is what stops a repeat.** The repository is
public, so a copy under `ghcr.io/whitewalls86/` can be a public package: no pull
secret on the VM, no `docker login` in CI, no cost. A digest in a registry we
own cannot be withdrawn or retagged by anyone else.

**What this plan is not.** It buys time, not a future. The build is frozen at
2025-09-07 and will receive no security fixes. Replacing the store is
[Plan 184](plan_184_replace_minio_with_garage.md). The time this buys is what
lets Plans 180–182 finish first, which was the sequencing decided on
2026-09-11: this plan now, the testing chain next, the store replacement after
it, and Plan 125's Iceberg work last.

## Design

**Copy the build production runs, not a nearby one.** The source is
`quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
the multi-arch index production already runs. The copy must preserve that
digest byte for byte, so production's image ID does not change and the only
thing that moves is where the image is pulled from.

**The destination is `ghcr.io/whitewalls86/minio`, tagged
`RELEASE.2025-09-07T16-13-09Z`, published public.** The repository is public,
so the package costs nothing and needs no pull secret on the VM and no
`docker login` in CI.

**The copy is a `workflow_dispatch` workflow, not a command someone ran once.**
It authenticates with `GITHUB_TOKEN` under `packages: write`, copies with a
digest-preserving tool (`skopeo copy --all` or `crane copy`), and fails unless
the pushed digest equals the source's. A workflow leaves a run record, can be
re-run, and needs no personal token; a command from a laptop leaves none of
those. Stage D generalises it to take any image.

**References name the tag and the digest:**
`ghcr.io/whitewalls86/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493…`. The
tag is for a reader; the digest is what cannot move. There are three live
references: `docker-compose.yml`, which CI's `postgres minio` jobs also read
through the `docker-compose.ci.yml` overlay (the overlay overrides environment
only), `docker-compose.lakehouse.ci.yml`, and `docker-compose.lakehouse.local.yml`.

**Production takes it through `scripts/redeploy.sh minio`,** which recreates
and health-gates the one service. Expect a brief MinIO restart; the image ID
should read `14cea493…` before and after.

**CAR-134's fitness question is answered here, not assumed.** The frozen
community build is fit for production in the short term, because it is the
build production has been running, and unfit in the long term, because it
receives no security fixes. That is [Plan 184](plan_184_replace_minio_with_garage.md)'s
case, and Stage C's record entry says so.

**Stage D widens the lesson to every external image, and the rule is
ownership, not pinning.** A digest pin alone would not have saved this
repository: once Docker Hub deleted the repository, a digest reference into it
returned 404 as well. Pinning prevents silent drift; only a copy we own
prevents disappearance. So the rule is that every image not built from this
repository is pulled from `ghcr.io/whitewalls86/*` by digest, reading the
default of a `${X_IMAGE:-…}` reference. Measured 2026-09-11: 18 distinct
external image references across the compose files, one digest-pinned
(`trawl`, on another account's registry), none owned. After Stage C the ledger
is seeded at the remaining 17, keyed on the full image reference, so changing
a reference removes its entry and the new one must be owned: any stage that
touches an image drains it on the way through, and Plan 184's Garage image
enters already vendored. **What nobody touches is drained by
[Plan 180](plan_180_seam_program.md) Stage K**, whose subject, compose as the
whole truth about configuration, this is; the rule files under seam 7 in Plan
180 Stage G's restructure. Each drain is the Stage D workflow plus one pin. The
rule lands in
`tests/rules/` and moves with [Plan 181](plan_181_rule_readers_are_code.md)'s
relocation of rule readers.

**Rejected on the way here:**

| Alternative | Why not |
|---|---|
| Pin quay.io's digest directly | the same bet Docker Hub was: a registry we do not control, publishing nothing newer than 2025-09-07 |
| mirror.gcr.io's `RELEASE.2025-09-07T16-13-09Z-cpuv1` | a different build from production's, held by a pull-through cache with no retention promise |
| Push the VM's local image | arm64 only; CI runners are amd64 |
| Build MinIO from source | larger, and changes the build production runs |
| Replace MinIO now | [Plan 184](plan_184_replace_minio_with_garage.md), an XL plan sequenced after Plans 180–182 |
| Pin every external image by digest, in place | a pin into a registry that deletes the repository fails exactly as `:latest` did |

## Stages

**Stages A, B and C share one issue by decision:** B's merge and C's deploy are
one change, and CAR-134's exit already reads "deployed, or the deploy
explicitly deferred with the reason recorded". Stage D is separate because its
rule is independent of the emergency.

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a-ghcr-holds-productions-exact-minio-build) | ghcr holds production's exact MinIO build | `next` | CAR-134 |
| 2 | [**B**](#stage-b-every-reference-pinned-to-it) | Every reference pinned to it | `—` | CAR-134 |
| 3 | [**C**](#stage-c-production-runs-it-from-ghcr) | Production runs it from ghcr | `—` | CAR-134 |
| 4 | [**D**](#stage-d-every-external-image-comes-from-a-registry-we-own) | Every external image comes from a registry we own | `—` | CAR-135 |

### Stage A: ghcr holds production's exact MinIO build

**State:** `next` · **Production-gated exit:** no

**Exit:** `ghcr.io/whitewalls86/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
pulls with no login for `linux/amd64` and `linux/arm64`, and its digest equals
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`'s; the workflow run that
produced it is linked from the record.

### Stage B: every reference pinned to it

**State:** `—` · **Production-gated exit:** no

**Exit:** all three live references name the ghcr digest; no `minio/minio`
image reference remains in any compose file; the six heavy jobs CAR-134 lists
are green on the pull request.

### Stage C: production runs it from ghcr

**State:** `—` · **Production-gated exit:** yes

**Exit:** `cartracker-minio` runs from the ghcr reference with image ID still
`sha256:14cea493…`, its healthcheck is healthy, and Service Down stays silent
for the `minio` and `minio_bucket` jobs, or the deploy is explicitly deferred
with the reason recorded; the record states the fitness answer above.

### Stage D: every external image comes from a registry we own

**State:** `—` · **Production-gated exit:** no

**Widened 2026-09-12 from compose images alone.** Dockerfile base images join
the ledger, because a base image pulled on every build is an external
dependency like any other. A runtime gate checks what CI actually pulls,
because a script can pull an image no compose file names: the container-health
contract pulled an untagged `alpine` from Docker Hub. Compose is where every
image is defined, so a new image dependency is a deliberate edit to compose,
never a line in a script.

**Exit:** a rule fails any compose image or Dockerfile base image not built
from this repository unless it is referenced from `ghcr.io/whitewalls86/*` by
digest or sits in a shrink-only ledger, seeded at the measured count (17
compose references and 5 base images, 2026-09-12) and keyed on the full image
reference, so changing a reference removes its entry. Every CI job that uses
Docker records the images on its runner, and a gate fails the run on any image
that is neither built here, owned, nor ledgered. No CI step or script names an
image compose does not define. The Stage A workflow copies any named image with
the digest check. Each rule is shown failing by a mutation entry in
`scripts/verify_testing_contract_mutations.py`.

## Record

### Stage A — ghcr holds production's exact MinIO build

**Issue:** CAR-134 · **Workflow run:** [`34669846795`](https://github.com/whitewalls86/new_car_tracker/actions/runs/34669846795)

Copied by hand on 2026-09-12 at 02:23 UTC, from the maintainer's laptop with
crane 0.22.1, signed in to ghcr with the maintainer's `gh` token (`write:packages`
added for the purpose):

    crane copy \
      quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e \
      ghcr.io/whitewalls86/minio:RELEASE.2025-09-07T16-13-09Z

It pushed the index, 969 bytes at `sha256:14cea493…d8936e`, and the three
platform manifests it lists: `linux/arm64` `sha256:9966a92a734f…`, `linux/amd64`
`sha256:a1a8bd4ac40a…` and `linux/ppc64le` `sha256:4a9aa577940a…`, which nothing
here runs. A first attempt was refused (`DENIED`) before any upload, because
the login had been saved under a mistyped host. Nothing was written.

The package was then made public, and `new_car_tracker` was given Write access
to it under Actions access, so the Stage A workflow's `GITHUB_TOKEN` can push
later. The API cannot read that setting back, so it stays unverified until the
workflow first runs.

Checked with no login, after `crane auth logout ghcr.io`, at 02:26 UTC:

| Command | Result |
|---|---|
| `crane digest ghcr.io/whitewalls86/minio:RELEASE.2025-09-07T16-13-09Z` | `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e` |
| `crane digest --platform linux/amd64 ghcr.io/whitewalls86/minio@sha256:14cea493…` | `sha256:a1a8bd4a…`, resolves |
| `crane digest --platform linux/arm64 ghcr.io/whitewalls86/minio@sha256:14cea493…` | `sha256:9966a92a…`, resolves |
| `crane digest quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` | `sha256:14cea493…d8936e`, equal |

Every clause of the exit is met except the last. The copy was made by hand to
unblock CI, not by the `workflow_dispatch` workflow the design names, so there
is no workflow run to link. That workflow is still to be built. Its first run
copies the same bytes again, which changes nothing in the registry, and that
run is the one this entry will link.

**The workflow run.**
[`34669846795`](https://github.com/whitewalls86/new_car_tracker/actions/runs/34669846795),
started from master at `971e9a9` on 2026-09-12, 03:14:54–03:15:04 UTC, with the
source and destination above. crane 0.22.1 installed and matched its release
checksum (`crane.tar.gz: OK`). `crane copy` reported `existing manifest:
RELEASE.2025-09-07T16-13-09Z@sha256:14cea493…` and pushed nothing, because the
hand copy had already put those bytes at that tag. Logged out, the check read
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e` for
both source and destination, platforms `linux/arm64, linux/amd64, linux/ppc64le`.

So this run re-checks the copy rather than producing it: the copy it links was
made by hand, and the run shows the workflow reaches the same digest from the
same source. Because nothing was written, the Write access granted under the
package's Actions access is still untested. The first copy of a new image, in
Stage D, will be the first real test of the push path.

### Stage B — every reference pinned to it

**Issue:** CAR-134 · **Commit:** `fef9737`

`fef9737` pins all three live references to
`ghcr.io/whitewalls86/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493…`:
`docker-compose.yml`, `docker-compose.lakehouse.ci.yml` and
`docker-compose.lakehouse.local.yml`. No `image: minio/minio` line remains in
any compose file. The `minio/minio` mentions left are history comments in
`ci.yml`, `docker-compose.ci.yml` and `tests/rules/test_ci_compose_parity.py`.

All six jobs the exit names passed together on `3c71943`, the merge of #419
into Plan 162 Stage R's #417, in the manually started run
[`34668714434`](https://github.com/whitewalls86/new_car_tracker/actions/runs/34668714434)
(02:49–02:53 UTC): dbt model tests, dbt build against a production snapshot,
SQL + Airflow metadata contracts, Lake integration tests (MinIO), SQL execution
coverage and Test invocation coverage. No job failed. The pull-request run on
the same commit,
[`34668716359`](https://github.com/whitewalls86/new_car_tracker/actions/runs/34668716359),
also passed.

**The six could not pass on #419 by itself.** `Test invocation coverage` is
defined only on #417's branch. And `scripts/ci_change_scope.py` does not select
`dbt build against a production snapshot` for a change that touches only compose
files, because its `SNAPSHOT_DBT_TRIGGERS` lists none of them, so #419's
pull-request run skipped it. That job ran only in the manually started runs,
which are not path-filtered and run every job. Changing MinIO's image does not,
by itself, trigger the one job that builds dbt against production's lake.

**#419's first full run failed on something else.**
[`34667850192`](https://github.com/whitewalls86/new_car_tracker/actions/runs/34667850192),
on `fef9737`, passed every job that uses MinIO and failed only `Unit tests (pytest)`. #418 had
inserted Plans 183 and 184 into the build order, so two of Plan 146's mutation
anchors no longer matched anything. Master had not shown it, because its run
after #418 was docs-only and skipped the unit tests. `ef85cf4` re-anchored both on
the order number alone, and the runs on `ef85cf4`,
[`34668304834`](https://github.com/whitewalls86/new_car_tracker/actions/runs/34668304834)
and
[`34668307139`](https://github.com/whitewalls86/new_car_tracker/actions/runs/34668307139),
passed.

The exit is met on #417, which carries #419's commits, rather than on #419's
own pull request.

### Stage C — production runs it from ghcr

**Issue:** CAR-134 · **Deployed:** 2026-09-12, 03:25 UTC

Run on the VM in tmux session `plan183-stage-c`. Each step went through
`~/plan183/run.sh`, which logs to `~/plan183/stage-c.log` with start and end
markers and each step's exit code. All four steps exited 0.

**Before.** `cartracker-minio` was running image
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
(`linux/arm64`, 57,548,825 bytes) under `minio/minio:latest`, up since
2026-08-31 18:17 UTC. That is the digest the ghcr copy carries, and it was
checked before anything changed.

**Rollback kept, 03:24 UTC.** The running image ID was given the local name
`cartracker-rollback/minio:pre-plan-183` and saved with
`docker image save --platform linux/arm64` to
`/mnt/data/backups/minio-pre-plan-183-arm64.tar`: 57,562,112 bytes, sha256
`500b55e3565a4a7f9a0695fcaf08a52754f66e0e7759dea620037520db88ace6`, holding
config `8f08aee6…` and 9 layers. Nothing on the VM prunes images, so both stay
until someone removes them. To roll back, recreate MinIO from that name with a
temporary override, leaving the checkout alone:

    printf 'services:\n  minio:\n    image: cartracker-rollback/minio:pre-plan-183\n' > /tmp/minio-rollback.yml
    docker compose -f docker-compose.yml -f /tmp/minio-rollback.yml up -d --no-deps minio

If the name is missing, `docker image load -i` on the file restores it first.

**Checkout.** `git pull --ff-only` in `/opt/cartracker` moved `6bcd3ac` to
`971e9a9`: 59 commits, 51 files. Of those, only
`ops/static_ops/generated/project-updates.json`, the public roadmap's data,
sits under a path a running container reads directly.

**Deploy, 03:25:39–03:25:52 UTC.** `bash scripts/redeploy.sh minio`. The drain
was confirmed after 0 s. Compose reported `minio Pulled`; the image was already
on the host under that digest. The container was recreated, `46c9f7106ed5…` to
`ebdea7263ead…`, reported healthy after 11 s, and deploy intent was released.

**After, 03:26–03:27 UTC.**

| Check | Result |
|---|---|
| image ID | `sha256:14cea493…`, unchanged |
| image reference | `ghcr.io/whitewalls86/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493…` |
| state | running, healthy; `/minio/health/live` returns HTTP 200 |
| `up{job=~"minio\|minio_bucket"}` | 1, 1 |
| `min_over_time(up{…}[15m])` at 03:27:33 | 1, 1: no scrape saw MinIO down |
| Prometheus alerts | none |

Service Down is a Grafana rule on `up` with `for: 2m`. With `up` never at 0 in
that window it could not fire. Grafana's own alert state was not read.

**Fitness.** The build is fit for production in the short term: it is,
byte for byte, the build production had been running since 2026-08-31. It is
unfit in the long term: the community image is frozen at 2025-09-07 and gets no
security fixes. That is [Plan 184](plan_184_replace_minio_with_garage.md)'s case.

The exit is met.

### Stage D — every external image comes from a registry we own

**Issue:** CAR-135 · **Commits:** `7e7be7c`, `1b32d50` · **Workflow run:** [`34700848629`](https://github.com/whitewalls86/new_car_tracker/actions/runs/34700848629)

**The image provenance gate's first green run.** The pull-request run on #422 at
`1b32d50`, 2026-09-12, 14:59:01–15:03:38 UTC. Every job passed. Documentation
tests was skipped by the path filter, so it owed no record. The gate read 16
records, one for every other job, and printed `ok: 16 record(s), every pulled
image is one this repository names`.

What each runner held, as the gate printed it:

| Job | Held | There before the job | Built here | Pulled |
|---|---:|---:|---:|---|
| `changes`, `lint`, `git-ref-hygiene`, `service-contracts`, `unit-tests`, `sql-execution-coverage`, `test-invocation-coverage` | 6 each | 6 | 0 | none |
| `docker-build` | 14 | 6 | 8 | none |
| `promtail-config` | 7 | 6 | 0 | `grafana/promtail:3.5.8` |
| `flaresolverr-contract` | 7 | 6 | 0 | `ghcr.io/flaresolverr/flaresolverr:v3.4.6` |
| `container-health-contract` | 8 | 6 | 0 | `tecnativa/docker-socket-proxy:0.3.0`, `redis:7-alpine` |
| `service-integration` | 8 | 6 | 0 | `postgres:16`, `flyway/flyway:10-alpine` |
| `dbt-models`, `schema-contracts`, `snapshot-dbt`, `lake-integration` | 9 each | 6 | 0 | `ghcr.io/whitewalls86/minio@sha256:14cea493…`, `postgres:16`, `flyway/flyway:10-alpine` |

The six images already on every runner are GitHub's own, preloaded on
`ubuntu-24.04` 20260907.300.1: `ghcr.io/github/github-mcp-server`,
`ghcr.io/github/gh-aw-mcpg`, `ghcr.io/github/gh-aw-firewall/{agent,api-proxy,squid}`
and `ghcr.io/dependabot/dependabot-updater-core`, all `:latest`. They were
reported and not judged. Every image a job pulled is one compose defines. The
MinIO pulls are the owned, digest-pinned copy; the other seven references are
ledgered. Base images pulled during `docker compose build` did not appear on the
runner at all. The rule covers them; this gate does not see them.

Read with:

    gh run view 34700848629 --job <Image provenance job id> --log

The per-job lines are the gate's own output, from
`scripts/check_ci_image_provenance.py`.

This meets the exit's runtime clause: every CI job records the images on its
runner, and the gate fails on any image that is neither built here, owned, nor
ledgered. It also shows, on a real run, that no CI step pulled an image compose
does not define. It says nothing about the other clauses: the ledger rule, the
Stage A workflow copying a named image, and the mutation entries.
