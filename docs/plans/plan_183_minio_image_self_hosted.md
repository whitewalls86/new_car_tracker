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

**Exit:** a rule fails any compose image not built from this repository unless
it is referenced from `ghcr.io/whitewalls86/*` by digest or sits in a
shrink-only ledger seeded at the measured count and keyed on the full image
reference, so changing a reference removes its entry; the Stage A workflow copies
any named image with the digest check; the rule is shown failing on an
unowned image by a mutation entry in `scripts/verify_testing_contract_mutations.py`.

## Record

### Stage A — ghcr holds production's exact MinIO build

**Issue:** CAR-134 · **Still owed:** the linked workflow run

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
