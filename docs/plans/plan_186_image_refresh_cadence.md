# Plan 186: Owned images, refreshed on a cadence

## What this plan is for

Refreshes every container image this project owns on a fixed cadence through
its vendor workflow, each refresh a reviewed one-line pull request vetted by
how much the image can break, so pinned images keep receiving security fixes
instead of freezing on the day they were copied.

## The case

**Raised 2026-09-12, from the other side of [Plan 183](plan_183_minio_image_self_hosted.md).**
Owning an image by digest stops anyone else from withdrawing or retagging it.
It also stops anything from changing it: no patch release, no Debian security
fix, nothing arrives unless someone copies a newer build and moves the pin.
Before Plan 183, a rebuild or a pull took whatever a tag pointed at that day,
which was drift nobody reviewed. Once Plan 183 Stage D's ledger is drained,
nothing moves unless someone moves it. The copy workflow can move any of them
at any time; what is missing is anything that makes sure someone does. Without
that routine, an owned image drifts toward what MinIO became, frozen at the day
it was copied, not because it cannot be updated but because nobody got round
to it.

**MinIO shows what frozen costs.** [Plan 184](plan_184_replace_minio_with_garage.md)'s
case records eight published vulnerabilities in range of the build Plan 183
pinned, none fixable, because that vendor stopped publishing. For the other
images the fixes do get published; a pin that is never refreshed simply never
takes them. After [Plan 185](plan_185_one_python_base_image.md) and
[Plan 180](plan_180_seam_program.md) Stage K, the owned set is eighteen compose
references and two base images, `python:3.13-slim-trixie` and
`apache/airflow`. The Airflow image runs Debian 12, which left regular security
support on 2026-07-11, so for those services a refresh is the only way a Debian
fix arrives at all.

**The mechanics already exist; the routine does not.**
`.github/workflows/vendor-image.yml` copies a named image by digest and fails
unless the copy's digest equals the source's. A refresh is one dispatch and one
edited line, and the pull request carrying that line is the unit a reviewer
reads. Rolling back is reverting the line: the previous digest is still in a
registry this project owns, which is exactly what ownership bought.

**What vets a refresh depends on what the image holds.** Three tiers, from what
the repository can check today:

| Tier | Example | What vets it | Exists today |
|---|---|---|---|
| same tag, new build | a Python or Debian patch under `python:3.13-slim-trixie` | CI, then a production deploy behind `redeploy.sh`'s health gate | yes, with one gap below |
| a version bump | `grafana/grafana` 11 to 12, `caddy` to a new minor | the changelog read, CI, and a deploy someone watches | yes |
| an image that migrates stored data | postgres, airflow, loki, grafana, lakekeeper | the upgrade rehearsed against a copy of production's data first | no |

**The gap in the first tier is that CI never runs the tests inside the image.**
CI's heavy jobs start the compose images, so a refreshed postgres or MinIO is
exercised by real tests. A refreshed base image is only built: the test suite
runs on the runner's own Python, so a base-image refresh is proven to build and
not proven to work.

**The third tier is [Plan 121](plan_121_staging_environment.md)'s capability,
not this plan's.** Plan 121 already records rehearsing an Airflow upgrade by
restoring a production dump and running `airflow db migrate` against it, since
CI builds the schema from empty and cannot see a migration that fails only on
real data. Staging sits at build-order position 22, behind Plan 69. So this
plan should not fold into Plan 121: the first two tiers can run as soon as the
images are owned, and only the stateful tier waits on staging. Until then those
images stay pinned and their refreshes are deliberate, reviewed exceptions.

**A cadence misses the urgent case.** A vulnerability published the day after a
refresh waits a whole cycle. Whether a scanner run against the owned digests
should decide which refreshes cannot wait is part of this plan's design, not a
finding it has made.

**Python packages have the same shape and are a separate question.** Plan 121's
"Shared Dependency Pinning" section argues that a constraints file needs "a
renewal mechanism, not just a floor". Whether that renewal rides the same
cadence as the images is a scope decision for this plan's start.

## Design

**Every owned image records where it came from.** A checked-in map names, for
each owned reference, the upstream tag it was copied from and its tier. Without
it nothing can tell that a newer build exists, and the tier decides how a
refresh is vetted.

**A monthly scheduled workflow does the routine; a person does the review.**
It resolves each source tag, and for each owned image that is behind, it copies
the new build with `.github/workflows/vendor-image.yml` and opens one pull
request moving the pin, labelled with the image's tier. Nothing reaches
production without a merge, and rolling back is reverting the line. The monthly
cadence was decided on 2026-09-12.

**Tiers are declared per image, not judged per pull request.** Tier 1 is a new
build under the same tag, tier 2 a version bump, and tier 3 an image that
migrates stored data. A tier-3 pull request is opened but marked as needing
rehearsal, and merges only after the upgrade has been rehearsed in staging
against a restored production dump. That is
[Plan 121](plan_121_staging_environment.md)'s capability, which is why only
Stage E waits on it.

**Pinned Python packages ride the same cadence.** Stage G takes over the
constraints-file work Plan 121's "Shared Dependency Pinning" section scoped:
an audit of what is shared or on a request path, pins at the versions
production runs, and a monthly check against PyPI that opens one pull request
per moved package. CI's unit suite and the OpenAPI contract gate from Plan 162
Stage Z vet those.

**Whether a scanner decides what cannot wait for the cycle is a decision this
plan makes, not one it assumes.** Stage F runs one against every owned digest
and records whether it earns a schedule.

**Rejected on the way here:**

| Alternative | Why not |
|---|---|
| Dependabot or Renovate | they track the reference as written, which is the ghcr copy, so they cannot see an upstream release, and their fix would point the reference back at Docker Hub, undoing Plan 183 Stage D's rule |
| refreshing at deploy time | the unreviewed drift Plan 183 removed |
| folding this into Plan 121 | only the stored-data tier needs staging; the other two can run as soon as images are owned |

## Stages

**Stages A and B share one issue:** both verify before a pull request opens.
C, G and E each need their code running — C and G in production, E in staging
first — so each has its own, and D and F, which sit between them, each have
their own as well.

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a-every-owned-image-knows-where-it-came-from) | Every owned image knows where it came from | `next` | CAR-144 |
| 2 | [**B**](#stage-b-staleness-is-visible) | Staleness is visible | `—` | CAR-144 |
| 3 | [**C**](#stage-c-monthly-refresh-pull-requests) | Monthly refresh pull requests | `—` | CAR-145 |
| 4 | [**D**](#stage-d-the-tests-run-inside-the-built-image) | The tests run inside the built image | `—` | CAR-146 |
| 5 | [**G**](#stage-g-pinned-packages-audited-expanded-and-refreshed) | Pinned packages audited, expanded and refreshed | `—` | CAR-147 |
| 6 | [**F**](#stage-f-whether-a-scanner-flags-urgent-refreshes) | Whether a scanner flags urgent refreshes | `—` | CAR-148 |
| 7 | [**E**](#stage-e-a-stored-data-refresh-rehearsed-in-staging) | A stored-data refresh rehearsed in staging | `—` | CAR-149 |

### Stage A: every owned image knows where it came from

**State:** `next` · **Production-gated exit:** no

**Exit:** a checked-in map names, for every image referenced from
`ghcr.io/whitewalls86/*`, the upstream tag it was copied from and its tier (1,
2 or 3); a rule in `tests/rules/` fails any owned reference with no entry and
any entry no reference uses; the rule is registered in `docs/TESTING.md` and
shown failing by a mutation entry in `scripts/verify_testing_contract_mutations.py`.

### Stage B: staleness is visible

**State:** `—` · **Production-gated exit:** no

**Exit:** a scheduled workflow, run monthly and on dispatch, resolves each
source tag in Stage A's map and lists in its run summary every owned image
whose source digest has moved, writing nothing; a run on master is linked from
the record.

### Stage C: monthly refresh pull requests

**State:** `—` · **Production-gated exit:** yes

**Exit:** the Stage B workflow copies each moved build with `vendor-image.yml`
and opens one pull request per image moving its pin, labelled with its tier,
with tier-3 pull requests marked as needing rehearsal; the first tier-1 refresh
it opened is merged, deployed with `scripts/redeploy.sh`, and healthy.

### Stage D: the tests run inside the built image

**State:** `—` · **Production-gated exit:** no

**Exit:** CI runs the unit suite inside each rebuilt service image, so a
base-image refresh is proven to work and not only to build.

### Stage E: a stored-data refresh rehearsed in staging

**State:** `—` · **Production-gated exit:** yes

**Exit:** one tier-3 refresh — postgres, airflow, loki, grafana or lakekeeper —
is rehearsed in Plan 121's staging environment against a restored production
dump, its migration completing, before its pull request merges; the rehearsal
is linked from the record. It waits on Plan 121 as well as on this plan's
order.

### Stage F: whether a scanner flags urgent refreshes

**State:** `—` · **Production-gated exit:** no

**Exit:** a scanner is run once against every owned digest, its findings for
the owned set are recorded, and the record states whether it runs on a
schedule and what severity opens a refresh early; if it does, that is a new
stage.

### Stage G: pinned packages audited, expanded and refreshed

**State:** `—` · **Production-gated exit:** yes

**Exit:** an audit of every service's `requirements.txt` records which packages
are shared or on a request path, and pins them in `constraints.txt` at the
versions production runs, read from the running containers; the monthly
workflow checks each pin against PyPI and opens one pull request per moved
package, vetted by CI's unit suite and OpenAPI contract gate; the first such
pull request is merged and deployed, and the services it rebuilt are healthy.
