# Plan 185: One Python base image

## What this plan is for

Moves every image this repository builds from a `python:` base onto one,
`python:3.13-slim-trixie`, so the Python version CI tests is the one production
runs, and the project owns and refreshes one Python base image instead of four.

## The case

**Raised 2026-09-12, while widening [Plan 183](plan_183_minio_image_self_hosted.md)
Stage D to base images.** The rule that every image comes from a registry this
project owns reads Dockerfile `FROM` lines as well as compose, and those lines
name five distinct base images, four of them Python:

| Base | Services | Why |
|---|---|---|
| `python:3.11-slim` | scraper, dashboard, ops, processing, container_health | no recorded reason: the oldest services, and container_health copied them |
| `python:3.13-slim` | archiver, dbt, dbt_runner | newer, or reworked for the ARM VM in April |
| `python:3.13-slim-bookworm` | lakehouse-worker | Spark 3.5 needs Java 17, which Debian 13 dropped |
| `python:3.12-slim-bookworm` | mlflow | mlflow 2.17.2 caps pyarrow below 18, and pyarrow 17 has no Python 3.13 ARM64 wheel |
| `apache/airflow:3.2.0` | the Airflow services | the Airflow project's own image, which ships Python 3.13.12 |

**Five production services run a Python that CI never tests.** Every one of
CI's fifteen Python setups installs 3.13, `ruff` targets `py313`, and the local
environment is 3.13. Code that uses anything newer than 3.11 passes every test,
passes the Docker build, which never imports it, and fails when the service
starts on the VM. Nothing has broken yet, and nothing stops it.

**An unsuffixed tag floats the operating system as well as the patch
release.** `python:3.11-slim` and `python:3.13-slim` are both Debian 13
(trixie) today. They moved there from Debian 12 when trixie shipped, on the
next rebuild, without anyone deciding it. The target names its Debian release
so that cannot recur.

**Trixie, not bookworm, and 3.13, not 3.12.** Debian 12's regular security
support ended on 2026-07-11; its LTS, to 2028-06-30, covers about 230 sponsored
packages on a best-effort basis. Python 3.12's security support ends in October
2028 and 3.13's in October 2029. Standardising on 3.12-bookworm would also move
everything already on 3.13, CI included, down a version.

**The two pins holding images off trixie are pre-production and removable.**
Both belong to Plan 112's lakehouse images, which production does not run, and
the maintainer confirmed on 2026-09-12 they are open to change:

- mlflow 3.16.0 (released 2026-09-04) accepts pyarrow below 26, and pyarrow
  has published Python 3.13 ARM64 wheels since 18.0.0. The upgrade is a major
  version, and the tracking server's SQLite store needs `mlflow db upgrade`.
- Spark 3.5.3 documents Java 8/11/17, and trixie ships only OpenJDK 21 and 25.
  Spark 4.1 documents Java 17/21, and Iceberg 1.11.0 publishes a Spark 4.1
  runtime (`iceberg-spark-runtime-4.1_2.13`), though not yet one for 4.2. The
  move changes the jars the lakehouse image downloads: the Iceberg runtime
  goes to Scala 2.13, and `hadoop-aws` and the AWS SDK bundle must match the
  Hadoop that Spark 4.1 ships. dbt-spark 1.10.3's `session` extra already
  accepts pyspark below 5; the comment in `lakehouse/requirements.txt` that
  says below 4 is stale.

**Airflow stays its own base.** The Airflow project chooses that image's
Debian, currently bookworm, and the tag chooses its Python, already 3.13. The
end state is two base images: `python:3.13-slim-trixie` and `apache/airflow`.

**It should land before or during [Plan 180](plan_180_seam_program.md) Stage K
drains the image ledger.** Plan 183 Stage D puts base images on that ledger, and
each distinct base is one copy into ghcr to make and one digest to refresh from
then on. Consolidating first means Stage K copies two base images, not five.
Since 2026-09-12 Stage K's exit names Dockerfile base images as well as
compose images; this plan drains the base images, and Stage K is the backstop
for any it has not.

## Design

**The base is an owned copy, used by digest.** `python:3.13-slim-trixie` is
copied into `ghcr.io/whitewalls86/python` with `.github/workflows/vendor-image.yml`,
and every Dockerfile that builds on Python names it by digest:
`FROM ghcr.io/whitewalls86/python:3.13-slim-trixie@sha256:…`. The Debian
release is in the tag so it cannot float, and the digest is what cannot move.
The namespace satisfies Plan 183 Stage D's ownership rule, so each move drains
a base-image entry from that rule's ledger and adds none.

**Airflow is pinned, not moved.** Its image carries its own Python and Debian,
chosen by the Airflow project, and the only lever this plan has is the tag.
`apache/airflow:3.2.0-python3.13` is byte-identical to the `3.2.0` in use
(`sha256:3cbaa475…`, read 2026-09-12). Naming it explicitly changes nothing
today and stops a future Airflow bump from changing Python by default. It is
copied into ghcr the same way.

**A rule keeps it from drifting back.** Every `FROM` line that builds on Python
must name the Python CI installs and a Debian release. Its ledger is seeded
with today's exceptions and drains as Stages B–E land, the same shrink-only
shape as Plan 183 Stage D's.

**The lakehouse images are proven on the VM, not in CI.** CI's Docker build
covers only `docker-compose.yml`'s services. Bringing `cartracker-lakehouse`
and `cartracker-mlflow` into it is Plan 125's job when those services reach
production, decided 2026-09-12; they do not owe it before then. Until then
Stages D and E prove themselves by building and running on the VM, in their
own compose projects.

**Accepted: a rebuild re-resolves every unpinned `requirements.txt`.**
`constraints.txt` already pins the four packages that decide the web stack's
schema, and every deploy re-resolves the rest today anyway.

**Rejected on the way here:**

| Alternative | Why not |
|---|---|
| `python:3.12-slim-bookworm` everywhere | Debian 12 has been in LTS since 2026-07-11, Python 3.12's support ends a year before 3.13's, and it would move CI and five images down a version |
| unsuffixed `python:3.13-slim` | floats the Debian release; it moved from bookworm to trixie with nobody deciding |
| a base image of our own, built here | one more image to build and patch, for no content this project needs |
| Airflow's `slim-3.2.0-python3.13` | a different image (`sha256:09bcb9eb…`) from the one production runs |
| extending CI's build to the lakehouse images now | Plan 125's job, when those services reach production |

## Stages

**Stages A and B share one issue:** both verify before a pull request opens.
C, D and E each need their code running — C in production, D and E on the VM
— so each has its own.

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a-the-python-a-service-builds-on-is-the-python-ci-tests) | The Python a service builds on is the Python CI tests | `next` | CAR-140 |
| 2 | [**B**](#stage-b-ghcr-holds-the-python-and-airflow-bases-and-the-unchanged-images-use-them) | ghcr holds the Python and Airflow bases, and the unchanged images use them | `—` | CAR-140 |
| 3 | [**C**](#stage-c-the-five-311-services-run-python-313) | The five 3.11 services run Python 3.13 | `—` | CAR-141 |
| 4 | [**D**](#stage-d-mlflow-3-on-the-standard-base) | mlflow 3 on the standard base | `—` | CAR-142 |
| 5 | [**E**](#stage-e-the-lakehouse-worker-on-spark-41) | The lakehouse worker on Spark 4.1 | `—` | CAR-143 |

### Stage A: the Python a service builds on is the Python CI tests

**State:** `next` · **Production-gated exit:** no

**Exit:** a rule in `tests/rules/` fails any Dockerfile `FROM` line on a Python
base, whether Docker Hub's `python` or its ghcr copy, whose Python version is
not the one CI's `setup-python` steps install or whose tag names no Debian
release; its shrink-only ledger is seeded with the exceptions measured at this
stage's start; the rule is registered in `docs/TESTING.md` and shown failing by
a mutation entry in `scripts/verify_testing_contract_mutations.py`.

### Stage B: ghcr holds the Python and Airflow bases, and the unchanged images use them

**State:** `—` · **Production-gated exit:** no

**Exit:** `ghcr.io/whitewalls86/python:3.13-slim-trixie` and
`ghcr.io/whitewalls86/airflow:3.2.0-python3.13` pull with no login for
`linux/amd64` and `linux/arm64`, each digest equal to its source's, with the
`vendor-image.yml` runs linked from the record; archiver, dbt and dbt_runner
build `FROM` the Python copy and airflow `FROM` the Airflow copy, by digest,
and CI is green; their entries in Plan 183 Stage D's ownership ledger and in
Stage A's ledger are deleted.

### Stage C: the five 3.11 services run Python 3.13

**State:** `—` · **Production-gated exit:** yes

**Exit:** scraper, ops, processing, dashboard and container-health build
`FROM` the owned Python base by digest and CI is green; each is redeployed with
`scripts/redeploy.sh`, reports healthy, and reports Python 3.13 from
`python --version` inside its running container; Service Down stays silent;
their ledger entries are deleted.

### Stage D: mlflow 3 on the standard base

**State:** `—` · **Production-gated exit:** yes

**Exit:** the `mlflow` target builds `FROM` the owned Python base with mlflow
3.x and a matching boto3; on the VM, in the `cartracker-mlflow` project,
`mlflow db upgrade` migrates the SQLite store, the server starts healthy, and
one run with an artifact in MinIO is recorded and read back; its ledger entries
are deleted.

### Stage E: the lakehouse worker on Spark 4.1

**State:** `—` · **Production-gated exit:** yes

**Exit:** the `lakehouse-worker` target builds `FROM` the owned Python base
with pyspark 4.1, OpenJDK 21, Iceberg 1.11.0's `iceberg-spark-runtime-4.1_2.13`,
and the `iceberg-aws-bundle`, `hadoop-aws` and AWS SDK jars matched to Spark
4.1's Hadoop; on the VM, Plan 112 Gate A2's PySpark–Iceberg round trip against
Lakekeeper and MinIO passes; the stale `pyspark>=3.0,<4.0` comment in
`lakehouse/requirements.txt` is corrected; its ledger entries are deleted.
