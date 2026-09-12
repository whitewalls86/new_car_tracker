# Plan 184: replace MinIO with Garage

## What this plan is for

Replaces MinIO, whose free community image is no longer published, with Garage,
a free, self-hosted, S3-compatible object store, after first proving Garage
answers every S3 operation this project's code makes, so the data lake stays
local and costs nothing.

## The case

**A frozen image is not a future.** [Plan 183](plan_183_minio_image_self_hosted.md)
copies the MinIO build production runs into a registry we own, which ends the
emergency and nothing else: that build is `RELEASE.2025-09-07` and will receive
no security fixes. What withdrew it was the vendor of an open-core product
deciding the free edition was over. The replacement has to be chosen with that
failure in mind. This plan came out of the same 2026-09-11 discussion that
produced Plan 183 ([CAR-134](https://linear.app/cartracker/issue/CAR-134)).

**Two constraints are fixed, set 2026-09-11: the store stays local, and it is
free.** Managed object storage is out at any price. That constraint costs less
than it looks, because CI needs a local S3 server whatever production runs, so
the image question never goes away by moving production elsewhere.

**The replacement has to be S3-shaped. It does not have to be MinIO-shaped.**
Five separate client implementations speak S3 to the store: boto3 (ten
operations, including the ranged `get_object` that
[Plan 131](plan_131_packed_cold_storage.md)'s pack reader resolves members
through, paginated `list_objects_v2` and batched `delete_objects`), s3fs, DuckDB
`httpfs`, Spark S3A, and Iceberg through Lakekeeper's `s3-compat` storage
profile, plus MLflow's artifact store. That protocol is the real contract. What
is specific to MinIO is a short, enumerable list: the `/minio/health/live`
healthcheck in three compose files and `ops/coordination_release.py`, two
`/minio/v2/metrics/*` scrape jobs, four infrastructure-dashboard panels, the
Service Down alert's job set, and the console, which nothing depends on. A
plain filesystem would satisfy every client except Lakekeeper, whose storage
profiles are object-store profiles, so it would close off Plan 125's chosen
catalog.

**Garage is the likeliest fit, on governance first.** It is AGPLv3, maintained
by Deuxfleurs, a non-profit collective, and its main funding has been public
grants (NGI POINTER 2021–22, NLnet 2023–25). It added a `GOVERNANCE.md` in July
2026, shipped v2.4.1 on 2026-09-08, and had a commit on 2026-09-10. It is the
candidate least likely to repeat what MinIO did. On the protocol, every
operation in the inventory above is implemented according to its compatibility
page. The checksum trailers recent boto3 sends by default, which once broke it,
were fixed in February 2025 (its issue #824); this repository runs boto3
1.43.56. It supports ARMv8 and, since v2.3.0, a `--single-node` mode, which has
no redundancy, as today's single-drive MinIO has none.

**The metrics mostly carry over.** Grafana consumes six `minio_*`-based panels
and the `up` alert, out of 106 `minio_*` series Prometheus holds. Request rate
and error rate map directly onto `api_s3_request_counter` and
`api_s3_error_counter`, the latter with a `status_code` label MinIO's panel
lacked, and Garage adds `garage_local_disk_avail`, its own view of the volume
that filled on the silver flush for five days in August. Per-bucket bytes and
object counts, which feed four panels, are not Prometheus metrics in Garage:
they come from the admin API's `GetBucketInfo` and need something to poll them.

**Garage has to be proven before it is built, because the unknowns are real.**
Lakekeeper's documentation names MinIO as tested and does not name Garage. The
image is `FROM scratch`, with no shell and no curl, so the in-container
healthcheck would be `garage node health` over RPC, whose exit behaviour is not
documented. Everything above comes from Garage's documentation and not yet from
running it against this repository's code. That proof has a home already:
[Plan 180](plan_180_seam_program.md)'s census grades the object-storage channel
at grade 1, and a check recording the S3 behaviour this code depends on, run
against whatever image compose names, is that seam's tier-2 evidence as much as
it is this plan's qualification test for any candidate.

**The migration is cheap now, and gets more expensive later.** The one bucket,
`bronze`, holds 26.34 GB across 416,064 objects, with 155.99 GB free on
`/mnt/data` (Prometheus, 2026-09-11). Plan 131's packfiles removed the object
count that makes store migrations fail, and
[Plan 145](plan_145_april_cutover_reconciliation.md) already proved a
per-object verified copy at 983,043 members with none refused. It has to
precede [Plan 125](plan_125_duckdb_to_iceberg_migration.md)'s production
Iceberg writes. Iceberg records absolute `s3://bronze/...` locations in table
metadata, carrying scheme and bucket but not the endpoint, so a same-bucket S3
swap after Iceberg is a storage-profile change, while any change of scheme or
bucket would mean rewriting metadata. Plan 125's catalog decision held MinIO
constant, and none of its R1–R7 guardrails is about storage neutrality. Today
the VM's only Lakekeeper warehouse, `cartracker_experiments`, sits on a spike
prefix nothing reads.

**It waits behind Plans 180–182, by decision.** Plan 183 makes the wait safe,
and the testing chain has two weeks of momentum. The order agreed on 2026-09-11
is Plan 183, then Plans 180–182, then this plan, then Plan 125.

## Design

**Prove, then build, with the fallback named before the proof runs.** Stage A
is a go/no-go. If Garage fails it, the same suite runs against versitygw and
the plan continues with whichever passes; if neither does, the plan comes back
for a decision rather than lowering the bar. Garage's documentation says it
fits; only running this repository's own calls against it can say so.

**The qualifying test is a conformance suite derived from call sites, not from
a feature list.** A pytest integration suite in which every test traces to a
real call in the S3 inventory: the ten boto operations, including a ranged
`get_object` against a real pack (Plan 131's reader resolves members that
way), `list_objects_v2` paginated across a page boundary, and batched
`delete_objects`; s3fs writing and reading hive-partitioned Parquet; DuckDB
`httpfs` `read_parquet` over a glob; Spark S3A reading silver and writing an
Iceberg table through Lakekeeper's `s3-compat` storage profile; an MLflow
artifact round trip. It takes its image from compose, so the same suite runs
against the pinned MinIO, which must pass as the baseline, and against any
candidate. It is filed as seam 5's tier-2 evidence in
[Plan 180](plan_180_seam_program.md)'s frame, which grades that channel 1.

**Keep what the code sees until the data has moved.** The bucket stays
`bronze`, keys do not move, and the `MINIO_*` environment variable names
survive until cutover, so no client code changes before Stage D. The names are
renamed in Stage E, when nothing called MinIO is left to name.

**The production move is a copy with per-object verification**, reusing
[Plan 145](plan_145_april_cutover_reconciliation.md)'s discipline: every key
compared on size and checksum, zero refused, writers paused over a short
window rather than dual-written. At 26.34 GB logical against 155.99 GB free
(2026-09-11), Garage stands beside MinIO on the same volume with room to
spare. The Lakekeeper warehouse `cartracker_experiments` is dropped and
registered again against the new endpoint, since nothing reads it.

**Garage is vendored from its first day.** It is pulled from
`ghcr.io/whitewalls86/garage` by digest, copied through
[Plan 183](plan_183_minio_image_self_hosted.md)'s workflow, which Plan 183
Stage D's rule then requires.

**Observability is rebuilt, not ported.** A `garage` scrape job replaces
`minio` and `minio_bucket`; request and error rate move to
`api_s3_request_counter` and `api_s3_error_counter`; per-bucket bytes and
object counts, which Garage exposes only through the admin API's
`GetBucketInfo`, are published by a poller; the Service Down job set and the
healthchecks follow. The image is `FROM scratch`, so the in-container
healthcheck is whatever Stage A measures `garage node health` to be, and
external probes use the unauthenticated `/health` on the admin port.

**Retiring MinIO deletes a volume, so it follows the host's rules for that.**
Compose prefixes the project name onto volume names; the live mount is
confirmed before anything is removed.

**Rejected on the way here:**

| Alternative | Why not |
|---|---|
| Stay on the frozen MinIO | no security fixes; Plan 183 buys time, not a future |
| SeaweedFS | sells an enterprise edition: the open-core shape MinIO had |
| RustFS | pre-1.0 and company-controlled |
| Managed S3 | the store stays local and free, by constraint |
| A plain filesystem | closes off Lakekeeper, whose storage profiles are object-store profiles |
| versitygw first | kept as the named fallback: Garage's governance is the stronger answer to what went wrong |
| Dual-write during migration | writers pausing for a window is simpler and Plan 145 proved the copy |

## Stages

**Stage A carries its own issue because it is a go/no-go**; B and C only make
sense after a go. D and E each need production to prove.

| Order | Stage | What it delivers | Est. | State | Issue |
|---:|:---:|---|---:|---|---|
| 1 | [**A**](#stage-a-garage-proven-against-our-s3-surface) | Garage proven against our S3 surface | 1 | `next` | CAR-136 |
| 2 | [**B**](#stage-b-garage-runs-in-ci-and-local) | Garage runs in CI and local | 1 (with C) | `—` | CAR-137 |
| 3 | [**C**](#stage-c-observability-for-garage) | Observability for Garage | (with B) | `—` | CAR-137 |
| 4 | [**D**](#stage-d-production-cuts-over) | Production cuts over | 3 | `—` | CAR-138 |
| 5 | [**E**](#stage-e-minio-retired) | MinIO retired | 2 | `—` | CAR-139 |

### Stage A: Garage proven against our S3 surface

**State:** `next` · **Production-gated exit:** no

**Exit:** the conformance suite passes against the pinned MinIO and against a
vendored Garage in a disposable compose stack; `garage node health`'s exit
codes are measured healthy and unhealthy; `GetBucketInfo` returns bytes and
objects for a bucket the suite filled. Or Garage is rejected with its failing
operations recorded, and the suite is run against versitygw.

### Stage B: Garage runs in CI and local

**State:** `—` · **Production-gated exit:** no

**Exit:** every compose file runs Garage as the store; buckets and keys are
created on start; the six heavy jobs and the conformance suite are green on
Garage.

### Stage C: observability for Garage

**State:** `—` · **Production-gated exit:** no

**Exit:** a `garage` scrape job replaces `minio` and `minio_bucket`; a poller
exposes per-bucket bytes and objects; the six panels are rewritten and the
Service Down job set updated; the healthcheck and the
`ops/coordination_release.py` probe use Garage; the observability config tests
pass.

### Stage D: production cuts over

**State:** `—` · **Production-gated exit:** yes

**Exit:** Garage runs beside MinIO on the VM; `bronze` is copied with zero
objects refused; the endpoint is switched and writers resumed; a ranged pack
read, a dbt build and the dashboards work off Garage; Service Down stays
silent.

### Stage E: MinIO retired

**State:** `—` · **Production-gated exit:** yes

**Exit:** the `minio` service, its console route and its volume are gone, the
volume's live mount confirmed before removal; the `MINIO_*` names are renamed;
nothing references MinIO outside history.
