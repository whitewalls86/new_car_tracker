# Plan 145 Stage 3 — implementation prompt (CAR-20)

Hand this to a fresh session once Stage 2 has finished. Everything needed to
execute is here or cited to a file and line.

---

## Before you start: is Stage 2 done?

```
ssh -i ssh-key-2026-04-08.key ubuntu@147.224.199.86 '~/p145status.sh'
```

Stage 2 is complete when `progress` reads `1172 / 1172`. If it is still
running, **stop and report** — do not start 3a against a partial population,
and never kill the `plan145` tmux session.

When it is done, confirm the manifest is whole:

```
ssh -i ssh-key-2026-04-08.key ubuntu@147.224.199.86 \
  'docker exec -w /app cartracker-archiver python -c "
from shared.minio import BUCKET, get_boto3_client
c = get_boto3_client(); n=0; tok=None
while True:
    kw={\"Bucket\":BUCKET,\"Prefix\":\"recovery/plan145/materialized/\"}
    if tok: kw[\"ContinuationToken\"]=tok
    p=c.list_objects_v2(**kw)
    n+=sum(1 for e in p.get(\"Contents\",[]) if e[\"Key\"].endswith(\".parquet\"))
    if not p.get(\"IsTruncated\"): break
    tok=p.get(\"NextContinuationToken\")
print(\"manifest shards:\", n)"'
```

Expect **1172**. Anything less means Stage 2 did not finish; stop and report.

---

## What you are building

Two new modes on `scripts/reconcile_april_detail.py`, which already has
`census` (Stage 1) and `materialize` (Stage 2). **Follow their structure
exactly**: lazy `shared.minio` imports inside functions, dry-run by default
with an explicit `--apply`, fail-closed gates that raise `ReconcileError`,
deterministic ordering, per-unit manifest shards written to MinIO so an
interruption loses only the in-flight unit.

Read the whole existing script before writing anything. It is ~1,145 lines and
its module docstring explains the population.

### Mode 1 — `dedupe` (Stage 3a)

Delete every materialized object whose content the April packs already hold.

- Both sides already record the hash. Materialize manifest shards under
  `recovery/plan145/materialized/*.parquet` have `raw_sha256` per row; pack
  sidecars under `html_packs/detail_page/2026/04/*.idx.parquet` have
  `raw_sha256` per member. **Do not re-hash any bytes.** This is a join of two
  written-down columns.
- Read sidecars with `shared.packfile.read_index_parquet`, or project just the
  `raw_sha256` column with pyarrow. 32 sidecars, ~557k members, about a minute.
- Consider only manifest rows whose `disposition` is `written` or `exists`.
  The other two dispositions never produced an object.
- Write a deletion manifest **before deleting anything**: one Parquet under
  `recovery/plan145/dedupe/` per source shard, carrying `object_key`,
  `raw_sha256`, the legacy locator, and the sidecar key that claimed it.
- Delete by exact key, in capped batches, recording a receipt per key. **Never
  by prefix.**
- Expect ~371,000 deletions (45.6% of the population, measured over 392 shards
  on 2026-08-27). If the rate is wildly off, stop and report rather than
  proceeding.

Safety, so you can reason about it: the content stays in the pack the whole
time, and `shared/minio.py:read_html` falls back to the pack for a missing
object (`read_packed_html`). Deleting a materialized twin never makes anything
unreachable.

### Mode 2 — `unpack` (Stage 3b)

Write every April pack member back as a loose `.html.zst` object.

- **Under its original `source_key`, from the sidecar.** Not a content-derived
  key. This is what preserves the `minio_path` → `artifact_id` join in
  `ops_normalized/artifacts_queue_events`, which is the only surviving
  attribution for these artifacts — `ops.artifacts_queue` rows were deleted by
  `archiver/processors/cleanup_queue.py:35` when they completed.
- Read members with a ranged-GET `PackReader`
  (`shared/packfile.py:388`). Work pack by pack, frame by frame: a frame is one
  ~16 MiB decompress serving ~1,000 members, so iterate members grouped by
  `frame_ordinal` and let `max_cached_frames=1` do the rest. Reading members in
  sidecar order without grouping would re-decompress the same frame repeatedly.
- Verify each member's sha256 against the sidecar's `raw_sha256` before
  writing. A mismatch stops the run — the packer already verified these, so a
  disagreement means the store moved.
- Write with `shared.minio.write_html`, which applies the production
  dictionary and level. The `april-processor` service already sets
  `HTML_COMPRESSION_DICT_ID`.
- Skip keys that already exist (`object_exists`), so the mode is idempotent and
  resumable.
- 557,065 members. At Stage 2's observed ~4.7 source-files/min the write rate
  is the bound; budget roughly 3 hours.

---

## Where it runs

```
docker compose run --rm april-processor python -m \
  scripts.reconcile_april_detail dedupe            # dry run: plans, reports, deletes nothing
docker compose run --rm april-processor python -m \
  scripts.reconcile_april_detail dedupe --apply
docker compose run --rm april-processor python -m \
  scripts.reconcile_april_detail unpack --apply
```

`april-processor` is profile-gated and already registered in the three
governance files CI enforces (`docker-compose.yml`,
`ops/coordination_contract.py`, `maintenance-running-set.txt`). Do not add
resource caps — pack-worker runs uncapped and that is the house precedent.

Run the long job under tmux on the VM so it survives disconnection, the way
Stage 2 is running now.

---

## Non-negotiables

1. **Never kill the `plan145` tmux session** or run `materialize --apply`.
2. **No production mutation outside** the deletions this issue authorizes and
   the unpacked objects. No silver rows, no `ops.artifacts_queue` rows, no
   Postgres writes at all.
3. **Deletion is by exact key from a written manifest, with receipts.** Never
   by prefix. The manifest is written before the first delete.
4. **Unpack preserves original keys.** A content-derived key here silently
   destroys the `artifact_id` attribution for 514,789 artifacts.
5. **Announce the blast radius** of any production command before running it —
   object counts, request counts, which prefix. Prefer data already written
   down (manifests carry `raw_sha256`, `html_len`, `compressed_len`,
   dispositions) over re-scanning production to re-derive it.

---

## Tests

Extend `tests/scripts/test_reconcile_april_detail.py`, which has 95 tests in
lettered sections; add new sections at the end. Run with:

```
LOG_PATH=/tmp/p145test.log .venv/bin/python -m pytest \
  tests/scripts/test_reconcile_april_detail.py -q -m "not integration"
.venv/bin/python -m ruff check .
```

Cover at least:

- a hash present in a sidecar plans a deletion; one absent does not;
- rows with `skipped_empty` / `skipped_non_success` are never planned for
  deletion;
- the deletion manifest is written before any delete, and a dry run deletes
  nothing;
- deletion refuses a key whose content is not in a sidecar;
- unpack writes under the sidecar's `source_key`, not a content-derived key;
- a member whose bytes do not match `raw_sha256` stops the run;
- an existing key is skipped rather than rewritten;
- members are grouped by frame so each frame decompresses once;
- both modes default to a dry run.

---

## Context you would otherwise have to rediscover

- **The plan:** `docs/plans/plan_145_april_cutover_reconciliation.md`, third
  revision, commit `4b698e7`. Read the *trust boundary* table before deciding
  anything about identity.
- **A NULL sidecar `listing_id` means silver has no observation for that
  object** — `archiver/processors/pack_bronze_html.py:431-456` LEFT JOINs
  silver on `artifact_id`, so the NULL is the join miss. 99,981 of 557,065
  April members. This is why Stage 6 can repair attribution.
- **Sidecar `listing_id` values are wrong** for 194,639 of 371,095 content
  matches. Use sidecars for `raw_sha256`, `source_key` and frame coordinates.
  Never for identity.
- **Legacy `artifact_id` is unusable** — `raw_artifacts` and
  `ops.artifacts_queue` are separate `bigserial` sequences whose integers
  collide.
- **Running production audits:** the memory note
  `reference_running_lake_audits` has the container/tunnel recipe. Short
  version: `cartracker-archiver` has duckdb + boto3 + pyarrow + `shared/`;
  `cartracker-processing` has bs4 + boto3 but no duckdb; run with `-w /app`.
- **Superseded work:** a previous session implemented a `parse` mode against
  the old two-store design. It was reverted before commit; the patch is in the
  session scratchpad only. Do not go looking for it — Stage 4 (CAR-26) reads
  one flat prefix and should be written fresh.

---

## When you are done

Report against the CAR-20 exit criteria: deletion count reconciled against the
sidecar join, zero keys deleted whose content was not in a verified pack, every
unpacked member verified, no duplicate content in the flattened population, and
the final object count (expect ~993,767).

Then stop. Stage 4 (CAR-26, parse) is a separate issue and should not be
started in the same run.
