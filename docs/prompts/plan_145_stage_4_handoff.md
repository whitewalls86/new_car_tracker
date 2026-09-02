# Plan 145 Stage 4 — implementation prompt (CAR-26)

Hand this to a fresh session. The machinery can be built and unit-tested now,
while Stage 3b is still unpacking; only the `--apply` run has to wait.

---

## Preflight: is Stage 3b done?

Stage 4 can be **written and tested** against fixtures at any time. It may only
be **run with `--apply`** once every pack is unpacked.

```
ssh -i ssh-key-2026-04-08.key ubuntu@147.224.199.86 \
  'docker exec -w /app cartracker-archiver python -c "
import io, sys
import pyarrow.parquet as pq
sys.path.insert(0, \"/app\")
from shared.minio import BUCKET, get_boto3_client
c = get_boto3_client(); ks=[]; tok=None
while True:
    kw={\"Bucket\":BUCKET,\"Prefix\":\"recovery/plan145/unpacked/\"}
    if tok: kw[\"ContinuationToken\"]=tok
    p=c.list_objects_v2(**kw)
    ks += [e[\"Key\"] for e in p.get(\"Contents\",[]) if e[\"Key\"].endswith(\".parquet\")]
    if not p.get(\"IsTruncated\"): break
    tok=p.get(\"NextContinuationToken\")
n=sum(pq.read_table(io.BytesIO(c.get_object(Bucket=BUCKET,Key=k)[\"Body\"].read())).num_rows for k in ks)
print(f\"unpack shards: {len(ks)}  members: {n:,}\")"'
```

Expect **32 shards, 557,065 members**. Fewer means 3b is still running: build and
test, but do not `--apply`. Never kill the unpack session.

---

## The population you are parsing

Reconciled 2026-08-28, after Stage 3a deleted 371,095 distinct duplicate
objects:

| source | objects |
|---|---:|
| materialized legacy bodies surviving dedupe | 425,978 |
| unpacked April pack members | 557,065 |
| **flattened population** | **983,043** |

Stage 3a emitted 371,495 successful deletion receipts but only 371,095
distinct object keys. The 400-row difference is idempotent re-deletes of the
same content-derived key appearing in two source manifests, as already recorded
in the Stage 3 evidence. Stage 4 counts distinct keys, not receipt rows. A
2026-08-28 manifest-only audit found all 371,095 materialized content twins in
the deletion manifests and receipts, with zero omitted twins and zero planned
deletions lacking an unpacked hash.

Both live in the same prefix, `html/year=2026/month=4/artifact_type=detail_page/`.
Materialized objects have content-derived stems; unpacked ones keep their
original scraper stems.

**Do not enumerate that prefix.** Listing a million keys costs ~1,000 LIST
requests and tells you less than the manifests already do. Build the input set
from three manifest families instead:

| manifest | shards | what it gives you |
|---|---:|---|
| `recovery/plan145/materialized/*.parquet` | 1,172 | `object_key`, `raw_sha256`, `html_len`, `disposition`, **`listing_id`**, **`fetched_at`**, `http_status`, legacy locator |
| `recovery/plan145/dedupe/*.parquet` | 2,344 | the `object_key`s Stage 3a deleted (deletion manifests + receipts, 2 per source) |
| `recovery/plan145/unpacked/*.parquet` | 32 | `source_key`, `raw_sha256`, `html_len`, `pack_key`, `frame_ordinal`, `artifact_id`, and sidecar `listing_id`/`fetched_at` |

```
inputs = (materialized rows with disposition in (written, exists))
         MINUS (object_keys in the dedupe deletion manifests)
         UNION (unpacked rows)
```

Every object is read through `shared.minio.read_html` by its key. Nothing reads
a pack directly at this stage — that is what Stage 3b was for.

---

## Identity: three tiers, in this order

This is the part to get right. **The unpack manifest's `listing_id` column
holds the sidecar's value and must be ignored** — it is wrong for 313,701 of
457,084 named April members, measured against the scraper's own record on
2026-08-27, because the packer reduces silver with
`any_value(listing_id) GROUP BY artifact_id` and one detail artifact
contributes ~6.7 differing listings. Stage 5b corrects it in the packer.

The sidecar's `fetched_at` is *not* affected — 100.00% exact for June,
99.98% for April, since one capture time is stamped on the primary and every
carousel row. It is still not used below, because tiers 1 and 2 already cover
everything it could and one rule is easier to hold than two.

**Tier 1 — the legacy manifest, by `raw_sha256`.** 797,073 identities.
Trustworthy: Stage 1 verified every hash, and silver corroborates the legacy
listing in 194,734 of 194,734 disagreements. This covers every materialized
survivor by construction, and also 52,041 unpacked members whose content came
from the legacy Parquet — including ~35,775 real pages that have no queue event
at all.

**Tier 2 — `ops_normalized/artifacts_queue_events`, by `minio_path`.** The
scraper's own record, written at capture time (lifecycle step 4). Partitioned
`year=/month=`; read months 3–5. Deduplicate to one row per key — an artifact
has several event rows — and take `listing_id` and `min(fetched_at)`. Covers
514,789 members: all 457,084 named, plus 57,705 whose sidecar identity is NULL.

```sql
SELECT regexp_replace(minio_path, '^s3://[^/]+/', '') AS key,
       any_value(listing_id) AS listing_id,
       min(fetched_at)       AS fetched_at
FROM read_parquet('s3://<bucket>/ops_normalized/artifacts_queue_events/year=2026/month=[345]/*.parquet',
                  hive_partitioning=1, union_by_name=1)
WHERE artifact_type = 'detail_page' AND minio_path IS NOT NULL
GROUP BY 1
```

DuckDB is **not** available in the processing image. Either pre-materialize this
lookup to a Parquet under `recovery/plan145/identity/` from the archiver image,
or read the three month files with pyarrow and build the dict in-process. Say
which you chose in the report.

**Tier 3 — the page itself.** `primary["listing_id"]` from the
`initial-activity-data` blob. Gives a listing but **no capture time**, so such a
row can be parsed and reported but never imported. Expect ~760 real pages here.

Record `listing_id_source` on every row as `legacy_manifest` / `queue_events` /
`parsed_page` / `none`, and `fetched_at_source` as `legacy_manifest` /
`queue_events` / `none`. Stage 5 filters on these.

Where two tiers both resolve, **tier 1 wins** and any disagreement is counted
and reported, never silently resolved.

---

## Parsing

Run `parse_cars_detail_page_html_v1`
(`processing/processors/parse_detail_page.py:292`) **unmodified**.

- **Decode exactly as production does:**
  `read_html(key).decode("utf-8", errors="replace")`, per
  `processing/routers/batch.py:87`. Any other decode changes parsed strings for
  encoding reasons and poisons the comparison, which is the entire deliverable.
  The parser takes `str`, not `bytes`.
- **URL argument:** build `https://www.cars.com/vehicledetail/{listing_id}/`
  from the *resolved tier-1 or tier-2* listing_id, or pass `None`. The parser
  falls back to the URL when the page has no data blob, so a wrong listing_id
  here manufactures a confident wrong identity.
- **Returns `(primary, carousel, meta)`.** Carousel rows are real observations
  of other listings from the same artifact — ~5.7 per page — and production
  writes all of them to silver regardless of `search_config` match.
- **`fetched_at` never comes from the parser or the run.**

### Row construction

Mirror `processing/writers/detail_writer.py:309-351`:

- one `source="detail"` row from `primary`;
- one `source="carousel"` row per hint that has a `listing_id`, a non-NULL
  `price` **and** a `body` — production drops the rest; count the drops here
  rather than discarding them silently;
- dealer fields are copied from the primary onto every carousel row;
- an `unlisted` page yields one row with NULL price/mileage and no carousel.

Two deliberate departures, because this must not touch Postgres:

- carousel `vin` stays NULL (production fills it from a `vin_to_listing`
  lookup) — Stage 5 must not compare that column;
- **no `artifact_id` column at all.** Legacy `artifact_id` is unusable, and
  recovered artifacts get a sequence-allocated one in Stage 5.

### Block pages — classify and exclude

`_detect_challenge` only catches Cloudflare's `Just a moment...`, so an Akamai
`Access Denied` body parses to `listing_state="active"` with every vehicle
field NULL. Importing those would inject tens of thousands of junk rows.

Classify a parsed page as `blocked_other` when the parser says `active` **and**
`listing_id`, `vin`, `price` and `make` are all NULL. Record it, emit no
observation rows, and count it. Do not modify the parser — the "runs unmodified"
property is what makes Stage 5's comparison meaningful (see CAR-27).

Expect roughly 54,341 such pages, nearly all under 512 bytes. Report the
size-band cross-tab so the cohort is a measurement rather than a surprise.

---

## Shape of the job

- **Cost:** 90.7 ms/page × 983,043 ≈ 25 core-hours.
- **Process pool required** — bs4/lxml is GIL-bound, one process ≈ one core.
  Default to `cpu_count() - 2`; the host has 4 and production needs some. No
  resource caps: pack-worker runs uncapped and that is the house precedent.
- **Work unit = one manifest shard** (1,172 materialized + 32 unpacked). Each
  unit writes its own output shards and is skipped on re-run if they exist, so
  an interruption loses one unit.
- Each worker must reset `shared.minio._boto3_client = None` and call
  `clear_pack_caches()` after fork; a shared boto3 client across processes
  yields truncated bodies under load.
- Verify each body's sha256 against the manifest before parsing. Stage 2 proved
  read-back and Stage 3b verified every member, so a disagreement means the
  store moved — **stop the run**, do not record it as a per-input failure.

### Output

Under `recovery/plan145/parsed/`:

- `rows/<unit>.parquet` — silver-shaped observations (the columns in
  `processing/writers/silver_writer.py:_POSTGRES_COLS` minus `artifact_id`,
  plus provenance: `content_sha256`, `object_key`, `listing_id_source`,
  `fetched_at_source`, legacy locator);
- `inputs/<unit>.parquet` — one row per input: hash, size, size band, resolved
  identity and its tier, outcome (`parsed` / `blocked_cloudflare` /
  `blocked_other` / `failed` / `missing_object`), carousel counts, error;
- `parse_report.json` — the aggregate, including the size cross-tab, the tier
  census, identity disagreements, and the block-page counts.

---

## Where it runs

```
docker compose run --rm april-processor python -m \
  scripts.reconcile_april_detail parse                      # dry run: plans and measures
docker compose run --rm april-processor python -m \
  scripts.reconcile_april_detail parse --apply --workers 2
```

`april-processor` is profile-gated and already registered in the three
governance files CI enforces. Run the long job under tmux on the VM.

---

## Non-negotiables

1. **Never kill the running unpack session**, and never re-run `dedupe --apply`.
2. **No production mutation.** Writes go only under `recovery/plan145/parsed/`.
   No Postgres, no silver, no `ops.artifacts_queue`.
3. **Decode exactly as production decodes.**
4. **Never use the sidecar's `listing_id`**, including the copy sitting in the
   unpack manifest. (Its `fetched_at` *is* correct where present — 100.00%
   exact for June, 99.98% for April — but tiers 1 and 2 already cover
   everything it could, so prefer them and keep one rule.)
5. **Announce the blast radius** of any production command before running it —
   object counts, request counts, which prefix. Prefer manifests over
   re-scanning production.
6. **Do not modify the parser.**

---

## Tests

Extend `tests/scripts/test_reconcile_april_detail.py` (1,083 lines, lettered
sections; add new ones at the end).

```
LOG_PATH=/tmp/p145test.log .venv/bin/python -m pytest \
  tests/scripts/test_reconcile_april_detail.py -q -m "not integration"
.venv/bin/python -m ruff check .
```

Cover at least:

- the decode matches `body.decode("utf-8", errors="replace")` on a body with
  invalid UTF-8;
- input set = materialized minus deleted, union unpacked;
- identity tier 1 beats tier 2; tier 2 used when tier 1 misses; tier 3 gives a
  listing but no time, and such a row is marked unimportable;
- a sidecar `listing_id` in the unpack manifest never reaches the parser or the
  output;
- an identity disagreement between tiers is counted, not resolved;
- one primary row plus one carousel row per qualifying hint; the three
  production drop rules; drops counted;
- an unlisted page yields one row, NULL price, no carousel;
- a Cloudflare challenge and an Akamai `Access Denied` are classified
  distinctly and both yield zero rows;
- carousel `vin` is NULL and no row carries `artifact_id`;
- a body whose hash disagrees with the manifest stops the run;
- a parse exception is recorded as `failed`, not lost;
- rows round-trip through the real Parquet schema;
- `parse` defaults to a dry run.

---

## Context you would otherwise rediscover

- **Plan:** `docs/plans/plan_145_april_cutover_reconciliation.md`, third
  revision. Read *The trust boundary* before deciding anything about identity.
- **A NULL sidecar `listing_id` means silver has no observation for that
  object** — `pack_bronze_html.py:441-451` LEFT JOINs silver on `artifact_id`.
- **Why the non-NULL sidecar values are wrong (fixed in Stage 5b).** That same `obs` CTE
  has no `source` filter and does `any_value(listing_id) GROUP BY artifact_id`
  over silver, where a detail artifact contributes one primary row *and* ~5.7
  carousel rows sharing the `artifact_id`. So `any_value` usually returns a
  carousel listing. **Do not "fix" this with a bare `source = 'detail'`
  filter** — that column is also the packer's sort key, so filtering it
  silently reorders every pack, and whether that helps or hurts compression is
  an open question that Plan 145 Stage 6 settles with a bounded trial. Not this
  stage's job either way; just do not propagate the naive fix.
- **Container dependencies:** `cartracker-archiver` has duckdb + boto3 +
  pyarrow + `shared/`; `cartracker-processing` has bs4 + boto3 but **no
  duckdb**. Run with `-w /app`. See the `reference_running_lake_audits` memory.
- **Prior art in the same file:** `run_materialize`, `run_dedupe` and
  `run_unpack` establish the mode structure, the dry-run default, the shard
  layout and the fail-closed style. Follow them.

---

## When you are done

Report against the CAR-26 exit criteria: every input parsed or explicitly
failed, the identity tier census, identity disagreements, the block-page and
size-band cross-tab, carousel fan-out, and total rows written.

Then stop. Stage 5 (CAR-21) is a separate issue with production write access
and its own gates.
