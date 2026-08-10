# Plan 129: Trained zstd Dictionary Compression for Bronze HTML

## Status

**Stage 0 complete and passed (−73.15% on held-out listings from held-out
months, 2026-08-10). Stages 1-4 are the implementation.** Comes out of Plan 114
Stage 3, which measured it as the cheapest storage win available and the
baseline any fancier scheme has to beat.

**This plan is reversible.** No data is discarded, every artifact stays
independently decompressable, and a bad dictionary choice is fixed by
recompressing. That is the main reason it should ship before
[Plan 130](plan_130_parser_input_projection.md), which is not reversible.

---

## Goal

Compress bronze HTML against a zstd dictionary trained on a corpus sample,
instead of compressing every page as an independent frame with empty history.

Target: **~73% reduction** in stored HTML bytes (measured, Stage 0), with no
loss of information and no change to the artifact-per-object model. Against
*physical* disk the expected reclaim is ~60% — see Stage 4 for why the two
numbers differ.

---

## Context

`shared/minio.py:write_html` compresses each page as its own zstd frame:

```python
cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
compressed = cctx.compress(content)
```

Every one of the ~3.9M objects therefore re-encodes the cars.com page shell
— `<head>`, framework bootstrap, analytics config, nav markup — from scratch,
because each frame starts with no history. Plan 114 Stage 3 measured that the
shell is the overwhelming majority of the bytes and is near-identical across
listings.

A dictionary is a fixed blob that both compressor and decompressor pre-load as
if it were already-seen history, so those bytes become short match references
instead of literals. It is a stock zstd feature: frames stay standard, carry a
dictionary ID in the header, and remain individually decodable.

### Measured evidence (Plan 114 Stage 3, 2026-08-08)

60 real artifacts, 12 listings. Dictionary trained on 6 listings and scored on
the **6 disjoint listings** it never saw:

| Approach | % of today | Saving |
|---|---|---|
| today: full page, plain zstd-9 | 100% | — |
| full page + dictionary (16 KB) | 87.1% | −12.9% |
| full page + dictionary (32 KB) | 72.5% | −27.5% |
| full page + dictionary (112 KB) | 38.8% | **−61.2%** |

Bigger dictionaries clearly win on full pages over this range; 112 KB is the
zstd `--train` default and was not the optimum, only the largest tried.

---

## Why This Beats The Alternatives

Plan 114 tested a content-addressed section store on the same sample and found
it **+223% worse** than the baseline. The reason is object overhead: MinIO's
~8 KB/object floor turned 60 artifacts into 556 section objects, costing 4.3 MiB
of padding against a 1.9 MiB baseline.

A dictionary has no such cost. The shared bytes live in **one** file, and a
reference to them is a match token of a few bits inside a stream that was being
written anyway. Amortized across 3.9M objects, even a 768 KB dictionary costs
~0.2 bytes per artifact.

It also captures redundancy at a granularity whole-section dedup cannot: a
dictionary can match a 30-byte run, whereas a section only dedups if every byte
matches — which is why section reuse reached 31% while the redundancy actually
present was ~70%.

---

## Design

### Dictionary lifecycle

The dictionary becomes a **permanent, critical dependency of every object
written against it.** This is the real cost of the plan and the design follows
from it.

1. **Train** offline from a corpus sample (see Stage 1). Never at write time.
2. **Register** the dictionary as an immutable, versioned artifact. zstd assigns
   a dictionary ID that is embedded in every frame written with it.
3. **Write** with the current dictionary. Record its ID in object metadata.
4. **Read** by looking up the dictionary ID from the frame, then decompressing.
5. **Retrain** when compression ratio drifts (cars.com markup changes). Old
   objects keep referencing old dictionaries, so **every dictionary ever used
   is retained forever.** At the Stage 0 optimum they are ~768 KB each; even a
   few dozen of them is nothing.

### Where the dictionary lives

Storing the only copy of the dictionary inside the bucket it protects is a
single point of total failure. Store it in at least two places:

- `bronze/dictionaries/zstd/v{n}.dict` — the working copy read at runtime.
- A row in Postgres (`ops.compression_dictionaries`) holding the bytes, the
  dictionary ID, training parameters, sample description, and `created_at`.

The Postgres copy is the recovery path and the provenance record. A dictionary
with no record of how it was trained cannot be audited or reproduced.

### Backward and forward compatibility

Frames written **without** a dictionary must keep decoding. `read_html` cannot
assume a dictionary is present, because ~3.9M existing objects have none.

The decode path is therefore:

```
read object
  -> parse frame header for dictionary ID
     -> id == 0        : decompress with no dictionary (today's objects)
     -> id in registry : decompress with that dictionary
     -> id unknown     : raise loudly. Never silently return garbage.
```

`zstandard` exposes `zstd.get_frame_parameters(data).dict_id`, so the ID is read
from the frame itself rather than trusted from object metadata. Metadata can be
lost or rewritten by a copy; the frame cannot.

### Existing objects

**Now Stage 4**, not a follow-up. This was originally deferred — "new writes
get the dictionary; a backfill decision comes after the write path has been
stable for a while" — on the assumption there was time to be patient. At 98%
disk there is not, and since Stages 1-3 free nothing that already exists, the
backfill *is* the deliverable. `scripts/recompress_bronze_html.py` (Plan 116)
is the starting point.

---

## Stages

### Stage 0 — Reproduce the measurement at corpus scale

**Status: complete. Gate passed at −73.15% (2026-08-10).**
`scripts/estimate_dictionary_savings.py` plus 40 unit tests in
`tests/scripts/test_estimate_dictionary_savings.py`. Run against production by
sampling from the dbt-runner container and measuring locally over an SSH tunnel
to MinIO (`--sample-only` / `--sample-in`; see `--help`). Results below.

The 61.2% figure comes from 60 artifacts in 12 listings from one capture era.
Before building anything, confirm it holds on a broad sample.

The script reports **four** splits rather than one, because the two holdouts
can each be gamed by the sample and the difference between them is the finding:

| Split | What its number means |
|---|---|
| `leaky_reference` | Artifact-level random split. Deliberately leaky, reported so the size of the leak is measured rather than assumed small. |
| `listing_disjoint` | Disjoint listings, same months. Comparable to the Plan 114 Stage 3 figure. |
| `month_disjoint` | Held-out months, listings may recur. Isolates markup drift. |
| `listing_and_month_disjoint` | Both. **This is the gate.** |

The strict split also excludes from test any listing appearing *anywhere* in
training, not merely in a training month — one capture of a listing is enough
to teach the dictionary that vehicle's text, and filtering by month alone would
be the same leak in a different costume.

The gate is decided on the strict split only, and a sample that cannot produce
one (a single capture month) is reported as **undecided** rather than passing.
`main` exits 0 on a pass, 1 on a fail, and 2 on undecided, so an unmeasurable
gate cannot read as a green run.

CLI:

```
--months 2026-03 2026-04 ...   Sample across capture eras, not one month
--sample-size N                Artifacts to sample (default 2000)
--dict-sizes 16 32 112 256     KB sizes to try
--optimize-cover               Use zstd's cover-parameter optimizer
--json-out PATH
--holdout-months YYYY-MM ...   Default: the most recent month sampled
--sample-only / --sample-out   Sample the lake, write JSON, stop (dbt-runner)
--sample-in PATH               Measure from a sample file (local, over a tunnel)
--only-splits NAME ...         Measure only these splits (size sweeps)
```

Sampling is deliberately **not** `audit_semantic_duplicate_html_hashes.fetch_sample`,
which selects the highest-duplicate-count groups: that over-represents repeat
captures of one listing by construction, which is the exact bias the
listing-disjoint split exists to defeat. A corpus storage estimate needs a
sample shaped like the corpus, so `fetch_corpus_sample` spreads the budget
evenly across months and orders by `hash(artifact_id)` — deterministic, because
a storage decision that cannot be reproduced cannot be audited.

**The measurement trap, stated because it is easy to fall into:** repeated
captures of one listing are near-identical, so a random train/test split lets
the dictionary memorise the test set. Plan 114 Stage 3 hit exactly this and had
to be re-run. **Split by `listing_id`, not by artifact**, and assert
train/test listing sets are disjoint.

Also sample across *time*: a dictionary trained and tested on one week will
overstate, because it never sees markup drift.

Gate: **≥40% saving on held-out listings from held-out months.** Below that,
stop — the operational cost of a permanent dictionary dependency is not worth a
marginal win.

### Stage 0 — measured, 2026-08-10

**1105 artifacts / 1091 listings / 5 capture months (193.5 MiB raw), zero fetch
failures.** Sampled evenly across months by `fetch_corpus_sample`, measured
locally over an SSH tunnel to production MinIO.

**GATE PASSED at 73.15%**, against a 40% bar.

Strict split (`listing_and_month_disjoint`): train 437 docs / 430 listings from
2026-04..07, test 130 docs / 130 listings from 2026-08, baseline 4.0 MiB of
plain zstd-9.

| Dictionary | % of today's zstd-9 | Saving |
|---|---|---|
| 16 KB | 88.63% | −11.37% |
| 32 KB | 75.09% | −24.91% |
| 112 KB | 42.93% | −57.07% |
| 256 KB | 31.49% | **−68.51%** |

The 112 KB row is the one directly comparable to Plan 114 Stage 3's −61.2%.
It comes in at −57.07% under the stricter holdout on an 18x larger sample —
close enough to call the original finding reproduced, and slightly lower in
exactly the direction a stricter measurement should move it.

**The trap that burned Plan 114 does not bite a corpus-shaped sample, and that
is a finding about sampling rather than about dictionaries.** All four splits
land within ~2.5 points of each other at 256 KB:

| Split | Saving @ 256 KB |
|---|---|
| `leaky_reference` | 70.86% |
| `listing_disjoint` | 71.07% |
| `month_disjoint` | 68.63% |
| `listing_and_month_disjoint` | 68.51% |

`listing_disjoint` scoring *above* `leaky_reference` is the tell: a broad random
sample draws 1091 distinct listings across 1105 artifacts, so there are almost
no repeat captures for an artifact-level split to leak. The ~5-point leak Plan
114 hit came from sampling the highest-duplicate groups, where repeat captures
are the sample. The guard is still right to keep — it costs nothing and the
next sample may not be so clean — but the honest reading is that **the
remaining spread is markup drift, not listing overlap**: essentially all of the
2.4-point drop comes from holding out a month, and none from holding out
listings.

**Dictionary size: the curve turns over at ~768 KB.** 256 KB was the largest
size in the first run and it won, so the sweep was extended on the gate split:

| Dictionary | % of today's zstd-9 | Saving | Gain over previous |
|---|---|---|---|
| 256 KB | 31.49% | −68.51% | — |
| 384 KB | 29.43% | −70.57% | +2.06 |
| 512 KB | 28.08% | −71.92% | +1.35 |
| 768 KB | 26.85% | **−73.15%** | +1.23 |
| 1024 KB | 26.91% | −73.09% | **−0.06** |

1 MB is *worse* than 768 KB, which is the signal that the dictionary has
outgrown what 437 training documents can support — past that point the trainer
is fitting material that does not generalise. So 768 KB is the optimum **for
this training set**, not a value to hard-code: Stage 1 trains on a far larger
sample and its optimum will likely sit higher. **Re-run this sweep in Stage 1
against the real training corpus and pick the size from that**, rather than
inheriting 768 KB from a 437-document fit.

The practical spread is narrow either way. Everything from 512 KB up is within
1.3 points, and the dictionary's own bytes are irrelevant at any of these sizes
— 768 KB across ~3.9M objects is 0.0002 bytes per artifact.

**Two caveats, both about the sample rather than the result:**

- **The months are 2026-04..08, not 2026-01..08.** `artifacts_queue_events`
  carries `minio_path` for detail pages only from 2026-04 onward, and April is
  partial (514,789 of 1,110,888 observations). The lake has detail observations
  back to 2026-01; the paths to their HTML do not exist, so those artifacts are
  unreachable for any object-level measurement. ~4.5 months of drift is a
  reasonable span, but it is a ceiling imposed by the events table, not a
  choice.
- **1105 of a 2000 budget.** Per-month caps went unfilled for the same reason.

### Stage 1 — Train and register the first dictionary

**Stage 0 finding to carry in:** pick the dictionary size by re-running
`estimate_dictionary_savings.py --only-splits listing_and_month_disjoint` over
this stage's (larger) training corpus. Stage 0's optimum of 768 KB was fitted
on 437 documents and 1 MB already regressed against it; a bigger training set
moves that boundary.

- `scripts/train_html_dictionary.py`: samples the corpus, trains, writes the
  dictionary to MinIO and Postgres, prints the dictionary ID and measured ratio.
- Schema migration for `ops.compression_dictionaries`.
- Deterministic: same sample + parameters must reproduce the same dictionary,
  so record the sample key list alongside it.

### Stage 2 — Read path first

Ship decompression support **before** anything writes with a dictionary. Deploy,
confirm nothing regressed, and only then enable writes. A read path that lands
after the write path is an outage.

- `shared/compression.py`: registry, frame-ID resolution, in-process cache
  (dictionaries are immutable, so the cache never needs invalidating).
- `read_html` resolves the dictionary ID from the frame header.
- Unknown dictionary ID raises a distinct exception, monitored.

**The compatibility survey, which is the part most likely to bite.** Anything
that decompresses bronze HTML *outside* `shared.minio.read_html` will break the
moment a dictionary frame exists, and it will break at read time, long after
the write looked fine. At least one such consumer is already known:
`scripts/audit_semantic_duplicate_html_hashes.py` reads objects through DuckDB
`read_blob`. Before Stage 3 writes a single dictionary frame, enumerate every
consumer — scripts, dbt models, `mc`/CLI usage, anything calling
`get_boto3_client().get_object` directly — and either route it through the
shared read path or confirm it only ever touches pre-dictionary objects.

### Stage 3 — Write path behind a flag

- `HTML_COMPRESSION_DICT_ID` env var. Unset = today's behaviour exactly.
- `write_html` compresses with the registered dictionary when set.
- Roll out to one service first (`processing` or `scraper`), watch the ratio
  metric, then enable everywhere.

### Stage 4 — Backfill existing objects

**This is where the headroom comes from.** Stages 1-3 change new writes only
and free zero existing bytes; on a disk at 98% that distinction is the whole
plan. Originally scoped out of the first cut, promoted here on 2026-08-10
because storage pressure made it the point rather than a follow-up.

- Extend `scripts/recompress_bronze_html.py` (Plan 116) to recompress against
  the registered dictionary. It already has `--apply`, `--checkpoint`,
  `--limit` and prefix selection, and it never deletes.
- The bronze bucket is **un-versioned** (verified 2026-08-10), so an in-place
  rewrite frees its space immediately — there are no old versions to expunge
  and no transient doubling beyond the single object in flight.
- Run in month-sized batches with checkpoints, so an interrupted pass resumes
  rather than restarts.

**Expected reclaim, with the object floor applied.** Do not quote the −73%
logical figure as a disk saving: MinIO's ~8 KB/object floor caps the physical
win, which is the same arithmetic that killed sectioning in Plan 114.

| | Today | After dictionary |
|---|---|---|
| Mean compressed payload | ~31.5 KB | ~8.5 KB |
| Physical per object (4 KB dir + 4 KB-rounded file) | ~40 KB | ~16 KB |
| ~3.9M objects | ~156 GB | ~62 GB |

So roughly **90-95 GB reclaimed, ~60% physical** against ~73% logical. Still
decisive, and worth restating whenever someone quotes the headline number.

### Stage 5 — Observability and retraining trigger

- Metric: compressed/raw ratio per write, by dictionary ID.
- Metric: decompression failures by reason.
- Alert when the rolling ratio degrades past a threshold — that is markup drift
  and the signal to retrain.

---

## Testing

### `tests/shared/test_minio_dictionary.py`

**Group A — round trip**
- Compress with dictionary, decompress with dictionary, bytes identical.
- Compress *without* dictionary, decompress via the new path, bytes identical
  (the 3.9M-existing-objects case).
- Level-3 legacy frames still decode.

**Group B — dictionary resolution**
- Dictionary ID is read from the frame, not from object metadata.
- Object metadata disagreeing with the frame does not change the outcome.
- Unknown dictionary ID raises the dedicated exception; never returns partial
  or garbage output.
- Registry cache returns the same dictionary object without re-fetching.

**Group C — write path flag**
- Flag unset produces byte-identical output to today's `write_html`.
- Flag set produces a frame whose `dict_id` matches the configured dictionary.

**Group D — measurement honesty** (`tests/scripts/test_estimate_dictionary_savings.py`)
- Train/test split is disjoint **by listing**, and a same-listing split is
  rejected rather than silently measured.
- Reported saving is computed on held-out documents only.
- Too small a sample returns no result rather than a meaningless ratio.

### Integration

- `tests/integration/shared/`: write with a dictionary to a test prefix, read
  back through the production read path, assert equality.
- Assert the audit/backfill scripts can still read both frame types.

---

## Files Changed

| File | Change |
|------|--------|
| `shared/minio.py` | Dictionary-aware `write_html` / `read_html` |
| `shared/compression.py` | New: dictionary registry, cache, frame-ID resolution |
| `scripts/estimate_dictionary_savings.py` | New: Stage 0 measurement — **done** |
| `scripts/train_html_dictionary.py` | New: train + register |
| `sql/migrations/` | New: `ops.compression_dictionaries` |
| `scripts/recompress_bronze_html.py` | Stage 4: recompress against the dictionary |
| `tests/shared/test_minio_dictionary.py` | New |
| `tests/scripts/test_estimate_dictionary_savings.py` | New — **done**, 40 tests |

---

## Success Criteria

| Metric | Gate |
|--------|------|
| Held-out saving (disjoint listings and months) | ≥40% — **met, 73.15%** |
| Physical disk reclaimed by the backfill | ~90-95 GB (~60%; the object floor caps it) |
| Consumers reading HTML outside `read_html` | Zero remaining before any dictionary write |
| Round-trip correctness | 100%, byte-identical, both frame types |
| Existing objects readable | 100%, unconditionally |
| Unknown dictionary ID | Raises; never silently wrong |
| Write path with flag unset | Byte-identical to today |

---

## Risks

| Risk | Mitigation |
|------|------------|
| Dictionary lost → all dictionary-compressed objects unreadable | Two independent stores (MinIO + Postgres); never delete a dictionary |
| Read path deployed after write path | Stage 2 ships before Stage 3, deliberately |
| Markup drift degrades ratio silently | Ratio metric + alert; retrain and register a new ID |
| Dictionary sprawl | IDs are immutable and tiny; retain all, never reuse an ID |
| Measured win doesn't generalize | Stage 0 gate on held-out listings *and* months — passed |
| A consumer decompresses outside `read_html` and breaks on dictionary frames | Stage 2 compatibility survey; at least one such consumer already known (`audit_semantic_duplicate_html_hashes.py` via DuckDB `read_blob`) |
| Backfill runs before the read path is everywhere | Stage 4 after Stage 2 and 3; a backfill that lands first is an outage across 3.9M objects, not one service |
| Backfill IO on a near-full disk | Bucket is un-versioned so rewrites free space per object; run in checkpointed month batches |

---

## Out of Scope

- Dropping any content — that is [Plan 130](plan_130_parser_input_projection.md).
- Packing multiple artifacts per object.
- Changing `ZSTD_LEVEL`.
- Retention or expiry of raw HTML. There is no lifecycle rule on the bucket and
  no HTML deletion anywhere in the codebase; whether that should change is a
  real question, and deliberately not this plan's.
