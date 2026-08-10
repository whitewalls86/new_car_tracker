# Plan 114: Sectioned HTML Artifact Audit

## Goal

Test whether raw detail-page HTML can be split into stable, reusable sections
and represented by a verified manifest without losing the ability to reprocess
artifacts later.

This plan is audit-first. It should not immediately replace full raw HTML
writes in the production scrape path.

If it succeeds, it creates the path to longer raw-artifact retention than a
simple 30-day window.

---

## Context

Plan 110 found that whole-file byte hashes do not match even when parsed vehicle
state is unchanged. Follow-up diffs showed the difference is often concentrated
in request/session/analytics regions while vehicle data remains stable.

That suggests the whole HTML file is the wrong dedup unit. The better unit may
be a set of named page sections.

---

## Hypothesis

For semantically unchanged detail pages:

- Vehicle and dealer sections often remain stable.
- Page shell and static references often remain stable.
- Volatile analytics/session sections change per request.
- A manifest can describe how to reconstruct parser-equivalent HTML from
  content-addressed sections.

If section-level reuse is high, we can later replace eligible full raw HTML
objects with compact manifests plus shared section objects.

---

## Sectioning Is Lossless. There Is No Second Tier.

This is the decision that shapes everything below, taken during Stage 1 after a
"normalized" tier had been built and measured.

Sections are contiguous, non-overlapping character slices covering the entire
document, so `reconstruct(extract_sections(html)) == html` byte-for-byte **by
construction**. `processing/html_sections.py` deliberately exposes no way to
drop, reorder, or rewrite a section.

The rejected tier key-sorted JSON script bodies and discarded sections believed
to be per-request noise. Two reasons it went:

- **It is an unrepeatable bet.** The equivalence gate can only ever validate
  against the *current* parser, while the entire point of retaining artifacts is
  reprocessing them under *future* parser versions. Once the original bytes are
  gone the decision cannot be re-run.
- **It bought almost nothing.** Measured saving was ~1.8% on one real fixture
  (192,546 -> 189,127 chars): dropping the "volatile" sections recovered 4,689
  chars, and key-sorting added 1,270 back.

`test_module_exposes_no_way_to_drop_or_rewrite_a_section` fails if
`normalize_section`, `tier_b_html`, or `volatile_drop_risks` reappear, or if a
`volatile` field is re-added to `Section`. That test is the guard on this
decision.

A consequence worth keeping: nothing reads *inside* a block. Boundaries come
from tag offsets, the `id` attribute, and class names only — there is no JSON
parsing in the module — so an upstream change to a payload's *shape* cannot move
a boundary or corrupt a slice.

---

## Section Taxonomy

The taxonomy below is what Stages 0-2 actually built, replacing the speculative
initial list. Sections the pages do not mark (`global_header`, `static_assets`,
`anti_bot_scripts`, `vehicle_detail_dom`) were **not** invented; the bytes
between real anchors are named honestly as filler.

| Section | Anchor | Notes |
|---------|--------|-------|
| `document_prefix` | — | Everything before the first anchor |
| `vehicle_activity_json` | `script#initial-activity-data` | Parser-critical |
| `als_json` | `script#initial-als-data` | The one genuinely per-listing script config |
| `vehicle_controller_json` | `script#CarsWeb.VehicleDetailController.show` | Parser-critical; holds the `"seller"` blob |
| `script_<slug(id)>` | any other `script[id]` | 9 further page-shell config blocks |
| `dealer_contact_block` | `.dealer-card` | Parser-critical |
| `carousel_block` | `div.listings-carousel` | Parser-critical |
| `filler_N` | — | Bytes between anchors, numbered in document order |
| `document_suffix` | — | Everything after the last anchor |

`PARSER_CRITICAL_SECTION_NAMES` names the four sections the parser reads. It
exists for the Stage 4 projection *measurement* only — storing just those four
is the same unrepeatable bet rejected above, so it is a number, not a path.

**Known gap:** `<title>` (read only when the activity JSON is absent,
`parse_detail_page.py:101`) and `spark-notification.unlisted-notification`
(`parse_detail_page.py:61`) are not anchored; they land inside filler. Harmless
while sectioning is lossless, but a precondition anyone adding pruning must
revisit first.

Refining this taxonomy is still in scope and still safe: finer anchors add
boundaries without discarding bytes, so every refinement stays inside the
lossless guarantee. That is the Stage 3 question.

---

## Audit Algorithm

For a sample of known semantic duplicate groups:

1. Fetch full raw HTML for each artifact.
2. Record element spans with `html.parser.HTMLParser`, converting `getpos()` to
   absolute character offsets. **Not** BeautifulSoup + `str(element)`: lxml
   re-serialization introduces byte differences everywhere and would conflate
   "sectioning is wrong" with "lxml normalized an attribute".
3. Slice the document at anchor boundaries. Every byte lands in exactly one
   section; nothing is normalized, dropped, or reordered.
4. Hash each section (`sha256` of the exact slice, flat global namespace).
5. Measure section-level hash reuse **within a listing** (across captures) and
   **across listings** as separate numbers. They have different implications
   and earlier drafts of this plan conflated them.
6. Measure the redundancy that exists *below* section granularity — line-level
   and character-level — to bound what a finer taxonomy could reach.
7. Estimate storage size for:
   - current full raw HTML
   - section objects plus per-artifact manifests
   - full raw retained only for failures/recent grace period

   Count **objects as well as bytes**. See "Storage Accounting" below: a
   bytes-only estimate is optimistic by roughly 8 KB per object.
8. Reconstruct from manifest + sections and assert byte equality with the source.
9. Run the existing parser against reconstructed HTML and compare parsed output
   against the source parse (the Stage 0 equivalence contract below).

---

## Equivalence Contract

Implemented as `parse_outputs_equivalent`. Two HTML strings are parser
equivalent when `parse_cars_detail_page_html_v1` returns:

- `primary` — compared **exactly**;
- `carousel` — compared **exactly**;
- `meta` — compared exactly **after excluding `html_len`**, with
  `primary_json_present`, `dealer_card_found`, `carousel_found`, `cards_found`
  and `cards_parsed` all required to match.

`html_len` is excluded only because the parser records raw input length
(`parse_detail_page.py:380`). Nothing else is relaxed.

Reconstruction is already guaranteed by construction, so this gate is not what
makes it safe — it is what catches a bug in the extractor, and it is the gate
any future change to `html_sections.py` must pass.

Byte-identical reconstruction also disarms two whole-document scans in the
parser that are invisible until something is dropped or reordered:

- `parse_detail_page.py:166` searches `"seller"\s*:\s*\{([^}]+)\}` over the
  whole serialized document, **first match wins**. Dropping any section earlier
  than `vehicle_controller_json` that happens to contain a `"seller"` object
  would silently change `dealer_phone` and the fallback `dealer_zip`.
- `parse_detail_page.py:72` searches the raw string for "no longer
  available"/"no longer listed". Dropping an analytics or anti-bot section
  containing that text flips `listing_state` from `unlisted` to `active`.

---

## Manifest Shape

Prototype manifest:

```json
{
  "manifest_version": 1,
  "artifact_id": 4171890,
  "listing_id": "49953eaa-2c73-4841-b419-7d77f81a534e",
  "source_minio_path": "s3://bronze/html/...",
  "parser_version": "cars_detail_page__v1",
  "source_raw_sha256": "...",
  "sections": [
    {
      "name": "vehicle_controller_json",
      "content_sha256": "...",
      "content_path": "s3://bronze/html_sections/sha256/...",
      "encoding": "utf-8",
      "normalized": false,
      "kind": "script",
      "length": 4096
    }
  ],
  "reconstruction": {
    "mode": "ordered_sections",
    "parser_equivalent_verified": true,
    "verified_at": "2026-07-01T00:00:00Z"
  }
}
```

`normalized` is present on every entry for schema compatibility and is always
`false`. `kind` is structural (`script` / `dom` / `filler`) and is never derived
from a block's contents. Section order in the manifest *is* the reconstruction
order.

The manifest is a derivative artifact. The initial audit does not delete source
HTML.

---

## Success Criteria

The approach is worth productionizing only if the audit shows:

| Metric | Gate |
|--------|------|
| Byte-identical reconstruction | 100% — by construction, so any miss is an extractor bug |
| Parser-equivalent reconstruction | 100% on sampled successful parses |
| Section extraction failure rate | Low enough to fallback without operational noise |
| Net storage savings after manifests | Meaningful enough to beat compression-only |
| Stable section reuse | Concentrated in large sections, not only tiny fragments |
| Failure handling | Full raw retained for parse/section failures |

Reconstruction correctness is the non-negotiable gate, and losslessness moved it
from ">= 99%" to exactly 100%: the audit does not accept a sampled artifact that
fails to round-trip byte-for-byte.

The storage gate is the one still open. Compression-only is the baseline to
beat, and section objects are stored compressed too, so the honest comparison is
*compressed* section store + manifests versus *compressed* full raw — not
uncompressed character counts, which flatter dedup by counting savings zstd
would have found anyway.

---

## Storage Accounting: Object Count Is A Cost

Measured on the production VM 2026-08-08 while diagnosing a full `/mnt/data`.
This bears directly on the storage gate above, which is the one still open.

MinIO stores every object as a *directory* plus an `xl.meta` file, inlining the
payload into `xl.meta` below 128 KB. On this single-drive backend that is a
floor of roughly **8 KB per object** (4 KB directory + 4 KB-rounded file)
regardless of how small the content is.

| Measure | Value |
|---|---|
| `bronze` logical size (MinIO `minio_bucket_usage_total_bytes`) | 149.4 GB |
| Objects in `bronze` | 3,918,760 |
| Mean object size | ~38 KB |
| Inodes used on `/mnt/data` | 8,774,058 of 13,107,200 (67%) |
| Inodes per object | ~2.24 |
| Physical consumption | ~172 GB (184 GB disk used less ~12 GB non-MinIO) |

At today's ~38 KB average the floor is ~20% overhead — the ~23 GB gap between
logical and physical above. Real, but not decisive, and **not** why the disk
filled (that was a stubbed retention query, unrelated to this plan).

**It becomes decisive for manifests.** A manifest is 1:1 with an artifact and
small. At ~1 KB each, 3.9M manifests stored one-per-object cost **~32 GB in
padding alone** — an 800% overhead on their own content. A projection that
models manifests by their serialized size will overstate the win substantially.

Consequences for Stage 3 and any production path:

- **Manifests should be batched**, not one object per artifact — e.g. one
  object per source per day, or Parquet rows keyed by `artifact_id`. This is
  the pattern `flush_silver_observations.py` and `flush_staging_events.py`
  already use for observations and events; raw HTML is currently the only
  dataset in the bucket stored one-object-per-record.
- **Section objects are the defensible case for individual objects**, because
  they are content-addressed and shared, so their count is bounded by *unique*
  sections rather than by artifact count. Stage 3 should report unique section
  count directly — it is the input that decides whether
  `bronze/html_sections/sha256/...` is viable as written in the manifest
  prototype.
- **Inode headroom is tighter than byte headroom.** 8.77M of 13.1M are already
  used. Section objects consume inodes at ~2.24 each, and that ceiling can be
  hit while the disk still reports free space.

### The Compression Baseline Should Include A Trained Dictionary

Success Criteria puts compression-only as the baseline to beat. That baseline
should be a *trained zstd dictionary*, not today's per-object compression.

Each page is currently compressed as an independent zstd frame
(`shared/minio.py:write_html`), so shared page shell is re-encoded from scratch
in every one of the 3.9M objects. A dictionary trained on a corpus sample
targets exactly that redundancy while keeping each artifact independently
decompressable.

This matters for gate honesty: much of the "static page shell dedups
corpus-wide for free" finding in Stages 0-2 — the six byte-identical
`script_*` config blocks — is redundancy a dictionary would also capture. If
the baseline is naive per-object zstd, sectioning gets credit for savings that
a much cheaper change would have delivered.

The two are complementary rather than competing: a dictionary attacks
boilerplate shared across *different* listings, sectioning attacks reuse across
*repeated captures of the same* listing. Measuring the dictionary baseline
first also bounds how much complexity the sectioning machinery has to justify.

---

## Production Path If Audit Succeeds

Later production rollout:

1. Scraper writes full raw HTML exactly as today.
2. Processing parses full raw HTML.
3. If parse succeeds, processing creates sections and manifest.
4. Reconstruction is verified against parser output.
5. Full raw HTML is retained for a recovery grace period.
6. Cleanup later deletes full raw HTML only for artifacts with verified
   manifests.
7. Parse failures, section failures, and unknown layouts keep full raw HTML.

This preserves idempotency while enabling longer logical retention.

---

## Relationship To Retention

Plan 110 deliberately avoids a 30-day automatic deletion window because Plan 114
may let us retain reprocessable artifacts for much longer.

If sectioned storage works, retention policy should become:

- Short/medium retention for full raw successful pages.
- Long retention for manifests and shared sections.
- Long or indefinite retention for parse failures and layout-change examples.
- Explicit emergency cleanup for storage pressure.

---

## Testing

### Unit Tests

- Section extractor returns deterministic section names and order.
- Sections are contiguous, non-overlapping, and cover the whole document.
- Section boundaries do not depend on block contents.
- The module exposes no way to drop, reorder, or rewrite a section.
- Different vehicle price/mileage data changes the relevant section hash.
- Manifest serialization is stable.

### Audit Tests

- Two semantically unchanged artifacts produce matching hashes for stable
  sections.
- Per-listing and cross-listing reuse are reported as separate numbers.
- Sub-section redundancy is reported as a bound on a finer taxonomy.
- Storage estimate includes section objects and manifest overhead.

### Reconstruction Tests

- Reconstructed HTML can be passed to the existing parser.
- Parser output from reconstructed HTML matches parser output from full raw HTML.
- Failed reconstruction does not mark the manifest verified.

### Integration Tests

- Audit script reads sampled artifacts from MinIO and writes a local/MinIO
  report.
- Section content objects are written under a test prefix.
- Full raw HTML is not deleted by the audit.

---

## Files Changed

| File | Change | Stage |
|------|--------|-------|
| `processing/html_sections.py` | Section extraction, manifest helpers, equivalence gate | 0-2 (done) |
| `tests/processing/test_html_sections.py` | Unit coverage for extraction/manifest logic | 0-2 (done) |
| `scripts/audit_sectioned_html_storage.py` | Audit runner over a real MinIO sample | 3 |
| `tests/scripts/test_audit_sectioned_html_storage.py` | Unit coverage for the audit's measurement functions | 3 |

---

## Stage Status

### Stages 0-2 — complete

`processing/html_sections.py` plus 43 unit tests. The equivalence gate passes on
all three fixtures. Two real fixtures (two different listings) gave the
measurements that shaped Stage 3:

| Measure | Result |
|---|---|
| Whole-section hashes shared across the two listings | **2.2%** of the document (4,294 / 192,546 chars) |
| Line-level common content between them | **~52%** of the document (100,159 chars) |

So roughly 50 percentage points of real redundancy exist but are unreachable at
the current granularity: the large fillers mix static page shell with
listing-specific content, and one differing byte spoils a 60KB slice.

| Section | Chars | Line-level common |
|---|---|---|
| `document_prefix` | 3,405 | 2,377 (70%) |
| `filler_3` | 60,712 | 52,187 (86%) |
| `filler_6` | 80,037 | 41,082 (51%) |
| `document_suffix` | 1,146 | 219 (19%) |

**~52% is a lower bound, not a ceiling.** `document_suffix` is the cautionary
case: 19% common line-for-line, but **99%** common character-for-character —
the entire difference between two unrelated listings is an 8-hex-character build
token (`68d40bf0` vs `ba4cf7d2`). Granularity units decide the answer, so
Stage 3 measures character-level too.

Of the seven sections hypothesised to be per-request volatile, six
(`script_datadog_config`, `script_datadog_global_context`,
`script_datadog_logs_config`, `script_event_stream_config`,
`script_third_party_flags`, `script_graphql_config`) are byte-identical across
the two listings; only `als_json` genuinely varies. These are static page shell,
not per-request noise, and they dedup corpus-wide for free. Caveat: n=2, same
capture era. Stage 3 settles it.

### Stage 3 — the question, restated

The original framing was "is section reuse high enough?". After Stages 0-2 it is:

> Can the filler sections be split finely enough to reach the redundancy that
> demonstrably exists — and is that reachable losslessly?

It is: finer anchors add boundaries without discarding bytes. Order of work:

1. Sample real duplicate groups from MinIO.
2. Report whole-section hash reuse within a listing and across listings,
   separately.
3. Measure the character-level bound, not just line-level.
4. Only then decide whether to refine anchors inside `filler_3` / `filler_6` /
   `document_prefix` / `document_suffix`.
5. Re-test the volatile/stable labels against the real sample.

Anchor refinement is a taxonomy change: entries in `_SCRIPT_ID_SECTION_NAMES` /
`_anchor_name`. Every existing invariant test should keep passing unchanged — if
one breaks, the refinement is wrong, not the test.

### Stage 3 — measured, 2026-08-08

`scripts/audit_sectioned_html_storage.py` over **60 real artifacts / 12
listings / 12 semantic duplicate groups**, sampled by
`audit_semantic_duplicate_html_hashes.fetch_sample` (highest-duplicate-count
groups — one had 3,345 captures of identical parsed state, so the
within-listing figure is an upper bound).

**The correctness gates passed completely.**

| Gate | Result |
|---|---|
| Byte-identical reconstruction | **100%** (60/60) |
| Parser-equivalent | **100%** (60/60) |
| Fetch/section failures | 0 |
| Lossy utf-8 decodes | 0 |

The sectioner works on production data, including page variants absent from the
fixtures. That result stands on its own regardless of what follows.

**The storage case does not survive contact with the real backend.**

| Measure | Result |
|---|---|
| Whole-section reuse, within a listing | 30.42% (upper bound) |
| Whole-section reuse, across listings (extra) | **0.65%** |
| Total, uncompressed chars | 31.07% |
| Compressed saving vs like-for-like zstd-9 baseline | **−223%** |
| Split penalty before dedup pays anything back | +19.4% |
| Section objects | **556** for 60 artifacts (9.27 each), 1,245 inodes |

The 8 KB/object floor is what kills it: 556 section objects cost 4.3 MiB of
padding against a 1.9 MiB baseline for the same content. Sectioning spends
more on object overhead than the entire corpus costs today.

**A trained dictionary beats it outright, for a fraction of the complexity.**

Trained on 30 documents and scored on the 30 held out (training and scoring on
the same documents overstated it by ~5 points, so the audit splits them):

| Measure | Result |
|---|---|
| Dictionary alone vs per-object zstd | **−62.2%** storage |
| Sectioning on top of the dictionary | −754.7% |

No manifests, no section objects, no reconstruction step, and every artifact
stays independently decompressable. This is the recommendation Stage 4 should
carry forward.

**Finer granularity is a trap, quantitatively.** Content-defined chunking over
20 artifacts, which is what a maximally fine lossless splitter could reach:

| Target | Gross dedup | Net after 8 KB/object |
|---|---|---|
| 256 B | 70.5% | **−622.9%** |
| 1 KiB | 60.9% | **−139.2%** |
| 4 KiB | 48.4% | **−20.9%** |

So the answer to the Stage 3 question is: **yes, the redundancy is reachable
losslessly — and reaching it costs far more than it saves.** Gross dedup rises
as granularity gets finer, exactly as the Stage 0-2 line/char gap predicted,
and net savings fall off a cliff at the same time. There is no target size
where this wins. Refining anchors inside `filler_3`/`filler_6` is therefore
**not** worth building: step 4 of the plan above is answered *no*, and answered
by measurement rather than by taste.

**The volatile hypothesis is refuted at n=60.** The n=2 finding — six of seven
"volatile" script configs byte-identical — was a same-capture-era artifact,
exactly as the Stage 0-2 handoff warned it might be:

| Section | n=2 verdict | n=60 verdict |
|---|---|---|
| `script_event_stream_config` | identical | identical (1 hash / 60) |
| `script_third_party_flags` | identical | identical (1 hash / 60) |
| `script_datadog_config` | identical | **5 hashes** |
| `script_datadog_logs_config` | identical | **5 hashes** |
| `script_graphql_config` | identical | **30 hashes** |
| `als_json` | varies | varies (60 hashes / 60) |

Worse for dedup, the *large* sections are the least stable: `filler_3` (72.9 KB
median) has 60 distinct hashes across 60 artifacts, and
`vehicle_controller_json`, `vehicle_activity_json` and `dealer_contact_block`
are likewise unique per capture — **even though every artifact in a group
parsed to identical vehicle state.** That is the Plan 110 finding reproduced at
section granularity, and it is why whole-section dedup reaches only 31%.

**Taxonomy defect found:** 57 of the 106 section names occur exactly once in
the whole sample, all matching `script_<digits>`. cars.com emits inline scripts
with random numeric ids, so the `script[id]` anchor rule mints a fresh section
name per capture. They are small (67 KB of 10.9 MB, 0.6%) but they inflate the
unique-object count, which is the cost that matters most here. The audit labels
these `seen_once` rather than `identical_corpus_wide` — a name seen once is the
least evidence of stability, not the most.

**Recommendation: do not productionize sectioned storage.** Keep
`processing/html_sections.py`; it is correct, lossless, and is the right tool
for *reprocessing* artifacts. It is simply not a storage win. Pursue the
trained zstd dictionary instead, and batch any manifests that survive.

Caveats: n=60 from the highest-duplicate groups, one capture era, single-drive
MinIO backend. The 8 KB floor is backend-specific — a packed-object layout
would change the arithmetic, though not the 0.65% cross-listing reuse.

### Follow-on measurements (2026-08-08, same sample)

Two alternatives were measured against the same 60 artifacts, with training and
test on **disjoint listings** (an artifact-level split lets a dictionary
memorise repeat captures of the same listing — this was caught and re-run):

| Approach | % of today's zstd-9 | Saving |
|---|---|---|
| full page + dictionary (112 KB) | 38.8% | −61.2% |
| projection to the 4 parser-critical sections | 17.8% | −82.3% |
| projection + dictionary (32 KB) | 3.6% | −96.4% |

The projection was parser-equivalent on **60/60 active listings** — but
produces a **0-byte** projection for challenge pages and flips unlisted pages
to `listing_state: 'active'`, because `<title>` and
`spark-notification.unlisted-notification` are in the unanchored "known gap"
above.

Both alternatives are now specified:

- **[Plan 129](plan_129_zstd_dictionary_compression.md)** — trained zstd
  dictionary. Reversible, no data loss. Ship first. **Stage 0 has since
  reproduced this at corpus scale** (1105 artifacts / 1091 listings / 5 months,
  held-out listings *and* held-out months): **−73.2%**, clearing its 40% gate.
  The −61.2% above was measured at 112 KB; the corpus run gets −57.1% at that
  size, and the saving keeps climbing to −73.2% at 768 KB before turning over.
- **[Plan 130](plan_130_parser_input_projection.md)** — parser-input
  projection. Irreversible; blocked on closing the taxonomy gap above.

`PARSER_CRITICAL_SECTION_NAMES` was described in this doc as "a number, not a
path". Plan 130 is that path being considered deliberately, with the trade
priced — not the measurement being quietly promoted.

---

## Out of Scope

- Production deletion of full raw HTML.
- Replacing scraper write behavior.
- SRP/results-page sectioning.
- ML refresh scoring.
- Automatic 30-day expiry.
