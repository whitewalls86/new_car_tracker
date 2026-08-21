# Plan 130: Parser-Input Projection (Truncating Raw HTML)

## Status

Draft. **Blocked on two prerequisites** (see Prerequisites below) — do not
implement the write path until both are done.

Comes out of Plan 114 Stage 3. This is the largest storage win measured, and
the only one on the table that is **irreversible**.

---

## Goal

Store only the page regions the parser actually reads, discarding the rest.

Target: **~82% reduction** in stored HTML bytes on its own, **~96%** stacked
with [Plan 129](plan_129_zstd_dictionary_compression.md).

---

## The Trade, Stated Plainly

Everything else in this track is reversible. This is not.

Once the discarded ~82% is gone, it cannot be recovered, and any future parser
that wants a field outside the retained sections has nothing to read. The
equivalence gate can only ever validate against **today's** parser, so keeping
only what today's parser reads is a bet that no future parser will want
anything else — a bet that cannot be re-run.

Plan 114 rejected a similar bet at Stage 1, when it bought 1.8%. The reasoning
was explicit: *"if I decide three weeks from now there's another field I want to
get, I don't want to risk data loss."* That reasoning has not changed; only the
price has.

| | Rejected at Stage 1 | This plan |
|---|---|---|
| Saving | 1.8% | ~82% (96% with Plan 129) |
| Reversible | No | No |

So the decision is: **~20 percentage points of storage is the price of being
able to reprocess.** That is a legitimate trade to take, and it is the owner's
call, not the implementer's. This plan documents how to take it safely if
taken.

---

## Context and Evidence

`PARSER_CRITICAL_SECTION_NAMES` in `processing/html_sections.py` names the four
sections `parse_cars_detail_page_html_v1` reads:

- `vehicle_activity_json` (`script#initial-activity-data`)
- `vehicle_controller_json` (`script#CarsWeb.VehicleDetailController.show`)
- `dealer_contact_block` (`.dealer-card`)
- `carousel_block` (`div.listings-carousel`)

Measured on 60 real artifacts / 12 listings (Plan 114 Stage 3, 2026-08-08):

| Measure | Result |
|---|---|
| Projection share of raw chars | 12.72% |
| Projection share of zstd-9 bytes | **19.32%** |
| Parser-equivalent on active listings | **60/60 (100%)** |

The char figure understates the cost: the retained sections are the
listing-specific JSON that compresses worst (60 distinct hashes across 60
artifacts), while the discarded bytes are the highly compressible boilerplate.
**Always quote the compressed figure.**

Stacked with a trained dictionary, on disjoint held-out listings:

| Approach | % of today | Saving |
|---|---|---|
| projection only | 17.8% | −82.3% |
| projection + dictionary (32 KB) | 3.6% | **−96.4%** |

### The object floor caps the benefit

MinIO's ~8 KB/object floor means content savings stop paying once an object is
under ~4 KB. Rough extrapolation to 3.9M objects:

| Approach | Content/object | Physical/object | Bucket physical |
|---|---|---|---|
| today | ~35 KB | ~38 KB | ~172 GB |
| Plan 129 only | ~13.7 KB | ~20 KB | ~78 GB |
| projection only | ~6.3 KB | ~12 KB | ~47 GB |
| projection + dictionary | ~1.3 KB | 8 KB (floor) | ~31 GB |

Projection alone already reaches the floor. **The extra 78% of content
reduction from stacking buys only ~33% physically.** To collect the rest,
artifacts would have to be packed into fewer, larger objects — out of scope
here, and a strong argument for doing Plan 129 first and reassessing.

Treat those GB figures as order-of-magnitude: extrapolated from 60 artifacts in
high-duplicate groups.

---

## Prerequisites (blocking)

### 1. The projection silently corrupts two listing states

Measured, not hypothesised:

```
challenge page:  6,559 chars -> projection 0 chars
  listing_state:  'blocked'  ->  'active'

unlisted page:   192,679 chars -> projection 36,478 chars
  listing_state:  'unlisted' ->  'active'
  unlisted_title / unlisted_message: lost
```

Both fail **toward `active`**, which is the worst direction: a delisted vehicle
is stored as still for sale, and a bot-block is stored as a successful scrape.
No exception, no missing field — just wrong data that looks correct. This would
also silently re-open the exact failure mode
[Plan 128](plan_128_false_block_detection.md) was written to close.

Root cause is the known gap already recorded in `processing/html_sections.py`:

- `spark-notification.unlisted-notification` ([parse_detail_page.py:61](../processing/processors/parse_detail_page.py#L61)) is **not anchored** — it lands in filler and is discarded.
- `<title>` ([parse_detail_page.py:101](../processing/processors/parse_detail_page.py#L101)), read for challenge detection when the activity JSON is absent, is **not anchored** either.
- A challenge page has **no anchors at all**, so its projection is zero bytes.

**Fix before anything else:** add both as anchors in `_anchor_name` and to
`PARSER_CRITICAL_SECTION_NAMES`. Every existing invariant test in
`tests/processing/test_html_sections.py` must keep passing unchanged — if one
breaks, the refinement is wrong, not the test.

### 2. Plan 129 should ship first

It is reversible, banks ~61% on its own, and materially changes this plan's
cost/benefit (see the object-floor table). Deciding on an irreversible 20-point
increment is a better decision once the reversible 61% is already in hand.

---

## Design

### Never project what you cannot verify

The projection is only written when the artifact is a **clean, parseable,
active detail page**. Everything else keeps full raw HTML, forever:

- parse failures
- challenge / blocked pages
- unlisted pages
- pages where the equivalence gate fails
- pages with no anchors, or with anchors the taxonomy does not recognise
- an unconditional random sample (see below)

This is not defensive padding: those are precisely the cases measured to break,
they are detectable, and they are rare.

### Verify per artifact, at write time, before discarding

For every artifact:

```
sections   = extract_sections(html)
projection = concat(sections where name in PARSER_CRITICAL_SECTION_NAMES)
ok, diffs  = parse_outputs_equivalent(html, projection)
if not ok: keep full raw, record why, emit metric
```

`parse_outputs_equivalent` already exists and is the Stage 0 contract. The
projection is stored **only** when the gate passes for that specific artifact.

### Grace period

Full raw is retained for **N days** (start at 30) after a verified projection is
written. Deletion is a separate, later, auditable job — never inline with the
write. This gives a window to notice a systematic problem while the evidence
still exists.

### Keep an unprojected holdout, permanently

Retain full raw for an unconditional **1% random sample** of artifacts,
forever. This is the only way to answer "what did we lose?" after the fact, and
the only way a future parser change can be evaluated against real pages rather
than against projections. 1% of ~31 GB is a rounding error; not having it is
unrecoverable.

### Storage layout

Projections are written as normal HTML objects under a distinct prefix, so the
two are never confused and the read path can tell what it has:

```
html/...                     full raw (today's layout, unchanged)
html_projected/...           verified projections
```

Object metadata records `x-amz-meta-projection: v1`, the parser version, and
the `source_raw_sha256`. Metadata is convenience; the prefix is the truth.

---

## Stages

### Stage 0 — Close the taxonomy gap (prerequisite 1)

- Anchor `<title>` as `document_title` and
  `spark-notification.unlisted-notification` as `unlisted_notice`.
- Add both to `PARSER_CRITICAL_SECTION_NAMES`.
- Re-run the projection probe against the challenge and unlisted fixtures.
- **Gate: both fixtures parser-equivalent under projection.** Until this passes
  there is nothing else to do.

### Stage 1 — Measure at corpus scale

Extend `scripts/audit_sectioned_html_storage.py` with a projection mode, or add
`scripts/estimate_projection_savings.py`:

- Sample across months and listing types — **must include unlisted, challenge
  and parse-failure artifacts**, which the Plan 114 Stage 3 sample did not.
- Report the equivalence pass rate by listing state, not just in aggregate.
- Report what fraction of the corpus would be exempt (kept as full raw); that
  fraction directly reduces the projected saving.
- Report compressed bytes, with and without the Plan 129 dictionary.

Gate: **≥99.9% equivalence on artifacts eligible for projection**, and a
measured exempt fraction small enough that the saving still justifies the trade.

### Stage 2 — Write projections alongside full raw

Write both. Delete nothing. This is a pure-addition stage that proves the write
path and the gate in production without risking anything.

- Metric: projections written, gate failures by reason, exempt count by reason.
- Run for at least one full retention cycle before Stage 3.

### Stage 3 — Read path prefers projection

- Reprocessing reads the projection when present and verified, else full raw.
- Assert parse equality against the retained full raw for a sample; alert on any
  mismatch.

### Stage 4 — Delete full raw, gated

Only after Stages 2 and 3 have been stable, and only for artifacts that:

- have a verified projection, **and**
- are past the grace period, **and**
- are not in the 1% permanent holdout, **and**
- are not exempt for any of the reasons above.

Deletion is a separate job with a dry-run mode and a hard cap on objects per
run. This is the irreversible step; it should feel deliberate.

---

## Testing

### `tests/processing/test_html_sections.py` (extend)
- `<title>` and the unlisted notification are anchored sections.
- The 43 existing invariants pass unchanged.

### `tests/processing/test_projection.py` (new)

**Group A — the states that broke**
- Unlisted fixture: projection preserves `listing_state == 'unlisted'` and both
  unlisted fields.
- Challenge fixture: projection is non-empty and preserves
  `listing_state == 'blocked'`.
- Active fixtures: projection parser-equivalent.

**Group B — the gate**
- An artifact failing equivalence is never projected.
- Gate failure records a reason and keeps full raw.
- A page with no recognised anchors is exempt, not projected to nothing.

**Group C — exemptions**
- Parse failure, unlisted, challenge, and holdout artifacts all retain full raw.
- The holdout sample is deterministic given an artifact ID (so it is stable
  across re-runs and auditable).

**Group D — deletion safety**
- Deletion refuses an artifact without a verified projection.
- Deletion refuses within the grace period.
- Deletion refuses a holdout artifact.
- Dry-run deletes nothing and reports what it would delete.

---

## Files Changed

| File | Change |
|------|--------|
| `processing/html_sections.py` | Anchor `<title>` + unlisted notification; extend critical set |
| `processing/projection.py` | New: build + verify projection, exemption rules |
| `scripts/estimate_projection_savings.py` | New: Stage 1 measurement |
| `scripts/delete_projected_raw_html.py` | New: Stage 4, dry-run by default |
| `tests/processing/test_html_sections.py` | Extend for new anchors |
| `tests/processing/test_projection.py` | New |
| `tests/fixtures/html/` | Add a **real** captured unlisted page (currently synthetic only) |

---

## Success Criteria

| Metric | Gate |
|--------|------|
| Unlisted + challenge preserved under projection | 100% — blocking |
| Equivalence on eligible artifacts | ≥99.9% |
| Compressed saving (with exemptions counted) | ≥70% |
| Full raw retained for every exempt artifact | 100% |
| Permanent holdout | 1%, never deleted |
| Deletion without verified projection | Impossible |

---

## Risks

| Risk | Mitigation |
|------|------------|
| **Future parser needs a discarded field** | Unmitigable by design. The 1% holdout is the only partial answer. This is the trade. |
| Silent `active` corruption of unlisted/blocked | Stage 0 is blocking; Group A tests pin it |
| Taxonomy drifts as cars.com changes markup | Gate runs per artifact at write time, so drift shows up as gate failures, not corruption |
| Deletion bug destroys full raw prematurely | Grace period + dry-run + per-run cap + Group D tests |
| Saving is smaller than modelled | Object floor already accounted for; Stage 1 measures the exempt fraction before commitment |

---

## Out of Scope

- Changing the parser.
- Packing multiple artifacts per object.
- SRP/results-page projection.
- Deleting anything before Stage 4.
