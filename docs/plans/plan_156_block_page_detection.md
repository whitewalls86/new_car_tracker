# Plan 156: Block-Page Detection Beyond Cloudflare

## Status

**BUILD ORDER, written 2026-08-27.** Priority **42 (medium)**. Effort **S**.

Trigger: Plan 145 Stage 5 completes. Until then the detail parser must run
unmodified, because Plan 145's whole deliverable is a comparison of recovered
parsed output against what production wrote to silver, and a parser change
invalidates that gate. Plan 145 therefore filters these pages inside its own
recovery pipeline and leaves the parser alone.

## Problem

`_detect_challenge` in `processing/processors/parse_detail_page.py` keys only
on Cloudflare's `Just a moment...` title. Any other block or interstitial page
falls through to the normal path, where the absent `initial-activity-data`
blob yields `listing_state="active"` with every vehicle field NULL — and
`detail_writer` writes that to silver as a real observation.

The failure is silent and in the wrong direction. A blocked fetch should leave
`blocked_cooldown` intact so backoff accumulates; instead it is recorded as a
successful capture of a live listing with no price.

Two variants are confirmed in production:

| variant | size | marker |
|---|---:|---|
| Akamai `Access Denied` | 430 B | `<TITLE>Access Denied</TITLE>`, *"You don't have permission to access … on this server."* |
| Cloudflare `Site Maintenance` | 26,179 B | `<title>Site Maintenance</title>`, an edge splash page |

## Evidence

Measured 2026-08-27 against `silver_normalized/observations`, `source=detail`
rows that are `active` with `price`, `vin` and `make` all NULL:

| month | detail rows | all-NULL | share |
|---|---:|---:|---:|
| April | 1,272,617 | 87,003 | **6.84%** |
| June | 1,124,122 | 271 | 0.02% |
| July | 907,090 | 178 | 0.02% |
| August | 758,549 | 19 | 0.00% |

**Current volume is negligible and this is not urgent.** August's 19 rows fall
inside a single 15-second window on 2026-08-20 and are all the maintenance
page — an incident, not a steady leak. April is the anomaly, and its cohort is
handled by Plan 145.

Two things the numbers establish anyway:

- The defect is **structural and still live**. Low volume today reflects how
  often the edge blocks us, not any protection in the code.
- April's 6.84% is very likely the `detail/active` null-price gap that Plan 145
  lists as out of scope. 54,341 April pack members sit in the 256–511 byte
  band, and the sampled ones are Akamai blocks.

## Objective

A detail page that is a block or interstitial is recognised as one whatever
serves it, reaches the same `skip` path a Cloudflare challenge reaches, and
never becomes a silver observation.

## Method

1. **Extend the marker set in `shared/challenge.py`, not only in the parser.**
   The scraper's solver-outcome counter classifies the same interstitials
   without a data blob to gate on (Plan 136 Stage 2), so a marker set that
   lives only in the parser would leave the two halves disagreeing.
2. **Keep the existing safety gate.** A page carrying a parseable
   `initial-activity-data` blob is never treated as a block, whatever else it
   contains. This is what stops a legitimate page with unlucky text from being
   discarded, and it must survive the change.
3. **Route an Akamai block like a 403.** It is a genuine denial, so it reaches
   the `skip` path that leaves `blocked_cooldown` intact rather than clearing
   it. Decide explicitly whether a maintenance splash is the same case or a
   retryable one — they are different failures wearing similar clothes.
4. Add a metric or log dimension distinguishing block variants, so the next
   incident is legible without a lake query.

## Gate

- Both confirmed bodies are classified as blocked, from real captures.
- A real detail page carrying a data blob is never classified as blocked, even
  when it contains block-like marker text.
- A block yields no silver observation and leaves `blocked_cooldown` intact.
- The parser and the scraper's solver-outcome counter agree on what a block is.
- No change to how a Cloudflare `Just a moment...` challenge is handled.

## Testing

- The two real bodies as fixtures, asserted end to end through the parser.
- A genuine detail page whose free text contains block-like markers, asserting
  the data-blob gate wins.
- The write path, proving no observation and an intact cooldown.
- A shared-marker-set test proving parser and scraper classify identically.

## Out of scope

- **Backfilling or deleting the existing all-NULL silver rows.** April's are
  Plan 145's; the ~470 across June–August are a separate cleanup question that
  should not be smuggled into a parser fix.
- Any change to Cloudflare challenge handling, which works.
- Detecting blocks at scrape time rather than parse time. Worth considering
  later; it is a scraper-architecture question, not this defect.

## Relationship to other plans

| Plan | Relationship |
|---|---|
| 145 | Must complete Stage 5 first — its comparison requires an unmodified parser. Its recovery pipeline filters these pages itself, and its April cohort is the largest instance of this defect |
| 136 | Owns the solver-outcome counter that shares the marker set |
