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

## Candidate Sections

Initial section taxonomy:

| Section | Notes |
|---------|-------|
| `document_prefix` | Doctype/head/page shell before known dynamic blocks |
| `global_header` | Header/nav/login shell |
| `vehicle_activity_json` | `initial-activity-data` normalized or isolated |
| `als_json` | `initial-als-data`; likely mostly volatile analytics |
| `vehicle_controller_json` | `CarsWeb.VehicleDetailController.show` |
| `dealer_contact_block` | Dealer name, phone/DNI, address, lead form |
| `vehicle_detail_dom` | Main vehicle facts visible in page body |
| `carousel_block` | Related/recommended listings |
| `static_assets` | Script/link references |
| `anti_bot_scripts` | Cloudflare/Akamai snippets, likely volatile/discardable |
| `document_suffix` | Closing scripts/body/html |

The audit should refine this taxonomy based on real pages.

---

## Audit Algorithm

For a sample of known semantic duplicate groups:

1. Fetch full raw HTML for each artifact.
2. Parse with BeautifulSoup or another deterministic HTML parser.
3. Extract named sections using explicit selectors and fallback boundaries.
4. Normalize each section only where safe:
   - Sort JSON keys for known JSON blobs.
   - Isolate volatile analytics/session sections rather than pretending they
     are stable.
   - Strip only fields that are known to be request/session identifiers.
5. Hash each section.
6. Measure section-level hash reuse within and across listings.
7. Estimate storage size for:
   - current full raw HTML
   - section objects plus per-artifact manifests
   - full raw retained only for failures/recent grace period
8. Attempt parser-equivalent reconstruction from manifest + sections.
9. Run the existing parser against reconstructed HTML and compare parsed output.

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
      "normalized": true
    }
  ],
  "reconstruction": {
    "mode": "ordered_sections",
    "parser_equivalent_verified": true,
    "verified_at": "2026-07-01T00:00:00Z"
  }
}
```

The manifest is a derivative artifact. The initial audit does not delete source
HTML.

---

## Success Criteria

The approach is worth productionizing only if the audit shows:

| Metric | Gate |
|--------|------|
| Parser-equivalent reconstruction | >= 99% on sampled successful parses |
| Section extraction failure rate | Low enough to fallback without operational noise |
| Net storage savings after manifests | Meaningful enough to beat compression-only |
| Stable section reuse | Concentrated in large sections, not only tiny fragments |
| Failure handling | Full raw retained for parse/section failures |

Exact thresholds should be set after the first sample run, but reconstruction
correctness is the non-negotiable gate.

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
- JSON normalization is deterministic and idempotent.
- Known volatile fields are isolated or normalized as expected.
- Different vehicle price/mileage data changes the relevant section hash.
- Manifest serialization is stable.

### Audit Tests

- Two semantically unchanged artifacts produce matching hashes for stable
  sections.
- Volatile analytics/session sections are identified separately.
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

| File | Change |
|------|--------|
| `scripts/audit_sectioned_html_storage.py` | New audit runner |
| `processing/html_sections.py` | Section extraction and manifest helpers |
| `tests/processing/test_html_sections.py` | Unit coverage for extraction/manifest logic |
| `tests/integration/processing/test_sectioned_html_audit.py` | MinIO-backed audit coverage |

---

## Out of Scope

- Production deletion of full raw HTML.
- Replacing scraper write behavior.
- SRP/results-page sectioning.
- ML refresh scoring.
- Automatic 30-day expiry.
