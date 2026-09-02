# Plan 122: Runtime Scraper Fetch Configuration

## Objective

Allow operational scrape parameters to be changed without rebuilding and
redeploying the scraper image.

The immediate driver is the FlareSolverr challenge timeout incident on
2026-07-07: the fixed default detail timeout moved from 30s to 90s, but this
kind of tuning should be runtime configuration instead of a code change.

## Scope

- Make detail fetch timeout configurable at runtime.
- Consider SRP fetch timeout and FlareSolverr `maxTimeout` as part of the same
  config surface.
- Prefer a simple source of truth that fits current operations:
  - environment variable for first pass, or
  - ops-managed settings table/API if we want live updates without container
    restart.
- Document defaults and safe bounds.
- Add tests proving explicit request payload overrides still win.

## Candidate Settings

| Setting | Current Behavior | Proposed Default |
|---------|------------------|------------------|
| `DETAIL_FETCH_TIMEOUT_S` | hardcoded default in `scrape_detail_fetch` | 90 |
| `SRP_FETCH_TIMEOUT_S` | hardcoded request timeout in SRP fetch | 30 |
| `FLARESOLVERR_MAX_TIMEOUT_S` | follows caller timeout | match fetch timeout |

## Non-Goals

- Do not change cooldown semantics in this plan.
- Do not remove the plain `curl_cffi` fallback.
- Do not redesign anti-detection or introduce proxy rotation here.

## Implementation Notes

Start with environment-driven configuration unless live ops editing becomes
important enough to justify a DB/API surface. If environment variables are used,
update `.env.example`, docs, and deployment notes.

If we later want live changes, add an ops-owned scrape settings table and have
the scraper read/cache values with a short TTL.

## Acceptance Criteria

- Detail timeout can be changed without rebuilding the scraper image.
- Existing payload-level `timeout_s` override still takes precedence.
- Defaults preserve current hotfix behavior.
- Unit tests cover default, env/config override, and payload override.
