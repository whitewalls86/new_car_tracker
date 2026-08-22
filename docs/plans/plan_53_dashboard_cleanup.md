# Plan 53: Dashboard Cleanup / Optimization

**Status:** In progress
**Priority:** Medium

Pipeline Health tab has 18 sections — too much scrolling. Consider:
- Collapsible sections or `st.expander` for less-critical sections
- Sub-tabs within Pipeline Health (e.g., "Active Runs", "History", "System Health")
- Move Processor Activity and Postgres Health into a "System" sub-tab

## Done
- File split complete (Plan 50). `app.py` reduced to 47 lines.
- Stale backlog query updated to use `ops_detail_scrape_queue` with claim-aware filtering.
- Price freshness chart updated with STALE bucket.

## Remaining
- Collapsible / sub-tab layout for Pipeline Health
- Any additional UX cleanup surfaced during use
