# Plan state reconciliation — Plan 146 Stage 0

**Swept 2026-08-21.** Every plan number this repository has ever named, settled
against its own document, its git history, and production evidence. This is the
input to Stage 1's backfill and Stage 2's table collapse; it is a **record of
one reconciliation**, not a surface that gets maintained. Nothing links to it
for status. [`PLANS.md`](PLANS.md) and [`completed_plans.md`](completed_plans.md)
do that.

Reproduce it with:

```bash
python scripts/audit_plan_state_history.py             # per-plan state timeline
python scripts/audit_plan_state_history.py --coverage  # numbers nothing records
python scripts/audit_plan_state_history.py --deleted   # documents that vanished
```

## What the sweep measured

| Measurement | Count |
|---|---|
| Plan numbers ever named anywhere (commits, index history, `docs/`) | 144 of 0-146 |
| Numbers never used at all — gaps in the number line, not lost plans | 3 (44, 85, 104) |
| Plan documents on disk | 79 files, **73 distinct plan numbers** |
| Revisions of `PLANS.md` walked | 134, from 2026-03-16 |
| **Plans in more than one status table** | **3** (114, 135, 136) |
| **Plan documents in no status table at all** | **24** |
| **Plans with no document and no archive row** | **9** (5, 6, 24, 52, 55, 56, 57, 59, 65) |
| Plan documents deleted at some point in history | 7 |

The middle three rows are the defect Plan 146 was written about, measured. The
index's own claim — "the Plan inventory covers 30 of 72 plan files" — was the
visible half. The invisible half is larger: **33 plans had no state recorded
anywhere**, and nine of those have no document either.

## Evidence tiers

Stage 1 writes dates into the archive, so every date carries its provenance:

- **observed** — a dated fact: a state transition in `PLANS.md`'s own history,
  or a commit whose subject names the plan and carries its implementation.
- **corroborated** — a date a document asserts *and* git supports.
- **inferred** — **a guess.** No transition was recorded, so the plan
  document's last-touched date stands in. Marked so it is never mistaken for a
  record.

## The contradictions

Three plans held two states at once. Each is settled below against its own
document, which is the tie-breaker rule Plan 146 establishes.

| Plan | Claimed | True state | Why, and where the evidence is |
|---|---|---|---|
| **114** | archive (2026-08-10) **and** backlog ("follow-on") | **archive** | The archive row is right. The backlog row's resume trigger — "new evidence overturns the measured negative storage result" — is a condition on a *finished* plan, not an open plan. It moves into `plan_114_*.md` as a reopening condition and the row is deleted |
| **135** | closeout **and** watch list ("Complete 2026-08-18") **and** neither completion record | **closeout** | The prose is wrong and the missing archive row is right: criterion 5 and Stage 5 both land 2026-08-23, so evidence is still pending. Git corroborates the 2026-08-18 work date (`b76fb44`, `8267e5c`) but a plan with two open gates is not archived. One closeout row, gate 2026-08-23 |
| **136** | closeout (Stage 3 decay window) **and** build order row 2 | **build order** | Closeout is for rows where *no code is owed*. Stages 3 and 4 both owe code, so 136 is active work whose next step is blocked. The decay window is not a closeout gate; it is the `Blocked by` value the build-order row already carries (`~2026-09-09`). The closeout row is deleted as a duplicate |

Two more that read as contradictions and are not:

| Plan | Reading | True state |
|---|---|---|
| **131** | watch list says "Complete", archive has it (2026-08-18) | **archive** — the archive is right; the watch-list row is the leak. Its live detail (one unmerged commit on `plan-131-packed-cold-storage`) belongs in the plan document |
| **129** | watch list only, "dict v1 live, backfill running" | **closeout** — deployed, with a running Stage 4 backfill as the pending evidence. Needs the `Lands`/`Gate` columns Stage 2 adds |

## Plans with a document and no state anywhere

Twenty-four. Ordered by resolution, with the evidence that settles each.

### Complete — an archive row is owed (12)

| Plan | Date | Tier | Evidence |
|---|---|---|---|
| 81 data migration | 2026-04-14 | corroborated | Document states "Complete — executed 2026-04-14" |
| 82 user management | 2026-04-14 | corroborated | Document states "shipped via PRs #64-#67, merged 2026-04-14" |
| 91 UUID type cleanup | 2026-04-20 | observed | Document: "complete when V018 ships". `db/migrations/V018__hot_tables_and_cleanup.sql` landed at `2661a29` |
| 92 service drain | 2026-04-27 | inferred | Document lists `scraper` and `processing` as pending; both expose `/ready` today, added at `580822e` (Plan 93) and `e95e426` (Plan 71). No transition was recorded, so the date is the later commit — **a guess** |
| 97 MinIO artifact store | 2026-04-17 | inferred | Document still reads "Required prerequisite"; the Current State prose and `shared/minio.py` say it shipped. Date is last-touched — **a guess** |
| 98 bronze data architecture | 2026-04-21 | inferred | Document reads "ACTIVE"; its staging→MinIO mechanism is the archived "Silver flush" row (2026-04-21). **A guess** |
| 101 dashboard restructure | 2026-04-29 | corroborated | Document: "COMPLETE (2026-04-29) — all 3 phases done" |
| 86 Grafana | 2026-04-29 | corroborated | Document: "COMPLETE (2026-04-29)", deployed to production |
| 105 VM migration | 2026-05-20 | corroborated | Document: "COMPLETE (2026-05-20)" |
| 116 recompression estimate | 2026-07-01 | corroborated | Document: "Findings — COMPLETE (2026-07-01)"; `scripts/estimate_recompression_savings.py` exists |
| 109 silver compaction | 2026-06-30 | observed | Document reads "PLANNED"; `archiver/processors/compact_silver.py` landed at `2a0037c`, "feat: add silver Parquet compaction (Plan 109)", with a DAG and tests |
| 71 Airflow migration | 2026-04-29 | corroborated | Already archived for "steps 8-9, 13". Its open steps 14-15 were the n8n cutover, completed by Plan 102 (2026-04-29). The existing row's scope note is corrected rather than a second row added |

Plans 109 and 91 are the sharpest illustration of the defect: **both shipped,
both still say "PLANNED" / "Not started" in their own documents.** Nothing ever
made the document and the code disagree out loud.

### Superseded — the world moved (4)

| Plan | Superseded by | Why it can never be built |
|---|---|---|
| 29 n8n API | Plan 102 | n8n is fully removed |
| 83 n8n workflow viewer | Plan 102 | Same — there are no workflows to view |
| 77 SQL query tests | Plan 84 | The document itself says so |
| 53 dashboard cleanup | Plan 101 | Its "Done" section is Plan 50's file split; the rest was restructured wholesale by Plan 101 (2026-04-29) |

### Backlog — never started, trigger stated (7)

| Plan | Trigger |
|---|---|
| 64 PgBouncer | Postgres connection exhaustion becomes the measured constraint |
| 70 type annotations | Opportunistic; pull forward with any large refactor of the annotated modules |
| 73 scraper refactor | Document defers it to "after Plan 72", which is archived — re-scope against the current scraper before starting |
| 103 test coverage | Plan 139 Stage D settles the coverage gate; re-scope or supersede then |
| 106 code review cleanup | Sourced from `CODE_REVIEW_2026-03-20.md`; re-audit before starting |
| 107 quality to 90 | Plan 139 Stage D settles the coverage gate; re-scope or supersede then |
| 117 lakehouse roadmap | Umbrella, never built directly. Archived when its arc (125 → 112/113 → 119 → 126/127) lands |

Plans 103 and 107 are deliberately **not** marked superseded by Plan 139.
They overlap it, but "overlaps" is not "superseded", and manufacturing a
supersession is the same error class as manufacturing a date.

### Closeout — code shipped, verification never done (1)

| Plan | Lands | Gate |
|---|---|---|
| 123 dbt incrementalization | next monitored production `dbt_build` | Phases 0-2 are built and deployed (`2026-07-09`/`07-10`); every unchecked box is a *production verification* — peak RSS against the 8 GB DuckDB budget, the hourly DAG's runtime drop, the OOM short-circuit firing. **Open since 2026-07-10 and the oldest row in the table** |

Plan 123 is why closeout needs a `Lands` column. It has been finished-but-unverified
for six weeks with nothing recording that fact.

## Plans with no document at all

Nine plan numbers were used, worked, and in four cases **shipped**, and are
recorded by no surface in this repository. Found by
`audit_plan_state_history.py --coverage`.

| Plan | Title, recovered from history | Resolution | Evidence |
|---|---|---|---|
| **5** | n8n webhook triggers | superseded by Plan 102 | Never started; last seen in the index 2026-03-20; n8n is gone |
| **6** | Async SRP scraping with DB-backed job tracking | **archive, 2026-03-17** *(observed)* | Index recorded "✅ Core complete" and then dropped the section. Three commits carry it: `45cc84e`, `e60ae34`, `9fd997c` |
| **24** | Flag dealer-unenriched VINs as `full_details_stale` | **archive, 2026-03-18** *(observed)* | `0377460`, "Plan 24: Flag dealer-unenriched VINs as full_details_stale". **Never appeared in `PLANS.md` at any revision** — it shipped without ever being indexed, and `full_details_stale` is still live in `ops_vehicle_staleness.sql` |
| **52** | Carousel hint backlog strategy | superseded by Plan 93 | Never started. Its goal — prune hints for vehicles outside scrape targets — is Plan 93's "carousel filtered against `search_configs`" |
| **55** | Dashboard review | superseded by Plan 101 | Never started; a review of a dashboard that was then restructured wholesale |
| **56** | Analytics next steps | superseded by Plan 117 | An open-ended placeholder, never started; the analytics roadmap it stood in for is Plan 117's arc |
| **57** | dbt build sub-workflow with retry/error handling | **archive, 2026-03-27** *(observed)* | Index heading carried `**[DONE]**` at that revision; implemented in the n8n "Build DBT" workflow, since decommissioned with n8n |
| **59** | Orphan cleanup workflow | **archive, 2026-03-27** *(observed)* | Index heading carried `**[DONE]**`. Reopened 2026-03-30 for a stale-`detail_scrape_claims` follow-on that never started and is superseded by Plan 102 |
| **65** | Authentication & authorization stack | **archive, 2026-04-09** *(observed)* | `eb96c41`, "Plan 65: auth stack (oauth2-proxy, scoped DB roles, Caddy forward_auth)". The design shipped with **oauth2-proxy in place of Authelia**. Its document was deleted the next day at `20bbcb3` and it was never archived |

Plan 65 is the worst case in the sweep and the reason `--coverage` is a
permanent mode rather than a one-off query: a **completed, production-bearing
plan** — it is why the site has authentication — vanished from the record
entirely when its document was deleted, and nothing noticed for four months.

## Deleted plan documents

`ls docs/` is complete for what is present and silent about what was removed.

| Plan | Document | Deleted | Outcome |
|---|---|---|---|
| 62 | `plan_62_cicd.md` | 2026-04-08 | Archived — fine |
| 63 | `plan_63_flyway.md` | 2026-04-08 | Archived — fine |
| 67 | `plan_67_n8n_credentials.md` | 2026-04-09 | Archived — fine |
| 68 | `plan_68_cloud_deployment.md` | 2026-04-09 | Archived — fine |
| 72 | `plan_72_parquet_archival.md` | 2026-04-08 | Archived — fine |
| 112 | `plan_112_gate_a_b_implementation_plan.md` | 2026-07-16 | Deliberately consolidated into `plan_112_*.md` — fine |
| **65** | `plan_65_auth.md` | 2026-04-10 | **Never archived.** See above |

Deleting a plan document on completion was the old convention and it works
*only* when the archive row is written first. Six times it was; once it was
not, and that once is unrecoverable from `docs/` alone.

Two further documents exist only on unmerged remote branches and never reached
`master`: `plan_82_self_hosted_runner.md` (`origin/fix/import-errors`) and
`plan_86_grafana_observability.md` (`origin/task/histrocial-data`). The first
collides with `plan_82_user_management.md` on plan number 82. Neither is in
scope for the tables — Stage 4's test reads the working tree — but the
collision is why that test keys on numbers parsed from tracked files only.

## Bookkeeping moves that are not disputes

Recorded for Stage 2's benefit; each is a row in the wrong table rather than a
contradiction.

| Plan | From | To |
|---|---|---|
| 120, 124, 144 | Plan inventory only | archive (2026-08-18, 2026-08-18, 2026-08-21) |
| 132, 137 | Plan inventory only | superseded by Plan 145 (2026-08-21) |
| 143, 133, 114, 128, 111, 110, 115, 95 | `PLANS.md` "Completed" | `completed_plans.md` — one archive, not two |
| 130 | "Paused or blocked" | backlog, trigger unchanged |
| 66, 122, 79, 94, 108, 88 | Backlog | backlog, unchanged |
| 89, 90, 118, 87 | Superseded | superseded, unchanged |

## What Stage 1 must not do

Every resolution above is dated from an observed transition, an implementing
commit, or a document git agrees with -- **except three**. Plans 92, 97 and 98
have no transition and no asserted date, so their dates are a last-touched
proxy. Those three are written as `inferred` and stay that way.
Promoting a guess to a record would make the archive look complete while making
it wrong, which is a worse defect than the gap it closes.
