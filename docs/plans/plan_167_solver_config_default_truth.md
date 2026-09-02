# Plan 167: Checked-In Solver Defaults Name the Wrong Container

## Status

**Backlog.** Written 2026-08-31 from a finding recorded but not fixed by
[Plan 138](plan_138_public_surface_refresh.md) Gate 0b, then widened while
scoping it: the finding named one line, and there are two.

Priority **conditional**. Effort **S** — Stage 1 is a two-line change, and
Stage 2 is a Compose change whose whole cost is the decision in front of it.

Nothing is broken in production. Production's `.env` sets
`FLARESOLVERR_URL=http://trawl:8191` explicitly, so neither stale default ever
fires there. This is a **fresh-clone defect**: the repository's checked-in
answer to "which solver does this project use?" is a container that has served
zero requests since 2026-07-07.

## The measurement

Both defaults name the vestigial container. Taken 2026-08-31 at `1f90284`:

| Location | Value |
|---|---|
| `.env.example:53` | `FLARESOLVERR_URL=http://flaresolverr:8191` |
| `docker-compose.yml:267` | `FLARESOLVERR_URL: ${FLARESOLVERR_URL:-http://flaresolverr:8191}` |
| production `.env` | `http://trawl:8191` — [`runbook_solver_oom_and_recycle.md:8`](../runbooks/runbook_solver_oom_and_recycle.md) |

Gate 0b named only the first. The second is the operative fallback: it is what
a clone with no `FLARESOLVERR_URL` in its `.env` actually gets, so fixing the
example file alone would leave the defect where it does its work.

The three facts that make this confusing rather than simply wrong are already
reconciled in [`ARCHITECTURAL_OVERVIEW.md`](../ARCHITECTURAL_OVERVIEW.md) §1 and
are restated in `.env.example` by Stage 0:

| Claim | Source of truth |
|---|---|
| The live solver is `trawl` | `docker-compose.yml:174` marks `flaresolverr` vestigial; `:209` defines `trawl`, digest-pinned |
| The variable kept its old name | There is no `TRAWL_URL`; `FLARESOLVERR_URL` is what the scraper reads |
| The protocol really is FlareSolverr's | `cf_session.py:187` POSTs `{"cmd": "request.get"}` to `${FLARESOLVERR_URL}/v1`, which `trawl` implements |

## Why flipping the default is not obviously correct

`trawl` and `redis-trawl` carry `profiles: ["trawl"]`, so a bare
`docker compose up -d` does not start them. That makes "name the live solver"
and "name a solver that is running" different changes:

- Flip the default only, and a fresh `up -d` points the scraper at a container
  that is not there — arguably more honest than today's silent no-op against a
  container that *is* there and does nothing, but a different failure shape.
- Drop the profile gate too, and `up -d` starts the real scrape path — which
  is what `EXPECTED_SERVICES` and Plan 142's `maintenance-running-set.txt`
  already assume, but it changes what a production `up -d` brings up.

The repository is already inconsistent about this and has said so once:
[Plan 138's Gate 0 baseline](../evidence/plan_138_stage_0_baseline_2026-08-31.md)
rejected "28 services without a profile gate" as the public service count
**because** it excludes `trawl` and `redis-trawl`, calling that "the wrong
public number in the most embarrassing possible way." The health contract
treats these two as production-required. Only Compose treats them as optional.

That is the actual question this plan exists to settle, and it is why the
two-line fix was not simply applied when the finding was recorded.

## Stages

### Stage 0 — State the truth in the file that misstates it

**Done 2026-08-31**, ahead of the rest, because it carries no runtime surface.
`.env.example` now separates the three facts above, marks the default below it
`KNOWN STALE` with the reason it is being left alone, and gives the commands to
run the live path locally. The defaults themselves are untouched.

Folded in: the `n8n` section heading and `N8N_USER_MANAGEMENT_JWT_SECRET` were
removed. n8n was decommissioned by [Plan 102](plan_102_decommission.md) on
2026-04-29 and the variable has had **zero references** outside `.env.example`
since. `TELEGRAM_API`, which was filed under that dead heading and is live in
six places, moved to a heading that names what it does.

**Exit:** the file no longer implies `trawl` is a trial, and the stale default
is labelled rather than silently wrong. Met.

### Stage 1 — Flip both defaults to `trawl`

Change `.env.example:53` and `docker-compose.yml:267` to `http://trawl:8191`.

Inert for production, which overrides both. The change is small; the reason it
is a stage rather than a typo fix is that it is only correct **together with a
decision on Stage 2** — a default naming a profile-gated container is a
deliberate choice, not an oversight, and should be recorded as one either way.

**Exit:** no checked-in file names `flaresolverr` as the solver the scraper
talks to, and the choice about profile gating is written down.

### Stage 2 — Decide whether `trawl` and `redis-trawl` stay profile-gated

The question, stated once: **should the live scrape path start by default?**

Arguments to remove the gate: `EXPECTED_SERVICES` includes both; Plan 142 gates
host maintenance on both; Plan 140 publishes absence for both; Gate 0 called the
non-profiled set the wrong denominator precisely because it omits both. Four
contracts already treat these as production-required.

Arguments to keep it: the gate is the only thing preventing a local `up -d`
from starting a 4 GB-capped browser solver and a Redis beside it, which is
real cost on a developer machine and was never the reason the gate was added.
Removing it also changes what a production `up -d` starts, which touches Plan
142's window and wants a soak rather than a merge.

A third option exists and may be the right one: keep the gate, and make the
scraper fail loudly at startup when `FLARESOLVERR_URL` names a host it cannot
resolve, so the misconfiguration is announced instead of discovered through a
403 rate.

**Exit:** one written verdict with its reason, and the Compose file matching it.

## Files

- `.env.example` — Stage 0 (done), Stage 1
- `docker-compose.yml` — Stage 1 default, Stage 2 profile decision
- `container_health/expected.py`, `maintenance-running-set.txt` — read in
  Stage 2 as evidence; changed only if Stage 2 removes the gate

## Out of scope

- **Renaming `FLARESOLVERR_URL`.** The protocol is FlareSolverr's and the
  variable is read by the scraper, tests, and Compose. A rename is a migration
  with no benefit this plan needs.
- **Removing the `flaresolverr` container.** It is retained deliberately, and
  `docker-compose.yml:174` records why. Removing it is a separate decision.
- **`TRAWL_IMAGE` and the digest pin.** [Plan 136](plan_136_solver_recycle_and_liveness.md)
  owns that pin and its soak.
- **`scripts/setup.ps1`**, which creates the decommissioned `n8n_data` and
  `cartracker_raw` volumes and omits the external `cartracker_pgdata`. Same
  class of defect — a stale checked-in default — but a different file and not
  about the solver.

## Success criteria

1. A fresh clone that copies `.env.example` and follows the file's own
   instructions reaches the live solver, or is told plainly why it does not.
2. No checked-in default names `flaresolverr` as the scraper's solver.
3. Whether `trawl` starts by default is a recorded decision with a reason,
   rather than an artifact of the order things were added to Compose.

## Intersections

### Plan 138 — public surface refresh

This plan exists because of Plan 138 Gate 0b, which found the `.env.example`
line while reconciling `ARCHITECTURAL_OVERVIEW` and deliberately did not fix
it. Plan 138 Stage 1a then put the same reconciliation into `README.md`, so the
README and `.env.example` now agree — and both are ahead of the defaults.

### Plan 136 — solver recycle and liveness

Owns `trawl`'s image pin, memory soak, and recycle policy, with a Stage 3
verdict due 2026-09-17. Stage 2 here should not land inside that measurement:
changing whether `trawl` starts by default changes the population Plan 136 is
measuring.

### Plan 142 — planned host maintenance

`maintenance-running-set.txt` is one of the four contracts that already treats
`trawl` as production-required, and is Stage 2's main evidence. If Stage 2
removes the profile gate, it should land clear of a maintenance window rather
than inside one.
