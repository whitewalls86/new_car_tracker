# Plan 124: Trawl Browser Solver Memory Guardrails

## Implementation Status: COMPLETE — verified in production 2026-08-18

- [x] `docker-compose.yml`: `trawl` gets `mem_limit: 4g`, `memswap_limit: 4g`,
      `pids_limit: 512`.
- [x] `docker-compose.yml`: `redis-trawl` gets `mem_limit: 512m`,
      `memswap_limit: 512m`.
- [x] `tests/test_observability_config.py::TestDockerComposeTrawlMemoryGuardrails`
      asserts the limits parse correctly (no Docker required).
- [x] Deployed to the production VM and verified with `docker inspect` /
      `docker stats`.
- [x] **Zero host-wide OOM events since deployment** — see the amended criterion
      below, which the original wording got wrong.
- [x] Runbook written: [runbook_solver_oom_and_recycle.md](runbook_solver_oom_and_recycle.md).

### Verified in production, 2026-08-18

| Check | Result |
|---|---|
| `trawl` limits | `Memory=4294967296 MemorySwap=4294967296 PidsLimit=512` |
| `redis-trawl` limits | `Memory=536870912 MemorySwap=536870912` |
| Live usage | **2.865 GiB / 4 GiB (71.6%)**, 206 of 512 PIDs |
| Solver actually solving | **188 `solved` vs 3 `403`** over 2h |
| Host-wide OOM since deployment | **zero** |

### The acceptance criterion was wrong, and the data says why

The original criterion read *"no new OOM entries appear in the kernel log during
normal scrape operation."* Read literally it **fails** — there have been 12 OOM
kills since deployment. Read correctly it passes decisively, because the
`constraint` field distinguishes two entirely different events:

| Window | Constraint | Count | Victim | anon-rss |
|---|---|---:|---|---|
| Jul 08 – Jul 12 (**before** limits) | `CONSTRAINT_NONE` — **host-wide** | 4 | `uvicorn`, `dbt` ×2, `camoufox-bin` | 8.1–12.9 GB |
| Jul 16 – Aug 15 (**after** limits) | `CONSTRAINT_MEMCG` — **container-local** | 12 | `camoufox-bin`, every one | 2.8–3.5 GB |

**That is the plan succeeding, not failing.** Goal 1 was *"bound Trawl/Camoufox
memory usage so solver failures stay local to the solver container."* The
2026-07-12 incident was a 9.5 GB `camoufox-bin` process taken down by a host-wide
OOM that threatened the whole stack. Since the limits, every kill has been the
kernel enforcing the container's own cgroup at ~3.2–3.5 GB, and **no host-wide
OOM has occurred at all.** The blast radius moved from the VM to one container.

**Amended criterion:** *no `CONSTRAINT_NONE` (host-wide) OOM events.* A
`CONSTRAINT_MEMCG` kill of `camoufox-bin` is the guardrail working and should
not page anyone.

### What this exposed, and what it means for Plan 136

`camoufox-bin` is OOM-killed roughly **every 1.5–4 days** — 12 times in a month,
at a consistent ~3.2–3.5 GB. That is a **browser memory leak the guardrail is
containing rather than curing.** Containing it is this plan's whole job, so it is
not a defect here, but two things follow:

- **[Plan 136](plan_136_solver_recycle_and_liveness.md) Stage 3's cadence
  question changes shape.** It proposes a weekly recycle reasoned from 22 days of
  container uptime, but the *browser processes* inside that container are already
  being recycled involuntarily every couple of days by the OOM killer. So "22-day
  state rot" cannot be about browser process age — it must be container-level
  state that survives a `camoufox-bin` restart. Worth resolving before choosing
  a recycle interval, since a weekly container recycle addresses different state
  than these kills do.
- **The 2026-08-14 outage contained one of these kills**, at 04:28 on Aug 15 —
  **7.5 hours after** the solve rate hit 0% at 21:00, and at a *lower* rss
  (2.83 GB) than the usual 3.2–3.5. That timing makes it a symptom of the
  degraded state rather than its cause.

Steady state is ~70% of the cap, so the headroom is real but not generous. If
solve quality ever degrades near the ceiling, raising `mem_limit` is the lever —
the host has 23.4 GB and the scraper uses 600 MB of it.

## Objective

Prevent browser-solver memory spikes from destabilizing the production VM.

The immediate driver is the production incident on 2026-07-12: the site stopped
responding, `docker ps` temporarily hung, and kernel logs showed the Linux OOM
killer terminated a `camoufox-bin` process inside the `cartracker-trawl`
container:

```text
Out of memory: Killed process ... camoufox-bin ... anon-rss: 9515688kB
```

At the time, `cartracker-trawl` had no container memory limit. One browser
solver process was therefore able to consume enough host memory to threaten the
entire production stack.

## Goals

1. Bound Trawl/Camoufox memory usage so solver failures stay local to the solver
   container.
2. Preserve current scraper behavior where possible: Trawl remains the primary
   browser-solver path, and FlareSolverr/plain fetch fallbacks are not
   redesigned here.
3. Add a simple operational runbook for identifying and recovering from solver
   OOM incidents.
4. Keep this hotfix small enough to deploy independently of Plan 123 and the
   lakehouse/adaptive-refresh work.

## Non-Goals

- Do not redesign anti-bot strategy or proxy rotation.
- Do not replace Trawl or FlareSolverr in this plan.
- Do not tune adaptive refresh, dbt, or ML feature generation.
- Do not solve scraper throughput; this plan is about blast-radius control.

## Scope

### Docker containment

Add hard resource limits to the browser-solver service in `docker-compose.yml`:

```yaml
trawl:
  mem_limit: 4g
  memswap_limit: 4g
  pids_limit: 512
```

Also bound the supporting Redis container:

```yaml
redis-trawl:
  mem_limit: 512m
  memswap_limit: 512m
```

The first containment pass used `3g` memory/swap, `pids_limit: 256`, and
`redis-trawl: 256m`. VM logs later showed the browser pool flapping with
`EAGAIN: resource temporarily unavailable` and Camoufox `SIGSEGV` during normal
challenge solving. The current default is therefore `4g` memory/swap,
`pids_limit: 512`, and `redis-trawl: 512m`: still bounded, but with enough
headroom for the two-browser pool.

### Deployment

Apply the hotfix without rebuilding unrelated services:

```bash
cd /opt/cartracker
git pull
docker compose up -d trawl redis-trawl
docker inspect cartracker-trawl --format 'memory={{.HostConfig.Memory}} memory_swap={{.HostConfig.MemorySwap}} pids_limit={{.HostConfig.PidsLimit}}'
docker stats cartracker-trawl cartracker-scraper --no-stream
```

If `trawl` is already in a bad state, restart it first:

```bash
docker compose restart trawl
```

### Verification

After deployment, confirm:

- `cartracker-trawl` is healthy.
- `docker inspect` shows non-zero memory and swap limits.
- scraper detail fetches still succeed through Trawl.
- a Trawl memory spike cannot consume host-level memory beyond the configured
  limit.
- no new OOM entries appear in the kernel log during normal scrape operation.

Useful commands:

```bash
docker compose ps trawl redis-trawl scraper
docker stats cartracker-trawl cartracker-scraper --no-stream
sudo journalctl -k --since '1 hour ago' | grep -iE 'oom|out of memory|killed process|hung task|blocked for more than'
```

### Observability Follow-Up

After the immediate hotfix, consider adding dashboard panels or alerts for:

- `cartracker-trawl` memory usage.
- `cartracker-trawl` restart count.
- solver request latency and failure rate.
- scraper fallback rate from Trawl to FlareSolverr/plain fetch.

This can remain follow-up work unless solver instability continues.

## Acceptance Criteria

- `docker-compose.yml` constrains `trawl` memory/swap and PID count.
- `redis-trawl` has a small memory/swap limit.
- VM deployment confirms the limits are active through `docker inspect`.
- Scraping can still solve at least one normal Cars.com challenge through the
  configured solver path.
- A documented runbook exists for checking solver OOM evidence and restarting
  the solver without rebooting the VM.

## Rollback

If Trawl cannot solve normal challenges with the current bounded defaults:

1. First reduce concurrency in `.env`:
   - `TRAWL_BROWSER_POOL_SIZE=1`
   - `TRAWL_BROWSER_CONTENT_PROCESSES=1`
   - `TRAWL_BROWSER_RECYCLE_AFTER_CONTEXTS=4`
2. Redeploy only `trawl`.
3. Re-test solver behavior and inspect OOM / restart / browser-pool logs.

Do not roll back to an unbounded solver container unless the scraper is fully
paused and the production blast radius is understood.
