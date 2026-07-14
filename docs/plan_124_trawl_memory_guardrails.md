# Plan 124: Trawl Browser Solver Memory Guardrails

## Implementation Status: Hotfix implemented, VM verification pending

- [x] `docker-compose.yml`: `trawl` gets `mem_limit: 4g`, `memswap_limit: 4g`,
      `pids_limit: 512`.
- [x] `docker-compose.yml`: `redis-trawl` gets `mem_limit: 512m`,
      `memswap_limit: 512m`.
- [x] `tests/test_observability_config.py::TestDockerComposeTrawlMemoryGuardrails`
      asserts the limits parse correctly (no Docker required).
- [ ] Deployed to the production VM and verified with `docker inspect` /
      `docker stats` (see Verification below).
- [ ] Confirmed no new OOM kernel log entries after a normal scrape cycle.

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
