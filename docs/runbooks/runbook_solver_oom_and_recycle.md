# Runbook: Solver OOM Evidence and Recycling

Operational companion to [Plan 124](plan_124_trawl_memory_guardrails.md) and
[Plan 136](plan_136_solver_recycle_and_liveness.md). Covers the browser solver
`cartracker-trawl` — checking for OOM evidence, and restarting it without
rebooting the VM.

> **The solver is `trawl`, not `flaresolverr`.** `FLARESOLVERR_URL=http://trawl:8191`
> kept its old name after the solver was swapped, and a vestigial
> `cartracker-flaresolverr` container is still running having served **zero**
> requests since 2026-07-07. It is pure misdirection during an incident. Always
> act on `cartracker-trawl`.

---

## 1. Is the solver actually working?

A healthy container is **not** evidence the solver works. On 2026-08-14 `trawl`
ran healthy for 8 hours at a **0% solve rate**. Check outcomes, not liveness:

```bash
docker logs --since 2h cartracker-scraper 2>&1 \
  | grep -ioE 'solved|403' | sort | uniq -c | sort -rn
```

Healthy looks like a large `solved` count against a handful of `403`s
(188 vs 3 on 2026-08-18). **Mostly `403` with few or no `solved` is an outage**,
even with a green container.

## 2. Check for OOM evidence

```bash
sudo journalctl -k --no-pager | grep -E 'oom-kill:' \
  | grep -oE 'constraint=[A-Z_]+' | sort | uniq -c

sudo journalctl -k --no-pager | grep -E 'Killed process' | tail -10
```

**The constraint field is the whole diagnosis:**

| constraint | meaning | severity |
|---|---|---|
| `CONSTRAINT_MEMCG` | Hit the **container's** limit. Blast radius contained — Plan 124's guardrail working as designed. | Routine |
| `CONSTRAINT_NONE` | **Host-wide** OOM. The kernel was out of memory system-wide and picked a victim. | Serious |

A `CONSTRAINT_MEMCG` kill of `camoufox-bin` is **expected background noise** on
this host — it has happened roughly every 1.5–4 days since the limits were
deployed, at ~3.2–3.5 GB anon-rss. It is a browser memory leak being contained,
not an incident.

`CONSTRAINT_NONE` has not occurred since 2026-07-12. If one appears, the
guardrail has been removed or something outside the solver is the problem —
check `dbt` and `uvicorn`, which were the other two host-wide victims in July.

## 3. Confirm the limits are still active

Limits live in `docker-compose.yml` and are applied at **container creation**.
A `docker restart` preserves them; a hand-run `docker run` would not.

```bash
docker inspect cartracker-trawl --format \
  'Memory={{.HostConfig.Memory}} MemorySwap={{.HostConfig.MemorySwap}} PidsLimit={{.HostConfig.PidsLimit}}'
```

Expect `Memory=4294967296 MemorySwap=4294967296 PidsLimit=512`. **Zero means
unlimited** and the guardrail is gone — recreate with
`docker compose up -d trawl`.

## 4. Current headroom

```bash
docker stats cartracker-trawl cartracker-redis-trawl --no-stream \
  --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.PIDs}}'
```

Steady state sits around **70%** of the 4 GB cap with ~200 of 512 PIDs. Climbing
past ~85% means an OOM kill is likely within hours — harmless, but expect a
brief solve-rate dip while the browser pool recovers.

## 5. Restart the solver

A plain restart resolved the 2026-08-14 outage completely, with **no image pull
needed** — the fault was stale in-container state.

```bash
docker restart cartracker-trawl
```

Takes ~4s for both browsers to warm. **No VM reboot, and no `docker compose
down`.** Then confirm recovery by outcomes, not by container status:

```bash
sleep 60
docker logs --since 5m cartracker-scraper 2>&1 | grep -ioE 'solved|403' | sort | uniq -c
```

### Do not restart mid-batch if avoidable

A naive restart fails every in-flight request, and the scraper's 403 handler
pushes each of those listings into a **12-hour cooldown**. A careless recycle
inflicts a small version of the outage it fixes. If the solver is degraded but
not dead, prefer waiting for the current `scrape_detail_pages` run to finish —
it has `max_active_runs=1`, so one run is the whole window.

Plan 136 Stage 3 automates this as a drain-aware weekly recycle. Until it lands,
the drain is manual.

## 6. What this runbook does not cover

- **Why** the solve rate collapses. Stage rot after long uptime is the working
  theory; [Plan 136](plan_136_solver_recycle_and_liveness.md) Stage 2 adds the
  outcome counters that would answer it.
- Automatic restart. Plan 136 Stage 4 owns restart authority.

---

## Two false leads, both cost real time

**A bare `curl` from the VM returns Cloudflare Error 1020, "Sorry, you have been
blocked."** That looks exactly like an IP ban and is not one — it is curl's TLS
fingerprint tripping a stricter rule. With `curl_cffi impersonate='chrome'` the
same host gets `cf-mitigated: challenge`, identical to a control request from a
residential IP. **Re-test with a browser fingerprint before concluding the
egress IP is banned.**

**`RestartCount` does not count manual restarts.** `docker inspect` showed
`Restarts=0` on a container that had been manually restarted hours earlier — the
field counts only automatic restarts by the restart policy. Use
`.State.StartedAt` to find out when the container actually last came up.
