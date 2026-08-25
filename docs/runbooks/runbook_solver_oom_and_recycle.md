# Runbook: Solver OOM Evidence and Recycling

Operational companion to [Plan 124](../plans/plan_124_trawl_memory_guardrails.md) and
[Plan 136](../plans/plan_136_solver_recycle_and_liveness.md). Covers the browser solver
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
ran healthy for 8 hours at a **0% solve rate**. Check outcomes, not liveness —
and read them from the **counter**, not from a log grep:

```bash
docker exec cartracker-prometheus wget -qO- \
  'http://localhost:9090/api/v1/query?query=cartracker_detail_fetch_total' \
  | python3 -m json.tool
```

`outcome="ok"` against `outcome="403"` and `outcome="error"`. **Mostly `403`
with few or no `ok` is an outage**, even with a green container.

> **The old `grep -ioE 'solved|403'` in this section overcounted, and is
> removed.** On 2026-08-25 it reported **12 `403`s** over a window where
> `cartracker_detail_fetch_total` reported **`403=0`** — an unanchored `403`
> matching lines that are not 403 responses. That is precisely the defect
> [Plan 141](../plans/plan_141_structured_log_ingestion_contract.md) was
> written for: `ct-403-log-spike` produced 49 of 51 alert annotations the same
> way, catching INFO lines from `shared.minio`. The bug was fixed in the alert
> rule and survived here. **Anywhere a bare `403` is grepped out of logs is
> suspect.**

The counters reset when `scraper` is recreated, so a small total after a deploy
means a short window, not a quiet solver. `cartracker_solver_requests_total` is
a *different* counter — one entry per session bootstrap, not per fetch — so a
value of 1 against 400 fetches is normal, not a gap.

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

A `CONSTRAINT_MEMCG` kill of `camoufox-bin` **used to be expected background
noise** on this host — roughly every 1.5–4 days, at ~3.2–3.5 GB anon-rss.

> **That stopped on 2026-08-18 and the absence is now the warning sign.** Zero
> kills in the 5.6 days to 2026-08-23, against 3 in the prior 18. The kill was
> in effect a free recycle, and the image in production has no periodic
> recycling of its own — so without it the leak climbs until the pool wedges,
> which is what happened on 2026-08-22 at 4 days. See
> [Plan 136 D7 and D8](../plans/plan_136_solver_recycle_and_liveness.md#d7--the-involuntary-recycle-stopped-and-the-leak-stopped-being-harmless).
> A kill appearing again is good news, not bad.

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

> **These thresholds describe the OLD image and are pending re-measurement.**
> Production moved to `trawl` **v1.4.2** on 2026-08-25 19:50:55, and its early
> behaviour is different in kind — it reclaims after a batch and plateaus,
> where the old build only ever climbed. **Plan 136 Stage 3b's 48-hour soak
> closes 2026-08-27 and owns the replacement numbers.** Until then, treat
> everything below as history, not as an operating threshold.

**On the old build (2026-07-06, revision `d0877c5`) — for historical reading
only.** The "steady state ~70%" figure quoted before 2026-08-25 was the
midpoint of a sawtooth that stopped existing when the OOM killer stopped
recycling the browser. Against the monotonic climb it described nothing. The
climb was measured on 2026-08-25 at **+40.5 MiB/h ≈ 971 MiB/day** from a
~727 MiB post-restart baseline — **65% faster than the ~590 MiB/day this page
used to state** — reaching the wedge band at about 4 days.

**Read the curve with `max_over_time` on the raw samples, not off a grid.** On
2026-08-25 the 5-minute view topped out at 80.0% of cap while the true 15s
peaks were already 84–86.5%, past the "wedge within hours" line and had been
for about five hours. A grid view understated it by ~280 MiB:

```bash
docker exec cartracker-prometheus wget -qO- \
  'http://localhost:9090/api/v1/query?query=max_over_time(cartracker_container_memory_bytes{container="trawl"}[1h])'
```

The **Container Memory Headroom** and **Solver Memory Against Its Cap** panels
on the Infrastructure dashboard chart it continuously (Plan 136 Stage 3a), but
they are grid-sampled — use them to see shape, and the query above to see
peaks. On the old build: past ~75% of cap a restart was due; past ~85%, a wedge
within hours. ~200 of 512 PIDs is normal on both builds.

## 5. Restart the solver

A plain restart resolved the 2026-08-14 outage completely — the fault was stale
in-container state.

```bash
docker restart cartracker-trawl
```

> **`TRAWL_IMAGE` is pinned by digest since 2026-08-25** (Plan 136 Stage 3b), so
> "no image pull needed" is no longer a property of a restart — it is now
> guaranteed by the pin, and a `docker compose up -d trawl` can no longer
> silently upgrade you. The pin lives in
> [`docker-compose.yml`](../../docker-compose.yml), **not** in `.env`; the
> previous digest is recorded beside it for rollback. If you need to roll back,
> change the pin and recreate — do not set `TRAWL_IMAGE` in `.env`, which is
> what hid a six-week drift in the first place.

Takes ~4s for both browsers to warm. **No VM reboot, and no `docker compose
down`.** Then confirm recovery by outcomes, not by container status:

```bash
sleep 60
docker exec cartracker-prometheus wget -qO- \
  'http://localhost:9090/api/v1/query?query=cartracker_detail_fetch_total'
```

Recreating `scraper` resets these counters; restarting only `trawl` does not,
so after a solver-only restart compare against the value you read before it.

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
  theory; [Plan 136](../plans/plan_136_solver_recycle_and_liveness.md) Stage 2 adds the
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
