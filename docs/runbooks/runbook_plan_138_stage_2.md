# Run Sheet: Plan 138 Stage 2 — Move the front door

Green-lighting the deploy that makes `/` the public landing page, forwards
`/info`, and gives the weekly recaps a route. This is build-order step 5 —
"deploy Caddy and `ops` together and run the full matrix".

> **This deploy was run on 2026-09-02 and reverted the same night. Do not run it
> again until build-order step 3b is deployed.**
>
> Everything in the unauthenticated matrix passed. `/dashboard` still broke:
> moving `/` to the ops landing page takes away the address Streamlit's
> client-side router falls back to, and the address its relative asset URLs
> resolve against. PR #338 deployed it, PR #340 reverted it, and the plan
> document's *Stage 2 evidence* section holds the measurements.
>
> **The mechanism is now understood and fixed** — step 3b gives Streamlit
> `--server.baseUrlPath=dashboard`, which is built and verified against a local
> image but **not yet deployed**. Deploy that first, confirm the dashboard in a
> browser, then run this sheet.
>
> This sheet is kept, and corrected, because three of its findings survive the
> revert and two of its own steps were wrong. Read the corrections before
> trusting any part of it.

**This deploy touches the reverse proxy that serves every route on the host.**
A bad Caddyfile does not degrade one page; Caddy refuses to start and `:80` and
`:443` go down together. Step 1 exists to make that impossible, and it costs
about two seconds.

**The automated suite cannot close this gate.** 3,463 unit tests pass, and they
assert the Caddyfile's *routing intent* and the ops application's *responses* —
both against a parsed file and a `TestClient`, neither against the running
system. Three things only a real deploy can show:

| What the suite cannot see | Step |
|---|---|
| **That `/dashboard` still reaches the app at all** — this is what failed | 6 |
| The dashboard's websocket actually connecting through the real proxy | 6 |
| That the ops image was rebuilt, so the new static assets exist | 5.4 |
| That an unauthenticated visitor gets content rather than a Google sign-in | 5.1 |

The first row was not in this table when the deploy was run. It was added
afterwards, from the failure, and it is the row that matters: the suite asserts
`/dashboard` *routes to* `dashboard:8501`, which stayed true throughout while the
page was unusable.

---

## 0. Preflight — record what is live before you change it

Run these **before** the pull, from your machine. They are the baseline that
tells a regression apart from a pre-existing condition.

```bash
for p in / /info /recaps /robots.txt /sitemap.xml /dashboard; do
  printf '%-14s ' "$p"
  curl -sS -o /dev/null -w '%{http_code} -> %{redirect_url}\n' "https://cartracker.info$p"
done
```

**Measured 2026-09-01**, against the live host at `fc3a0e9`. None of it is a
fault:

| Path | Now | Why |
|---|---|---|
| `/` | 302 → `/oauth2/sign_in?rd=…` | the catch-all sends it to Streamlit, which is behind OAuth |
| `/info` | `200 text/html` | today's landing page |
| `/recaps` | 302 → `/oauth2/sign_in?rd=…` | no route yet; the catch-all swallows it |
| `/robots.txt`, `/sitemap.xml` | 302 → `/oauth2/sign_in?rd=…` | same |
| `/static_ops/generated/recaps/2026-08-30.html` | `200 text/html` | already public, and already unlinked — Stage 7 recorded this |
| `/dashboard` | 302 → `/oauth2/sign_in?rd=…` | correct, and must stay this way |

The bounce is Caddy's own `/oauth2/sign_in` handler, one hop before Google — so
"redirects to Google" below means a 302 whose `location` is
`https://cartracker.info/oauth2/sign_in?rd=…`.

### Where to run these

Steps 1–4 run on the host over SSH; step 5 runs from your machine, because the
point is what the public internet sees. Step 6 runs in a real browser — it is
the one check that cannot be done from a terminal.

```bash
ssh -i C:/Users/mille/PycharmProjects/cartracker-scraper/ssh-key-2026-04-08.key \
    ubuntu@147.224.199.86
```

**The checkout is `/opt/cartracker`.** Not `~/new_car_tracker` — there is no
such directory on this host, whatever the Plan 131 run sheet says.

---

## 1. The order, and why it is this order

**Caddy first, then `ops`.** Not the other way round, and the reason is the URL
on your resume.

There is no atomic way to swap both. One of them lands first, and for the
seconds in between the system is a mix of old and new. The two orders fail
differently:

| Order | During the window | Verdict |
|---|---|---|
| **`ops` first** | new ops answers `/info` with a 308 to `/`; old Caddy still routes `/` to Streamlit → **`/info` bounces visitors into a Google sign-in** | breaks the one URL that is printed on a resume, LinkedIn and GitHub |
| **Caddy first** | new Caddy routes `/` and `/recaps` to old ops → `/` redirects to `/admin/searches/` (OAuth) and `/recaps` 404s. `/info` still serves the page | breaks only URLs nobody has been given yet |

Caddy-first costs nothing real: `/` **already** sends an unauthenticated visitor
to OAuth today, so routing it to old ops changes which OAuth bounce they get,
not whether they get one.

**Shrink the window anyway.** Build the ops image *before* touching Caddy, so
step 5 is a cached rebuild and a recreate rather than a full build:

```bash
cd /opt/cartracker
git pull
docker compose build ops      # no recreate; the running container is untouched
```

---

## 2. Validate the Caddyfile before restarting into it — THE GATE

**Corrected 2026-09-02. The first version of this step validated the wrong
file and passed.** It said to run `caddy validate` inside the *running*
container, on the reasoning that a bind mount means the container is already
reading the new file. That reasoning is wrong, and it is wrong in the way this
repository has already paid for once.

`./Caddyfile:/etc/caddy/Caddyfile:ro` is a **single-file** bind mount. It pins
the inode resolved at container start, and `git pull` *replaces* the file rather
than editing it in place — so the running container goes on reading the old,
now-unlinked copy. Measured during the 2026-09-02 attempt, after the pull:

```text
6c83ad730e90bd35f244b83722ef17be  /opt/cartracker/Caddyfile      (host, new)
1fcd5674ebb02e784d20bdc0c586cc56  /etc/caddy/Caddyfile           (container, old)
```

The gate returned `Valid configuration` against that stale copy. This is
`redeploy.sh` decision 4 — "a SIGHUP reload is worse than useless there, it logs
*Completed loading of configuration file* against the stale config, which is how
this went unnoticed twice on 2026-08-20" — in a different costume. **Check
`redeploy.sh`'s own decision list before inventing a pre-flight gate; it has
already thought about this.**

Validate in a **throwaway container**, which resolves the path now:

```bash
docker run --rm -v /opt/cartracker/Caddyfile:/etc/caddy/Caddyfile:ro \
  caddy:latest caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
```

Required output ends with `Valid configuration`.

**If it does not, stop here.** Nothing has changed yet and nothing is down.
`git log -1 --stat Caddyfile` and fix it before going on. Restarting Caddy on an
invalid config takes the whole site off the internet, including the routes this
deploy does not touch.

Warnings about `Unnecessary header_up X-Forwarded-Proto` and `input is not
formatted` are pre-existing and are not a failure.

### Read back the route table — not optional, and it is what caught the stale gate

Caddy does not evaluate `handle` blocks in source order; it sorts them by matcher
specificity. This is the only check that shows Caddy's *own* resolution rather
than the file's text — and on 2026-09-02 it is what revealed that the gate above
had validated the wrong file, because it printed the **pre-Stage-2** table with
no `/`, no `/recaps` and no `/robots.txt`.

Run it against the **running** container first. That tells you which file the
container is actually on:

```bash
docker compose exec caddy caddy adapt \
  --config /etc/caddy/Caddyfile --adapter caddyfile 2>/dev/null \
  > /tmp/adapt-running.json
```

Then against a throwaway, which resolves the path now:

```bash
docker run --rm -v /opt/cartracker/Caddyfile:/etc/caddy/Caddyfile:ro \
  caddy:latest caddy adapt --config /etc/caddy/Caddyfile --adapter caddyfile \
  2>/dev/null > /tmp/adapt-new.json
```

Print either one:

```bash
python3 -c 'import json,sys
cfg=json.load(open(sys.argv[1]))
for r in cfg["apps"]["http"]["servers"]["srv0"]["routes"][0]["handle"][0]["routes"]:
    m=r.get("match",[{}]); print(m[0].get("path") if m else "CATCH-ALL")' /tmp/adapt-new.json
```

**Before** the restart the two must *differ* — that is the inode pinning, and
seeing them match before a restart means the pull did not land. **After** the
restart they must agree. In the new table `["/"]` must appear as its own entry
and the last line must be `CATCH-ALL`; if `/` reads as `["/*"]`, stop.

---

## 3. Apply the Caddyfile

```bash
bash scripts/redeploy.sh --restart caddy
```

`--restart` is the correct mode: same image, same Compose config, but the
process must re-read a bind-mounted file. The script waits for the health check
and then verifies the mounted file is the current one.

Confirm immediately, from your machine:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' https://cartracker.info/info
```

`200`. If this is anything else, Caddy is not serving and you should roll back
now (step 8) rather than continue.

---

## 4. Deploy `ops`

```bash
bash scripts/redeploy.sh ops
```

Build + recreate. The image build is what carries `ops/routers/public.py`, the
template's new `<head>`, and the two new static files — **`favicon.svg` and
`og-preview.png` are image-side, not under `generated/`**, so Stage 7's
`git pull` publishing mechanism does *not* cover them. If you skip this build
they 404 and the rest of the page still looks fine.

The regenerated recap HTML *is* under `generated/`, so the `git pull` in step 1
already published it.

---

## 5. The unauthenticated matrix

From your machine, not the host — the point is what the public internet sees.

### 5.1 Nothing public bounces to Google

```bash
for p in / /info /recaps /recaps/2026-08-30 /robots.txt /sitemap.xml \
         /static_ops/generated/project-updates.json /favicon-check; do
  printf '%-46s ' "$p"
  curl -sS -o /dev/null -w '%{http_code} %{content_type} -> %{redirect_url}\n' \
    "https://cartracker.info$p"
done
```

| Path | Required | Notes |
|---|---|---|
| `/` | `200 text/html` | the landing page, no redirect |
| `/info` | `308` → `https://cartracker.info/` | exactly one hop |
| `/recaps` | `200 text/html` | the index |
| `/recaps/2026-08-30` | `200 text/html` | one recap page, no `.html` |
| `/robots.txt` | `200 text/plain` | **not** `text/html` — HTML here means a Google sign-in page |
| `/sitemap.xml` | `200 application/xml` | same |
| `/static_ops/generated/project-updates.json` | `200 application/json` | unchanged by this stage |
| `/favicon-check` | `302` → Google | a nonsense path must still fall to the authenticated catch-all |

Any `text/html` where the table says `text/plain` or `application/xml` is the
failure this stage is most likely to produce: the status code is 200 and the
body is a login screen.

### 5.2 The redirect does not loop

```bash
curl -sS -o /dev/null -w '%{num_redirects} %{url_effective}\n' -L \
  https://cartracker.info/info
```

`1 https://cartracker.info/`. Two or more means `/` and `/info` are pointing at
each other.

### 5.3 The sitemap names each page once, and names nothing protected

```bash
curl -sS https://cartracker.info/sitemap.xml > /tmp/sitemap.xml
grep -c '<loc>' /tmp/sitemap.xml                              # expect 22
grep -o '<loc>[^<]*</loc>' /tmp/sitemap.xml | sort | uniq -d  # expect no output
grep -E 'static_ops|dashboard|admin|request-access|grafana|airflow' /tmp/sitemap.xml
```

**22** = the root, `/recaps`, and 20 recap pages. The third command must print
nothing: `/static_ops/generated/recaps/*.html` is public and is a second address
for the same content, and the sitemap deliberately names only the canonical one.

```bash
curl -sS https://cartracker.info/robots.txt | grep -E 'Allow|Sitemap|Disallow: /dashboard'
```

### 5.4 The new static assets actually shipped

```bash
for f in /static_ops/favicon.svg /static_ops/og-preview.png; do
  printf '%-34s ' "$f"
  curl -sS -o /dev/null -w '%{http_code} %{content_type} %{size_download}\n' \
    "https://cartracker.info$f"
done
```

`200 image/svg+xml` and `200 image/png ~39127`. A 404 here means step 4 did not
rebuild the image.

### 5.5 The duplicate URL points home

```bash
curl -sS https://cartracker.info/static_ops/generated/recaps/2026-08-30.html \
  | grep -o '<link rel="canonical"[^>]*>'
```

Must name `https://cartracker.info/recaps/2026-08-30`. This is what stops a
crawler indexing the same recap twice.

### 5.6 Everything that was protected still is

```bash
for p in /dashboard /admin /admin/users /grafana /airflow /pgadmin /minio /request-access; do
  printf '%-18s ' "$p"
  curl -sS -o /dev/null -w '%{http_code} -> %{redirect_url}\n' "https://cartracker.info$p"
done
```

Every one must still redirect to Google. A `200` on any of these is a
security regression and is an immediate rollback.

---

## 6. The check no terminal can run — the dashboard

**This is Gate 2's hard half. On 2026-09-02 it failed, and it is the only step
that caught the failure.** Everything above passed.

Streamlit runs with no `--server.baseUrlPath`. It believes it is mounted at `/`
and serves `/_stcore/health`, `/_stcore/stream` (its websocket) and its whole
static bundle from the root. Nothing rewrites those paths — they resolve only
because Caddy's final catch-all forwards everything unmatched to
`dashboard:8501`.

### The coupling is broader than this plan said, and the plan is wrong

Stage 2's rationale predicted one failure: widen the root matcher to `/*` and
Streamlit loses `/_stcore/*` while `/dashboard` still returns 200.
`tests/test_caddy_public_routes.py` asserts exactly that, correctly, and it is
**blind to what actually happened.**

The root matcher was an exact `/`. `/_stcore/*` kept reaching the dashboard —
the adapted route table confirmed it. `/dashboard` broke anyway.

What was measured on 2026-09-02:

| Observation | Evidence |
|---|---|
| Streamlit serves one SPA shell for every path | `/dashboard` and `/` returned byte-identical bodies from `dashboard:8501` — same `etag`, both 11,141 bytes. Routing is entirely client-side |
| `/dashboard` errored "page not found", then landed on `/` | reported from the browser; `/` is the ops landing page after this stage, so the fallback address is gone |
| `/dashboard/` with a trailing slash **also** failed | "it just takes longer to fail" — so this is *not* simply Streamlit reading the last path segment as a page name |
| A Streamlit session did run | `docker logs cartracker-dashboard` at 00:38:26 shows `use_container_width` deprecation warnings, which only fire when the script executes and renders widgets |

**The mechanism, found after the revert from two requests that were available
all along.** Streamlit answers every unrecognised path with its SPA shell, and
that shell links its assets **relatively** (`./static/js/index….js`). So
`/static/js/index….js` returned `application/javascript` at 451,569 bytes while
`/dashboard/static/js/index….js` returned `text/html` at 11,141 — the shell
again. At `/dashboard/` the relative asset resolves under the prefix, the browser
refuses to run HTML as a module, and the app never boots. At bare `/dashboard`
the asset resolves to `/static/…` via the catch-all, the app *does* boot, and
then the router bounces to `/`. `/dashboard` was never a route Streamlit
recognised; `/` being Streamlit was what hid that.

**The fix is build-order step 3b**, built 2026-09-02:
`--server.baseUrlPath=dashboard` in `dashboard/Dockerfile` plus the Compose
healthcheck path, held by `tests/test_dashboard_base_path.py`. After it,
`/dashboard/static/js/…` serves the real 527,226-byte bundle and `/static/js/…`
is a 404 — **Streamlit no longer claims the origin root**, so the catch-all
coupling this sheet was written to preserve no longer exists. Deploy 3b and
confirm the dashboard in a browser before running this sheet again.

### The check itself

In a browser, signed in as a `viewer` (not admin — use the least-privileged
account that should work). **Do this before running the matrix in step 5, not
after** — on 2026-09-02 every check in step 5 passed and told us nothing:

1. Open DevTools → **Network**, tick *Preserve log*, filter on `_stcore`.
2. Load `https://cartracker.info/dashboard` — **the bare path, no trailing
   slash**, because that is what `ops/email.py` sends every new `viewer` and what
   the sidebar links use. Then load `/dashboard/` as a separate case; on
   2026-09-02 the two failed differently and the difference is a clue.
3. Check, in order:

| Check | Required |
|---|---|
| The URL bar, after load settles | still `/dashboard` — **not** bounced to `/` |
| `/_stcore/stream` | status **101** (Switching Protocols) and the connection stays open |
| Any `_stcore` request | **no** 4xx, 5xx, or `(failed)` |
| The page itself | widgets and data render — not a blank page, not a spinner that never resolves, not "Please wait…" |
| Console tab | no errors |
| Interact with a filter or control | the page responds; a dead websocket looks fine until you touch something |

4. Hard-reload (Ctrl+Shift+R) and confirm it survives a cold cache.

**Capture the Network tab before rolling back.** The 2026-09-02 attempt was
reverted without it, which is why the mechanism is still open.

**A blank or permanently-loading dashboard here is the failure this whole
runbook exists for.** Roll back Caddy (step 8) — it is a Caddyfile problem, not
an ops problem.

Then, still signed in, click **Project home** in the dashboard sidebar. It must
land on `https://cartracker.info/` and show the landing page.

`/_stcore/health` is *also* the Compose healthcheck's path, but that check runs
inside the container against `localhost:8501` and never crosses Caddy. A green
`dashboard` container proves nothing about this step.

---

## 7. Green-light criteria

**Reordered 2026-09-02.** Step 6 was last on this list and it was the only item
that failed. Everything above it passed, which is precisely the shape of the
problem: a list that ends with the only check that can fail lets you feel six
green ticks of progress toward a conclusion none of them support. It goes first
now.

Ship it when **all** of these hold:

- [ ] **Step 6: a `viewer` loads `/dashboard` — the bare path — the URL stays
      `/dashboard`, `/_stcore/stream` is 101, no failed `_stcore` request, and the
      page is interactive.** If this fails, stop; nothing below it matters.
- [ ] Step 6 again for `/dashboard/`, with the trailing slash.
- [ ] Step 2 printed `Valid configuration`, **from a throwaway container**, before
      Caddy was restarted.
- [ ] The route table read back after the restart matches the new file.
- [ ] Every row of 5.1 matches, with `/robots.txt` and `/sitemap.xml` returning
      their real content types rather than `text/html`.
- [ ] `/info` redirects in exactly one hop to `/`.
- [ ] The sitemap holds 22 URLs, no duplicates, and names nothing behind OAuth.
- [ ] `favicon.svg` and `og-preview.png` return 200.
- [ ] Every path in 5.6 still redirects to the sign-in.

The first one is not optional and not inferable from a status code. On
2026-09-02 the seven below it all passed against a broken dashboard.

### Worth doing, not gating

- Paste `https://cartracker.info/` into a Slack or LinkedIn message box and
  confirm the preview card shows the CarTracker artwork and the description.
- Lighthouse at mobile width (Stage 5's targets: accessibility ≥ 95, SEO ≥ 95,
  best practices ≥ 95, performance ≥ 90, no horizontal overflow at 360 px).
  **Performance will likely miss** until Stage 3b deals with the 41.7 MB hero
  video; that is a known open item, not a Stage 2 regression.

---

## 8. Rollback

Independent, and Caddy is the one that matters.

**Caddy** — restores every route to its pre-deploy behaviour in seconds:

```bash
git checkout HEAD~1 -- Caddyfile     # or the pre-merge SHA
docker compose exec caddy caddy validate \
  --config /etc/caddy/Caddyfile --adapter caddyfile
bash scripts/redeploy.sh --restart caddy
```

Old Caddy in front of new ops is the "`ops` first" row of step 1's table:
`/info` will 308 to `/`, which the catch-all sends to Streamlit and OAuth. So if
you roll back Caddy, **roll back ops too** unless you are actively debugging:

```bash
git checkout <pre-merge-sha> -- ops/
bash scripts/redeploy.sh ops
```

Nothing in this stage writes to a database and there is no migration, so there
is no data to undo. The regenerated recap HTML under
`ops/static_ops/generated/recaps/` is reverted by the same `git checkout` and
republished by the next `git pull` — it needs no container action either way.

---

## Appendix — what this stage changed, at a glance

| Surface | Before | After |
|---|---|---|
| `/` | Streamlit, behind OAuth | public landing page |
| `/info` | the landing page | 308 → `/` |
| `/recaps`, `/recaps/YYYY-MM-DD` | did not exist | public, 20 pages + index |
| `/robots.txt`, `/sitemap.xml` | swallowed by the catch-all | public, real content |
| `/static_ops/generated/recaps/*.html` | public, unlinked | public, and carries `rel=canonical` to `/recaps/…` |
| `/dashboard`, `/admin`, infra tools | authenticated | **unchanged** |
| Streamlit's `/_stcore/*` | authenticated catch-all | **unchanged — and that is the point** |
