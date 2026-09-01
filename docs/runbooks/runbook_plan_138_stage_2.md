# Run Sheet: Plan 138 Stage 2 — Move the front door

Green-lighting the deploy that makes `/` the public landing page, forwards
`/info`, and gives the weekly recaps a route. This is build-order step 5 —
"deploy Caddy and `ops` together and run the full matrix".

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
| The dashboard's websocket actually connecting through the real proxy | 6 |
| That the ops image was rebuilt, so the new static assets exist | 5.4 |
| That an unauthenticated visitor gets content rather than a Google sign-in | 5.1 |

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

The Caddyfile is a bind mount, so the **running** Caddy container is already
reading the new file off disk. It has not acted on it yet. That means you can
ask the live container whether the config it would load is valid, before you
give it the chance to fail on it:

```bash
docker compose exec caddy caddy validate \
  --config /etc/caddy/Caddyfile --adapter caddyfile
```

Required output ends with `Valid configuration`.

**If it does not, stop here.** Nothing has changed yet and nothing is down.
`git log -1 --stat Caddyfile` and fix it before going on. Restarting Caddy on an
invalid config takes the whole site off the internet, including the routes this
deploy does not touch.

Warnings about `Unnecessary header_up X-Forwarded-Proto` and `input is not
formatted` are pre-existing and are not a failure.

### Read back the route table

Optional, and worth the twenty seconds on this particular deploy, because it is
the only check that shows Caddy's *own* resolution rather than the file:

```bash
docker compose exec caddy caddy adapt \
  --config /etc/caddy/Caddyfile --adapter caddyfile 2>/dev/null \
  | python3 -c 'import json,sys
cfg=json.load(sys.stdin)
for r in cfg["apps"]["http"]["servers"]["srv0"]["routes"][0]["handle"][0]["routes"]:
    m=r.get("match",[{}]); print(m[0].get("path") if m else "CATCH-ALL")'
```

`["/"]` must appear as its own entry, and the last line must be `CATCH-ALL`.
If `/` reads as `["/*"]`, the dashboard is about to break — see step 6.

---

## 3. Apply the Caddyfile

```bash
./scripts/redeploy.sh --restart caddy
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
./scripts/redeploy.sh ops
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

## 6. The check no terminal can run — the dashboard websocket

**This is Gate 2's hard half and the reason this stage was written the way it
was.** Everything above can pass while the dashboard is broken.

Streamlit runs with no `--server.baseUrlPath`. It believes it is mounted at `/`
and serves `/_stcore/health`, `/_stcore/stream` (its websocket) and its whole
static bundle from the root. Nothing rewrites those paths — they resolve only
because Caddy's final catch-all forwards everything unmatched to
`dashboard:8501`. This stage took `/` away from that catch-all. If the root
matcher is a prefix rather than an exact match, Streamlit loses its own
machinery **and `/dashboard` goes on returning 200 the whole time.**

In a browser, signed in as a `viewer` (not admin — use the least-privileged
account that should work):

1. Open DevTools → **Network**, tick *Preserve log*, filter on `_stcore`.
2. Load `https://cartracker.info/dashboard`.
3. Check, in order:

| Check | Required |
|---|---|
| `/_stcore/stream` | status **101** (Switching Protocols) and the connection stays open |
| Any `_stcore` request | **no** 4xx, 5xx, or `(failed)` |
| The page itself | widgets and data render — not a blank page, not a spinner that never resolves, not "Please wait…" |
| Console tab | no errors |
| Interact with a filter or control | the page responds; a dead websocket looks fine until you touch something |

4. Hard-reload (Ctrl+Shift+R) and confirm it survives a cold cache.

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

Ship it when **all** of these hold:

- [ ] Step 2 printed `Valid configuration` before Caddy was restarted.
- [ ] Every row of 5.1 matches, with `/robots.txt` and `/sitemap.xml` returning
      their real content types rather than `text/html`.
- [ ] `/info` redirects in exactly one hop to `/`.
- [ ] The sitemap holds 22 URLs, no duplicates, and names nothing behind OAuth.
- [ ] `favicon.svg` and `og-preview.png` return 200.
- [ ] Every path in 5.6 still redirects to Google.
- [ ] **Step 6: a `viewer` loads the dashboard, `/_stcore/stream` is 101, no
      failed `_stcore` request, and the page is interactive.**

The last one is not optional and not inferable from a status code.

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
./scripts/redeploy.sh --restart caddy
```

Old Caddy in front of new ops is the "`ops` first" row of step 1's table:
`/info` will 308 to `/`, which the catch-all sends to Streamlit and OAuth. So if
you roll back Caddy, **roll back ops too** unless you are actively debugging:

```bash
git checkout <pre-merge-sha> -- ops/
./scripts/redeploy.sh ops
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
