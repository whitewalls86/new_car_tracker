# Plan 165: Service Subdomain Routing

## Status

Written 2026-08-31, out of scoping [Plan 138](plan_138_public_surface_refresh.md).
Not started. Backlog, triggered by [Plan 69](plan_69_terraform.md) landing.

Priority, effort and position are proposed in [`docs/PLANS.md`](../PLANS.md),
which owns all three; this document does not choose them.

## Decision in one paragraph

Move every proxied service off a path prefix on `cartracker.info` and onto its
own host — `grafana.cartracker.info`, `airflow.cartracker.info`, and so on —
keeping the apex for the public landing page and the OAuth callback. The
authorization model does not change: each host keeps the same `forward_auth`
pair and the same role requirement it has today. The win is that four
per-service subpath settings, which exist only to tell an application it is
mounted somewhere other than its own root, all disappear.

## The problem

Every service behind a path prefix has to be told about that prefix, in its own
configuration language, and the setting has nothing to do with what the service
does:

| Setting | Location | Exists only because of path routing |
|---|---|---|
| `GF_SERVER_ROOT_URL: https://cartracker.info/grafana` | `docker-compose.yml:942` | yes |
| `GF_SERVER_SERVE_FROM_SUB_PATH: 'true'` | `docker-compose.yml:943` | yes |
| `AIRFLOW__API__BASE_URL: "https://cartracker.info/airflow"` | `docker-compose.yml:760` | yes |
| `SCRIPT_NAME: /pgadmin` | `docker-compose.yml:612` | yes |
| `handle_path /minio*` — the only `handle_path` in the file | `Caddyfile` | yes |

The MinIO row is the sharpest one. It is the single place in the Caddyfile that
strips the prefix rather than passing it through, because the MinIO console has
no equivalent of the other three settings. That asymmetry is invisible until
someone copies a neighbouring `handle` block and finds the console broken.

**The dashboard is the case with no setting at all, and it is the fragile one.**
`dashboard/Dockerfile` runs Streamlit with no `--server.baseUrlPath`, so
Streamlit believes it is mounted at `/`. It works today only because the
Caddyfile's final catch-all `handle { … reverse_proxy dashboard:8501 }` also
answers `/_stcore/*` — the health, stream and static routes Streamlit serves
from the root. The `/dashboard*` block is not what makes the dashboard work; the
catch-all is. Nothing states that, and no test asserts it.

That coupling is why this plan is worth doing rather than merely tidy: a host of
its own gives Streamlit its whole origin back and removes a dependency on
catch-all ordering that currently holds by accident.

## What is already in place

**The hardest part is done.** `oauth2-proxy/oauth2-proxy.cfg:27` already sets:

```
cookie_domains  = [".cartracker.info"]
```

The session cookie is already scoped to the wildcard domain, so a cookie issued
at the apex is already sent to every subdomain. No re-authentication, no second
cookie, no per-host session.

**Google's OAuth configuration does not need to change.** `redirect_url` is
pinned to `https://cartracker.info/oauth2/callback`
(`oauth2-proxy/oauth2-proxy.cfg:14`) and stays pinned. A sign-in initiated from
a subdomain travels to Google, returns to the apex callback, and is redirected
onward via the `rd=` parameter that the Caddyfile's `redir` lines already set.
The one config change that enables it is `whitelist_domains`, which is what
permits an `rd=` target outside the callback's own host.

## Constraints measured, not assumed

**No wildcard certificate without a custom image.** `docker-compose.yml` runs
`image: caddy:latest` with no `build:` stanza. The stock image carries no DNS
provider plugin, and a wildcard `*.cartracker.info` certificate requires the
DNS-01 challenge, which requires one. Per-host certificates over HTTP-01 or
TLS-ALPN need no plugin and no image change.

**So the DNS shape is one A record per host, not a wildcard.** For six or so
hosts that is the cheaper trade: it keeps `caddy:latest` and it keeps the set of
valid hostnames explicit rather than making every typo resolve.

## Why this is not part of Plan 138

Recorded here so the question is not re-opened.

**It would put two causes behind one gate.** Plan 138's Gate 2 is "a fresh
unauthenticated session gets HTTP 200 at `/`; `/dashboard` enters OAuth; `/info`
redirects once to `/`". That gate is written against a single-host route table,
and the Caddy site block is already the riskiest edit in Plan 138. Moving six
services to new hosts in the same change roughly doubles Stage 5's route matrix
and gives every Gate 2 failure two candidate explanations.

**It is the wrong kind of work for that plan.** Plan 138 is a public-surface
plan whose non-goals open with "Making the dashboard, admin UI, Airflow,
Grafana, MinIO, or pgAdmin public." Subdomain routing does not make any of them
public — the `forward_auth` pair moves with the route — but every service it
touches is on that list. It is an authenticated-service concern, and Plan 138's
subject is the unauthenticated one.

## Why the trigger is Plan 69

[Plan 121](plan_121_staging_environment.md) already targets
`https://dev.cartracker.info` as the staging entry point, so a second hostname
is arriving on that track regardless of this plan. Plan 69 is the plan that
imports the existing VM, network and firewall into Terraform until `plan` shows
no diff, and `docs/PLANS.md` already sequences it ahead of Plan 121 so that
staging and production come from one module set.

Running this plan first would mean Plan 69 imports a DNS and routing shape that
this plan then immediately changes — the exact rework Plan 69 exists to stop.
Running it after means the new records are authored as code the first time
anyone writes them down.

## Target host and access contract

The role requirement on each row is the one that host has **today**; this plan
moves routes, not permissions.

| Host | Access | Serves |
|---|---|---|
| `cartracker.info` | Public | Plan 138 landing page, `/static_ops/*`, `/oauth2/*`, `/request-access*` |
| `dashboard.cartracker.info` | `viewer`+ | Streamlit at its own root |
| `grafana.cartracker.info` | `admin` | Grafana at its own root |
| `airflow.cartracker.info` | `admin` | Airflow at its own root |
| `minio.cartracker.info` | `admin` | MinIO console at its own root |
| `pgadmin.cartracker.info` | `admin` | pgAdmin at its own root |

Every host needs its own `handle /oauth2/*` block reaching `oauth2-proxy:4180`,
because the sign-in redirect has to be answerable on the host the browser is
already on.

## Open questions

These are the decisions the plan cannot make before it starts. None is
answerable from the tree today.

1. **Where does the `ops` service land?** It is the only service that is both
   public and admin: it serves `/info` and `/static_ops/*` (public), `/auth/check`
   (called by Caddy itself), `/request-access*` (authenticated), and `/admin*`
   (observer+). The public half must stay on the apex for Plan 138. Whether the
   admin half moves to `admin.cartracker.info` or stays as a path on the apex is
   an open choice, and splitting one service across two hosts may cost more than
   it saves.

2. **Do the old paths redirect, or stop answering?** A permanent redirect from
   `/grafana*` to the new host preserves every bookmark and every link in an old
   alert, at the cost of keeping the path table alive. Plan 138 sets a precedent
   for the redirect answer with `/info` → `/`.

3. **Does `/admin/snapshots/adaptive-refresh*` move?** It is routed directly to
   `ops` ahead of the generic `/admin*` block so its `SNAPSHOT_DOWNLOAD_TOKEN`
   bearer auth runs instead of an OAuth redirect. CI and local scripts call it by
   URL, so moving it is a breaking change to callers outside this repository.

4. **Is `whitelist_domains` sufficient on its own,** or does the `rd=` round trip
   through the apex callback also need `cookie_csrf_per_request` or a
   `relative_redirect_url` change? This is answerable only by standing one host
   up.

5. **Who owns the DNS records** — the registrar by hand, or Plan 69's Terraform?
   The answer decides whether this plan writes records or writes modules.

## Stages

### Stage 0 — Prove one host end to end

Stand up exactly one service on its own host — Grafana is the candidate, being
the one whose subpath settings are the most explicit — while `/grafana*`
continues to work. Add `whitelist_domains`, one A record, one Caddy site block,
and drop `GF_SERVER_SERVE_FROM_SUB_PATH`.

**Gate 0:** an unauthenticated request to `grafana.cartracker.info` completes the
full Google sign-in and lands back on that host with a working session, using the
existing apex callback and no new Google console entry. Open question 4 is
answered by this stage and by nothing before it.

### Stage 1 — Move the remaining services

One host per deploy, each removing its subpath setting in the same change, in
ascending blast radius: pgAdmin, MinIO, Airflow, dashboard last.

The dashboard is last because it is the one whose current routing is not what it
appears to be. Its move must be accompanied by a test that asserts `/_stcore/*`
resolves on the new host, so the catch-all coupling is replaced by something
stated rather than by a different accident.

### Stage 2 — Retire the path table

Apply the open-question-2 decision, remove the settled `handle` blocks, and
delete `handle_path /minio*` along with the asymmetry it created.

## Success criteria

1. Every service answers on its own host with the role requirement it had before,
   verified by an external route matrix run unauthenticated and as each role.
2. All five subpath settings in the table above are gone from the tree.
3. One sign-in grants a session on every host, with no second Google prompt.
4. Streamlit's `/_stcore/*` routes are asserted by a test rather than served by
   catch-all ordering.
5. `caddy:latest` is still the image, with no `build:` stanza added.
6. Rollback is the previous Caddyfile plus the previous Compose environment; no
   migration and no DNS deletion is required to roll back, because the old paths
   are retired only in Stage 2.
