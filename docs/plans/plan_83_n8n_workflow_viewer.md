# Plan 83: n8n Workflow Viewer — Read-only Portfolio Page

**Status:** Planned
**Priority:** Low — portfolio nicety, depends on Plan 82 (observer role)

## Overview

Build a read-only workflow viewer in the ops UI, accessible to `observer` and
above. Pulls workflow definitions from n8n's internal API and renders them as
static diagrams. Gives portfolio visitors a clear view of the automation design
without access to the live n8n editor.

---

## Why not just show n8n directly?

n8n community edition has no read-only role. Any logged-in user can edit and
trigger workflows. Even with Caddy gating entry, an observer-role user inside
n8n has full editor access. The viewer page solves this by consuming n8n's API
server-side and presenting a safe, static representation.

---

## What to show

### Workflow diagram

n8n exposes workflow definitions via its REST API (`GET /api/v1/workflows`).
Each workflow is a JSON graph of nodes and connections. Options for rendering:

1. **Mermaid flowchart** — ops generates a Mermaid diagram server-side from the
   node graph. Rendered in the browser via the Mermaid JS library (CDN, no
   build step). Simple, no extra dependencies. Node labels show the n8n node
   type and name.

2. **SVG export** — n8n can export workflow screenshots via Playwright headless
   (fragile, heavyweight). Not recommended.

**Recommendation: Mermaid.** The conversion from n8n's node/connection graph to
a Mermaid `flowchart LR` is straightforward and produces a clean portfolio
artifact.

### Execution history

A table of recent executions per workflow: timestamp, trigger, status
(success/error), duration. Pulled from `GET /api/v1/executions`. Shows the
system is live and active without exposing the editor.

---

## Components

### 1. n8n API key

n8n exposes a REST API authenticated by API key. Generate a key in the n8n UI
(`Settings → API`) and store it as `N8N_API_KEY` in `.env`. The ops container
calls n8n internally on `cartracker-net` — this key never leaves the server.

### 2. Ops: `/admin/workflows` endpoint

New route in ops (observer+ access):

```
GET /admin/workflows
  - Calls n8n GET /api/v1/workflows (internal, cartracker-net)
  - For each active workflow, converts node graph → Mermaid diagram string
  - Calls n8n GET /api/v1/executions?limit=20 for recent history
  - Renders a template: one section per workflow, diagram + execution table
```

The Mermaid conversion runs server-side in Python — no client-side JSON
handling, no n8n credentials in the browser.

### 3. Mermaid conversion logic

n8n workflow JSON → Mermaid `flowchart LR`:

- Each node becomes a labelled box: `nodeId[NodeType: Name]`
- Each connection becomes an arrow: `nodeId1 --> nodeId2`
- Trigger nodes get a stadium shape: `nodeId([Trigger: Name])`
- Error branches get a red-styled edge (Mermaid `:::error` class)

Implemented as a pure function `workflow_to_mermaid(workflow_json) -> str` in
a new `ops/utils/n8n.py` module.

### 4. Template

Static HTML page with:
- Workflow name + active/inactive badge
- Mermaid diagram (rendered client-side by Mermaid JS from the server-generated
  diagram string embedded in the page)
- Recent executions table (last 20, paginated)
- "Last synced" timestamp

No live polling — page is fetched fresh on each load.

### 5. Caddyfile

`/admin/workflows` falls under the existing `/admin/*` observer+ block added in
Plan 82 — no Caddyfile changes needed.

---

## New environment variables

| Variable | Where | Purpose |
|---|---|---|
| `N8N_API_KEY` | `.env` only | Key for ops → n8n internal API calls |

---

## Rollout Order

1. Generate n8n API key, add `N8N_API_KEY` to `.env` and ops container env
2. Write `ops/utils/n8n.py` — API client + Mermaid converter
3. Add `/admin/workflows` route and template to ops
4. `docker compose build ops && docker compose up -d ops`

## Dependencies

- Plan 82 must be complete first (observer role must exist before building the
  page that targets it)
