# Plan 174: Public Site Shape

## What this plan is for

Settles what shape the public site should be and builds it: a home for the
maintainer's published articles, a weekly recap collection worth landing on
rather than a bare list of dates, a way to reach the application a granted role
grants, and cards you can operate with a keyboard.

## The case

**Carved out of [Plan 138](plan_138_public_surface_refresh.md) on 2026-09-04**,
at that plan's closeout. Plan 138 set out to make the public surface honest
against the repository and to build the generators that keep it that way, and it
did: the README and landing copy were rewritten from a reconciled contract, `/`
became the public root, the roadmap and the weekly recaps are projected from
source control, and the durable rules moved to
[`docs/PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md). Four pieces of it were not
about honesty. They were about what the site *looks like*, how it behaves, and
where it lets a reader go, and they are what this plan inherits:

| From Plan 138 | The need it carries |
|---|---|
| **1g** | The published articles are reachable from the site, dated, framed as point-in-time, and adding one is a procedure rather than a list edit |
| **3d** | The weekly recaps are worth arriving at — today `/recaps` is a bare list of twenty links, and a reader has to gamble on a week before seeing whether any of it is worth reading |
| **3a** | The landing page's clickable `<div>` cards become real controls: focusable, announced, operable without a mouse, with active state carried by something other than colour |
| **the navigation pane** | The site has a way to get from one destination to another. `ops/templates/info.html` has no `<nav>` at all, so every destination is reached from prose or not at all |

The fourth was never a lettered stage. Plan 138 left navigation **deliberately
unstarted and unticketed**, deferred behind its Stage 8 destination inventory on
the grounds that you cannot build a nav before you know what it points at. Stage
8 has since landed that inventory as `docs/PUBLIC_SURFACE.md`, so the condition
it was waiting on is met and the deferral has expired. It is absorbed here on
2026-09-04 rather than left homeless, because it is the same question as the
other three: where the site's destinations are and how a reader reaches them.

They are separated from Plan 138 rather than left in it because **Plan 138's
remaining work stopped being sequential.** Its own build order says so: four of
its five open items depended on nothing in the list, so the order expressed
priority and nothing enforced it. What is left after Stage 4 closes is not a
residue of an unfinished plan; it is one coherent question that plan never asked
— *what shape should the public site be?* — and the answer to it is not yet
known.

**Nothing blocks this plan, and it is not waiting on an event.** Every piece of
it is buildable against the tree as it stands today: `/recaps` and its generator
exist, the corpus is four known articles, and the cards on `/` are already
there. It sits low in the build order because it is worth less right now than
what sits above it — the site is honest, which was the expensive problem, and
what remains is that it is plainer and less operable than it should be. That is
a priority, not a dependency, and it should not be written up as one. The open
design question is real but it is `plan-start`'s to settle in its interview,
not a gate to park behind.

### Two things that are settled, and are not the shape question

Both were closed inside Plan 138 on their own evidence, and neither turns on how
the site ends up looking:

- **The articles are out of scope as maintained content** (Stage 1f). An article
  is a point-in-time artifact, correct as of its date and never revised. This
  plan links them; it does not maintain them, reconcile them against the tree,
  or fix the contradiction in P5.
- **The recap generator inlines its own `_STYLE` and loads no external
  stylesheet.** Plan 138 specified that the pages "share `info.css`" and the
  shipped generator does not; the tree was right and the stage text was wrong.
  That is a correction to a stale spec rather than a design decision, though a
  new shape may of course change what the pages load.

### Where the thinking had got to, which is not the same as decided

Plan 138 took a run of presentation decisions in the first days of September and
then stopped. **They are recorded here as the last position, not as a design
this plan is bound to** — the shape is still open, so a section that locks it
would be answering the question this plan exists to ask. The
reason to write them down at all is that the reasoning behind them was expensive
and is easy to lose: whoever picks this up should start from the last position
and argue with it, rather than re-deriving it from nothing or, worse, rebuilding
a version Plan 138 already tried and withdrew.

Every item below is available to discard. Plan 138 holds the argument for each
one in full, and the argument is what to read — the conclusion is only where it
happened to land.

| Last position | Taken | How exposed it is |
|---|---|---|
| **Two pages, one door to each** — `/recaps` for the account of what happened, `/writings` for the published articles, and one "more depth" section on `/` holding both doors plus the newest article as a card | 2026-09-03 | **This *is* the shape question.** It already superseded an earlier "inline what is small and finite, index what grows" placement, so it is the second answer, not the first |
| **`/recaps` leads with the newest published week rendered in full**, index beneath, with `rel=canonical` pointing at the week's own page and a per-week §5 note beside the collection-level one | 2026-09-02 | Presentation, and fully in scope to revisit. The canonical and note mechanics are consequences of the full render — if it goes, they go with it |
| **No generator for the corpus** — recaps project a repository source that grows on its own; the four articles are hand-written with no source here, and the weight of that stage is an add-an-article reconciliation held by a commit gate | 2026-09-01 | **Reopened by the corpus question.** Planning documents *do* have a repository source, so admitting them as a third corpus puts the generator question back on the table for all of them |
| **Outbound links are visibly outbound** — a recap link stays on this site, an article link leaves it, and a section mixing the two must differentiate them | 2026-09-03 | The most durable of the four: it is a rule about honesty rather than about layout, and it survives most shapes. Where it applies still moves with the shape |

**One item is not on that list because it is not a layout preference.** Plan 138
decided that whatever `/recaps` renders in full **must state its week
prominently at the top** — 11 of 31 weeks hold no commits and unpublished weeks
are skipped, so the newest published recap can be weeks behind today, and a
full-bleed article that does not say which week it is reads as "here is where
things stand," which it is not. That is a truth-contract requirement, and it
binds any shape that renders a recap in full.

### The three gaps it owns

From [the gap list](../PUBLIC_SURFACE.md#the-gap-list):

- **P1** — `/dashboard` is linked from no public surface. `ops/templates/info.html`
  has no `<nav>` and names the route nowhere; its only calls to action are
  `/request-access`, at the hero and the footer. A visitor who requests access,
  is granted a role, and returns has no path to the thing they were granted.
  **This is the gap that makes navigation more than a convenience**: the other
  two are about reaching content, this one is about reaching the product.
- **P2** — `/recaps` has a canonical route, a sitemap entry, and no inbound
  link. The landing page's only recap mention resolves to GitHub. Whatever shape
  `/` settles into has to reach it from somewhere.
- **P5** — Article A contradicts Article C on bronze retention, and both stay
  published under the same name. Accepted and dated by Plan 138 Stage 1f; it
  lands here because linking the articles at all is the first time the
  contradiction becomes something this repository points at, whichever surface
  ends up doing the pointing.

### 3a is here on purpose, and it is last on purpose

The landing page's eighteen service and highlight cards are clickable `<div>`
elements: not focusable, not announced, not operable without a mouse, with
active state signalled by `border-color` and a `box-shadow` ring alone. Two
near-identical toggle blocks in `ops/static_ops/info.js` drive them.

Two of the stage's items are already retired and do not come with it — 1b
restored the heading outline and gave the pipeline diagram non-colour encoding,
both held by tests; 3b retired the reduced-motion defect when the hero video
left the page. What remains is the controls themselves, and `aria-expanded`
carries the active state for free, which is why the colour-only defect is fixed
in the same change rather than separately.

**It is nonetheless sequenced behind the shape work, by decision on 2026-09-04.**
The maintainer has had no complaints about it, and rebuilding cards whose
treatment may change once the site's shape is settled is work done twice. The
cost of that decision is stated rather than assumed: **a live public page stays
keyboard-inoperable for as long as this plan waits**, and that is a defect
standing open by choice, not an oversight. If the shape decision stalls, 3a is
the piece to pull forward on its own — it touches `/`'s existing cards and
nothing either reading surface introduces.

### Sequencing, to the extent it can be known yet

**3a is independent of the other two under any shape** — it touches `/`'s
existing cards, and neither reading surface introduces or removes them.

Between 3d and 1g there was an edge, and it belongs to the last position rather
than to the plan: under the two-page split, `/` carries a door to `/recaps` and
3d is what makes that door lead somewhere worth landing on, so 3d comes first. A
different shape may not have that edge at all, or may reverse it. Recorded so it
is not rediscovered, not so it is obeyed.

**Navigation is where the shape question is sharpest**, and it is why the four
pieces belong in one plan rather than four. A `<nav>` is a list of the site's
destinations, so it cannot be designed before the destinations are known — and
two of the candidate destinations are the reading surfaces 1g and 3d would
create. Building the nav first means guessing at them; building it last means
the reading surfaces each invent their own way of being reached. That is the
argument for settling the shape once, across all four, rather than four times.

## Design

**The design is a frame plus one open question, and saying so is the design.**
Plan 138 left a run of presentation positions that
[the case](#where-the-thinking-had-got-to-which-is-not-the-same-as-decided)
records as input rather than constraint. Writing them here as settled would
answer the question this plan exists to ask, so what this section fixes is the
part that binds *any* shape, and Stage A settles the rest.

### What binds any shape

**The CSP does not reopen.** The deployed policy is `img-src 'self' data:`,
verified against the live site on 2026-09-03, and it is the one constraint that
a working-looking page can violate silently. Every preview image is fetched
once, committed under `ops/static_ops/`, and served fingerprinted from this
origin under the existing one-year `immutable` policy. **The page that links the
articles must not be the page that reopens the CSP.**

**A weight budget, because Plan 138 Stage 3b just spent one.** 3b removed a
41.7 MB hero video on the argument that the page led with its weakest asset.
Four uncompressed previews would quietly hand a fraction of that weight back.
The budget is **150 KB per preview and 600 KB for the set**, in a modern format
with dimensions on the element so cards do not reflow on load. An article whose
image cannot meet that is listed without one — the card degrades, not the page.

**A public route costs a fixed list, and none of it needs inventing.** Stage 2
built the pattern and the `Caddyfile` repeats it six times: a handler in
`ops/routers/public.py`, an entry in the `paths` list beside it, a `handle`
block importing `public_response_policy` and `public_document_cache`, a row in
the external route matrix, and rows in **both** of
[`PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md)'s tables — the route contract and
the destination inventory. The second is the one that gets forgotten, and
forgetting it is how `/recaps` shipped with a canonical route, a sitemap entry
and no inbound link, in that order.

**Everything renders with JavaScript disabled at 360 px with no horizontal
overflow**, and no markup skips a heading level.

### The corpus rule, and the one field that can break it

Cards carry only the immutable facts of a published artifact: title,
publication date, URL with the per-session `trackingId`/`lipi` parameters
stripped, preview image, and a snippet. The snippet is the field that can
violate the rule while looking like it complies:

> **A snippet says what the article is about. It never says what the system is,
> does, or currently has.** If a repository change could make the snippet false,
> it is an annotation and belongs nowhere in the list.

*"How I learned my cost model was measuring the wrong noun"* describes the
article and is true for as long as the article exists. *"Explains how our
compression works"* describes the tree, and goes false the next time storage
changes with nothing to say so. Both fit the same slot and read the same way to
a reviewer who is not looking for the difference.

**Snippets are hand-written, and the cost is stated rather than waved past:** no
test can distinguish those two sentences, so the snippet is held by the add-time
gate in Stage C and by nothing else. If the call is wrong the recovery is four
snippets rewritten by hand, with no mechanism to unbuild.

### Two truth-contract requirements that outlive any layout

- **A recap rendered in full states its week, prominently, at the top.** 11 of
  31 weeks hold no commits and unpublished weeks are skipped, so the newest
  published recap can be weeks behind today. A full-bleed article that does not
  say which week it is reads as "here is where things stand," which it is not.
- **Outbound links are visibly outbound.** A recap link stays on this site; an
  article link leaves it. A list that flattens an anchor, a page and a
  third-party URL into one undifferentiated set is a small lie of omission.

### What Stage A settles

1. **How many reading surfaces there are** — specifically, whether the planning
   documents join the articles and the recaps as a third published corpus. This
   is the question that reopens the generator: planning documents *do* have a
   repository source, unlike the four hand-written articles.
2. **What `/` holds to reach them**, and how many doors that is.
3. **Whether there is a `<nav>`**, or the landing page carries the doors itself.

### Rejected, and why — carried forward so they are not retried

| Rejected | Why |
|---|---|
| **A per-article annotation** saying how each article relates to the tree today | A second, decaying copy of a reconciliation the record already holds properly. Every repository change would silently falsify it |
| **Snippets lifted mechanically** from each article's own subtitle or lede | Verifiable, and holdable by a test the way the date is — but the platform's ledes are weak and this is a portfolio surface whose value is the author's register. It spends the asset to buy the test |
| **The articles listed inline on `/`**, under "inline what is small and finite, index what grows" | Withdrawn 2026-09-03. Pairing a page you read against four links inlined somewhere on `/` is one destination and one appetiser, not one section with two destinations |
| **Building the navigation first** | A `<nav>` is a list of destinations, and two candidate destinations do not exist until Stage D |
| **Doing the semantic-controls work first** | The maintainer's priority call, 2026-09-04. Its cost is named in [the case](#3a-is-here-on-purpose-and-it-is-last-on-purpose): a live public page stays keyboard-inoperable while this plan waits |

## Stages

**`Order` is numbered and rewritten freely; `Stage` is lettered and never
changes.** The two agree today because all six were thought of in one sitting; a
stage discovered later takes `G` and slots into the order wherever it belongs.

Estimates are recorded per issue rather than per stage, because that is the
grain they were given at.

| Order | Stage | What it delivers | State | Issue | Estimate |
|---:|:---:|---|---|---|---:|
| 1 | [**A**](#stage-a) | the shape decision | `next` | CAR-84 | 1 |
| 2 | [**B**](#stage-b) | recap presentation | — | CAR-85 | 2, for B+C+D |
| 3 | [**C**](#stage-c) | the corpus and the add-an-article gate | — | CAR-85 | — |
| 4 | [**D**](#stage-d) | `/writings` as a public route | — | CAR-85 | — |
| 5 | [**E**](#stage-e) | navigation, and gap P1 | — | CAR-86 | 1, for E+F |
| 6 | [**F**](#stage-f) | semantic card controls | — | CAR-86 | — |

**CAR-85 closes on a deploy, not on merge** — Stage D's exit needs `/writings`
live behind Caddy. CAR-84 and CAR-86 verify locally.

### Stage A

**Settle the site's shape.** Answer the three questions above, write the answer
into `## Design` with the alternatives rejected on the way, and carry it into
[`PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md)'s destination inventory as intent.
No code.

**Exit:** every destination in the inventory has a decided `Linked from` naming
the stage that will provide it — no cell left reading `nothing` or `not yet
built` without an owner; and the planning-documents question is answered yes or
no with its reason recorded.

### Stage B

**Recap presentation.** Build what Stage A decided for `/recaps`, which today is
a bare list of twenty links a reader has to gamble on. Changes `render_index` in
`scripts/build_public_recaps.py` rather than any template.

**Exit:** `/recaps` renders per Stage A's decision; if it renders a week in
full, that week is stated prominently at the top and `rel=canonical` points at
the recap's own page, with the sitemap gaining no second entry for the copy;
`--check` covers the new output the way it covers everything else the generator
writes; the page renders with JavaScript disabled and no horizontal overflow at
360 px.

### Stage C

**The corpus, and adding an article as a procedure.** A committed data file read
at render time — no generator, because the four hand-written articles have no
repository source to project. The weight of the stage is the gate:
`scripts/public_surface_gate.py` gains a third gated path, and the direction
differs from the two it has. Today's gate asks *surface → tree*, "is this claim
still true?" The corpus gate asks *article → surfaces*, twice: does this
contradict them, and should they have taken something from it? Same hook, same
digest stamp, different questions — so a second mode, not a wider glob.

**Exit:** an entry without a publication date fails a test; no linked URL
carries the per-session `trackingId`/`lipi` parameters; and adding an entry
without the two-way reconciliation having run against that exact staged content
is blocked by the hook rather than by memory.

### Stage D

**`/writings` as a public route.** The route, the cards, the self-hosted
previews, and whatever door or doors Stage A put on `/`. This is the stage that
pays the public-route contract in full, and the first authored — rather than
generated — public page this work produces, which puts it inside
`public_surface_gate.py`'s scope.

**Exit:** `/writings` serves 200 with the public response policy, appears in the
sitemap and in both of `PUBLIC_SURFACE.md`'s tables, and has a row in the
external route matrix; every card renders its date, its snippet and the
point-in-time framing, and carries nothing beyond the immutable facts; outbound
links are visibly outbound; every preview is served same-origin inside the
weight budget **with the CSP unchanged from what Stage 3c deployed, asserted** —
a widened `img-src` is the one regression here that still looks like a working
page.

**Gate 1g's demonstration half comes across owed, not met.** It was written as
"demonstrated by adding Article D through the procedure rather than by hand",
and Article D is planned, not yet written. This stage ships the mechanism, and
the initial commit of the data file proves the gate fires and clears. It does
**not** prove the reconciliation caught anything, because articles A, B and C
were reconciled by hand in Plan 138 Stage 1f. Recording it as owed is the point:
a gate closed on entries that were already reconciled would be the exact "check
you must remember" that Stage 1c argued against.

### Stage E

**Navigation, and the gap that makes it more than a convenience.** Build what
Stage A decided for getting between destinations. Gap **P1** is the reason this
is not cosmetic: `/dashboard` is linked from no public surface, so a visitor who
requests access, is granted a role, and returns has no path to the thing they
were granted.

**It is fifth on purpose, and the cost is stated.** A `<nav>` is a list of
destinations and two of them do not exist until Stage D, so building it earlier
means guessing. What that buys in coherence it pays for in exposure: **P1 stays
open longest of the three gaps**, and it is the one about reaching the product
rather than reaching content.

**Exit:** `/dashboard` is reachable from a public surface by the treatment Stage
A decided; every destination in the inventory has a real inbound link or a
recorded reason it has none; P1 is closed in `PUBLIC_SURFACE.md` rather than
restated.

### Stage F

**Semantic card controls.** The landing page's eighteen service and highlight
cards are clickable `<div>` elements driven by two near-identical toggle blocks
in `ops/static_ops/info.js`. They become real controls — buttons with associated
panels, or native `<details>`/`<summary>`.

Two items from the original stage are already retired and do not come with it:
Plan 138 Stage 1b restored the heading outline and gave the pipeline diagram
non-colour encoding, both held by tests, and Stage 3b retired the
reduced-motion defect when the hero video left the page.

**Exit:** every card is focusable, announced, and operable without a mouse;
`aria-expanded` and `aria-controls` are present on every custom control that
remains; active state is carried by something other than colour — `aria-expanded`
carries it for free, which is why the colour-only defect is fixed here rather
than separately; and all of it is held by tests rather than by inspection.
