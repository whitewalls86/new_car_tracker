# The ideal state of every seam, and the rules that hold it there

Plan 162 working analysis, 2026-09-10 — the second half of
[the seam census](seam_rule_census.md). The census sorted what exists; this
specifies what each seam looks like at **grade 3** — one mechanism, truths
derived rather than compared, border guards keyed on single signatures —
and maps every current rule to *keep*, *upgrade*, or *retire-on-expiry*.

**The method this document assumes**, proven in miniature by Stage AL's
by-hand-callers rule: an ideal rule is writable and runnable *before* its
mechanism exists, because the ledger simply holds everything — the ledger
**is** the change census. What an ideal rule needs before it can be written
is the mechanism's *name and signature*, not its build. So the sequence is:
specify the mechanisms (this document) → stub them → land the ideal rules
and seed their ledgers → write the skills against the ideal corpus → convert
and drain, seam by seam, with scaffolding retiring on declared expiries.

**One suite, never two.** Ideal rules land beside the current ones. The
current heuristic readers stay through the interim because they are the only
thing checking *correctness* of the ledgered majority — an ideal rule knows
a route is unconverted; only the heuristic knows its declaration drifted.
Every scaffolding rule names the ledger whose emptying retires it, and the
meta-class gains one rule to enforce that (below), so retirement is forced
the way waiver-deletion already is, not remembered.

---

## Seam 1 — code ↔ SQL engine. **Already ideal.**

The existence proof for grade 3–4 and the template the rest of this
document copies: one mechanism (statements in `*/sql/` files, one loader,
the execution recorder), border guards keyed on the statement shape itself,
declared residue (Spark fragments, `assert True`). **Every rule: keep.**
No mechanism work. The drain program should not touch this seam except to
cite it.

## Seam 2 — code ↔ owned vocabularies and shapes

**Ideal mechanism (mostly exists):** `shared/db_vocabularies.py` for tier 1;
for tier 2, the member lists in `tests/external_vocabulary_census.py` become
**generated** by the interrogation scripts that already ask the owners
(Airflow venv, installed curl_cffi, MinIO), gated like contracts. The
verdicts and `OUT_OF_SCOPE` reasons stay human — only the members generate.

**Ideal rules:** the Stage W pair, unchanged (they are already border
guards). The census hygiene rules (`entry-names-a-real-thing`) **retire**
into the generation gate when the member lists generate.

**Code change:** `schema.yml` declares `data_type` (G20), which closes the
relation-shape rule's declared type-blindness — the one upgrade this seam
has been waiting for.

## Seam 3 — dbt ↔ schema ↔ fixtures ↔ sources. **Ideal by delegation.**

Enforcement lives in dbt itself (`contract: enforced`), which is grade 3 by
construction. **Keep everything**; the `data_type` change above is the only
upgrade. No new mechanism.

## Seam 4 — service ↔ service over HTTP. **The core of the program.**

Three mechanisms, each thin, each usable by new code the day it lands:

**M1 — declarations derive from the envelope.** A helper in `shared/`
(final home with `api_envelope.py`):

```python
responses = refusals(Conflict, DatabaseUnavailable)
# -> {409: {"description": Conflict.meaning},
#     503: {"description": DatabaseUnavailable.meaning}}
```

A per-route refinement names its member — `NotFound.described("No user
with that id; nobody was revoked.")` — and the helper **refuses a
refinement on a code the envelope splits** (503 today), so
declaration↔meaning agreement for ambiguous codes is unconstructable-wrong
rather than compared. Body kinds ride the same helper family (`html_page()`,
`stream()`, `none()`), which is what drains the shape/kind ledger. The
handler's side of the same coin: **refusals are raised as members and
success is a returned model** — already the envelope's contract.

**M2 — the client seam.** `shared/service_routes.py`, **generated** from
`contracts/*.json` and gated like them: one member per operation, carrying
service, verb, path template, request-model name and declared codes. A
client in `shared/` that takes a route member and a request **model** (not
a dict), raises the typed refusal on a refusal code, and returns the parsed
response model — which makes status-read, outcome-kept, meaning-branching
and request-satisfaction structural in one move. The Airflow side gets a
**generated copy** under `airflow/plugins/` (both halves generated, so the
equality rule is a generation gate, not a hand-sync), and `post_json`
becomes the DAG client taking registry members. Tier 2: Airflow's own API
publishes OpenAPI; its one called endpoint gets a registry entry generated
from that, with the recorded difference in audience — a diff in ours says
*fix the service*, in theirs *they changed, fix our caller*.

**M3 — the double builder.** `tests/service_double.py`:

```python
service_double(package, verb, path, code, **overrides) -> requests.Response
```

A **real** `Response` — body from `service_response()` (so the contract),
status set, `raise_for_status`/`ok`/`.json()` all answering as production
would. The conftest fixtures hand out only these.

**The ideal rule set** (signature → ledger seed at landing):

| Guard | Signature | Seeds at |
|---|---|---|
| no hand-written refusal declaration | a `responses=` value that is not a call into the helper family | ~every route |
| no bare raise | `HTTPException` at any call site | ~48 sites |
| no retyped status | member-code literal at a call site (**exists**, keep) | 80 keys |
| no ad-hoc caller | owned host named outside the client (**exists** as by-hand rule; signature sharpens to "no `requests` import outside the client module" when M2 lands) | 17 modules |
| no dict payload | a non-model handed to the client | with M2 |
| no hand-built double | an HTTP seam configured except through `service_double()` (subsumes the `.json.return_value` signature) | ~50 sites |
| vocabulary boundary | declared code outside the standard's table (**exists**, keep) | 1 (`/admin` 307) |
| generation gates | contracts (**exists**), registry, envelope copies — generated, committed, diffed | — |
| FastAPI's injected 422 | (**exists**, keep — the framework writes it, no derivation removes it) | 2 permanent |
| tier 3 | the cars.com corpus rules (**exist**, keep — a recording is the only honest instrument) | — |

**Retirement map** (scaffolding → the ledger whose emptying retires it):

| Retires | When empty |
|---|---|
| `_exit_codes` heuristics + `test_every_handlers_exits_are_readable` | unreadable-exits (58) — exits become lookups |
| `test_every_route_declares_the_statuses_it_can_return`'s reader | hand-written-declarations ledger — the dict derives |
| `test_the_artifact_declares_no_code_its_handler_cannot_return` | both above — the chain handler→envelope→decorator→artifact closes by generation |
| the meaning rule's raise-site reader | unproven-meanings (28) + hand-written declarations — agreement structural |
| `test_every_response_declares_a_shape_or_a_kind`'s ledger half | bodyless (89) — the kind helpers declare it |
| status-read, outcome-discarded, DAG-status readers | by-hand callers (17) — the client is the reader |
| the four fabrication readers (bodies, codes, objects, + seam resolution) | hand-built doubles — the builder is the derivation |

What survives at grade 4: the border guards in the table, the generation
gates, the 422 rule, the corpus rules, and the caller floor (two
independent readers held equal). Roughly **24 rules become ~10**, and every
retirement is forced, not remembered.

**Drain items that are repairs, not conversions:** the four `/ready` 503s
declare `Busy` (artifact moves, wire does not), `ops`'s mismatched 503
meanings likewise, `/admin` 307→308 (wire — a deploy decision).

## Seam 5 — code ↔ object storage. **The mechanism must be designed.**

The invariant the ideal state must deliver, stated now so Stage AM designs
toward it rather than re-deriving it: **every address is computed from one
declaration** (`shared/lake_layout.py` — bucket names, prefixes, key
builders; Stage W's move for addresses, hand-written because we own the
layout and nothing can generate it), **every exchange is named** (writer,
reader, artifact — the census table of four), and **the bronze three-party
exchange gets an artifact** that the writer's key, the queue row and the
packfile sidecar are all compared against, because today the fallback that
would notice a loss is the one that exists to hide a rewrite. Ideal border
guard: no address-shaped literal outside the layout module — the SQL
detector's shape, pointed at keys. The S3 error-code strings join as this
seam's tier-2 vocabulary. **The manifest registry rules: keep** — that
artifact is already at grade 3. The rest is Stage AM's design work, with
this section as its exit's shape.

## Seam 6 — CI ↔ production runtime. **Already ideal.**

The resolved-config diff is the grade-3 move (nothing enumerates fields, so
no field is missed); the rest are cheap signatures with declared residue
(the four platform behaviours only a second platform can see). **Keep
everything.** No mechanism work.

## Seam 7 — config ↔ deployment. **Ideal-ish; declare the pattern, tightened.**

The pair-shaped, both-directions rules per artifact are intrinsic to config
having many files — the volume is not a smell. This seam owns the
"one file, two readers, no parser" pattern the client seam's Airflow copy
cites. **Keep everything**; no mechanism work beyond declaring the pattern.

**Tightened 2026-09-10, by Plan 162 Stage AK.** The pattern as first written
says a config artifact arrives with its pair — two readers, both directions.
It says nothing about what a *field* must look like for the pair to assert on
it exactly, and that omission is where this seam's rules are actually weak.

**A config artifact arrives with its pair, and every field its pair asserts
on is structured enough for the assertion to be exact.** A freeform prose
field admits exactly one exact claim: that it is present. Any stronger claim
about it is a heuristic wearing a number. Where a field must carry more than
presence, it carries **named parts**, and the readers assert on the parts.

**Measured, and it is not one instance.** Three artifacts pass through one
parser — `load_health_exemptions` reads `healthcheck-exemptions.txt`,
`maintenance-running-set.txt` and `deploy-followers.txt` — and each asserts
the same reason field differently: `len(reason) > 40` in the first two, the
identical constant written twice in two files, and no reason check at all in
the third. Across the wider suite the idiom appears **six times in five
files with three different constants** (`> 40` four times, `>= 40`, `>= 60`),
every one of them outside `tests/rules/` and therefore invisible to
`test_no_rule_guards_itself_with_a_guessed_number`, whose ledger reads that
directory alone.

**`deploy-followers.txt` is the existence proof already in the tree.** It
skips the length heuristic and asserts a required *part* instead —
`"docker restart" in entry` — with the reason recorded on the assertion: *"a
warning without a command is how this stayed unfixed for two days."* That is
the right instinct, applied to one entry of one artifact and never declared
for the seam. Declaring it is this seam's whole remaining job.

## Seam 8 — metrics and logs. **Grade 0 → 3 directly.**

The only seam that gets built from nothing, and the one that already cost
eight silent hours. **Mechanism:** `shared/metric_names.py` — every
`cartracker_*` name declared once; emitters import the member. **Ideal
rules,** the Stage W pair pointed at observability: no module emits a
metric name except from the declaration; every declared name is emitted
somewhere; every name a dashboard or alert rule queries is a declared
member, both directions — the configs are already parsed by
`test_observability_config.py`, so the comparator's reader half exists.
Logs: fold Plans 141/160's checker contract into this seam's section as its
second half; its shape is already right. **Ledger seeds at every emitted
name** (unmeasured — the census's first task in this seam is the count).

## The mocking cross-cut, restated once

One statement — *a test double derives from the owner artifact of the seam
it stands on* — with one builder per seam family: `service_double()` (HTTP),
`fixture_for()`/`produced_by()` (function producers, exist),
migration-applied fixtures (relations, exists as the rule's demanded shape).
`mocker`-everywhere and the fidelity plugin: keep, they are instruments.
The four HTTP fabrication readers retire per the seam-4 map; the statement
itself moves from four restatements to one section the instances cite.

## The meta-class: one addition

Everything keeps. One new rule, which is what makes this whole document
enforceable rather than aspirational: **every scaffolding rule names its
expiry ledger, and a scaffolding rule whose named ledger has emptied
fails until it is deleted** — the waiver-outlives-its-owner clause, applied
to rules themselves. With it, the retirement maps above are load-bearing;
without it they are prose, and this repository knows what happens to prose.

---

## What lands where, in order

1. **Mechanism stubs** — `refusals()` + kind helpers, `service_routes`
   generator + client + Airflow copy, `service_double()`,
   `metric_names.py`; `lake_layout.py` waits for Stage AM's design. Each is
   dozens of lines, usable by new code immediately.
2. **Ideal rules + ledgers** — the guards in the tables above, seeded at
   everything; the seeds are the change census. The expiry meta-rule lands
   with them.
3. **Skills** — rewritten against the ideal corpus (the three Stage AL
   drafts teach the interim convention and are superseded by this; that is
   why they were paused).
4. **Convert and drain, per seam** — HTTP first (largest ledgers, mechanisms
   cheapest), metrics second (smallest build, worst blind spot already
   realised), object storage behind Stage AM's design. Retirements fire
   automatically. Seam 7 is the exception to the ordering rather than to the
   sequence: its drain is six bounds across three artifacts, small enough to
   land with the ideal rules and ahead of the skills, which have to be
   written against a seam 7 whose pattern is already final.

This supersedes the drain-tail of Stage AL's scope, absorbs Stage AM's
exit shape, and reshapes Stage AH (skills against the ideal corpus, not
the interim one). Whether that is a restaging of Plan 162 or a successor
plan is a `plans`-skill decision; this document is written to be either's
design section. The `docs/TESTING.md` restructure then follows the same
spine — one section per seam: parties, owner + tier, statements, rules per
statement with keep/upgrade/expiry marked, ledgers, grade, skill — which is
what finally makes "is this rule redundant" a question with a mechanical
answer.
