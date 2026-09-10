# Plan 162 Stage AL — write the seam rules and seed their ledgers

**This prompt is the entry point. The contract carries the standard, the plan
carries the argument and the measurements; this carries the working
instructions.** Read what is named below and only what is named —
`tests/rules/test_testing_contract.py` is 6,000+ lines and you need four
functions from it.

| Read | Why |
|---|---|
| `docs/TESTING.md` **§How a service reaches another service** | The standard. The eight statements, the code meanings, the body kinds. If it and this prompt disagree, it wins |
| `docs/plans/plan_162_testing_census_and_restructure.md` **§Stage AL** | Every measurement below, with its reasoning and the rejected alternatives |
| `docs/TESTING.md` **§Specified here, not yet asserted** | The five rows you are converting into rules |
| `shared/db_vocabularies.py` | The precedent, whole. A declaration in Python, the migration still the owner, a pair of rules reading both ways |
| `tests/rules/test_the_artifact_declares_what_the_handler_returns.py` | Whole, 243 lines. You are rewriting its reader and its floor |
| `tests/rules/test_testing_contract.py` — `_codes_at_call_sites`, `_imported_helpers`, `_assert_exactly`, `test_every_route_declares_the_statuses_it_can_return` | The four you need. Do not read the file |
| `tests/rules/test_no_rule_guards_itself_with_a_guessed_number.py` | What a floor may say. Stage AK owns this and your floors must already satisfy it |

Branch from `master` at **`0dc736f`**. **Do not merge anywhere** — that is the
maintainer's decision and no document, this one included, can make it for them.

## The one-sentence version

Every rule about the seam between services keys on a *shape*, there is more
than one shape, so each reader measures what it recognises and passes — the
artifact rule judges **12 of 93** declarations while its floor asserts `==` and
goes green.

## What you are not doing

**No service behaviour changes.** Not `202`, not `429`, not `Retry-After`, not a
payload envelope. Those are recorded as rejected in §Stage AL and they change
what production sends to live callers.

**No conversion.** You are not converting 128 call sites or 81 handlers. You are
writing the rules that *measure* them and seeding the ledgers that hold them.
Draining is later work, by other sessions, with skills that do not exist yet.

## The move that makes this possible

**A ledger entry does not have to be a violation. It can be "not yet
converted."** That is what lets every rule be written and seeded before any
conversion, and it is why nothing here should go red. `_assert_exactly` asserts
the violation set *equals* the ledger in both directions, so a seeded ledger is
green, a new violation fails, and a repaired one fails until its entry is
deleted.

## Step 0 — reproduce the baseline before you change anything

**Do not write a reader before you have watched the current one measure 12%.**

```python
# PYTHONPATH=. python this
from tests.rules.test_the_artifact_declares_what_the_handler_returns import (
    _exit_codes, _handler_index, _handler_name, artifact_declarations,
)
indexes, judged, skipped = {}, 0, 0
for service, route, verb, op_id, declared in artifact_declarations():
    indexes.setdefault(service, _handler_index(service))
    handler = indexes[service].get(_handler_name(op_id, route, verb))
    if handler is None:
        continue
    codes, complete = _exit_codes(*handler)
    judged += bool(complete and codes)
    skipped += not (complete and codes)
print(judged, skipped)          # expect: 12 81
```

**You must see `12 81`.** If you see anything else, the tree has moved since
2026-09-10 and every number below needs re-measuring before you trust it.

## The numbers your readers must reproduce

A reader that finds fewer is a reader with a hole. Each is a check on your work,
not a target to code toward.

| Measurement | Value |
|---|---|
| declarations in `contracts/*.json` | **93** |
| judged by the artifact rule today | **12** |
| skipped, exit unreadable | **81** — 71 bare JSON returns, 25 non-JSON framework responses, 1 other |
| status codes written as a literal at a call site | **128** — ops 94, archiver 18, dbt_runner 9, shared 4, scraper 3 |
| modules calling a service we own | **17**, reaching **35** distinct endpoints |
| one service's code importing another's | **2** — `ops/coordination_drain.py` → `airflow.dags.coordination_contract`, `ops/coordination_release.py` → `container_health.expected` |
| HTTP response objects configured as bare mocks | **24** across 6 files, by `.json.return_value` |
| unshaped error responses | **76**, all in `ops` — 37 JSON, 28 HTML template, 11 other |

## Step 1 — the three independent rules

None needs a new convention. Write, run, seed, move its row out of *Specified
here* into the `Asserted by` table.

1. **No service imports another service's package.** Seed **2**. Both are a
   shared declaration misfiled into a service package, and §Stage AL says what
   each needs — one moves to `shared/`, one is closed by `ops` reading
   `maintenance-running-set.txt` instead. `shared/` is not a violation; that is
   both sides importing a common library.
2. **Every endpoint has a caller, or is declared externally reachable.**
   `tests/test_caddy_public_routes.py` already declares what a stranger may
   reach — point at it rather than writing a second list.
3. **No test fabricates a response object's behaviour.** The body half already
   exists (`test_no_mock_invents_a_service_response`). The gap is the object:
   `mocker.Mock().raise_for_status()` does not raise, which is why
   `tests/ops/test_coordination_drain.py::test_service_503_body_is_still_known_positive_evidence`
   passes while production returns `unknown`. Reproduce that before you write
   the rule — §Stage AL has the two-line repro.

## Step 2 — the declaration module

`shared/api_envelope.py`. **Data, not a refactor**: one class per declared
meaning, each carrying its status code as a literal *in that file and nowhere
else*. `container_health` cannot import `shared/`, so it gets a local copy and a
rule asserting the copy matches — the same constraint Stage AA already solved
for `api_models.py`, designed for rather than discovered.

The meanings come from `docs/TESTING.md`'s code table. **`Busy` and
`DatabaseUnavailable` are both 503 and must be different classes** — that
distinction is the entire point of the stage.

## Step 3 — the coupled pair, in one commit

**These two land together or the suite is genuinely red rather than seeded.**
This was measured: converting one service fixed the first and broke the second.

- **Rewrite `_exit_codes`** so a `return <Model>(...)` credits the decorator's
  success code, a non-JSON framework response credits it too, and a raised
  declared refusal resolves through the envelope module. Keep the rule that one
  unreadable exit disqualifies the handler.
- **Change `_codes_at_call_sites`** to resolve declared refusals as well as
  call-site literals. Its docstring refuses a class *inventory* and it is right
  to — resolve the member from the envelope module, which is a lookup, not a
  list. This is `test_no_module_retypes_a_database_vocabulary_it_could_import`'s
  shape exactly.

Both land **before any handler is converted**, when every handler still looks the
way both readers expect. Then:

- **Seed "every handler's exits are readable" at 81.**
- **Seed "no route retypes a status the declaration names" at 128.**

## Step 4 — the meaning rule

Statement 7, and the only one with no existing rule at all. Every declared
response's meaning comes from the envelope module, compared in both directions.
Seed with the live defects §Stage AL names: 29 `ops` 503s declaring `"Database
unavailable."` against five real meanings, and four `/ready` 503s declaring `"A
dependency this service needs is not reachable."` for *busy*.

## What every rule owes, without exception

1. **It lives in `tests/rules/`.** Membership is the filesystem and three
   obligations follow from the directory —
   `test_every_test_in_the_rules_directory_is_named_in_the_contract` will fail
   you otherwise.
2. **A floor that is a derived equality or a bare non-emptiness. Never a chosen
   number.** Stage AK's standard, and your floors must satisfy it on arrival
   rather than joining its ledger of 24.

   **And a floor can be exact about the wrong thing.** The artifact rule's floor
   asserts every declaration *resolves* to a handler — true, derived,
   unguessable, and it passes while 81 handlers go unread. Ask what your floor
   would fail to notice.
3. **A mutation** (Stage AF). A rule nobody has watched fail is a rule nobody
   knows works.
4. **A row in `docs/TESTING.md`'s `Asserted by` table**, moved out of *Specified
   here, not yet asserted*. That column is read by
   `test_every_asserted_rule_names_a_real_test`.
5. **Ledger entries keyed on file-plus-handler or file-plus-expression, never a
   line number.** `GUESSED_BOUND_WAIVERS` says why: every edit above one rots
   it, and a rotted waiver either grandfathers something nobody chose or
   silently stops covering what it named.

## The failure mode to be afraid of

**Not redness. Silence.** Every reader defect this repository has found moved
the same number in the same direction — *"fewer routes examined, more codes
credited, a healthier looking result. None of those made a test go red."* Ten
new readers is ten chances at that, which is what the table of numbers above and
the floor obligation exist to close.

## Verifying

- `PYTHONPATH=. python -m pytest tests/rules -q -m "not integration"` — green,
  with ledgers seeded rather than violations unwaived.
- `PYTHONPATH=. python -m pytest tests/ -q -m "not integration"` — was **4051
  passed** at `0dc736f`.
- `PYTHONPATH=. python scripts/generate_service_contracts.py --check` — must
  exit 0 and the artifact must not move. **If a contract changes, you changed
  service behaviour**, which is out of scope.

Commit through the **`commit-plan-attribution`** skill; a `PreToolUse` hook
blocks `git commit` otherwise. Record what you measured in
`plan_162 §Record` via the **`note-evidence`** skill as you go, not at the end.
