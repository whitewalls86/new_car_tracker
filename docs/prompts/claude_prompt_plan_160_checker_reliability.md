# Plan 160 — make the Promtail contract checker able to say "I don't know"

**This prompt is the entry point. The plan carries the argument and the
evidence; this carries the working instructions.** Read what is named below and
only what is named — `test_observability_config.py` is 1,500+ lines and you need
one class from it.

| Read | Why |
|---|---|
| `docs/plans/plan_160_promtail_contract_checker_reliability.md` | The authority. If it and this prompt disagree, it wins |
| `scripts/verify_promtail_contract.py` | Whole, ~160 lines. The thing you are changing |
| `tests/fixtures/observability/plan_141_log_contract.json` | The corpus. 22 cases; you need the `expected.retained` field per case |
| `.github/workflows/ci.yml` **lines 82-105** | How the job runs, and the pinned image |
| `tests/test_observability_config.py` **`TestStructuredLogContract`** | The Python model asserted against the same corpus — the thing the checker is an oracle for |

Work on branch **`feature/plan-160-promtail-checker-reliability`**, branched
from `master` at `1d8b088`. The docs commit is already there. **Do not merge
the branch anywhere** — that is the maintainer's decision and no document, this
one included, can make it for them.

## The one-sentence version

The checker has three possible states in reality — retained, dropped, and *not
observed* — and encodes only two, so an entry lost to a shutdown race is
reported as a log-contract violation.

## Stage 0 is mandatory and comes first

**Do not write the fix before you have watched the bug happen.**

The race is load-dependent. Stage G of Plan 139 recorded the checker passing
10/10 locally and concluded it was CI-only; that conclusion is what kept this
unfixed for a week. A fast, idle machine never loses the race — so load the
machine:

```bash
# one suggested shape; any real contention will do
for i in $(seq 8); do docker build -q . >/dev/null 2>&1 & done
for i in $(seq 20); do python scripts/verify_promtail_contract.py || echo "REPRODUCED on run $i"; done
```

Vary the load until the **unmodified** script reports a mismatch on
**unmodified** inputs. Record in the plan document as *Evidence — Stage 0*:
the load that did it, how many runs it took, and which fixtures went missing.

**If you cannot reproduce it under any load, stop and report that.** The
mechanism in the plan is then not fully understood and Stage 1 would be a guess
wearing a fix's clothing. Say so plainly rather than proceeding.

### macOS note, if you are on the maintainer's machine

`docker run -v` cannot mount from `/var/folders/...`, which is what
`tempfile.TemporaryDirectory()` returns. The script fails there with
`Unable to parse config: ... is a directory` before it ever replays anything.
That is an environment limitation, **not** the bug you are hunting — write the
generated config somewhere Docker shares (under the repo, or `$HOME`) to get a
local run at all. Note also that a local image is `arm64` while CI and
production are `amd64`.

## Stage 1 — hold stdin open

The current call creates the race itself:

```python
completed = subprocess.run([...], input="\n".join(lines) + "\n", capture_output=True, check=True)
```

`input=` writes the lines and **immediately closes stdin**, and `-stdin` mode
treats EOF as "shut down now." Promtail is told to exit in the same breath it is
given its work.

Waiting *after* the call cannot help: `subprocess.run` already blocks until the
process exits and drains stdout, so by then the lines were never written at all.
The waiting has to happen **before EOF**.

Move to `Popen`: write, flush, leave stdin **open**, read stdout incrementally
until the expected number of entries has arrived, *then* close stdin.

**Wait for a count, not a duration.** The corpus declares the target per batch —
`sum(1 for c in group if c["expected"]["retained"])`. A `sleep()` would mostly
work and would still be a guess.

> **The trap:** `processing` / `application_file` expects **zero** retained
> entries. "Wait until the expected count arrives" cannot be the only exit
> condition or that batch hangs until the deadline every run. Handle it
> deliberately; it is the case most likely to be missed.

Also note Promtail prints a 15-line client-config banner to stdout before any
entry, and stderr is empty. `_ENTRY` already skips the banner — do not
"fix" that.

## Stage 2 — the third verdict

This is the stage that actually matters. Stage 1 makes the race rare; Stage 2
makes the remainder legible. **Shipping Stage 1 alone leaves an instrument that
still cannot say "I don't know," only quieter.**

On a batch that under-delivers, retry up to three times, then report:

- **fail** — the same line missing on every attempt. Name it.
- **inconclusive** — the missing set varied, or a retry recovered it. Distinct
  from failure, never counted as a contract violation, and it must print that a
  retry was needed so the race's real frequency starts being recorded.

A real regression is deterministic; a race is not. That difference is the whole
disambiguation.

## Stage 3 — prove it

Re-run Stage 0's load against the fixed checker. The gate: the load that
reliably produced a mismatch no longer produces a **failure**, and anything
incomplete names itself inconclusive. Record before and after in the plan.

Then verify the check still *works*: break a stage in a scratch copy of
`promtail.yml` deliberately, and confirm the checker fails and names the line.
A checker that never fails is not a fix.

## Tests

`tests/scripts/test_verify_promtail_contract.py` does not exist — the script is
invoked straight from `ci.yml:105`. Create it. The verdict logic can be tested
without Docker by driving it with recorded output shapes; see the plan's Tests
section for the five cases, including the zero-retained batch.

## What not to do

- **Do not narrow when the CI job runs.** Considered and rejected 2026-08-30,
  and the reasoning is in the plan: today the noise is legible *because* the PR
  was obviously unrelated. Scope the job tighter and the next false failure
  lands on a change that did touch `promtail.yml`, where noise and regression
  look identical.
- **Do not move it to a local-only pre-push run.** Same reason, plus the race is
  load-dependent, so a busy laptop reproduces it with less context to diagnose.
- **Do not fix it with a `sleep`.** It would probably pass CI and it would still
  be a guess. Wait for the count.
- **Do not rewrite the corpus or `promtail.yml`.** Neither has been wrong in any
  of the four occurrences. If you find yourself editing either, stop — you have
  diagnosed something else and should say so.
- **Do not touch the Python model.** Plan 141 owns the contract; this plan owns
  the instrument.

## When you are done

Report the Stage 0 load and repro count, the before/after from Stage 3, the
deliberate-breakage check, and the test file. Leave the branch checked out where
you found it, and leave merging to the maintainer.
