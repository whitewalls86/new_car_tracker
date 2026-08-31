# Plan 160: The Promtail Contract Checker Cannot Say "I Don't Know"

## Status

**Build order — ready to start.** Split out of
[Plan 139](plan_139_test_suite_maintenance.md) Stage G on 2026-08-30, after a
fourth occurrence produced two runs of the same branch that disagreed with each
other. Stage G is removed from Plan 139 and its evidence is folded in below.

Priority **85**. Effort **XS**, plus a local reproduction that must come first.

**This is an instrument fault, not a test failure.** Nothing in `promtail/`,
the fixture corpus, or the checker has been wrong on any of the four
occurrences. The checker reports a verdict about the log contract when what it
actually has is an incomplete observation, and it has no way to say so.

---

## What the checker is for

Three artifacts describe one thing, and the corpus is the contract between them:

| Artifact | What it is | Checked by |
|---|---|---|
| `tests/fixtures/observability/plan_141_log_contract.json` | 22 fixture lines with expected outcomes — retained/dropped, `level`, `logger`, `status`, `drop_reason` | the contract itself |
| `classify_line()` | a Python **reimplementation** of Promtail's pipeline semantics | `TestStructuredLogContract` in `tests/test_observability_config.py` — fast, deterministic, in the unit suite |
| `promtail/promtail.yml` + the real Go binary | what actually runs in production | `scripts/verify_promtail_contract.py` — the CI job that flakes |

The dashboards and alert selectors are built on the Python model, so the model
being right is load-bearing. `ci.yml:98-101` states the job's purpose exactly:

> Syntax validation proves the config loads, not that it classifies. The unit
> tests only ever ask the Python model. This replays the fixture corpus through
> the real Go pipeline, which is the only thing that catches the model drifting
> away from what production actually does.

**That is an oracle test, and it is the right thing to have.** A hand-written
model of another implementation's regex, JSON and labeldrop semantics will
drift, and nothing else in the repo would notice. This plan does not question
the check's existence. It fixes the instrument.

## The defect

**Absence is overloaded.** `_run()` returns a dict keyed by line text, and
`main()` scores anything missing as a policy decision:

```python
if expected["retained"] and labels is None:
    failures.append(f"{case['name']}: corpus says retained, Promtail dropped it")
```

"Promtail dropped this by policy" and "I failed to observe this" produce the
**identical** signal. The checker has three possible states in reality —
retained, dropped, and *not observed* — and only encodes two.

Everything else in this plan follows from that. The race below is why the third
state occurs; the missing verdict is why it is indistinguishable from a
finding.

### Why this is worse than an ordinary flaky test

Plan 139 Stage E's asymmetry applies directly: a false negative costs time, a
false positive **manufactures evidence**. During Plan 141's Stage 4 soak, a
false "Promtail dropped it" is indistinguishable from a real contract
violation, so the failure is not a re-run — it is evidence that cannot be
trusted in either direction.

It is also why narrowing *when* the job runs is not a fix and was rejected on
2026-08-30. Today the noise is legible as noise because the pull request
plainly could not have affected the log contract. Scope the job to only the
files it actually reads and the next false failure lands on a change that
*did* touch `promtail.yml` — where noise and regression look the same, and the
only disambiguating signal has been removed. **Reducing exposure to an
instrument that cannot say "I don't know" makes its output more dangerous, not
less.**

## Evidence — four occurrences

| Date | Change under test | Mismatch reported |
|---|---|---|
| 2026-08-25 | "nothing in `promtail/`, the corpus, or the script itself" | `airflow_warning` |
| 2026-08-26 | Plan 142 host-maintenance client and tests ([run 32979183839](https://github.com/whitewalls86/new_car_tracker/actions/runs/32979183839)) | `oauth_lifecycle_severity_wins_over_status` |
| 2026-08-30 | Plan 134, deleting two Airflow DAGs ([run 33341201637](https://github.com/whitewalls86/new_car_tracker/actions/runs/33341201637)) | `airflow_warning` **and** `oauth_upstream_failure` |
| 2026-08-30 | the same branch, one commit later ([run 33344258643](https://github.com/whitewalls86/new_car_tracker/actions/runs/33344258643)) | `oauth_upstream_failure` only |

**The last two are the decisive pair.** Same branch, same four inputs, nothing
changed that the checker reads — and `airflow_warning` was "dropped" in one run
and retained in the next. No configuration change can produce that. The
checker's verdict is not a function of its inputs.

None of the four occurrences touched any file the checker reads. Its complete
input set is:

- `promtail/promtail.yml`
- `tests/fixtures/observability/plan_141_log_contract.json`
- `scripts/verify_promtail_contract.py`
- `docker-compose.yml`, for the image pin

## The mechanism, corrected

Plan 139 Stage G attributed the loss to Promtail exiting before flushing, and
reasoned from a batch where "exactly one went missing." The 2026-08-30 runs
refine that, and one earlier hypothesis in this plan's own history was wrong
and is recorded here so nobody re-derives it.

**It is not tail truncation.** The missing lines are mid-batch:
`airflow_warning` is line 2 of the 4-line `airflow-scheduler/container_stdout`
batch, and `oauth_upstream_failure` is line 3 of 7 for `oauth2-proxy`, with the
later lines present in both cases. A process that exits early loses its *tail*.

**It is not output interleaving either.** Measured locally on 2026-08-30
against `grafana/promtail:3.5.8`: Promtail writes its 15-line client-config
banner **and** the dry-run entries to stdout, and **stderr is empty**. There is
no second stream to interleave with. The banner lines simply do not match
`_ENTRY` and are skipped.

**What is actually happening.** Promtail is a streaming agent, not a filter. It
reads a source, batches entries, and ships them; `-dry-run` swaps the network
client for one that prints, and `-stdin` makes it read to EOF and shut down.
Entries still moving through that pipeline when shutdown wins are never written
to stdout at all. The loss is arbitrary rather than positional because the
pipeline is concurrent.

> **Corrected by Stage 0.** This paragraph originally ended "and it does not
> reproduce on an unloaded machine because nothing delays the entries long
> enough to lose the race." That is wrong. It reproduced on the *first*
> unloaded run and in 12 of 20, and adding load lowered the rate rather than
> raising it. The window is opened by Promtail's own shutdown path, not by
> host contention; contention was a plausible amplifier and turned out not to
> be the cause. Nothing else in the mechanism changes — and the fix does not
> depend on the amplifier, only on where the waiting happens.

### Why "wait longer" does not work, and where the waiting belongs

The natural fix is to wait for the output. It does nothing, and the reason is
worth stating because it is counterintuitive:

```python
completed = subprocess.run([...], input="\n".join(lines) + "\n", capture_output=True)
```

`subprocess.run` already blocks until Promtail **exits** and reads its stdout to
EOF. When that call returns, the process is dead, the pipe is closed and
drained, and we hold every byte Promtail ever wrote. There is nothing left to
wait for. The lines were never written; they died inside Promtail.

**But `input=` writes the lines and immediately closes stdin**, and `-stdin`
mode treats EOF as "shut down now." We hand Promtail its input and, in the same
breath, tell it to exit. We are creating the race ourselves, at maximum
pressure.

So the waiting is right — it belongs **before EOF, not after exit**. Hold stdin
open until the output has arrived, and there is nothing in flight to lose when
the process is finally allowed to shut down.

## Stages

### Stage 0 — Reproduce it locally (required, before any fix)

**A fix for a race that cannot be observed failing is a fix that cannot be
verified.** Stage G recorded the checker passing 10/10 locally, and a fast
machine never loses the race, so the obvious reading is that this can only be
proven over days of CI. That reading is wrong: the race is load-dependent, so
load the machine.

Run the checker under contention — a parallel `docker build`, several
concurrent invocations of the checker itself, or a CPU load generator — and
keep going until the current script reports a mismatch on unmodified inputs.

Record in this document, as *Evidence — Stage 0*: what load reproduced it, how
many runs it took, and which fixtures went missing. **If it cannot be
reproduced locally under any load, stop and say so** — the mechanism above is
then not fully understood, and Stage 1 would be a guess dressed as a fix.

This stage touches no code.

#### Evidence — Stage 0

**Reproduced on the first run, with no load at all.** The premise that the race
needs a loaded machine is wrong, and it is worth correcting because it is what
kept this filed as CI-only for a week. Stage G's 10/10 clean local runs were
not measuring what they appeared to measure.

Environment: the maintainer's macOS host, `arm64`, 8 cores, Docker Desktop
29.5.2, `grafana/promtail:3.5.8` pulled locally. The script needs `TMPDIR` on a
Docker-shared path — `TMPDIR="$HOME/.plan160-tmp"` — because `docker run -v`
cannot mount `/var/folders/...`. That is the environment limitation the prompt
warns about, not the defect, and the script is otherwise unmodified.

```bash
mkdir -p "$HOME/.plan160-tmp"
export TMPDIR="$HOME/.plan160-tmp"
for i in $(seq 20); do python scripts/verify_promtail_contract.py; done
```

**12 of 20 sequential, unloaded runs reported a mismatch**, and the missing set
varied run to run on byte-identical inputs:

| Runs reporting it | Fixture reported as "Promtail dropped it" | Batch |
|---:|---|---|
| 4 | `airflow_critical` | `airflow-apiserver` (4 lines, 2 retained) |
| 4 | `application_structured_new_shape` | `scraper` (1 line, 1 retained) |
| 2 | `application_missing_logger` | `ops` (2 lines, 2 retained) |
| 1 | `oauth_interactive_redirect` | `oauth2-proxy` (7 lines, 6 retained) |
| 1 | `oauth_lifecycle_severity_wins_over_status` | `oauth2-proxy` |
| 1 | `airflow_task_logger_error` | `airflow-scheduler` (4 lines, 2 retained) |

Two runs reported two fixtures at once; eight were clean.

**`application_structured_new_shape` is the finding that settles the
mechanism.** It is the *only* line in the `scraper` batch — one line in, one
entry expected. A single-line batch losing its single entry cannot be tail
truncation, cannot be positional, and cannot be a classification difference.
The line was never written to stdout at all, which is exactly what the plan's
corrected mechanism predicts.

Adding load did **not** raise the rate: with eight CPU burners saturating all
cores, 3 of 10 runs failed. The contention appears to slow Promtail's shutdown
path as much as its pipeline, so the plain sequential loop is the reproduction
to re-run. The mismatch rate is far higher here than in CI, which is consistent
with `arm64` emulation overhead in Docker Desktop's VM widening the window.

**Confirming the fix's premise before writing it.** A separate probe held stdin
open after writing the 7-line `oauth2-proxy` batch: all 6 expected entries
arrived at **t+0.51s, with stdin still open**. Promtail does not need EOF to
emit entries, so waiting for a count before EOF is a real completion signal
rather than a hope.

### Stage 1 — Hold stdin open, and wait for the expected count

Two changes in `scripts/verify_promtail_contract.py`, both inside `_run()` and
its caller.

1. **Replace `subprocess.run(input=...)` with `Popen`.** Write the lines, flush,
   and leave stdin **open**. Read stdout incrementally until the expected number
   of entries has arrived, then close stdin and let the process exit.

2. **Wait for a count, not a duration.** The corpus already declares how many
   entries each batch must produce, so the target is known before the run
   starts:

   | Batch | Lines | Expected retained |
   |---|---:|---:|
   | `airflow-apiserver` / `container_stdout` | 4 | 2 |
   | `airflow-dag-processor` / `container_stdout` | 3 | 1 |
   | `airflow-scheduler` / `container_stdout` | 4 | 2 |
   | `oauth2-proxy` / `container_stdout` | 7 | 6 |
   | `ops` / `application_file` | 2 | 2 |
   | `processing` / `application_file` | 1 | 0 |
   | `scraper` / `application_file` | 1 | 1 |

   A fixed `sleep` would mostly work and would still be a guess. Waiting for the
   count is a **positive** completion signal rather than an inference from
   silence.

Note the `processing` batch expects **zero** retained entries, so "wait until
the expected count arrives" cannot be the only exit condition. That batch needs
the deadline path, and it is the case most likely to be missed.

### Stage 2 — Add the third verdict

Even with Stage 1, a deadline can expire. When it does, the checker must report
**inconclusive** — distinct from both pass and fail, and never counted as a
contract violation.

On an incomplete batch, retry it up to three times. A real regression is
deterministic and loses the same line every attempt; a race is not. Report:

- **fail** — the same line is missing on every attempt, with the line named.
- **inconclusive** — the batch under-delivered and the missing set varied, or a
  retry recovered it. Print that a retry was needed, so the race stops being
  invisible and its true frequency starts being recorded.

**Stage 2 is what makes the check trustworthy, and it is separable from
Stage 1.** Stage 1 makes the race rare; Stage 2 makes the remaining cases
legible. Shipping Stage 1 alone leaves an instrument that still cannot say "I
don't know" — quieter, and wrong in the same way.

### Stage 3 — Re-verify under the Stage 0 load

Re-run the Stage 0 reproduction against the fixed checker. The gate is that the
load which reliably produced a mismatch no longer produces a **failure**, and
that any inconclusive result names itself as one.

Record the before and after in this document. This is the stage that turns the
fix from plausible into demonstrated, and it is the reason Stage 0 is
mandatory.

#### Evidence — Stage 3

The Stage 0 loop, re-run verbatim against the fixed checker on the same
machine:

| | Failures | Inconclusive | Clean |
|---|---:|---:|---:|
| **Before** — 20 unloaded runs | **12** | n/a — the verdict did not exist | 8 |
| **After** — 20 unloaded runs | **0** | 1 | 19 |

The one incomplete observation named itself, and recovered on its retry:

```
INCONCLUSIVE: 1 entry the checker could not observe. This is an incomplete
observation, not a contract violation:
  - oauth2-proxy/oauth_auth_failure: not observed on 1 of 2 attempts, retained
    on 1 -- the checker lost it, the pipeline did not drop it
```

That run exited **0**. Under the old checker the same event would have been
`oauth_auth_failure: corpus says retained, Promtail dropped it` and a red
build — which is the whole defect, observed being fixed.

**A genuine regression still fails.** In a scratch copy of `promtail.yml` the
`ops` drop selector was widened from `{service="ops", level=""}` to
`{service="ops", level="INFO"}`, so two lines the corpus says are retained are
really dropped. The checker exited **1** and named both, and — the part that
matters — classified them as deterministic rather than racy:

```
2 contract mismatch(es):
  - application_old_shape: corpus says retained, Promtail dropped it on all 3 attempts
  - application_missing_logger: corpus says retained, Promtail dropped it on all 3 attempts
```

Cost: a clean run went from ~1.6s to ~7s. Most of that is the `processing`
batch riding its 3s grace, since a batch expecting zero entries has no positive
signal to wait for. Against a job that already pulls and starts a container per
batch, this is not worth optimising.

**Inconclusive exits 0 deliberately.** A non-zero code would leave exactly the
red build the plan set out to remove. The signal is the named stderr block, so
the race's real frequency starts being recorded in CI logs — on this machine it
is now roughly 1 run in 20, down from 12.

## Files

| File | Change |
|---|---|
| `scripts/verify_promtail_contract.py` | `_run()` holds stdin open and reads to an expected count; `main()` gains the inconclusive verdict and the retry |
| `tests/scripts/test_verify_promtail_contract.py` | **New.** The checker has no test today |
| `docs/plans/plan_160_promtail_contract_checker_reliability.md` | Stage 0 and Stage 3 evidence |

## Tests

The checker is invoked directly from `ci.yml:105` and has no test of its own,
so the parsing and verdict logic can be unit-tested without Docker by feeding
`_run()`'s output shape directly:

- A batch whose output contains every expected entry passes.
- A batch missing an entry on **every** attempt fails, and names the line.
- A batch missing an entry on one attempt and not the next is **inconclusive**,
  not a failure.
- A batch expecting zero retained entries (`processing`) terminates on the
  deadline rather than hanging.
- The client-config banner lines are ignored rather than parsed as entries.

## Out of scope

- **Narrowing when the CI job runs.** Considered and rejected on 2026-08-30 —
  see [Why this is worse than an ordinary flaky test](#why-this-is-worse-than-an-ordinary-flaky-test).
  Revisit only once the checker can report inconclusive, at which point it is a
  question about cost rather than trust.
- **Moving the check to a local pre-push run.** Discussed 2026-08-30. The
  instinct is sound and matches Plan 139's 11s pre-push convention, but the
  race is load-dependent, so a busy laptop reproduces it with *less* context to
  diagnose it — no run history, no second run to compare against. It also runs
  the reference implementation on arm64 while production and CI are amd64,
  which weakens an oracle whose entire job is fidelity to production. Worth
  revisiting as a fast path once Stage 2 lands, never as the only place it runs.
- **Replacing Promtail's dry-run with a real sink.** Pointing the client at a
  local HTTP receiver would use Promtail's designed delivery path — batching,
  retries, drain on `Stop()` — instead of a debug printer. It is the more
  thorough fix and roughly triple the work. If Stages 1–3 do not hold, this is
  the next thing to try.
- **Whether the corpus still describes production.** Nothing verifies that. The
  checker proves config-and-model agree about 22 fixed lines; every fixture
  would still pass if a service changed its log format tomorrow. That belongs to
  [Plan 141](plan_141_structured_log_ingestion_contract.md), and it is probably
  what people assume this check already does.
- **Migrating off Promtail.** Promtail is on a deprecation path in favour of
  Grafana Alloy. Confirm against current Grafana documentation before investing
  further in a bespoke harness — but the fix here is ~20 lines, which is cheap
  enough not to wait on that decision.

## Success criteria

1. The load that reproduced a mismatch in Stage 0 no longer produces a failure.
2. An incomplete observation reports as **inconclusive**, never as "Promtail
   dropped it".
3. A genuine contract regression — verified by deliberately breaking a stage in
   a scratch config — still fails, and names the line.
4. The checker has a test file, and it covers the zero-retained batch.
5. Stage 0 and Stage 3 evidence is recorded here, with the load described
   precisely enough to re-run.

## Intersections

### Plan 139 — test suite construction and maintenance

Stage G of Plan 139 was this plan's origin and has been removed from that
document. Plan 139 keeps the surrounding argument that made this legible: Stage
E's false-positive asymmetry, and Stage H's finding that a gating assertion the
author's own verification cannot reach is de-gated in practice.

### Plan 141 — structured log ingestion contract

Plan 141 owns what the log contract *says*; this plan owns whether the
instrument that checks it can be believed. The distinction is Stage G's and it
is the reason this is not filed under Plan 141.

### Plan 154 — container log coverage

Plan 154 admits new streams, which means new fixtures in the same corpus. Every
fixture added there inherits this checker's reliability, so landing Stages 1–2
first keeps that work from being judged by an instrument that cannot say "I
don't know".
