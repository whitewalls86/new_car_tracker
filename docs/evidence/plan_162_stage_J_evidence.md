# Plan 162 Stage J — mechanising the encoding-sensitive I/O guard

**Legacy:** Stage 6b · **Issue:** CAR-60 · **Closed:** 2026-09-01

The record entry this belongs to is [`plan_162` §Record](../plans/plan_162_testing_census_and_restructure.md#record), under Stage J. It carries the summary; the sections below are the detail.

---

#### The measurement that decided the design

`PLW1514` was the obvious answer and the stage began by sizing it. Measured on
this branch at `144db69`:

| | Sites |
|---|---|
| `PLW1514` (`--preview`, explicit selection) | **28** |
| `read_text`/`write_text` with no `encoding=` | **213** |
| Ruff's share of the class | **~13%** |

**The stage's brief recorded 22 and the number is 28.** The difference is not
drift in the repository — it is that the 22 was measured before Stage H merged.
The count is stated here as re-measured rather than carried forward, because a
figure quoted from a stale branch is exactly the kind of unchecked claim this
plan exists to stop.

Every one of the 28 is a directly-constructed receiver or a builtin `open`. The
shapes ruff never reports: **92 built with `/` from a fixture path** — the
idiom the defect used and nearly every fixture-writing test here uses — and
roughly 110 more on a plain name. Finding 3 of the stage's brief was correct
and, if anything, understated it.

#### The class was dormant, not live, and that changed the cost argument

The development machine is Windows with `cp1252` and UTF-8 mode off, which is
precisely the environment that exposes this. The suite on that machine, before
any change: **3401 passed in 36s.** All 213 sites were already there and not
one of them was failing.

That is the finding that ruled out the Windows runner. **A Windows job added
today would have gone green and caught nothing** — it only earns its cost when
a future commit puts a non-ASCII character through one of these calls. It bills
at twice the minutes of a Linux runner, it cannot run the Docker, dbt or
Postgres legs, so it would be a unit-only eleventh job, and
[PEP 686](https://peps.python.org/pep-0686/) is Final for **Python 3.15**,
where UTF-8 mode becomes the default and the class stops existing. The
repository is on 3.13 in all ten jobs. Paying a permanent recurring cost to
guard a class with a known expiry, against a job that catches nothing on the
day it lands, is the trade that was declined.

**This is a decision, not an omission**, and the thing it gives up is named in
success criterion 2: path separators, line endings, case-insensitive filename
collisions and locale-dependent collation stay invisible to CI.

#### Why the rule is a test and not a ruff setting

Ruff resolves a receiver by type. `Path("b.md").write_text(...)` is flagged;
`(tmp_path / "a.md").write_text(...)` is not, with or without a `Path`
annotation on the fixture. Ruff has no plugin interface, so a check that reads
these calls has to be Python, and it lives beside the route and mocker rules
because it is the same kind of rule.

**The two instruments were given the halves each reads correctly.** `PLW1514`
owns `open` and `tempfile.NamedTemporaryFile`, where type inference is the
right approach and a name-only rule would be wrong — `tarfile.open` and
`os.open` take no encoding and would be false positives. The new rule owns
`read_text` and `write_text`, which only `pathlib` defines, so the method name
is proof on its own and no inference is needed. No gap between them across
those two shapes, and no call reported twice.

**That last sentence was first written as "no gap between them" without
qualification, and it was wrong.** The two static instruments between them
cover `open`, `NamedTemporaryFile`, `read_text` and `write_text` — the shapes
somebody thought to name. They do not cover the encoding class, and the way
that was found is worth recording: this stage had already been committed when
PEP 597 was checked, and turning its `EncodingWarning` on found **21 more
sites in two shapes neither instrument could see at the time** —
`subprocess.run(text=True)` without an encoding, which decodes a child
process's output through the locale, and `logging.RotatingFileHandler`. Ten of
the 21 are production or scripts, including three in `dbt_runner/app.py`
capturing dbt's output and one in `archiver/processors/disk_usage.py`. Both
shapes are named by the static rule now, so the sentence is true again — but it
was bought rather than reasoned to, and the record says which.

The `RotatingFileHandler` instance mattered more than its count. It writes the
ops log that `ops/routers/admin.py` reads, and this stage had just pinned that
reader to an explicit UTF-8 — so the sweep had made the pair *inconsistent*
where it had previously been merely undefined. Fixing only what a static rule
can see is how that happens.

#### The runtime check that found them, and why it is not in CI

[PEP 597](https://peps.python.org/pep-0597/) is Final in Python 3.10 and adds
`EncodingWarning`, raised from inside CPython whenever a text operation falls
back to the locale encoding. Turning it on — `PYTHONWARNDEFAULTENCODING=1`,
with the warning as an error — is how the 21 sites above were found, after this
stage had already been committed. **It earned its place as a discovery tool and
was then deliberately not kept**, which is a distinction worth stating clearly
because the first instinct was to wire it into CI, and doing so failed twice in
a way that taught the actual lesson.

**It is an interpreter-wide flag, so it has no notion of whose code it is
judging.** Enabled in CI it measured dbt's and Airflow's own file handling
against this repository's policy. dbt is invoked in-process by
`tests/integration/dbt/real_build.py`, so `dbt.tracking`, `dbt.compilation` and
`dbt.parser.manifest` raised inside our pytest process; Airflow's config loader
did the same in the isolated venv job. Neither is our read passed downward —
both are third-party code doing its own I/O on its own files, which this plan
has no standing to fail a build over.

**The escape hatch made it worse rather than better.** Silencing a module by
name is the only lever the warnings machinery offers, and each ignore revealed
the next frame down the same call chain: ignoring `airflow.configuration`
surfaced stdlib `configparser`, one layer beneath it. Two CI rounds, each
~2.5 minutes, with no way to know how many remained — and no way to find out
locally, because that suite only exists inside a CI-only venv.

**And the attribution those ignores depend on is not reliable.** The same
`configparser.read()` with no encoding was blamed on **the calling file**
locally and on **`configparser.py:739`** in CI. So a module-scoped ignore added
for a library's sake can silence the identical defect in our own code, without
a trace. An exception list that cannot be trusted to mean what it says is worse
than no exception list, because it reads as coverage.

**The scope test settles it.** This stage exists because a test written on one
operating system behaves differently on another — *our* tests, *our* fixtures,
*our* subprocess calls. A guard that also arbitrates dbt's internals is
answering a question nobody asked, at the cost of an unreliable exception list
that fails open. So the two shapes it found are checked the same way everything
else here is: statically, over this repository's files, where ownership is not
in question and no ignore is needed.

**What that gives up, stated rather than glossed.** The static rule only sees
shapes someone has named, so the *next* unnamed shape will not be caught by
anything. That is a real loss and it is the price of not measuring other
people's code. `EncodingWarning` remains available as a developer tool for
exactly the job it did here — run it by hand when hunting for what no rule
names yet:

```
PYTHONWARNDEFAULTENCODING=1 python -m pytest -m "not integration" -W error::EncodingWarning
```

#### The exit criterion, demonstrated rather than asserted

The stage's second criterion asks for a mechanism that fails on
`(tmp_path / "a.md").write_text("—")` with no `encoding=`. Both tools were run
against that exact line:

| Tool | Result |
|---|---|
| `ruff --select PLW1514 --preview` | `All checks passed!` |
| `test_every_text_read_and_write_states_its_encoding` | **fails** |

That comparison is kept as an assertion, not a note.
`test_the_encoding_rule_sees_the_shape_ruff_cannot` pins all three receiver
shapes the repository writes and pins the correct calls as clean, so if this
rule ever narrows back to what ruff already sees, it fails instead of going
quiet. The detection was split into `_encoding_free_text_io` for no other
reason than to make that test possible: a structural check nothing exercises
reports a clean repository whether or not it still works.

#### What was swept, and why the sweep is safe rather than merely large

All **213** sites were fixed; none were waived. The waiver list stays at 56.
Waiving instead would have taken it to 269 and broken the one property the
plan's three waiver assertions exist to protect — that the list only shrinks.

**The sweep cannot change behaviour, and that is provable rather than hoped
for.** Every one of these calls already runs in Linux CI, where the default
encoding is UTF-8; writing `encoding="utf-8"` explicitly makes them do what
they were already doing there. It was verified from both ends: green on Linux
in CI, and green on the `cp1252` machine before (3401) and after (**3403**, the
two new tests) — the platform where a wrong encoding would have shown up
immediately.

The edit was applied by AST position rather than by regex, in bytes rather than
text. Both mattered: `col_offset` is a UTF-8 **byte** offset and this
repository's docstrings are full of em-dashes, so a character-indexed insert
would have landed in the wrong column on exactly the files this stage is about;
and the working tree is CRLF, so a `read_text`/`write_text` round-trip would
have rewritten every line ending in all 50 files. The diff was checked for
mixed endings afterwards and has none.

Twenty-four lines went over the 100-character limit once the keyword was added
and were wrapped — fifteen sharing one shape, nine individually.

The 21 `subprocess` and logging sites were swept the same way afterwards. With
those fixed, the rule that now covers all three shapes passes on an **empty
waiver list**, which is the check that the sweep and the rule agree.

#### What CI said, and what only CI could have said

Green on `c7d1d33`, run
[`33539915522`](https://github.com/whitewalls86/new_car_tracker/actions/runs/33539915522),
all ten jobs (`Documentation tests` skipped by design on a changeset that is not
docs-only). PR
[#332](https://github.com/whitewalls86/new_car_tracker/pull/332).

**It took three runs, and the two red ones are the evidence for the design.**
The local suite could not have produced either: both failures were third-party
code running inside jobs that exist only in CI.

| Run | Head | Result |
|---|---|---|
| [`33537879926`](https://github.com/whitewalls86/new_car_tracker/actions/runs/33537879926) | `f9a702b` | 9/11 — dbt and Airflow jobs red |
| [`33538571583`](https://github.com/whitewalls86/new_car_tracker/actions/runs/33538571583) | `27288e6` | 10/11 — Airflow job red |
| [`33539915522`](https://github.com/whitewalls86/new_car_tracker/actions/runs/33539915522) | `c7d1d33` | **green** |

The first red run failed on dbt's own `dbt.tracking`, `dbt.compilation`,
`dbt.parser.manifest` and `dbt.utils.utils`, plus `airflow.configuration` — 32
occurrences of the latter. The second, after those were silenced by name,
failed on stdlib `configparser.py:739`: the layer beneath the module that had
just been ignored, reached through the same call chain. **The escape hatch was
uncovering offenders one frame at a time, with no way to see how many were
left**, because that suite runs in a venv built only by CI.

That is the run that ended the approach rather than the one that fixed it. Two
rounds of ~2.5 minutes each bought one fact worth more than a green build: a
guard that has to be told, module by module, whose code it is allowed to judge
is not measuring what this stage set out to measure.

**The third run is green because the question changed**, not because the last
module was found. The shapes are checked statically over this repository's
files, and CI never had to arbitrate dbt's file handling at all. Nothing in the
`ci.yml` diff survives; the only workflow change in the merged branch is none.

**The waiver list is unchanged at 56.** `ENCODING_WAIVERS` is empty and joins
`ALL_WAIVERS`, so the three assertions that keep the list honest now cover this
rule too: a waiver here that stopped describing a violation would fail, as would
one naming a missing gap entry or an archived owner.

#### What was deliberately not done

- **No Windows runner**, for the reasons recorded above. This is the stage's
  substantive decision and success criterion 2 now names what it costs.
- **`.open()` on a non-`pathlib` receiver is not checked by the new rule.**
  `tarfile.open`, `os.open` and `pyarrow`'s filesystem `open` share the name
  and take no encoding, so a name-only rule would report them and be wrong.
  Ruff's type inference covers the `open` family instead, which is the whole
  point of splitting the two.
- **`PYTHONUTF8` was not set anywhere.** It would make the class disappear on
  every machine that had it, but it is an interpreter start-up flag: a developer
  running `pytest` without it still diverges, so it moves the harness dependency
  rather than removing it. Explicit `encoding=` needs no environment to be
  correct. `PYTHONWARNDEFAULTENCODING` is a different proposition — it changes
  no behaviour, it only makes the fallback audible — and it was used once, by
  hand, rather than wired into CI.
- **Bytes-mode `subprocess` calls were not touched.** Only text mode qualifies,
  because a bytes-mode call has no encoding to state. A call that gains
  `text=True` later is caught by the rule the moment it does, without needing
  to be executed.
- **The 3.15 upgrade was not scheduled here.** PEP 686 will retire this class,
  but that is a version bump with its own consequences and it is not Plan 162's
  to make.
