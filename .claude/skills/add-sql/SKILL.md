---
name: add-sql
description: "Add a SQL statement to this repository so it lands to the contract on the first attempt — where the .sql file goes, how it is loaded, the testability seam it needs, and the Layer 2 test that discharges its coverage obligation. Use when writing a new query, moving an inline statement into a file, closing a G5 or G15 waiver, or adding SQL to a directory that holds none yet. This skill authors the file, the loader wiring and the test: it adds no waiver, exempts no root, and never satisfies a rule by narrowing what the rule measures."
---

# Adding SQL to this repository

Every production statement lives in a `.sql` file, is loaded rather than typed,
and executes against a real engine in CI. Three rules enforce that and they are
checked separately, so a statement can satisfy one and fail the others. This is
the order that gets all three in one pass.

**Read this before writing the file, not after the suite fails.** The
obligations below are each derivable by running `pytest
tests/test_testing_contract.py` and reading the failure, and that is how they
were learned. It costs about six suite runs. This page costs one.

## The five steps

### 1. Put the file where its owner lives

`<root>/sql/<verb>_<what>.sql`, one statement per file — `ops/sql/`,
`archiver/sql/`, `shared/sql/`, `scripts/sql/`. Statements for one consumer may
nest a directory deeper, as `archiver/sql/lake_snapshot_selectors/` does.

**`tests/` is exempt from the production corpus; nothing else is.** A statement
under `scripts/` is production SQL and owes everything below. Test SQL is a
different path with different rules — see `tests/sql_loader.py`.

### 2. Load it, never type it

```python
from shared.query_loader import load_query

SQL_DIR = Path(__file__).resolve().parent / "sql"
SELECT_THING = load_query(SQL_DIR, "select_thing")
```

`load_query` returns `SqlText`, a `str` subclass carrying the file it came
from. That origin is what the execution recorder credits, and it survives
`.format()`. Every driver still sees a `str`.

### 3. Give it a seam, or it cannot be tested

A statement that hardcodes a production path or schema cannot be pointed at a
fixture, so no Layer 2 test can execute it and the coverage gate will fail it.

Take the path as a defaulted keyword argument:

```python
def fetch_months(con, pattern, *, silver_path: str = SILVER_PATH):
    return con.execute(SELECT_THING.format(silver_path=silver_path), [pattern])
```

`read_parquet` and relation names take a literal, so they are interpolated;
values are bound with `?` or `%s`. Interpolating a *value* the driver would
have bound is the defect, not interpolation itself.

### 4. Write the Layer 2 test

**Layer 2 is `tests/integration/sql/` and `tests/integration/archiver/`, and
nowhere else.** `tests/integration/scripts/` is Layer 4; a test placed there
does not discharge the obligation, and the failure will name the `.sql` file
rather than the test, so the cause is not obvious.

The test must:

- **name the `.sql` stem** somewhere in the module — importing the loaded
  constant is the honest way, since it also proves the text under test is the
  file on disk
- **author no SQL of its own.** Build fixtures with `pyarrow`, a fixture
  helper, or a statement under `tests/sql/` — never a literal. The rule is
  mechanical, and following it is what keeps the recorder's attribution clean:
  the only statements the test executes are the production ones
- **execute the statement**, not just load it, and assert on the result

### 5. If the directory is new, classify it

A `scripts/` subdirectory nothing classifies fails
`test_every_script_directory_is_classified`. Add a row to *Where scripts sit,
and what the directory declares* in `docs/TESTING.md`. The second cell is
parsed by regex and must be exactly `yes` or `**no**` — no other wording
matches, and `**no**` obliges a matching `[tool.coverage.run] omit` entry.

## Trip-wires

Each of these cost a suite run to discover. None is in an error message.

| What bites | Why |
|---|---|
| A brace in a SQL **comment** | The whole file goes through `str.format`. `` `{path}` `` in prose raises `KeyError: 'path'` |
| Naming a function in a comment | A statement's text is the whole file. A test asserting `"random()" not in query` reads comments too |
| Putting the test in `tests/integration/scripts/` | Layer 4. Only `sql/` and `archiver/` count as Layer 2 |
| Writing fixtures with `COPY ... TO` | That is the test authoring SQL, which is now a rule |
| Adding `<root>/` to `_SQL_EXEMPT_ROOTS` | Shrinks the denominator every coverage number is a fraction of. It is asserted both ways and needs a row arguing for it |

## Verify before committing

```bash
python -m pytest tests/test_testing_contract.py -q
python -m pytest tests/integration/sql/<your_test>.py -q -m integration
python -m ruff check .
```

Then prove the recorder sees it — this is the check the gate actually runs, and
the only one that distinguishes "a test names the file" from "the file's text
reached an engine":

```bash
SQL_EXECUTION_RECORD=/tmp/rec python -m pytest <your test> -q -m integration
python -c "import json,glob; print(sorted({o for f in glob.glob('/tmp/rec/*.json') \
  for e in json.load(open(f))['executions'] for o in e['origins']}))"
```

Your `.sql` path must appear. If it does not, the statement is not executing
and the gate will fail on it, whatever the tests say.

Last, confirm the corpus grew by what you added:

```bash
python -c "from tests.test_testing_contract import production_sql_files as f; print(len(f()))"
```

## What this skill will not do

- **Add a waiver.** `INLINE_SQL_WAIVERS` and `SQL_LITERAL_WAIVERS` only shrink.
  A statement that cannot execute in CI is a conversation, not a tuple append.
- **Exempt a root** to make a number go green. That is the one edit that makes
  a gate read 100% by counting less.
- **Decide a script is spent.** `scripts/oneoff/` means the owning plan has
  archived **and** nothing binding names it. Both halves. A script an active
  script imports is production however finished it looks, and its plan's
  archival does not change that.
