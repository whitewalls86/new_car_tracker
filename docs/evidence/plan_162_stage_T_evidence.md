# Plan 162 Stage T — what the suite duplicates at 4,483 tests, 2026-09-07

Stage T's scope after the 2026-09-04 split: the Python half of the duplication
the 2026-09-01 table measured. The SQL half — 96 ad-hoc `INSERT`s, 161
read-back `SELECT`s — became Stage X's denominator and is recorded here as a
delta observed, not re-litigated.

## The recipe, and a confession about it

**The 2026-09-01 measurement's recipe was never written down.** No evidence
file exists for it and no §Record entry carries one — the same situation Plan
138 Stage 9 hit with its churn number, resolved the same way: the recipe below
was re-derived, and the comparison that carries weight is before-vs-after
*under this recipe*, not this recipe against the remembered numbers.

One row cross-checks exactly: on the pre-stage tree this recipe reads **55
module-local seed helpers**, the number the plan recorded. Tests collected
reads 4,483 against the recorded 3,988 because Stages S, U and X landed
between the two readings, not because the recipe differs.

The predicates, stated so the next reading is the same reading:

- **test module**: `tests/**/*.py`, excluding `tests/scripts/oneoff/` (Stage G
  declared it spent) and `tests/sql/` (not Python).
- **tests collected**: `python -m pytest --collect-only -q`.
- **ad-hoc `INSERT`**: any string constant in a test module containing
  `INSERT INTO` (case-insensitive), found by AST walk.
- **read-back `SELECT`**: any string constant starting `SELECT ` and
  containing `FROM`.
- **module-local seed helper**: a module-level function whose name starts with
  `_` and whose body contains a `.execute(`/`.executemany(`/`execute_values(`
  call or loads a seed/insert `.sql`.
- **duplicated helper**: the same module-level `_name` defined in more than
  one test module; **byte-identical** means equal after dedent and
  comment/blank-line stripping via `tokenize`.

The measurement script is `scripts` -classifiable nowhere (this plan is not
archived, so `scripts/oneoff/` cannot cite it), so it lives in this file:

<details><summary>measure_dup.py, verbatim</summary>

```python
import ast
import hashlib
import io
import json
import sys
import textwrap
import tokenize
from pathlib import Path

ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
TESTS = ROOT / "tests"


def test_modules():
    for p in TESTS.rglob("*.py"):
        rel = p.relative_to(ROOT).as_posix()
        if rel.startswith("tests/scripts/oneoff/") or rel.startswith("tests/sql/"):
            continue
        yield p, rel


def strip_comments(src: str) -> str:
    out = []
    try:
        for tok in tokenize.generate_tokens(io.StringIO(src).readline):
            if tok.type in (tokenize.COMMENT, tokenize.NL):
                continue
            out.append((tok.type, tok.string))
    except tokenize.TokenizeError:
        return src
    return " ".join(s for _, s in out)


class SqlLiteralFinder(ast.NodeVisitor):
    def __init__(self):
        self.inserts = []
        self.selects = []

    def visit_Constant(self, node):
        if isinstance(node.value, str):
            u = node.value.upper()
            if "INSERT INTO" in u:
                self.inserts.append(node.value)
            if u.lstrip().startswith("SELECT ") and "FROM" in u:
                self.selects.append(node.value)


def body_has_execute(fn: ast.AST) -> bool:
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            f = node.func
            if isinstance(f, ast.Attribute) and f.attr in ("execute", "executemany"):
                return True
            if isinstance(f, ast.Name) and f.id == "execute_values":
                return True
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            v = node.value.lower()
            if v.endswith(".sql") and ("seed" in v or "insert" in v):
                return True
    return False


inserts_total = 0
seed_helpers = []
select_texts = {}
defs = {}

for p, rel in test_modules():
    src = p.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        continue
    finder = SqlLiteralFinder()
    finder.visit(tree)
    inserts_total += len(finder.inserts)
    for s in finder.selects:
        select_texts.setdefault(" ".join(s.split()), []).append(rel)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not node.name.startswith("_"):
                continue
            seg = ast.get_source_segment(src, node) or ""
            norm = strip_comments(textwrap.dedent(seg))
            h = hashlib.sha1(norm.encode()).hexdigest()[:10]
            defs.setdefault(node.name, []).append((rel, node.lineno, h))
            if body_has_execute(node):
                seed_helpers.append((rel, node.name, node.lineno))

dup = {k: v for k, v in defs.items() if len({r for r, _, _ in v}) > 1}
ident = sum(1 for v in dup.values() if len({h for _, _, h in v}) == 1)
print(f"ad-hoc INSERT literals: {inserts_total}")
print(f"distinct SELECT literals: {len(select_texts)}")
print(f"module-local seed helpers: {len(seed_helpers)}")
print(f"duplicated _names across files: {len(dup)}  (byte-identical: {ident})")
```

</details>

## The deltas, all of them

| Measure | 2026-09-01 | before this stage | after |
|---|---:|---:|---:|
| Tests collected | 3,988 | 4,483 | **4,483** — the stage adds none and deletes none |
| Ad-hoc `INSERT` statements in test modules | 96 | 2* | 2* — **Stage X's delta**, observed here |
| Distinct read-back `SELECT`s in test modules | 161 | 0 | 0 — Stage X's delta, observed here |
| Module-local seed helpers | 55 | 55 | **47** |
| Helper names defined in >1 test module | not measured | 52 | **43** |
| …byte-identical after normalization | not measured | 19 | **12** |

\* Neither survivor is a seed: one is
`tests/test_testing_contract.py`'s own mutation-fixture string (input to the
rule that forbids the pattern), the other a docstring in
`test_processing_queries.py` that mentions the words. Zero real.

The duplication the 2026-09-01 table led with was the SQL, and Stage X
dissolved it before this stage ran — the 17-times-retyped queue `SELECT`
exists nowhere in Python. What Stage X left is measured below under
*the instrument*.

## What was consolidated — seven groups that genuinely share intent

| Was | Now | Sites |
|---|---|---|
| `_parse_dsn` ×4 conftests (plus `_DEFAULT_URL`/`_DATABASE_URL` retyped beside each) | defined once in `tests/integration/conftest.py`, imported by the ops/processing/scripts conftests | 3 defs deleted |
| `_get_conn` ×3 (`test_access_requests`, `test_search_crud`, `test_delete_stale_emails`) | deleted — each re-derived the root conftest's existing `db_conn_factory`; the fixtures that called them now take it | 3 defs deleted |
| `_insert_detail_claim` ×2 (`test_maintenance`, `test_maintenance_api`) | `insert_detail_claim` factory fixture, `tests/integration/ops/conftest.py`; its `.sql` deduped 2→1 under `tests/sql/integration/ops/conftest/` | 5 call sites |
| `_get_price_obs`/`_get_vin_mapping`/`_count_silver` ×2 each (`test_write_detail`, `test_write_srp`) | `get_price_obs`/`get_vin_mapping`/`count_silver` fixtures closing over `vc`, processing conftest; three `.sql` files deduped 6→3 under its conftest dir | 17 call sites |
| `_make_tar_zst` ×3 (lake-snapshot script tests) | `make_tar_zst` in new `tests/scripts/conftest.py` — the `test_lake_snapshot_common` variant survived because it is a strict superset (its `raw_members` is what expresses the hostile-archive cases) | 11 call sites |
| `_presentation` ×2 (`test_info`, `test_public_routes`) | `make_presentation` in `tests/ops/conftest.py` | 30 call sites |
| `_load_html_fixture` ×2 + `_FIXTURE_DIR` ×2 (`test_html_sections`, `test_parse_detail_page` — same directory) | `load_html_fixture` in `tests/processing/conftest.py` | 27 call sites |

Where a surviving helper restates a production literal (an artifact type, a
status string) it was moved as-is: deriving those from production's enums is
Stage W's sweep, and doing it piecemeal here would blur what W measures.

## What was left alone, and why — each one a decision

**Twin parser test modules** — `_activity_script`, `_dealer_card`,
`_make_v3_card` (28 identical lines), duplicated between
`tests/processing/` and `tests/scraper/processors/`. These test **two
different production modules**: the detail-page parser exists in both
`processing/processors/` and `scraper/processors/`, and the test twins mirror
that production duplication. Consolidating the helpers would couple the tests
of two independently evolving modules. The production duplication itself is
real and is not this stage's to fix.

**Callers want different data** — the plan's own examples dissolved this way:

- `_insert_artifact` ×3: the ticket's flagship. Post-Stage-X each loads a
  *different* per-module statement with different columns and arity — one
  backdates `created_at` and writes event rows, the others vary type/status.
  Same name, three intents; a shared version would grow parameters until it
  served nobody.
- `_seed` ×4: two FakeS3 unit variants with disjoint knobs (one packs, one
  writes sidecars), two integration variants parameterized differently (bucket
  root vs artifact type) over divergent `_page` builders.
- `_cleanup` ×2 (37 vs 21 lines): different table sets per writer.
- `_patch_shared_db_kwargs` ×2: identical but for docstrings once
  `_parse_dsn` was shared; its `autouse` scope is a per-suite decision, and
  hoisting it would either impose it on suites that must not have it (ops sets
  env vars before app import instead) or replace two six-line fixtures with
  two shims plus a root definition.

**Below the consolidation floor** — byte-identical but two to five lines, no
natural existing home, and the helper is shorter than the import-plus-plumbing
that would replace it: `_compose`, `_load` (compose-config loaders),
`_dag_files`/`_tree`/`_kwarg` (AST readers in the two airflow census tests),
`_fake_cursor`, `_failed`, `_random_listing_id` (`str(uuid.uuid4())`).

**Unverifiable** — `_base_uri` ×2 in `tests/integration/lakehouse/`, which is
`DORMANT_SUITES`-declared until Plan 125 Gate C. A change no test can run is a
change this stage does not make.

**Name collisions, not duplication** — `_run` ×7, `_args` ×3, `_row` ×3,
`_payload` ×3, `_load_dag_module` ×3 and the remaining 2× divergent groups:
same name doing unrelated jobs. Nothing shared to extract.

The 12 byte-identical groups that remain after the stage are exactly the
twin-parser, floor, and dormant sets above — every one a recorded decision.

## The instrument: found, and its limit stated

The exit demands a plain statement on whether a mechanical instrument for
"two helpers do the same thing" exists. **For helpers that execute SQL, it
does now, and it is not the execution recorder** — the plan's prediction that
only runtime recording could answer it was pessimistic. Stage X's extraction
made the question textual: two helpers that share no token but execute the
same statement now own two files under `tests/sql/` whose normalized text is
equal. Measured 2026-09-07:

| | |
|---|---:|
| Test `.sql` files | 385 |
| Names used in more than one module directory | 65 (188 files) |
| **Identical-content groups** (normalized, case-folded) | **43, spanning 96 files** |
| …groups whose copies do not even share a filename | 7 |

The recipe: hash `" ".join(text.split()).lower()` per file, group by hash —
static, derived, no runtime needed.

**It is recorded as an available instrument, not made a contract rule.** Two
modules seeding a table identically is legal and usually correct — the
per-module mirror is the convention Stage X chose deliberately, so a ratchet
on identical content would either fight that convention or push authors
toward artificial divergence to silence it. And the drift risk that made this
class matter is already closed at the root:
`test_every_test_statement_plans_against_the_migrated_schema` PREPAREs every
copy against the migrated catalogue, so a renamed column fails all seven
variants of `insert_ops_price_observations.sql` loudly, found or not.
Consolidation after Stage X buys legibility, not safety.

**For helpers that execute no SQL** — tar builders, FakeS3 seeds, page
builders, DSN parsers — no mechanical instrument exists and none was found:
their sameness is behavioural, not textual, and this file's by-name,
by-normalized-body measurement is the strongest cheap reading available. That
half leaves prose behind, and this is the plain statement the exit requires.

## Verification

Local Postgres per the CI recipe (`postgres:16`, Flyway to V050, same
placeholders as `ci.yml`):

- before any change: `tests/integration/{ops,processing,sql,scripts}`
  **439 passed, 35 skipped**
- after all seven consolidations, plus `test_delete_stale_emails.py`:
  **441 passed, 35 skipped** — same skip set (MinIO- and dict-gated)
- unit suite: **3,789 passed** (`-m "not integration"`), ruff clean
- collection unchanged at **4,483** — the stage moved definitions and deleted
  duplicates; it added and removed no test

`tests/scripts/oneoff/` untouched, per Stage G. No read-back assertion became
a production `.sql` file: the three moved read-backs live under
`tests/sql/integration/processing/conftest/`, inside Stage X's separate root
and outside `production_sql_files()`.
