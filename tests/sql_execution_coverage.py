"""Which production statements are not yet known to execute, and who owns that.

Plan 162 Stage X. The runtime half of this rule is
``scripts/check_sql_execution_coverage.py``, which runs in a CI job that can
see every other job's record; this module is the part that can be asserted
without one, and it is the ledger.

**It replaces a proxy, and the proxy is why the ledger starts non-empty.**
``test_every_production_sql_file_is_touched_by_a_layer_2_test`` credits a file
when a Layer 2 module *names its stem as a whole word* -- the weakest available
reading, which this plan has said so about since the day it was written. It
reports all 161 files covered with no waivers. Recorded execution is the
strongest reading, and the difference between the two is not a regression: it
is the reading finally being honest, and every entry below is a file the old
rule was crediting to a test that never ran it.

The ratchet is the same one the ``*_WAIVERS`` tuples in
``tests/test_testing_contract.py`` carry, and for the same reasons: an entry
names one file, an owner plan and the date it was measured; an entry whose file
*does* execute fails as stale, so a repair cannot pile up behind a list that
still claims it is outstanding; and the list only shrinks.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Unrecorded:
    """One production ``.sql`` file no CI job was observed executing."""

    path: str
    gap: str
    owner: int
    since: date


MEASURED = date(2026, 9, 6)

#: Seeded from the first CI-wide record, and deliberately not from a local run.
#: A developer machine without MinIO, a dbt-built DuckDB file or the Airflow
#: metadata schema reports 60 files unexecuted, and 53 of those execute in a job
#: it was not running -- so a ledger seeded locally would be a list of this
#: container's gaps wearing the repository's name.
UNRECORDED: tuple[Unrecorded, ...] = ()
