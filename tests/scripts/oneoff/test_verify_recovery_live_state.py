"""Plan 145 Stage 5 slice 3 -- the V040 live-state proof.

These hold the invariants a reviewer cannot check by reading: the verifier
refuses to run without a named window (and opens no connection when it does),
both V040 snapshots are taken inside one transaction, and a live mutation
between the snapshots fails the proof.
"""
import hashlib
import json
import re

from scripts.oneoff.verify_recovery_live_state import RELATIONS, run


class _Store:
    def __init__(self):
        self.txid = 7000
        self.txid_moves = False
        self.rows = {rel: [(rel, "seed")] for rel in RELATIONS}

    def next_txid(self):
        if self.txid_moves:
            self.txid += 1
        return self.txid

    def mutate(self, rel):
        self.rows[rel] = self.rows[rel] + [(rel, "canary")]


class _Cur:
    def __init__(self, store, conn):
        self.store = store
        self.conn = conn
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.conn.statements.append(s)
        if s.startswith("SET TRANSACTION"):
            self._result = None
            return
        if "txid_current()" in s:
            self._result = (self.store.next_txid(),)
            return
        m = re.search(r"FROM (\S+) t$", s)
        if m:
            data = self.store.rows.get(m.group(1), [])
            # order-independent, like the real bit_xor digest
            digest = 0
            for row in data:
                digest ^= int(hashlib.md5(repr(row).encode()).hexdigest()[:16], 16)
            self._result = (len(data), digest)
            return
        raise AssertionError(f"unexpected SQL: {s}")

    def fetchone(self):
        return self._result


class _Conn:
    def __init__(self, store):
        self.store = store
        self.autocommit = True
        self.rolled_back = 0
        self.closed = False
        self.statements = []

    def cursor(self):
        return _Cur(self.store, self)

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        self.closed = True


def _boom():
    raise AssertionError("a connection must not be opened before the window check")


def test_refuses_without_a_named_window_and_opens_no_connection(capsys):
    rc = run([], connect=_boom)
    assert rc == 2
    assert "REFUSED" in capsys.readouterr().err


def test_clean_run_passes_in_one_transaction_and_rolls_back(tmp_path):
    store = _Store()
    conn = _Conn(store)
    report_path = tmp_path / "r.json"

    rc = run(["--window", "april-cutover-2026", "--report", str(report_path)],
             connect=lambda: conn, canary=lambda: None)

    assert rc == 0
    report = json.loads(report_path.read_text())
    assert report["window"] == "april-cutover-2026"
    assert report["passed"] is True
    assert report["txid"]["single_transaction"] is True
    assert report["changed_relations"] == {}
    assert conn.rolled_back == 1 and conn.closed is True


def test_a_mutation_between_the_snapshots_fails_the_proof(tmp_path):
    store = _Store()
    report_path = tmp_path / "r.json"

    rc = run(["--window", "w", "--report", str(report_path)],
             connect=lambda: _Conn(store),
             canary=lambda: store.mutate("ops.blocked_cooldown"))

    assert rc == 1
    report = json.loads(report_path.read_text())
    assert report["passed"] is False
    assert "ops.blocked_cooldown" in report["changed_relations"]
    before = report["changed_relations"]["ops.blocked_cooldown"]["before"]["rows"]
    after = report["changed_relations"]["ops.blocked_cooldown"]["after"]["rows"]
    assert after == before + 1


def test_snapshots_not_in_one_transaction_fail_even_with_no_mutation(tmp_path):
    store = _Store()
    store.txid_moves = True                      # a fake that leaves the txn
    report_path = tmp_path / "r.json"

    rc = run(["--window", "w", "--report", str(report_path)],
             connect=lambda: _Conn(store), canary=lambda: None)

    assert rc == 1
    report = json.loads(report_path.read_text())
    assert report["txid"]["single_transaction"] is False
    assert report["changed_relations"] == {}     # nothing changed; the txn did
    assert report["passed"] is False


def test_a_failing_canary_command_fails_the_check(tmp_path):
    store = _Store()
    report_path = tmp_path / "r.json"

    # A shell builtin, not an interpreter. The command only has to exit 3; what
    # is under test is that a non-zero canary fails the check and lands its
    # returncode in the report. Naming an interpreter dragged its path through
    # the shell's quoting rules -- `shlex.quote` is POSIX and `cmd.exe` does
    # not honour it, so this test failed on Windows and passed in CI, which is
    # the harness deciding the outcome. `exit 3` needs no quoting and means the
    # same thing to /bin/sh and cmd.exe.
    rc = run(["--window", "w", "--report", str(report_path),
              "--canary-cmd", "exit 3"],
             connect=lambda: _Conn(store))

    assert rc == 1
    report = json.loads(report_path.read_text())
    assert report["canary"]["returncode"] == 3
    assert report["passed"] is False


def test_both_snapshots_are_taken_on_one_cursor_before_any_rollback(tmp_path):
    store = _Store()
    conn = _Conn(store)

    run(["--window", "w", "--report", str(tmp_path / "r.json")],
        connect=lambda: conn, canary=lambda: None)

    # two txid probes + two full relation sweeps, all before the single rollback
    txid_probes = [s for s in conn.statements if "txid_current()" in s]
    assert len(txid_probes) == 2
    # READ COMMITTED, not REPEATABLE READ: RR would freeze the data snapshot at
    # the first statement and the second snapshot could never see the canary.
    assert conn.statements[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert conn.rolled_back == 1
