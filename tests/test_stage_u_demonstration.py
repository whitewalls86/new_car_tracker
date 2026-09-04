"""A deliberately undeclared skip, so that the gate is watched failing.

Plan 162 Stage U / CAR-81. **This file is temporary and the next commit on
this branch deletes it.**

Stage U's exit says the mechanism is *demonstrated by an undeclared skip
failing a run, not asserted*, and the distinction is the point: every check in
`tests/test_declared_skips.py` drives the hook with stub reports in the unit
job, which proves the logic and proves nothing about whether the plugin is
loaded in a real CI run. This file is the other half. It skips, nothing
declares it, and the `Unit tests (pytest)` job goes red for it -- a job
`REQUIRE_LAYER_2_EXECUTION` never reached.

What the red run should show: pytest reporting everything as passed with one
extra skip, and the step exiting 1 anyway on the strength of the `Declared
skips` section. A skip is not a failure, which is exactly why nothing noticed
before this stage.
"""
import pytest


def test_an_undeclared_skip_turns_this_job_red():
    pytest.skip("nothing declares this skip, and that is the demonstration")
