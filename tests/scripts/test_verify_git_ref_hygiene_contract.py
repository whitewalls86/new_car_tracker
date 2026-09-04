"""The ref-hygiene contract checker's verdict logic (Plan 164 Stage 3).

The checker is invoked straight from ``ci.yml`` and talks to a live remote, so
what is driven here is the pure half: given a settings payload, given a list of
merged branches, does it refuse? The network half is exercised only by the CI
job, the same split ``verify_promtail_contract`` uses.

The case worth naming is the **absent field**. A settings payload that does not
carry ``deleteBranchOnMerge`` at all must fail, not pass -- an unreadable
setting is not a satisfied one, and a checker that treats absence as success is
worse than no checker, because it reports green forever.
"""

from __future__ import annotations

import pytest

from scripts.verify_git_ref_hygiene_contract import (
    MERGED_BRANCH_BUDGET,
    REQUIRED_REMOTE_SETTINGS,
    check_accretion,
    check_settings,
    main,
)

COMPLIANT = {
    "deleteBranchOnMerge": True,
    "squashMergeAllowed": False,
    "rebaseMergeAllowed": False,
}


class TestSettings:
    def test_a_compliant_remote_has_no_violations(self):
        assert check_settings(COMPLIANT) == []

    @pytest.mark.parametrize("field", sorted(REQUIRED_REMOTE_SETTINGS))
    def test_every_required_setting_is_actually_checked(self, field):
        # Asserted from the contract rather than enumerated by hand: adding a
        # setting to REQUIRED_REMOTE_SETTINGS and forgetting to check it is the
        # failure this parametrisation exists to make impossible.
        flipped = dict(COMPLIANT)
        flipped[field] = not REQUIRED_REMOTE_SETTINGS[field]
        problems = check_settings(flipped)
        assert any(field in problem for problem in problems)

    @pytest.mark.parametrize("field", sorted(REQUIRED_REMOTE_SETTINGS))
    def test_an_absent_field_fails_rather_than_passes(self, field):
        missing = {k: v for k, v in COMPLIANT.items() if k != field}
        problems = check_settings(missing)
        assert any(field in problem for problem in problems)

    def test_an_empty_payload_fails_on_everything(self):
        # The shape a token without repository scope would produce. Fails
        # closed, loudly, rather than reporting a clean remote.
        assert len(check_settings({})) == len(REQUIRED_REMOTE_SETTINGS)

    def test_it_names_every_violation_in_one_run(self):
        broken = {"deleteBranchOnMerge": False, "squashMergeAllowed": True}
        problems = check_settings(broken)
        assert len(problems) == 3  # two wrong, one absent


class TestAccretion:
    def test_at_the_budget_is_not_a_violation(self):
        assert check_accretion([f"b{i}" for i in range(MERGED_BRANCH_BUDGET)]) == []

    def test_one_over_the_budget_is(self):
        problems = check_accretion([f"b{i}" for i in range(MERGED_BRANCH_BUDGET + 1)])
        assert len(problems) == 1
        assert "1 over the budget" in problems[0]

    def test_the_message_names_offenders_so_it_is_actionable(self):
        problems = check_accretion(["zeta", "alpha"], budget=1)
        assert "alpha" in problems[0]

    def test_the_budget_only_shrinks(self):
        # A ratchet is only a ratchet if raising it is a visible edit. This
        # records the value so a silent bump shows up as a failing test in the
        # same diff that raised it.
        assert MERGED_BRANCH_BUDGET <= 67, (
            "MERGED_BRANCH_BUDGET is a ratchet and only ever shrinks. Raising it "
            "means the remote accreted refs again; fix that instead."
        )


class TestTheCommandLine:
    def test_a_clean_remote_exits_zero(self, mocker, capsys):
        mocker.patch(
            "scripts.verify_git_ref_hygiene_contract.remote_settings", return_value=COMPLIANT
        )
        mocker.patch(
            "scripts.verify_git_ref_hygiene_contract.merged_remote_branches", return_value=[]
        )
        assert main([]) == 0
        assert "OK:" in capsys.readouterr().out

    def test_a_drifted_remote_exits_nonzero_and_says_which_setting(self, mocker, capsys):
        mocker.patch(
            "scripts.verify_git_ref_hygiene_contract.remote_settings",
            return_value={**COMPLIANT, "deleteBranchOnMerge": False},
        )
        mocker.patch(
            "scripts.verify_git_ref_hygiene_contract.merged_remote_branches", return_value=[]
        )
        assert main([]) == 1
        assert "deleteBranchOnMerge" in capsys.readouterr().err

    def test_an_unreachable_remote_fails_rather_than_skipping(self, mocker, capsys):
        from scripts.verify_git_ref_hygiene_contract import VerificationError

        mocker.patch(
            "scripts.verify_git_ref_hygiene_contract.remote_settings",
            side_effect=VerificationError("gh: not authenticated"),
        )
        assert main([]) == 1
        assert "not authenticated" in capsys.readouterr().err
