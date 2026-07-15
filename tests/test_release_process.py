from __future__ import annotations

import json
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "release-please-config.json"
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "release-please.yml"


class ReleaseProcessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        cls.workflow_text = WORKFLOW_PATH.read_text(encoding="utf-8")
        cls.workflow = yaml.safe_load(cls.workflow_text)
        cls.steps = cls.workflow["jobs"]["release-please"]["steps"]
        cls.steps_by_name = {step["name"]: step for step in cls.steps}

    def test_release_please_generates_draft_pr_and_updates_uv_lock(self) -> None:
        package = self.config["packages"]["."]

        self.assertIs(self.config["draft-pull-request"], True)
        self.assertEqual(
            package["extra-files"],
            [
                {
                    "type": "toml",
                    "path": "uv.lock",
                    "jsonpath": ("$.package[?(@.name.value=='signal-hermes-router')].version"),
                }
            ],
        )

    def test_existing_pending_pr_is_drafted_before_release_please_runs(self) -> None:
        capture = self.steps_by_name["Capture pending release PR state"]
        capture_run = capture["run"]
        names = [step["name"] for step in self.steps]

        self.assertLess(
            names.index("Capture pending release PR state"),
            names.index("Run Release Please"),
        )
        for expected in (
            "--state open",
            '--base "$BASE_BRANCH"',
            '--label "autorelease: pending"',
            "number,isDraft,headRefOid,autoMergeRequest",
            'if [ "$count" -gt 1 ]',
            'gh pr ready "$number" --repo "$REPO" --undo',
        ):
            self.assertIn(expected, capture_run)

    def test_noop_restores_only_the_exact_previous_ready_state(self) -> None:
        restore = self.steps_by_name["Restore unchanged pending release PR"]
        condition = restore["if"]
        restore_run = restore["run"]

        for expected in (
            "always()",
            "!cancelled()",
            "steps.pending_pr.outcome == 'success'",
            "steps.release.outcome == 'success'",
            "steps.release.outputs.prs_created != 'true'",
            "steps.pending_pr.outputs.was_ready == 'true'",
        ):
            self.assertIn(expected, condition)
        self.assertIn("for attempt in 1 2 3 4 5", restore_run)
        self.assertIn("could not read pending release PR head after 5 attempts", restore_run)
        self.assertIn('if [ "$live_head_sha" != "$PREVIOUS_HEAD_SHA" ]', restore_run)
        self.assertIn('gh pr ready "$PR_NUMBER" --repo "$REPO"', restore_run)
        self.assertIn('if [ "$RESTORE_AUTO_MERGE" = "true" ]', restore_run)
        self.assertIn("--auto --squash --delete-branch", restore_run)

    def test_release_head_is_synchronized_before_checkout_and_validation(self) -> None:
        names = [step["name"] for step in self.steps]

        self.assertLess(
            names.index("Ensure release PR branch is current"),
            names.index("Check out release PR branch"),
        )
        self.assertLess(
            names.index("Check out release PR branch"),
            names.index("Validate generated release head"),
        )
        current_run = self.steps_by_name["Ensure release PR branch is current"]["run"]
        self.assertIn('gh pr update-branch "$PR_NUMBER" --repo "$REPO"', current_run)
        self.assertIn("mergeStateStatus is $state after update", current_run)
        self.assertIn("BEHIND|DIRTY|UNKNOWN", current_run)

    def test_validation_is_nonmutating_and_precedes_live_head_gate(self) -> None:
        names = [step["name"] for step in self.steps]
        validation_run = self.steps_by_name["Validate generated release head"]["run"]

        self.assertEqual(
            validation_run.splitlines(),
            [
                "uv lock --check",
                "uv run --locked python scripts/check-public-boundary.py",
                "git diff --exit-code -- CHANGELOG.md pyproject.toml uv.lock",
            ],
        )
        self.assertLess(
            names.index("Validate generated release head"),
            names.index("Verify release PR head"),
        )
        self.assertLess(
            names.index("Verify release PR head"),
            names.index("Mark release PR ready"),
        )

    def test_live_head_gate_rejects_stale_or_unmergeable_head(self) -> None:
        verify_run = self.steps_by_name["Verify release PR head"]["run"]

        self.assertIn("--json headRefOid,mergeStateStatus", verify_run)
        self.assertIn('if [ "$live_head_sha" != "$checked_out_sha" ]', verify_run)
        self.assertIn("::error::release PR head changed after validation", verify_run)
        for state in ("BEHIND", "DIRTY", "UNKNOWN"):
            self.assertIn(f'[ "$state" = "{state}" ]', verify_run)

    def test_ready_approval_and_auto_merge_follow_validation_gate(self) -> None:
        names = [step["name"] for step in self.steps]

        self.assertLess(
            names.index("Verify release PR head"),
            names.index("Mark release PR ready"),
        )
        self.assertLess(
            names.index("Mark release PR ready"),
            names.index("Approve release PR"),
        )
        self.assertLess(
            names.index("Approve release PR"),
            names.index("Enable squash auto-merge"),
        )
        for name in (
            "Mark release PR ready",
            "Approve release PR",
            "Enable squash auto-merge",
        ):
            self.assertIn(
                "steps.verify_release_pr_head.outcome == 'success'",
                self.steps_by_name[name]["if"],
            )

    def test_workflow_contains_no_post_generation_repair_commit(self) -> None:
        forbidden = (
            "perl -0pi",
            "chore(release): normalize",
            "git commit",
            "git push",
        )

        for value in forbidden:
            self.assertNotIn(value, self.workflow_text)


if __name__ == "__main__":
    unittest.main()
