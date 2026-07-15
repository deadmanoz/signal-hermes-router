from __future__ import annotations

import json
import os
import subprocess
import tempfile
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
            'release_branch="release-please--branches--$BASE_BRANCH"',
            '--head "$release_branch"',
            "number,isDraft,headRefOid,autoMergeRequest,isCrossRepository",
            "select(.isCrossRepository == false)",
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
        self.assertIn("pending release PR head changed while restoring ready state", restore_run)
        self.assertIn("trap redraft_on_failure EXIT", restore_run)
        self.assertIn("trap - EXIT", restore_run)
        self.assertIn('if [ "$RESTORE_AUTO_MERGE" = "true" ]', restore_run)
        for flag in ("--auto", "--squash", "--delete-branch"):
            self.assertIn(flag, restore_run)
        self.assertIn('--match-head-commit "$PREVIOUS_HEAD_SHA"', restore_run)

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
        self.assertIn("--json headRefOid,mergeable", current_run)
        self.assertIn("compare/$BASE_BRANCH...$head_sha", current_run)
        self.assertIn("CONFLICTING", current_run)
        self.assertIn('behind_by" -gt 0', current_run)
        self.assertNotIn("mergeStateStatus", current_run)

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
            names.index("Publish validated release PR"),
        )

    def test_live_head_gate_uses_draft_independent_merge_evidence(self) -> None:
        verify_run = self.steps_by_name["Verify release PR head"]["run"]

        self.assertIn("--json headRefOid,mergeable", verify_run)
        self.assertIn("compare/$BASE_BRANCH...$live_head_sha", verify_run)
        self.assertIn('if [ "$live_head_sha" != "$checked_out_sha" ]', verify_run)
        self.assertIn("::error::release PR head changed after validation", verify_run)
        self.assertIn("CONFLICTING", verify_run)
        self.assertIn('behind_by" != "0', verify_run)
        self.assertNotIn("mergeStateStatus", verify_run)

    def test_ready_approval_and_auto_merge_follow_validation_gate(self) -> None:
        names = [step["name"] for step in self.steps]
        publish = self.steps_by_name["Publish validated release PR"]
        publish_run = publish["run"]

        self.assertLess(
            names.index("Verify release PR head"),
            names.index("Publish validated release PR"),
        )
        self.assertIn("steps.verify_release_pr_head.outcome == 'success'", publish["if"])
        self.assertLess(publish_run.index("gh pr ready"), publish_run.index("gh pr review"))
        self.assertLess(publish_run.index("gh pr review"), publish_run.index("gh pr merge"))
        self.assertIn("compare/$BASE_BRANCH...$live_head_sha", publish_run)
        self.assertIn("protection/required_status_checks", publish_run)
        self.assertIn('if [ "$strict" != "true" ]', publish_run)
        self.assertIn("trap redraft_on_failure EXIT", publish_run)
        self.assertIn("trap - EXIT", publish_run)
        self.assertIn("changed or became unmergeable during ready transition", publish_run)
        self.assertIn("changed or became unmergeable before auto-merge", publish_run)
        self.assertIn('--match-head-commit "$validated_sha"', publish_run)

    def test_publish_failure_after_ready_redrafts_pr(self) -> None:
        publish_run = self.steps_by_name["Publish validated release PR"]["run"]

        with tempfile.TemporaryDirectory() as tmp:
            call_log = Path(tmp) / "gh-calls.log"
            harness = f"""
set -e -o pipefail
git() {{
  printf '%s\\n' "$VALIDATED_SHA"
}}
gh() {{
  printf '%s\\n' "$*" >> "$CALL_LOG"
  if [[ "$*" == *"protection/required_status_checks"* ]]; then
    printf 'true\\n'
  elif [[ "$*" == *"compare/$BASE_BRANCH"* ]]; then
    printf '0\\n'
  elif [[ "$*" == *"--json headRefOid,mergeable"* ]]; then
    printf '{{"headRefOid":"%s","mergeable":"MERGEABLE"}}\\n' "$VALIDATED_SHA"
  elif [[ "$*" == *"--json headRefOid --jq .headRefOid"* ]]; then
    printf '%s\\n' "$VALIDATED_SHA"
  elif [[ "$*" == "pr review "* ]]; then
    return 42
  fi
}}
{publish_run}
"""
            env = {
                **os.environ,
                "APPROVAL_TOKEN": "approval-token",
                "BASE_BRANCH": "main",
                "CALL_LOG": str(call_log),
                "PR_NUMBER": "123",
                "REPO": "example/repo",
                "VALIDATED_SHA": "a1" * 20,
            }

            result = subprocess.run(
                ["bash", "-c", harness],
                check=False,
                capture_output=True,
                env=env,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            calls = call_log.read_text(encoding="utf-8")
            self.assertIn("pr ready 123 --repo example/repo\n", calls)
            self.assertIn("pr ready 123 --repo example/repo --undo\n", calls)

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
