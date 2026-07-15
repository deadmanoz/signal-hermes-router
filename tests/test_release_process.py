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

        self.assertEqual(
            self.workflow["jobs"]["release-please"]["defaults"]["run"]["shell"],
            "bash",
        )
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

    def test_pending_pr_noop_enters_full_validation_and_recovers_draft(self) -> None:
        resolve = self.steps_by_name["Resolve release PR number"]
        resolve_run = resolve["run"]

        self.assertEqual(resolve["if"], "${{ steps.release.outcome == 'success' }}")
        self.assertIn('elif [ -n "$PENDING_PR_NUMBER" ]', resolve_run)
        self.assertIn('number="$PENDING_PR_NUMBER"', resolve_run)
        self.assertIn('branch="release-please--branches--$BASE_BRANCH"', resolve_run)
        self.assertIn('if [ "$PENDING_WAS_READY" = "true" ]', resolve_run)
        self.assertIn('enable_auto_merge="$PENDING_AUTO_MERGE"', resolve_run)
        self.assertIn('enable_auto_merge="true"', resolve_run)
        self.assertIn('echo "has_release_pr=true"', resolve_run)

        for name in (
            "Ensure release PR branch is current",
            "Validate generated release diff",
            "Check out release PR branch",
            "Set up uv",
            "Install Python 3.12",
            "Validate generated release head",
            "Verify release PR head",
        ):
            self.assertIn(
                "steps.release_pr.outputs.has_release_pr == 'true'", self.steps_by_name[name]["if"]
            )

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
        current = self.steps_by_name["Ensure release PR branch is current"]
        checkout = self.steps_by_name["Check out release PR branch"]
        diff_validation = self.steps_by_name["Validate generated release diff"]
        self.assertIn('gh pr update-branch "$PR_NUMBER" --repo "$REPO"', current_run)
        self.assertIn("--json headRefOid,mergeable", current_run)
        self.assertIn("compare/$BASE_BRANCH...$head_sha", current_run)
        self.assertIn("CONFLICTING", current_run)
        self.assertIn('behind_by" -gt 0', current_run)
        self.assertEqual(current["id"], "current_release_pr")
        self.assertIn('echo "head_sha=$head_sha" >> "$GITHUB_OUTPUT"', current_run)
        self.assertLess(
            names.index("Ensure release PR branch is current"),
            names.index("Validate generated release diff"),
        )
        self.assertLess(
            names.index("Validate generated release diff"),
            names.index("Check out release PR branch"),
        )
        self.assertEqual(
            checkout["with"]["ref"],
            "${{ steps.current_release_pr.outputs.head_sha }}",
        )
        self.assertIs(checkout["with"]["persist-credentials"], False)
        self.assertNotIn("token", checkout["with"])
        self.assertEqual(
            diff_validation["env"]["EXPECTED_HEAD_SHA"],
            "${{ steps.current_release_pr.outputs.head_sha }}",
        )
        self.assertNotIn("mergeStateStatus", current_run)

    def test_generated_diff_allowlist_precedes_checkout_and_branch_execution(self) -> None:
        names = [step["name"] for step in self.steps]
        validation_run = self.steps_by_name["Validate generated release diff"]["run"]

        self.assertLess(
            names.index("Validate generated release diff"),
            names.index("Check out release PR branch"),
        )
        self.assertLess(
            names.index("Validate generated release diff"),
            names.index("Validate generated release head"),
        )
        for expected in (
            "headRefOid,changedFiles",
            "pulls/$PR_NUMBER/files?per_page=100",
            "--paginate",
            "--slurp",
            'if [ "$listed_files" != "$changed_files" ]',
            'if [ "$live_head_sha" != "$EXPECTED_HEAD_SHA" ]',
            '"CHANGELOG.md"',
            '".release-please-manifest.json"',
            '"pyproject.toml"',
            '"uv.lock"',
        ):
            self.assertIn(expected, validation_run)

        cases = {
            "allowed": (
                [[{"filename": "CHANGELOG.md"}, {"filename": "uv.lock"}]],
                0,
            ),
            "unexpected": (
                [[{"filename": "CHANGELOG.md"}, {"filename": "scripts/payload.sh"}]],
                1,
            ),
            "incomplete": (
                [[{"filename": "CHANGELOG.md"}]],
                1,
            ),
        }
        for name, (files, expected_failure) in cases.items():
            with self.subTest(name=name):
                changed_files = 2
                harness = f"""
set -e -o pipefail
gh() {{
  if [[ "$*" == *"--json headRefOid,changedFiles"* ]]; then
    printf '{{"headRefOid":"%s","changedFiles":%s}}\\n' "$EXPECTED_HEAD_SHA" "$CHANGED_FILES"
  elif [[ "$*" == *"pulls/$PR_NUMBER/files?per_page=100"* ]]; then
    printf '%s\\n' "$FILES_JSON"
  elif [[ "$*" == *"--json headRefOid --jq .headRefOid"* ]]; then
    printf '%s\\n' "$EXPECTED_HEAD_SHA"
  fi
}}
{validation_run}
"""
                env = {
                    **os.environ,
                    "CHANGED_FILES": str(changed_files),
                    "EXPECTED_HEAD_SHA": "a1" * 20,
                    "FILES_JSON": json.dumps(files),
                    "PR_NUMBER": "123",
                    "REPO": "example/repo",
                }
                result = subprocess.run(
                    ["bash", "-c", harness],
                    check=False,
                    capture_output=True,
                    env=env,
                    text=True,
                )
                self.assertEqual(result.returncode != 0, bool(expected_failure), result.stderr)

    def test_validation_is_nonmutating_and_precedes_live_head_gate(self) -> None:
        names = [step["name"] for step in self.steps]
        validation_run = self.steps_by_name["Validate generated release head"]["run"]

        self.assertEqual(
            validation_run.splitlines(),
            [
                "uv lock --check",
                "uv run --locked python scripts/check-public-boundary.py",
                "git diff --exit-code -- CHANGELOG.md .release-please-manifest.json pyproject.toml uv.lock",
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
        self.assertIn("steps.release_pr.outputs.has_release_pr == 'true'", publish["if"])
        self.assertIn("steps.verify_release_pr_head.outcome == 'success'", publish["if"])
        self.assertLess(publish_run.index("gh pr ready"), publish_run.index("gh pr review"))
        self.assertLess(publish_run.index("gh pr review"), publish_run.index("gh pr merge"))
        self.assertIn("compare/$BASE_BRANCH...$live_head_sha", publish_run)
        self.assertIn("protection/required_status_checks", publish_run)
        self.assertIn("Administration read permission", publish_run)
        self.assertIn('if [ "$strict" != "true" ]', publish_run)
        self.assertIn("trap redraft_on_failure EXIT", publish_run)
        self.assertLess(
            publish_run.index("trap redraft_on_failure EXIT"),
            publish_run.index('gh pr ready "$PR_NUMBER" --repo "$REPO"\n'),
        )
        self.assertIn("trap - EXIT", publish_run)
        self.assertIn("changed or became unmergeable during ready transition", publish_run)
        self.assertIn("changed or became unmergeable before auto-merge", publish_run)
        self.assertIn('if [ "$ENABLE_AUTO_MERGE" = "true" ]', publish_run)
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
                "ENABLE_AUTO_MERGE": "true",
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
