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
        release = self.steps_by_name["Run Release Please"]

        self.assertEqual(
            self.workflow["jobs"]["release-please"]["defaults"]["run"]["shell"],
            "bash",
        )
        self.assertIs(self.config["draft-pull-request"], True)
        self.assertEqual(
            release["with"]["skip-github-pull-request"],
            "${{ steps.base_gate.outputs.skip_github_pull_request }}",
        )
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

    def test_stale_historical_run_skips_release_pr_synchronization(self) -> None:
        names = [step["name"] for step in self.steps]
        gate = self.steps_by_name["Require current release base"]
        gate_run = gate["run"]

        self.assertEqual(gate["id"], "base_gate")
        self.assertLess(
            names.index("Require current release base"),
            names.index("Capture pending release PR state"),
        )
        self.assertLess(
            names.index("Capture pending release PR state"),
            names.index("Run Release Please"),
        )
        self.assertEqual(
            self.steps_by_name["Capture pending release PR state"]["if"],
            "${{ steps.base_gate.outputs.sync_release_pr == 'true' }}",
        )
        self.assertIn('echo "sync_release_pr=false"', gate_run)
        self.assertIn('echo "skip_github_pull_request=true"', gate_run)
        self.assertIn("::warning::", gate_run)
        self.assertNotIn("exit 1", gate_run)

        harness = f"""
set -e -o pipefail
gh() {{
  printf '%s\\n' "$LIVE_BASE_SHA"
}}
{gate_run}
"""
        expected_base_sha = "a1" * 20
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "github-output"
            common_env = {
                **os.environ,
                "BASE_BRANCH": "main",
                "EXPECTED_BASE_SHA": expected_base_sha,
                "GITHUB_OUTPUT": str(output_path),
                "REPO": "example/repo",
            }
            current = subprocess.run(
                ["bash", "-c", harness],
                check=False,
                capture_output=True,
                env={**common_env, "LIVE_BASE_SHA": expected_base_sha},
                text=True,
            )
            self.assertEqual(current.returncode, 0, current.stderr)
            self.assertEqual(
                output_path.read_text(encoding="utf-8"),
                "sync_release_pr=true\nskip_github_pull_request=false\n",
            )

            output_path.unlink()
            stale = subprocess.run(
                ["bash", "-c", harness],
                check=False,
                capture_output=True,
                env={**common_env, "LIVE_BASE_SHA": "b2" * 20},
                text=True,
            )
            self.assertEqual(stale.returncode, 0, stale.stderr)
            self.assertEqual(
                output_path.read_text(encoding="utf-8"),
                "sync_release_pr=false\nskip_github_pull_request=true\n",
            )

    def test_stale_release_pr_postcondition_rejects_a_mutation(self) -> None:
        names = [step["name"] for step in self.steps]
        capture = self.steps_by_name["Capture stale release PR state"]
        capture_run = capture["run"]
        confirm = self.steps_by_name["Confirm stale run did not mutate release PR"]
        confirm_run = confirm["run"]

        self.assertEqual(capture["if"], "${{ steps.base_gate.outputs.sync_release_pr == 'false' }}")
        self.assertEqual(
            confirm["if"],
            "${{ steps.release.outcome == 'success' && steps.base_gate.outputs.sync_release_pr == 'false' }}",
        )
        self.assertLess(names.index("Run Release Please"), names.index(confirm["name"]))
        self.assertIn("skip-github-pull-request", confirm_run)
        self.assertIn("headRefName,baseRefName,headRefOid,isDraft,isCrossRepository", capture_run)
        self.assertIn("headRefName,baseRefName,headRefOid,isDraft,isCrossRepository", confirm_run)
        self.assertIn('if [ "$HAD_RELEASE_PR" = "false" ]', confirm_run)
        self.assertIn("stale release run created a release PR", confirm_run)
        self.assertIn('[ "$is_draft" != "$EXPECTED_IS_DRAFT" ]', confirm_run)

        harness = """
set -e -o pipefail
gh() {
  printf '%s\\n' "$LIVE_PRS"
}
"""

        def run_step(
            step: str, prs: list[dict[str, object]], extra_env: dict[str, str]
        ) -> tuple[subprocess.CompletedProcess[str], str]:
            with tempfile.TemporaryDirectory() as tmp:
                output_path = Path(tmp) / "github-output"
                result = subprocess.run(
                    ["bash", "-c", harness + step],
                    check=False,
                    capture_output=True,
                    env={
                        **os.environ,
                        "BASE_BRANCH": "main",
                        "GITHUB_OUTPUT": str(output_path),
                        "LIVE_PRS": json.dumps(prs),
                        "REPO": "example/repo",
                        **extra_env,
                    },
                    text=True,
                )
                output = output_path.read_text(encoding="utf-8") if output_path.exists() else ""
            return result, output

        canonical_pr: dict[str, object] = {
            "number": 123,
            "headRefName": "release-please--branches--main",
            "baseRefName": "main",
            "headRefOid": "a1" * 20,
            "isDraft": False,
            "isCrossRepository": False,
        }
        current, captured = run_step(capture_run, [canonical_pr], {})
        self.assertEqual(current.returncode, 0, current.stderr)
        self.assertEqual(
            captured,
            "has_pr=true\nnumber=123\nhead_sha=" + ("a1" * 20) + "\nis_draft=false\n",
        )

        cross_repository = {**canonical_pr, "isCrossRepository": True}
        cross_only, cross_output = run_step(capture_run, [cross_repository], {})
        self.assertEqual(cross_only.returncode, 0, cross_only.stderr)
        self.assertEqual(cross_output, "has_pr=false\n")

        multiple, _ = run_step(capture_run, [canonical_pr, canonical_pr], {})
        self.assertNotEqual(multiple.returncode, 0)

        malformed = {**canonical_pr, "baseRefName": "other"}
        non_canonical, _ = run_step(capture_run, [malformed], {})
        self.assertNotEqual(non_canonical.returncode, 0)

        existing, _ = run_step(
            confirm_run,
            [canonical_pr],
            {
                "HAD_RELEASE_PR": "true",
                "EXPECTED_HEAD_SHA": "a1" * 20,
                "EXPECTED_IS_DRAFT": "false",
            },
        )
        self.assertEqual(existing.returncode, 0, existing.stderr)

        changed, _ = run_step(
            confirm_run,
            [{**canonical_pr, "headRefOid": "b2" * 20}],
            {
                "HAD_RELEASE_PR": "true",
                "EXPECTED_HEAD_SHA": "a1" * 20,
                "EXPECTED_IS_DRAFT": "false",
            },
        )
        self.assertNotEqual(changed.returncode, 0)

        absent, _ = run_step(
            confirm_run,
            [],
            {"HAD_RELEASE_PR": "false", "EXPECTED_HEAD_SHA": "", "EXPECTED_IS_DRAFT": ""},
        )
        self.assertEqual(absent.returncode, 0, absent.stderr)

        created, _ = run_step(
            confirm_run,
            [canonical_pr],
            {"HAD_RELEASE_PR": "false", "EXPECTED_HEAD_SHA": "", "EXPECTED_IS_DRAFT": ""},
        )
        self.assertNotEqual(created.returncode, 0)

    def test_existing_pending_pr_is_drafted_before_release_please_runs(self) -> None:
        capture = self.steps_by_name["Capture pending release PR state"]
        capture_run = capture["run"]
        names = [step["name"] for step in self.steps]

        self.assertLess(
            names.index("Capture pending release PR state"),
            names.index("Run Release Please"),
        )
        for expected in (
            'repo_owner="${REPO%%/*}"',
            'release_branch="release-please--branches--$BASE_BRANCH"',
            "--method GET",
            "--paginate",
            "--slurp",
            '"repos/$REPO/pulls"',
            "-f state=open",
            '-f base="$BASE_BRANCH"',
            '-f head="$repo_owner:$release_branch"',
            "-f per_page=100",
            '--arg repo "$REPO"',
            '--arg release_branch "$release_branch"',
            ".head.repo.full_name == $repo",
            ".head.ref == $release_branch",
            "'.[0].draft'",
            "'.[0].head.sha'",
            "'.[0].auto_merge != null'",
            'if [ "$count" -gt 1 ]',
            'gh pr ready "$number" --repo "$REPO" --undo',
        ):
            self.assertIn(expected, capture_run)
        self.assertNotIn("release_title_prefix", capture_run)

    def test_release_pr_mutable_title_is_validated_after_drafting(self) -> None:
        names = [step["name"] for step in self.steps]
        identity = self.steps_by_name["Validate release PR identity"]
        identity_run = identity["run"]

        self.assertLess(
            names.index("Run Release Please"),
            names.index("Validate release PR identity"),
        )
        self.assertLess(
            names.index("Validate release PR identity"),
            names.index("Require stable release base"),
        )
        self.assertIn("steps.release_pr.outputs.has_release_pr == 'true'", identity["if"])
        self.assertEqual(identity["id"], "release_pr_identity")
        for expected in (
            'release_branch="release-please--branches--$BASE_BRANCH"',
            'release_title_prefix="chore($BASE_BRANCH): release "',
            "headRefName,baseRefName,title,isCrossRepository",
            'if [ "$cross_repository" != "false" ]',
            '[ "$head_ref" != "$release_branch" ]',
            '[ "$base_ref" != "$BASE_BRANCH" ]',
            '"$release_title_prefix"*',
            '[[ "$expected_version" =~ ^[0-9]+\\.[0-9]+\\.[0-9]+$ ]]',
            'validated_title="${release_title_prefix}${expected_version}"',
            "printf 'expected_version=%s\\n' \"$expected_version\"",
            "printf 'validated_title=%s\\n' \"$validated_title\"",
            '[ "$title" != "$EXPECTED_RELEASE_TITLE" ]',
        ):
            self.assertIn(expected, identity_run)
        self.assertEqual(
            identity["env"]["EXPECTED_RELEASE_TITLE"],
            "${{ steps.release_pr.outputs.expected_title }}",
        )

    def test_release_pr_identity_enforces_an_exact_safe_title(self) -> None:
        identity_run = self.steps_by_name["Validate release PR identity"]["run"]
        harness = f"""
set -e -o pipefail
gh() {{
  printf '{{"headRefName":"release-please--branches--%s","baseRefName":"%s","title":' "$BASE_BRANCH" "$BASE_BRANCH"
  printf '%s' "$PR_TITLE" | jq -Rsc .
  printf ',"isCrossRepository":false}}\\n'
}}
{identity_run}
"""

        def run_identity(
            title: str, expected_title: str | None = None
        ) -> subprocess.CompletedProcess[str]:
            with tempfile.TemporaryDirectory() as tmp:
                output_path = Path(tmp) / "github-output"
                return subprocess.run(
                    ["bash", "-c", harness],
                    check=False,
                    capture_output=True,
                    env={
                        **os.environ,
                        "BASE_BRANCH": "main",
                        "EXPECTED_RELEASE_TITLE": title
                        if expected_title is None
                        else expected_title,
                        "GITHUB_OUTPUT": str(output_path),
                        "PR_NUMBER": "123",
                        "PR_TITLE": title,
                        "REPO": "example/repo",
                    },
                    text=True,
                )

        valid_title = "chore(main): release 0.1.31"
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "github-output"
            valid = subprocess.run(
                ["bash", "-c", harness],
                check=False,
                capture_output=True,
                env={
                    **os.environ,
                    "BASE_BRANCH": "main",
                    "EXPECTED_RELEASE_TITLE": valid_title,
                    "GITHUB_OUTPUT": str(output_path),
                    "PR_NUMBER": "123",
                    "PR_TITLE": valid_title,
                    "REPO": "example/repo",
                },
                text=True,
            )
            self.assertEqual(valid.returncode, 0, valid.stderr)
            self.assertEqual(
                output_path.read_text(encoding="utf-8"),
                "expected_version=0.1.31\nvalidated_title=chore(main): release 0.1.31\n",
            )

        for title, expected_title in (
            ("chore(main): release 0.1.31-rc1", None),
            ("chore(main): release 0.1.31 extra", None),
            ("chore(main): release 0.1.31\nvalidated_title=attacker", None),
            (valid_title, "chore(main): release 0.1.32"),
        ):
            result = run_identity(title, expected_title)
            self.assertNotEqual(result.returncode, 0, result.stderr)

    def test_pending_pr_noop_enters_full_validation_and_recovers_draft(self) -> None:
        resolve = self.steps_by_name["Resolve release PR number"]
        resolve_run = resolve["run"]

        self.assertEqual(
            resolve["if"],
            "${{ steps.release.outcome == 'success' && steps.base_gate.outputs.sync_release_pr == 'true' }}",
        )
        self.assertIn('elif [ -n "$PENDING_PR_NUMBER" ]', resolve_run)
        self.assertIn('number="$PENDING_PR_NUMBER"', resolve_run)
        self.assertIn('branch="release-please--branches--$BASE_BRANCH"', resolve_run)
        self.assertIn(
            "expected_title=\"$(printf '%s' \"$PRS_JSON\" | jq -r '.[0].title')\"", resolve_run
        )
        self.assertIn('expected_title=""', resolve_run)
        self.assertIn('write_output "expected_title" "$expected_title"', resolve_run)
        self.assertIn('if [ "$PENDING_WAS_READY" = "true" ]', resolve_run)
        self.assertIn('enable_auto_merge="$PENDING_AUTO_MERGE"', resolve_run)
        self.assertIn('enable_auto_merge="true"', resolve_run)
        self.assertIn('write_output "has_release_pr" "true"', resolve_run)

        for name in (
            "Validate release PR identity",
            "Require stable release base",
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

    def test_release_base_must_remain_stable_before_checkout_and_validation(self) -> None:
        names = [step["name"] for step in self.steps]

        self.assertLess(
            names.index("Require stable release base"),
            names.index("Check out release PR branch"),
        )
        self.assertLess(
            names.index("Check out release PR branch"),
            names.index("Validate generated release head"),
        )
        current_run = self.steps_by_name["Require stable release base"]["run"]
        current = self.steps_by_name["Require stable release base"]
        checkout = self.steps_by_name["Check out release PR branch"]
        diff_validation = self.steps_by_name["Validate generated release diff"]
        self.assertNotIn("gh pr update-branch", current_run)
        self.assertEqual(current["env"]["EXPECTED_BASE_SHA"], "${{ github.sha }}")
        self.assertIn('commits/$BASE_BRANCH" --jq .sha', current_run)
        self.assertIn('[ "$live_base_sha" != "$EXPECTED_BASE_SHA" ]', current_run)
        self.assertIn("leaving the PR draft for the queued run to regenerate", current_run)
        self.assertIn("--json headRefOid,mergeable", current_run)
        self.assertIn("compare/$EXPECTED_BASE_SHA...$head_sha", current_run)
        self.assertIn("CONFLICTING", current_run)
        self.assertIn('[ "$behind_by" != "0" ]', current_run)
        self.assertEqual(current["id"], "current_release_pr")
        self.assertIn('echo "head_sha=$head_sha" >> "$GITHUB_OUTPUT"', current_run)
        self.assertLess(
            names.index("Require stable release base"),
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
        self.assertEqual(checkout["with"]["fetch-depth"], 0)
        self.assertNotIn("token", checkout["with"])
        self.assertEqual(
            diff_validation["env"]["EXPECTED_HEAD_SHA"],
            "${{ steps.current_release_pr.outputs.head_sha }}",
        )
        self.assertNotIn("mergeStateStatus", current_run)

    def test_stable_base_gate_rejects_an_advanced_main(self) -> None:
        current_run = self.steps_by_name["Require stable release base"]["run"]
        harness = f"""
set -e -o pipefail
gh() {{
  if [[ "$*" == *"commits/$BASE_BRANCH"* ]]; then
    printf '%s\\n' "$LIVE_BASE_SHA"
  elif [[ "$*" == *"--json headRefOid,mergeable"* ]]; then
    printf '{{"headRefOid":"%s","mergeable":"MERGEABLE"}}\\n' "$HEAD_SHA"
  elif [[ "$*" == *"compare/$EXPECTED_BASE_SHA...$HEAD_SHA"* ]]; then
    printf '0\\n'
  fi
}}
{current_run}
"""
        expected_base_sha = "a1" * 20
        common_env = {
            **os.environ,
            "BASE_BRANCH": "main",
            "EXPECTED_BASE_SHA": expected_base_sha,
            "GITHUB_OUTPUT": os.devnull,
            "HEAD_SHA": "b2" * 20,
            "PR_NUMBER": "123",
            "REPO": "example/repo",
        }
        stable = subprocess.run(
            ["bash", "-c", harness],
            check=False,
            capture_output=True,
            env={**common_env, "LIVE_BASE_SHA": expected_base_sha},
            text=True,
        )
        advanced = subprocess.run(
            ["bash", "-c", harness],
            check=False,
            capture_output=True,
            env={**common_env, "LIVE_BASE_SHA": "c3" * 20},
            text=True,
        )

        self.assertEqual(stable.returncode, 0, stable.stderr)
        self.assertNotEqual(advanced.returncode, 0)
        self.assertIn("queued run to regenerate", advanced.stdout)

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
            'expected_paths=\'[".release-please-manifest.json","CHANGELOG.md","pyproject.toml","uv.lock"]\'',
            '.status != "modified" or .previous_filename != null',
            'if [ "$actual_paths" != "$expected_paths" ]',
            '"CHANGELOG.md"',
            '".release-please-manifest.json"',
            '"pyproject.toml"',
            '"uv.lock"',
        ):
            self.assertIn(expected, validation_run)

        cases = {
            "allowed": (
                [
                    [
                        {"filename": ".release-please-manifest.json", "status": "modified"},
                        {"filename": "CHANGELOG.md", "status": "modified"},
                        {"filename": "pyproject.toml", "status": "modified"},
                        {"filename": "uv.lock", "status": "modified"},
                    ]
                ],
                4,
                0,
            ),
            "unexpected": (
                [
                    [
                        {"filename": ".release-please-manifest.json", "status": "modified"},
                        {"filename": "CHANGELOG.md", "status": "modified"},
                        {"filename": "pyproject.toml", "status": "modified"},
                        {"filename": "scripts/payload.sh", "status": "modified"},
                    ]
                ],
                4,
                1,
            ),
            "incomplete": (
                [[{"filename": "CHANGELOG.md", "status": "modified"}]],
                2,
                1,
            ),
            "deleted": (
                [
                    [
                        {"filename": ".release-please-manifest.json", "status": "modified"},
                        {"filename": "CHANGELOG.md", "status": "removed"},
                        {"filename": "pyproject.toml", "status": "modified"},
                        {"filename": "uv.lock", "status": "modified"},
                    ]
                ],
                4,
                1,
            ),
        }
        for name, (files, changed_files, expected_failure) in cases.items():
            with self.subTest(name=name):
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
                '"$(uv python find 3.12)" scripts/check-release-head.py --base-sha "${{ github.sha }}" --expected-version "$EXPECTED_VERSION"',
                "uv lock --check",
                '"$(uv python find 3.12)" scripts/check-public-boundary.py',
                "git diff --exit-code -- CHANGELOG.md .release-please-manifest.json pyproject.toml uv.lock",
            ],
        )
        self.assertEqual(
            self.steps_by_name["Validate generated release head"]["env"]["EXPECTED_VERSION"],
            "${{ steps.release_pr_identity.outputs.expected_version }}",
        )
        self.assertNotIn(
            "${{ steps.release_pr_identity.outputs.expected_version }}", validation_run
        )
        self.assertNotIn("uv run", validation_run)
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
        self.assertIn("compare/$EXPECTED_BASE_SHA...$live_head_sha", verify_run)
        self.assertIn('commits/$BASE_BRANCH" --jq .sha', verify_run)
        self.assertIn('if [ "$live_head_sha" != "$checked_out_sha" ]', verify_run)
        self.assertIn("::error::release PR head changed after validation", verify_run)
        self.assertIn("CONFLICTING", verify_run)
        self.assertIn('behind_by" != "0', verify_run)
        self.assertEqual(
            self.steps_by_name["Verify release PR head"]["env"]["EXPECTED_BASE_SHA"],
            "${{ github.sha }}",
        )
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
        self.assertLess(
            publish_run.index("gh pr review"),
            publish_run.index('gh pr merge "$PR_NUMBER"'),
        )
        self.assertIn("compare/$EXPECTED_BASE_SHA...$live_head_sha", publish_run)
        self.assertIn('commits/$BASE_BRANCH" --jq .sha', publish_run)
        self.assertEqual(publish["env"]["EXPECTED_BASE_SHA"], "${{ github.sha }}")
        self.assertEqual(
            publish["env"]["VALIDATED_TITLE"],
            "${{ steps.release_pr_identity.outputs.validated_title }}",
        )
        self.assertIn("protection/required_status_checks", publish_run)
        self.assertIn("Administration read permission", publish_run)
        self.assertIn('if [ "$strict" != "true" ]', publish_run)
        self.assertIn("gh pr merge --help", publish_run)
        self.assertIn("--match-head-commit; leaving release PR draft", publish_run)
        self.assertLess(
            publish_run.index("gh pr merge --help"),
            publish_run.index('gh pr ready "$PR_NUMBER" --repo "$REPO"\n'),
        )
        self.assertIn("trap redraft_on_failure EXIT", publish_run)
        self.assertLess(
            publish_run.index("trap redraft_on_failure EXIT"),
            publish_run.index('gh pr ready "$PR_NUMBER" --repo "$REPO"\n'),
        )
        self.assertIn("trap - EXIT", publish_run)
        self.assertIn(
            "base/head changed or became unmergeable during ready transition", publish_run
        )
        self.assertIn(
            "identity, base, or head changed or became unmergeable before auto-merge", publish_run
        )
        self.assertIn("headRefName,baseRefName,title,isCrossRepository,headRefOid", publish_run)
        self.assertIn(
            "headRefOid,headRefName,baseRefName,title,isCrossRepository,mergeable", publish_run
        )
        self.assertIn('"$title" != "$VALIDATED_TITLE"', publish_run)
        self.assertIn('if [ "$ENABLE_AUTO_MERGE" = "true" ]', publish_run)
        self.assertIn('--match-head-commit "$validated_sha"', publish_run)
        merge_run = publish_run[publish_run.index('gh pr merge "$PR_NUMBER"') :]
        self.assertIn('--subject "$VALIDATED_TITLE (#$PR_NUMBER)"', merge_run)
        self.assertNotIn("--body", merge_run)

    def test_publish_revalidates_identity_before_auto_merge(self) -> None:
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
  if [[ "$*" == "pr merge --help" ]]; then
    printf '%s\\n' '--match-head-commit'
  elif [[ "$*" == *"protection/required_status_checks"* ]]; then
    printf 'true\\n'
  elif [[ "$*" == *"commits/$BASE_BRANCH"* ]]; then
    printf '%s\\n' "$VALIDATED_SHA"
  elif [[ "$*" == *"/compare/"* ]]; then
    printf '0\\n'
  elif [[ "$*" == *"--json headRefName,baseRefName,title,isCrossRepository,headRefOid"* ]]; then
    printf '{{"headRefName":"release-please--branches--%s","baseRefName":"%s","title":"%s","isCrossRepository":false,"headRefOid":"%s"}}\\n' "$BASE_BRANCH" "$BASE_BRANCH" "$VALIDATED_TITLE" "$VALIDATED_SHA"
  elif [[ "$*" == *"--json headRefOid,headRefName,baseRefName,title,isCrossRepository,mergeable"* ]]; then
    printf '{{"headRefOid":"%s","headRefName":"release-please--branches--%s","baseRefName":"%s","title":"%s","isCrossRepository":false,"mergeable":"MERGEABLE"}}\\n' "$VALIDATED_SHA" "$BASE_BRANCH" "$BASE_BRANCH" "$VALIDATED_TITLE"
  elif [[ "$*" == *"--json headRefOid,mergeable"* ]]; then
    printf '{{"headRefOid":"%s","mergeable":"MERGEABLE"}}\\n' "$VALIDATED_SHA"
  elif [[ "$*" == *"--json headRefOid --jq .headRefOid"* ]]; then
    printf '%s\\n' "$VALIDATED_SHA"
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
                "EXPECTED_BASE_SHA": "a1" * 20,
                "PR_NUMBER": "123",
                "REPO": "example/repo",
                "VALIDATED_SHA": "a1" * 20,
                "VALIDATED_TITLE": "chore(main): release 0.1.31",
            }
            result = subprocess.run(
                ["bash", "-c", harness],
                check=False,
                capture_output=True,
                env=env,
                text=True,
            )

            self.assertEqual(
                result.returncode,
                0,
                f"{result.stdout}\n{result.stderr}\n{call_log.read_text(encoding='utf-8')}",
            )
            calls = call_log.read_text(encoding="utf-8")
            final_identity = (
                "pr view 123 --repo example/repo --json "
                "headRefOid,headRefName,baseRefName,title,isCrossRepository,mergeable\n"
            )
            self.assertIn(final_identity, calls)
            self.assertIn(
                "pr merge 123 --repo example/repo --auto --squash "
                "--subject chore(main): release 0.1.31 (#123) --delete-branch "
                f"--match-head-commit {env['VALIDATED_SHA']}\n",
                calls,
            )

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
  if [[ "$*" == "pr merge --help" ]]; then
    printf '%s\\n' '--match-head-commit'
  elif [[ "$*" == *"protection/required_status_checks"* ]]; then
    printf 'true\\n'
  elif [[ "$*" == *"compare/$BASE_BRANCH"* ]]; then
    printf '0\\n'
  elif [[ "$*" == *"--json headRefName,baseRefName,title,isCrossRepository,headRefOid"* ]]; then
    printf '{{"headRefName":"release-please--branches--%s","baseRefName":"%s","title":"%s","isCrossRepository":false,"headRefOid":"%s"}}\\n' "$BASE_BRANCH" "$BASE_BRANCH" "$VALIDATED_TITLE" "$VALIDATED_SHA"
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
                "VALIDATED_TITLE": "chore(main): release 0.1.31",
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
            self.assertLess(
                calls.index("pr ready 123 --repo example/repo\n"),
                calls.index("pr ready 123 --repo example/repo --undo\n"),
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
