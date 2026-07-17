from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "check_release_head", ROOT / "scripts" / "check-release-head.py"
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def release_files(version: str, previous: str | None = None) -> dict[str, str]:
    changelog = "# Changelog\n\n"
    if previous is not None:
        changelog += (
            f"## [{version}](https://github.com/example/project/compare/v{previous}...v{version}) "
            "(2026-07-15)\n\n### Bug Fixes\n\n* release fix\n\n"
        )
    changelog += "## [0.1.27](https://github.com/example/project/releases/tag/v0.1.27)\n"
    return {
        "CHANGELOG.md": changelog,
        ".release-please-manifest.json": json.dumps({".": version}),
        "pyproject.toml": (
            f'[project]\nname = "signal-hermes-router"\nversion = "{version}"\ndependencies = []\n'
        ),
        "uv.lock": (
            'version = 1\nrevision = 3\nrequires-python = ">=3.12"\n\n'
            '[[package]]\nname = "signal-hermes-router"\n'
            f'version = "{version}"\nsource = {{ editable = "." }}\n'
        ),
    }


class ReleaseHeadValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base = release_files("0.1.27")
        self.head = release_files("0.1.28", previous="0.1.27")

    def test_accepts_only_version_changes_and_prepended_changelog(self) -> None:
        self.assertEqual(MODULE.validate_release_head(self.base, self.head, "0.1.28"), [])

    def test_rejects_non_version_project_changes(self) -> None:
        self.head["pyproject.toml"] += 'description = "injected"\n'
        self.assertIn(
            "pyproject.toml changes more than the project version",
            MODULE.validate_release_head(self.base, self.head, "0.1.28"),
        )

    def test_rejects_non_version_lock_changes(self) -> None:
        self.head["uv.lock"] += '\n[[package]]\nname = "injected"\nversion = "1.0.0"\n'
        self.assertIn(
            "uv.lock changes more than the project package version",
            MODULE.validate_release_head(self.base, self.head, "0.1.28"),
        )

    def test_rejects_manifest_shape_changes(self) -> None:
        self.head[".release-please-manifest.json"] = json.dumps({".": "0.1.28", "other": "9.9.9"})
        self.assertIn(
            "release manifest must contain only a string '.' version",
            MODULE.validate_release_head(self.base, self.head, "0.1.28"),
        )

    def test_rejects_rewritten_changelog_history(self) -> None:
        self.head["CHANGELOG.md"] = self.head["CHANGELOG.md"].replace("0.1.27", "0.0.0")
        self.assertIn(
            "CHANGELOG.md must prepend one release section without changing history",
            MODULE.validate_release_head(self.base, self.head, "0.1.28"),
        )

    def test_rejects_incorrect_expected_version(self) -> None:
        self.assertIn(
            "release manifest version does not match the expected release version",
            MODULE.validate_release_head(self.base, self.head, "0.1.29"),
        )

    def test_rejects_multiple_prepended_release_sections(self) -> None:
        extra_section = (
            "## [0.1.29](https://github.com/example/project/compare/v0.1.28...v0.1.29) "
            "(2026-07-16)\n\n### Bug Fixes\n\n* extra release\n\n"
        )
        self.head["CHANGELOG.md"] = self.head["CHANGELOG.md"].replace(
            "## [0.1.27]", extra_section + "## [0.1.27]"
        )
        self.assertIn(
            "CHANGELOG.md must prepend exactly one release section",
            MODULE.validate_release_head(self.base, self.head, "0.1.28"),
        )


if __name__ == "__main__":
    unittest.main()
