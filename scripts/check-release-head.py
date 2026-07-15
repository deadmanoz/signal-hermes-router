#!/usr/bin/env python3
"""Validate that a release PR contains only Release Please transformations."""

from __future__ import annotations

import argparse
import copy
import json
import re
import subprocess
import tomllib
from pathlib import Path


GENERATED_PATHS = (
    "CHANGELOG.md",
    ".release-please-manifest.json",
    "pyproject.toml",
    "uv.lock",
)
VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


def _project_version(document: dict[str, object], source: str) -> str:
    project = document.get("project")
    if not isinstance(project, dict):
        raise ValueError(f"{source} has no project table")
    version = project.get("version")
    if not isinstance(version, str):
        raise ValueError(f"{source} has no project version")
    return version


def _lock_project_version(document: dict[str, object], source: str) -> str:
    packages = document.get("package")
    if not isinstance(packages, list):
        raise ValueError(f"{source} has no package array")
    matches = [
        item
        for item in packages
        if isinstance(item, dict) and item.get("name") == "signal-hermes-router"
    ]
    if len(matches) != 1 or not isinstance(matches[0].get("version"), str):
        raise ValueError(f"{source} must contain one versioned signal-hermes-router package")
    return matches[0]["version"]


def _normalize_project_version(document: dict[str, object], version: str) -> dict[str, object]:
    normalized = copy.deepcopy(document)
    project = normalized["project"]
    assert isinstance(project, dict)
    project["version"] = version
    return normalized


def _normalize_lock_version(document: dict[str, object], version: str) -> dict[str, object]:
    normalized = copy.deepcopy(document)
    packages = normalized["package"]
    assert isinstance(packages, list)
    for item in packages:
        if isinstance(item, dict) and item.get("name") == "signal-hermes-router":
            item["version"] = version
    return normalized


def validate_release_head(base: dict[str, str], head: dict[str, str]) -> list[str]:
    errors: list[str] = []
    try:
        base_manifest = json.loads(base[".release-please-manifest.json"])
        head_manifest = json.loads(head[".release-please-manifest.json"])
        base_pyproject = tomllib.loads(base["pyproject.toml"])
        head_pyproject = tomllib.loads(head["pyproject.toml"])
        base_lock = tomllib.loads(base["uv.lock"])
        head_lock = tomllib.loads(head["uv.lock"])

        if set(base_manifest) != {"."} or not isinstance(base_manifest["."], str):
            errors.append("base manifest must contain only a string '.' version")
            return errors
        if set(head_manifest) != {"."} or not isinstance(head_manifest["."], str):
            errors.append("release manifest must contain only a string '.' version")
            return errors

        base_version = base_manifest["."]
        release_version = head_manifest["."]
        if not VERSION_RE.fullmatch(base_version) or not VERSION_RE.fullmatch(release_version):
            errors.append("manifest versions must use numeric X.Y.Z form")
            return errors
        if tuple(map(int, release_version.split("."))) <= tuple(map(int, base_version.split("."))):
            errors.append("release version must increase from the base version")

        if _project_version(base_pyproject, "base pyproject.toml") != base_version:
            errors.append("base pyproject.toml version does not match the base manifest")
        if _lock_project_version(base_lock, "base uv.lock") != base_version:
            errors.append("base uv.lock version does not match the base manifest")
        if _project_version(head_pyproject, "release pyproject.toml") != release_version:
            errors.append("release pyproject.toml version does not match the release manifest")
        if _lock_project_version(head_lock, "release uv.lock") != release_version:
            errors.append("release uv.lock version does not match the release manifest")

        if _normalize_project_version(head_pyproject, base_version) != base_pyproject:
            errors.append("pyproject.toml changes more than the project version")
        if _normalize_lock_version(head_lock, base_version) != base_lock:
            errors.append("uv.lock changes more than the project package version")

        prefix = "# Changelog\n\n"
        base_changelog = base["CHANGELOG.md"]
        release_changelog = head["CHANGELOG.md"]
        if not base_changelog.startswith(prefix) or not release_changelog.startswith(prefix):
            errors.append("CHANGELOG.md must retain its canonical heading")
        else:
            base_body = base_changelog[len(prefix) :]
            release_body = release_changelog[len(prefix) :]
            if not base_body or not release_body.endswith(base_body):
                errors.append(
                    "CHANGELOG.md must prepend one release section without changing history"
                )
            else:
                new_section = release_body[: -len(base_body)]
                https_scheme = "https" + "://"
                github_host = re.escape("github" + ".com")
                heading = re.compile(
                    rf"^## \[{re.escape(release_version)}\]"
                    rf"\({https_scheme}{github_host}/[^/]+/[^/]+/compare/"
                    rf"v{re.escape(base_version)}\.\.\.v{re.escape(release_version)}\)"
                    r" \([0-9]{4}-[0-9]{2}-[0-9]{2}\)\n"
                )
                if not heading.match(new_section) or not new_section.strip():
                    errors.append(
                        "CHANGELOG.md release heading does not match the version transition"
                    )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, tomllib.TOMLDecodeError) as exc:
        errors.append(str(exc))
    return errors


def _read_base_files(base_sha: str) -> dict[str, str]:
    contents: dict[str, str] = {}
    for path in GENERATED_PATHS:
        result = subprocess.run(
            ["git", "show", f"{base_sha}:{path}"],
            check=True,
            capture_output=True,
            text=True,
        )
        contents[path] = result.stdout
    return contents


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-sha", required=True)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    base = _read_base_files(args.base_sha)
    head = {path: (root / path).read_text(encoding="utf-8") for path in GENERATED_PATHS}
    errors = validate_release_head(base, head)
    if errors:
        print("Release head validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1
    print("Release head transformations validated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
