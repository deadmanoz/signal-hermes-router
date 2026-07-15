#!/usr/bin/env python3
from __future__ import annotations

import re
import subprocess
import sys
from ipaddress import ip_address
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]

SKIP_DIRS = {
    ".git",
    ".venv",
    ".beads",
    ".claude",
    "__pycache__",
    ".ruff_cache",
    "dist",
    "htmlcov",
    "private",
}
SKIP_FILES = {"uv.lock", "security_best_practices_report.md"}
TEXT_SUFFIXES = {".md", ".py", ".toml", ".yaml", ".yml", ".txt", ".json"}
FORBIDDEN_FILENAMES = {"config.yaml", "routes.yaml"}

PHONE_RE = re.compile(r"\+\d{6,15}")
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
LONG_BASE64_RE = re.compile(r"\b[A-Za-z0-9+/]{40,}={0,2}\b")
HEX_RE = re.compile(r"\b[0-9a-fA-F]{40,}\b")
URL_RE = re.compile(r"https?://[^\s>)\"']+")
GITHUB_COMMIT_URL_RE = re.compile(
    r"https:"
    r"//github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/commit/[0-9a-fA-F]{40}"
    r"(?=$|[\s)\]}>.,;:'\"])"
)
GROUP_ID_RE = re.compile(r"\bgroup_id\s*[:=]\s*(?:[\"']([^\"']+)[\"']|([^#\s]+))", re.IGNORECASE)
FRIENDLY_NAME_RE = re.compile(
    r"\bfriendly_name\s*[:=]\s*(?:[\"']([^\"']+)[\"']|([^#\n]+))", re.IGNORECASE
)
SECRET_ASSIGNMENT_RE = re.compile(
    r"\b(?:api[_-]?key|password|passwd|token|secret)\b\s*[:=]\s*[\"'][^\"']+[\"']",
    re.IGNORECASE,
)
DEPLOYMENT_URL_CONTEXT_RE = re.compile(
    r"\b(?:base[_-]?url|signal[_-]?base[_-]?url|endpoint|host|hostname|server|daemon)\b",
    re.IGNORECASE,
)
PRIVATE_HOST_SUFFIXES = (
    ".corp",
    ".home",
    ".internal",
    ".lan",
    ".local",
    ".private",
)

ALLOWED_PHONES = {"+00000000000"}
ALLOWED_UUIDS = {"00000000-0000-0000-0000-000000000000"}
ALLOWED_URL_HOSTS = {
    "127.0.0.1",
    "::1",
    "localhost",
    "github.com",
    "hermes-agent.nousresearch.com",
    "signal.test",
    "test",
}
ALLOWED_ROUTE_VALUES = {
    "GROUP",
    "OTHER",
    "SIGNAL_GROUP_ID_BASE64_EXAMPLE",
    "group",
    "group-id",
    "group-id=",
    "group-one",
    "group-two",
    "missing-group",
    "shadow-group",
}


def main() -> int:
    findings: list[str] = []
    for relative in candidate_files():
        path = ROOT / relative
        if relative.name in FORBIDDEN_FILENAMES:
            findings.append(f"{relative}: private deployment filename must not be public")
            continue
        if not should_scan(relative, path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        check_text(relative, text, findings)

    if findings:
        print("Public/private boundary check failed:", file=sys.stderr)
        for finding in findings:
            print(f"  - {finding}", file=sys.stderr)
        return 1
    print("Public/private boundary check passed")
    return 0


def candidate_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    if result.returncode == 0:
        return [Path(line) for line in result.stdout.splitlines() if line]
    return [path.relative_to(ROOT) for path in ROOT.rglob("*") if path.is_file()]


def should_scan(relative: Path, path: Path) -> bool:
    if relative.name in SKIP_FILES:
        return False
    if any(part in SKIP_DIRS for part in relative.parts):
        return False
    if path.is_symlink():
        return False
    return relative.suffix in TEXT_SUFFIXES or relative.name in {"LICENSE", ".gitignore"}


def check_text(relative: Path, text: str, findings: list[str]) -> None:
    for line_number, line in enumerate(text.splitlines(), 1):
        public_commit_spans = [match.span() for match in GITHUB_COMMIT_URL_RE.finditer(line)]
        for phone in PHONE_RE.findall(line):
            if phone not in ALLOWED_PHONES:
                findings.append(f"{relative}:{line_number}: phone-like identifier {phone!r}")
        for uuid in UUID_RE.findall(line):
            if uuid.lower() not in ALLOWED_UUIDS:
                findings.append(f"{relative}:{line_number}: UUID-like identifier {uuid!r}")
        for match in LONG_BASE64_RE.finditer(line):
            value = match.group(0)
            if HEX_RE.fullmatch(value):
                continue
            if any(
                allowed_start <= match.start() and match.end() <= allowed_end
                for allowed_start, allowed_end in public_commit_spans
            ):
                continue
            if "EXAMPLE" not in value and "PLACEHOLDER" not in value:
                findings.append(f"{relative}:{line_number}: long base64-like value")
        for match in GROUP_ID_RE.finditer(line):
            if match.group(1) is None and not _allow_unquoted_route_value(relative):
                continue
            value = _first_match_group(match)
            if value not in ALLOWED_ROUTE_VALUES and "EXAMPLE" not in value.upper():
                findings.append(f"{relative}:{line_number}: non-synthetic group_id {value!r}")
        for match in FRIENDLY_NAME_RE.finditer(line):
            if match.group(1) is None and not _allow_unquoted_route_value(relative):
                continue
            value = _first_match_group(match).strip()
            if not any(token in value.lower() for token in ("example", "synthetic", "test")):
                findings.append(f"{relative}:{line_number}: non-synthetic friendly_name {value!r}")
        if SECRET_ASSIGNMENT_RE.search(line) and "ROUTER_TEST_SECRET" not in line:
            findings.append(f"{relative}:{line_number}: possible inline secret assignment")
        check_urls(relative, line_number, line, findings)


def check_urls(relative: Path, line_number: int, line: str, findings: list[str]) -> None:
    for value in URL_RE.findall(line):
        value = value.rstrip(".,;:")
        parsed = urlparse(value)
        hostname = (parsed.hostname or "").lower()
        if _hostname_allowed(relative, line, hostname):
            continue
        findings.append(f"{relative}:{line_number}: unexpected hostname {hostname!r}")


def _hostname_allowed(relative: Path, line: str, hostname: str) -> bool:
    if hostname in ALLOWED_URL_HOSTS:
        return True
    if hostname.endswith((".example", ".invalid", ".test")):
        return True
    if _is_loopback_ip(hostname):
        return True
    if relative.suffix.lower() == ".md":
        return _is_public_markdown_reference(line, hostname)
    return False


def _is_public_markdown_reference(line: str, hostname: str) -> bool:
    if DEPLOYMENT_URL_CONTEXT_RE.search(line):
        return False
    if _is_private_like_host(hostname):
        return False
    return "." in hostname and re.fullmatch(r"[a-z0-9.-]+", hostname) is not None


def _is_private_like_host(hostname: str) -> bool:
    if not hostname:
        return True
    try:
        parsed = ip_address(hostname)
    except ValueError:
        if "." not in hostname:
            return True
        return hostname.endswith(PRIVATE_HOST_SUFFIXES)
    return parsed.is_private or parsed.is_link_local or parsed.is_reserved


def _is_loopback_ip(hostname: str) -> bool:
    try:
        return ip_address(hostname).is_loopback
    except ValueError:
        return False


def _first_match_group(match: re.Match[str]) -> str:
    for value in match.groups():
        if value is not None:
            return value
    return ""


def _allow_unquoted_route_value(relative: Path) -> bool:
    return relative.suffix.lower() in {".md", ".txt", ".yaml", ".yml"}


if __name__ == "__main__":
    raise SystemExit(main())
