#!/usr/bin/env python3
"""Generate categorized release notes from git log.

Usage: release_notes.py <prev_tag> <new_tag>
Prints markdown to stdout: sectioned highlights + full commit list.
Categories are keyword-matched on commit subjects (no conventional-commit
requirement).
"""

import re
import subprocess
import sys

CATEGORIES = [
    ("✨ Features", ["add", "feat", "implement", "wire", "introduce", "support"]),
    ("🐛 Fixes", ["fix", "bug", "retry", "scope", "correct", "resolve"]),
    ("📚 Docs", ["doc", "readme", "handoff", "guide"]),
    ("🔧 Chores", ["beads", "ci", "release", "workflow", "fmt", "chore", "bump", "installer", "install"]),
]


def git_log(prev: str, new: str):
    out = subprocess.run(
        ["git", "log", "--pretty=%h|%s", f"{prev}..{new}"],
        check=True, capture_output=True, text=True,
    ).stdout
    commits = []
    for line in out.splitlines():
        if "|" in line:
            sha, subject = line.split("|", 1)
            commits.append((sha.strip(), subject.strip()))
    return commits


def categorize(subject: str) -> str:
    s = subject.lower()
    for name, keywords in CATEGORIES:
        if any(k in s for k in keywords):
            return name
    return "📦 Other"


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    prev, new = sys.argv[1], sys.argv[2]
    commits = git_log(prev, new)
    if not commits:
        print("_No new commits._")
        return 0

    buckets: dict[str, list[tuple[str, str]]] = {}
    for sha, subject in commits:
        buckets.setdefault(categorize(subject), []).append((sha, subject))

    order = [name for name, _ in CATEGORIES] + ["📦 Other"]
    for name in order:
        items = buckets.get(name)
        if not items:
            continue
        print(f"### {name}")
        for sha, subject in items:
            print(f"- `{sha}` {subject}")
        print()

    print(f"**{len(commits)} commits** since `{prev}`")
    return 0


if __name__ == "__main__":
    sys.exit(main())
