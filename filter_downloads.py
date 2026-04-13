#!/usr/bin/env python3
"""
Find files < 15 MB in archive_root/downloads/icpsr and archive_root/downloads/ukdataservice,
list them, then update .gitignore so only files >= 15 MB in those folders are ignored.
"""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).parent
LIMIT_BYTES = 15 * 1024 * 1024  # 15 MB

TARGET_DIRS = [
    REPO_ROOT / "archive_root" / "downloads" / "icpsr",
    REPO_ROOT / "archive_root" / "downloads" / "ukdataservice",
]

GITIGNORE_PATH = REPO_ROOT / ".gitignore"


def collect_files(dirs):
    small, large = [], []
    for target in dirs:
        for path in sorted(target.rglob("*")):
            if path.is_file():
                size = path.stat().st_size
                rel = path.relative_to(REPO_ROOT)
                if size < LIMIT_BYTES:
                    small.append((rel, size))
                else:
                    large.append((rel, size))
    return small, large


def fmt_mb(size_bytes):
    return f"{size_bytes / 1024 / 1024:.2f} MB"


def update_gitignore(large_files):
    text = GITIGNORE_PATH.read_text()

    # Remove the blanket downloads/ ignore
    lines = text.splitlines()
    lines = [l for l in lines if l.strip() != "archive_root/downloads/"]

    # Build the new block
    large_section = [
        "",
        "# Large files (>= 15 MB) in icpsr and ukdataservice — excluded from git",
    ]
    for rel, _ in sorted(large_files):
        large_section.append(str(rel))

    new_text = "\n".join(lines + large_section) + "\n"
    GITIGNORE_PATH.write_text(new_text)
    print(f"Updated {GITIGNORE_PATH}")


def main():
    small, large = collect_files(TARGET_DIRS)

    print("=" * 70)
    print(f"FILES < 15 MB  (will be tracked by git) — {len(small)} files")
    print("=" * 70)
    for rel, size in small:
        print(f"  {fmt_mb(size):>10}  {rel}")

    print()
    print("=" * 70)
    print(f"FILES >= 15 MB (will be ignored by git) — {len(large)} files")
    print("=" * 70)
    for rel, size in large:
        print(f"  {fmt_mb(size):>10}  {rel}")

    print()
    update_gitignore(large)
    print(f"\nDone. {len(small)} files will be tracked, {len(large)} files ignored.")


if __name__ == "__main__":
    main()
