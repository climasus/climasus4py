#!/usr/bin/env python3
"""migrate-from-v0.2.py — Migrate climasus4py v0.2 code to v0.3 API names.

Applies all 9 public function renames in-place (or --dry-run) across .py and .ipynb files.

Usage:
    python tools/migrate-from-v0.2.py .                    # migrate all .py/.ipynb
    python tools/migrate-from-v0.2.py src/ --dry-run       # preview only
    python tools/migrate-from-v0.2.py notebook.ipynb       # single file
"""
import argparse
import json
import re
import sys
from pathlib import Path

RENAMES = {
    "sus_import": "sus_data_import",
    "sus_clean": "sus_data_clean_encoding",
    "sus_standardize": "sus_data_standardize",
    "sus_variables": "sus_data_create_variables",
    "sus_aggregate": "sus_data_aggregate",
    "sus_read": "sus_data_read",
    "sus_quality": "sus_data_quality_report",
    "sus_spatial": "sus_spatial_join",
    "sus_chat_ai": "sus_chat",
}


def _apply_renames(text: str) -> tuple[str, list[str]]:
    changes = []
    for old, new in RENAMES.items():
        pattern = rf"\b{re.escape(old)}\b"
        if re.search(pattern, text):
            text = re.sub(pattern, new, text)
            changes.append(f"  {old} -> {new}")
    return text, changes


def _migrate_py(path: Path, dry_run: bool) -> bool:
    original = path.read_text(encoding="utf-8")
    migrated, changes = _apply_renames(original)
    if not changes:
        return False
    print(f"{'[DRY-RUN] ' if dry_run else ''}  {path}")
    for c in changes:
        print(c)
    if not dry_run:
        path.write_text(migrated, encoding="utf-8")
    return True


def _migrate_ipynb(path: Path, dry_run: bool) -> bool:
    nb = json.loads(path.read_text(encoding="utf-8"))
    changed = False
    for cell in nb.get("cells", []):
        if cell.get("cell_type") not in ("code", "markdown"):
            continue
        new_source = []
        for line in cell.get("source", []):
            new_line, chgs = _apply_renames(line)
            if chgs:
                changed = True
            new_source.append(new_line)
        cell["source"] = new_source
    if changed:
        print(f"{'[DRY-RUN] ' if dry_run else ''}  {path}")
        if not dry_run:
            path.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate climasus4py v0.2 -> v0.3 API names")
    parser.add_argument("path", nargs="?", default=".", help="File or directory to migrate (default: .)")  # noqa: E501
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    args = parser.parse_args()

    root = Path(args.path)
    files = [root] if root.is_file() else list(root.rglob("*.py")) + list(root.rglob("*.ipynb"))
    files = [f for f in files if ".git" not in str(f) and "__pycache__" not in str(f)]

    total = 0
    for f in sorted(files):
        if f.suffix == ".py":
            if _migrate_py(f, args.dry_run):
                total += 1
        elif f.suffix == ".ipynb":
            if _migrate_ipynb(f, args.dry_run):
                total += 1

    dry = args.dry_run
    print(f"\n{'[DRY-RUN] ' if dry else ''}{'Modified' if not dry else 'Would modify'}: {total} file(s).")  # noqa: E501
    sys.exit(0)


if __name__ == "__main__":
    main()
