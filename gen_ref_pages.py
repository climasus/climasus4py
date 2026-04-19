"""Generate the API reference pages automatically from source code.

This script is executed by mkdocs-gen-files during the build process.
It walks the src/climasus package tree, creates one Markdown page per
public module, and writes a SUMMARY.md consumed by mkdocs-literate-nav
to build the navigation automatically.
"""

from pathlib import Path

import mkdocs_gen_files

ROOT = Path(__file__).parent.parent
SRC = ROOT / "src"

nav = mkdocs_gen_files.Nav()

for path in sorted(SRC.rglob("*.py")):
    module_path = path.relative_to(SRC).with_suffix("")
    doc_path = path.relative_to(SRC).with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    parts = tuple(module_path.parts)

    # __init__ → index page for the package
    if parts[-1] == "__init__":
        parts = parts[:-1]
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")
    # skip private / version modules
    elif parts[-1].startswith("_"):
        continue

    if not parts:
        continue

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        ident = ".".join(parts)
        fd.write(f"# `{ident}`\n\n::: {ident}\n")

    mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(ROOT))

# Write SUMMARY.md — consumed by literate-nav to auto-build the nav tree
with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
