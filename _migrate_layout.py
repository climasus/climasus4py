"""Script de migração: reorganiza src/climasus4py/ → climasus4py/ na raiz."""
import re
import shutil
import pathlib

BASE = pathlib.Path(__file__).parent
SRC = BASE / "src" / "climasus4py"
PKG = BASE / "climasus4py"

# Limpa o destino (pode ter resquícios)
if PKG.exists():
    shutil.rmtree(PKG)

# Copia src/climasus4py/ → climasus4py/ (preserva estrutura interna)
shutil.copytree(SRC, PKG)

# Remove src/ inteiro
shutil.rmtree(BASE / "src")

# Converte imports absolutos → relativos em todos os arquivos .py do pacote
def convert_imports(pkg_root: pathlib.Path) -> None:
    for pyfile in pkg_root.rglob("*.py"):
        rel = pyfile.relative_to(pkg_root)
        depth = len(rel.parts) - 1  # 0 = top-level, 1 = subpackage

        original = pyfile.read_text(encoding="utf-8")
        content = original

        if depth == 0:
            # src/__init__.py: from climasus4py. → from .
            content = re.sub(r"from climasus4py\.", "from .", content)
        else:
            # dentro de um subpacote (core/, enrichment/, io/, utils/)
            subpkg = rel.parts[0]  # e.g. "core"
            # mesma subpacote: from climasus4py.<subpkg>. → from .
            content = re.sub(rf"from climasus4py\.{subpkg}\.", "from .", content)
            # outros subpacotes: from climasus4py.<other>. → from ..<other>.
            content = re.sub(
                r"from climasus4py\.([a-z_]+)\.",
                lambda m: f"from ..{m.group(1)}.",
                content,
            )
            # from climasus4py.<other> import (sem dot final — utils import X)
            content = re.sub(
                r"from climasus4py\.([a-z_]+) import",
                lambda m: (
                    "from . import" if m.group(1) == subpkg
                    else f"from ..{m.group(1)} import"
                ),
                content,
            )

        if content != original:
            pyfile.write_text(content, encoding="utf-8")
            print(f"  updated: {rel}")

print("Convertendo imports...")
convert_imports(PKG)

# Verifica que não sobrou nenhum import absoluto de climasus4py
bad = []
for f in PKG.rglob("*.py"):
    for i, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
        if re.search(r"from climasus4py\.", line):
            bad.append(f"{f.relative_to(PKG)}:{i}: {line.strip()}")
if bad:
    print("AVISO — imports absolutos restantes:")
    for b in bad:
        print(f"  {b}")
else:
    print("OK — nenhum import absoluto restante.")

files = sorted(str(f.relative_to(PKG)) for f in PKG.rglob("*.py") if "__pycache__" not in str(f))
print(f"\nclimasas4py/ criado com {len(files)} arquivos .py")
