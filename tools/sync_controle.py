"""Copia os CSVs de controle para dentro do repositorio, para versiona-los.

Os arquivos canonicos vivem UM NIVEL ACIMA da raiz do repo, em
``CLIMASUS/``, porque e de la que o ``Quadro geral.ipynb`` os le (seis
referencias) e o ``PLANO_FASE2.md`` os cita. Mover romperia esses
caminhos, entao o repositorio guarda copias em ``docs/controle/``.

Copia so tem valor se a divergencia for DETECTAVEL, e nao apenas
improvavel. Por isso o script compara os hashes antes e depois e falha
alto quando a copia no repo esta a frente do original -- o que significa
que alguem editou a copia, que e o unico jeito de perder trabalho aqui.

    python tools/sync_controle.py            # verifica e copia se preciso
    python tools/sync_controle.py --check    # so verifica, nao escreve
"""

from __future__ import annotations

import hashlib
import shutil
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
ORIGEM = RAIZ.parent
DESTINO = RAIZ / "docs" / "controle"

ARQUIVOS = ("MELHORIAS.csv", "PARIDADE_R_PYTHON_v7.csv")


def _hash(caminho: Path) -> str:
    return hashlib.sha256(caminho.read_bytes()).hexdigest()[:16]


def main() -> int:
    apenas_checar = "--check" in sys.argv
    DESTINO.mkdir(parents=True, exist_ok=True)
    problemas = 0

    for nome in ARQUIVOS:
        origem, destino = ORIGEM / nome, DESTINO / nome
        if not origem.exists():
            print(f"  FALTA no canonico: {origem}")
            problemas += 1
            continue

        h_origem = _hash(origem)
        h_destino = _hash(destino) if destino.exists() else None
        tam = origem.stat().st_size / 1024

        if h_origem == h_destino:
            print(f"  {nome:28s} {tam:6.0f} KB  em sincronia ({h_origem})")
            continue

        if apenas_checar:
            print(f"  {nome:28s} {tam:6.0f} KB  DIVERGENTE "
                  f"(canonico {h_origem}, repo {h_destino})")
            problemas += 1
            continue

        # Se a copia do repo for MAIS NOVA que o canonico, alguem editou o
        # lugar errado e copiar por cima apagaria esse trabalho.
        #
        # Mas mtime futuro nao e prova de edicao. O shutil.copy2 PRESERVA o
        # mtime da origem, entao uma sincronizacao feita quando o canonico
        # estava com relogio adiantado deixa o destino carimbado no futuro
        # para sempre -- e a partir dai toda sincronizacao seguinte era
        # recusada, mesmo com o canonico contendo estritamente mais coisa.
        # Aconteceu em 15/09/2026: a copia estava marcada 19:24 as 17:57,
        # com 224.016 bytes contra 227.826 do canonico. Um destino no
        # futuro e relogio, nao trabalho de alguem.
        agora = time.time()
        mt_destino = destino.stat().st_mtime if destino.exists() else 0.0
        mt_origem = origem.stat().st_mtime
        if mt_destino > agora + 1:
            print(f"  {nome:28s} AVISO: a copia esta carimbada no futuro "
                  f"({mt_destino - agora:.0f}s a frente); mtime ignorado "
                  f"nesta comparacao. Sincronizando pelo conteudo.")
        elif mt_destino > mt_origem + 1:
            print(f"  {nome:28s} RECUSADO: a copia em docs/controle/ e mais "
                  f"nova que o canonico em {ORIGEM.name}/. Edite o canonico, "
                  f"ou reconcilie a mao antes de sincronizar.")
            problemas += 1
            continue

        shutil.copy2(origem, destino)
        print(f"  {nome:28s} {tam:6.0f} KB  copiado ({h_destino} -> {h_origem})")

    return 1 if problemas else 0


if __name__ == "__main__":
    print(f"canonico: {ORIGEM}")
    print(f"repo    : {DESTINO.relative_to(RAIZ)}")
    raise SystemExit(main())
