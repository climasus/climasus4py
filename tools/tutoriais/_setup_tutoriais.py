"""Shared setup for the tutorial generator scripts.

Mirrors climasus4r's ``scripts/_setup_tutoriais.R``: the "Climate Forest"
palette, a plotnine theme matching ``tema_climasus()``, and ``save_fig``.

The tutorial pages show code without running it; the figures they display
are produced once by the ``tutorial_*.py`` scripts next to this file, run
from any working directory (the DATASUS cache, ``dados/cache``, is created
there):

    python tools/tutoriais/tutorial_01_importacao.py
"""

from __future__ import annotations

from pathlib import Path

from plotnine import element_blank, element_line, element_rect, element_text, theme, theme_minimal

REPO = Path(__file__).resolve().parents[2]
FIG_DIR = REPO / "docs" / "tutoriais" / "figuras"

# Same values as cf_palette in climasus4r.
CF_PALETTE = {
    "primary": "#2E7D32",    # verde floresta
    "secondary": "#558B2F",  # verde folha
    "accent": "#EF6C00",     # laranja queimado
    "text_main": "#1B5E20",  # texto escuro
    "bg_page": "#F1F8E9",    # fundo menta suave
}
CF_COLORS = [CF_PALETTE["primary"], CF_PALETTE["secondary"], CF_PALETTE["accent"],
             "#A5D6A7", "#1B5E20"]


def tema_climasus(base_size: int = 12):
    """plotnine counterpart of climasus4r's ``tema_climasus()``."""
    return theme_minimal(base_size=base_size) + theme(
        plot_title=element_text(color=CF_PALETTE["text_main"], weight="bold",
                                size=base_size * 1.25),
        plot_subtitle=element_text(color=CF_PALETTE["secondary"], size=base_size),
        plot_caption=element_text(color="#52665A", size=base_size * 0.8, ha="left"),
        axis_title=element_text(color=CF_PALETTE["text_main"], size=base_size * 0.95),
        axis_text=element_text(color="#3A4A3E"),
        panel_grid_major=element_line(color="#E0E0E0", size=0.35),
        panel_grid_minor=element_blank(),
        legend_title=element_text(color=CF_PALETTE["text_main"], weight="bold"),
        legend_position="bottom",
        plot_background=element_rect(fill="white", color="white"),
        panel_background=element_rect(fill="white", color="white"),
        strip_text=element_text(color=CF_PALETTE["text_main"], weight="bold"),
    )


def save_fig(plot, name: str, width: float = 8, height: float = 5, dpi: int = 150) -> Path:
    """Save *plot* as ``docs/tutoriais/figuras/<name>.png``."""
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    caminho = FIG_DIR / f"{name}.png"
    plot.save(caminho, width=width, height=height, dpi=dpi, verbose=False)
    print(f"   figura: {caminho.relative_to(REPO)}")
    return caminho
