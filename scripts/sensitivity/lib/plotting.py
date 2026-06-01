"""plotting.py — thesis style helpers (consistent fonts, DPI, colors).

Call setup_thesis_style() once at the top of every notebook.
save_figure(fig, name) exports both PDF and PNG to figures/.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns

THESIS_DPI = 300
FIGURES_DIR = Path("figures")


def setup_thesis_style() -> None:
    """Apply the project-wide thesis matplotlib style."""
    sns.set_theme(style="whitegrid", context="paper")
    plt.rcParams.update({
        "figure.dpi": THESIS_DPI,
        "savefig.dpi": THESIS_DPI,
        "font.family": "serif",
        "font.serif": ["Times New Roman", "DejaVu Serif", "Computer Modern"],
        "font.size": 10,
        "axes.titlesize": 11,
        "axes.labelsize": 10,
        "legend.fontsize": 9,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
    })
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def save_figure(fig: plt.Figure, name: str) -> None:
    """Save fig to figures/<name>.pdf and figures/<name>.png."""
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIGURES_DIR / f"{name}.pdf", bbox_inches="tight")
    fig.savefig(FIGURES_DIR / f"{name}.png", bbox_inches="tight")
