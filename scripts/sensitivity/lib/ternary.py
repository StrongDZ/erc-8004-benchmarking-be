"""ternary.py — Ternary scatter helper for the composite-weight simplex.

The 4-weight simplex (w_rep, w_svc, w_pub, w_com) is 3-D; we project to a 2-D
ternary plot for any three of the weights and colour by a metric (e.g. Spearman ρ)
or by the omitted fourth weight. Each plotted point's three coordinates need not
sum to 1 — mpltern renormalises per point, so dropping the 4th weight is fine for
visual inspection.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import mpltern  # noqa: F401 — registers the "ternary" projection on import
import pandas as pd


def ternary_scatter(
    df: pd.DataFrame,
    t_col: str,
    l_col: str,
    r_col: str,
    color_col: str,
    cmap: str = "viridis",
    title: str | None = None,
) -> plt.Figure:
    """Scatter df rows on a ternary plot.

    df must contain t_col, l_col, r_col (the three corner axes) and color_col.
    Returns the Figure for the caller to persist via save_figure().
    """
    fig = plt.figure(figsize=(7, 6))
    ax = fig.add_subplot(projection="ternary")
    sc = ax.scatter(df[t_col], df[l_col], df[r_col], c=df[color_col], cmap=cmap, s=15)
    ax.set_tlabel(t_col)
    ax.set_llabel(l_col)
    ax.set_rlabel(r_col)
    cb = fig.colorbar(sc, ax=ax, shrink=0.7, pad=0.1)
    cb.set_label(color_col)
    if title:
        ax.set_title(title, pad=20)
    return fig
