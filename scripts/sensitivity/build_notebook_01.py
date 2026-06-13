"""One-shot builder for notebooks/01_cluster_a.ipynb.

Run from scripts/sensitivity/:  .venv/bin/python build_notebook_01.py
Keeps the notebook source in code (easy to regenerate / diff) rather than
hand-editing JSON. Safe to delete after the .ipynb is committed.
"""

import nbformat as nbf

nb = nbf.v4.new_notebook()
SNAP_ID = "snap_20260613_185128"

cells = []

cells.append(nbf.v4.new_markdown_cell(
    "# Chương 4.1 — Sensitivity Analysis của Cụm A: Scoring Core (Reputation v2)\n\n"
    "5 tham số: Alpha, TBaseDays (decay), C (confidence prior), Gamma, Theta "
    "(reliability).\n\n"
    "Mô hình reputation v2 (theo `scorerefresh/replay.go`):\n"
    "`Reputation = Quality·Confidence·Reliability·100`, với `Quality = A/B`, "
    "`A = Σ wᵢ·dᵢ·vᵢ`, `B = Σ wᵢ·dᵢ`, `Confidence = B/(C+B)`, "
    "`Reliability = 1/(1+γ·N^θ)`. Trọng số `wᵢ` nay là **quality-based** (`fb.Wi` "
    "persisted, từ Cụm C) — biến thể price-based (β, K) đã bị bỏ vì thiếu dữ liệu "
    "giá task, nên β/K KHÔNG còn trong cụm này.\n\n"
    "Output từ `sensitivity bench --cluster=A`. Đổi `SNAP_ID` ở cell dưới nếu chạy "
    "trên snapshot khác.\n\n"
    "> **Phát hiện chính (theo tornado trên chain-8453):** `TBaseDays` và `Alpha` "
    "(điều khiển decay) chi phối mạnh nhất, rồi tới `C` (confidence prior). "
    "`Gamma`/`Theta` (reliability) ảnh hưởng nhỏ vì streak fail hiếm trong dữ liệu "
    "quality-only."
))

cells.append(nbf.v4.new_code_cell(
    "import sys\n"
    "sys.path.insert(0, '..')\n"
    "from lib import setup_thesis_style, save_figure, load_oat, load_tornado, load_sobol, load_grid\n"
    "\n"
    "import matplotlib.pyplot as plt\n"
    "import seaborn as sns\n"
    "import numpy as np\n"
    "\n"
    "setup_thesis_style()\n"
    "\n"
    f"SNAP_ID = \"{SNAP_ID}\"  # snapshot ID from `sensitivity snapshot list`\n"
    "OUTPUT_DIR = f\"../output/{SNAP_ID}\"\n"
    "CLUSTER = \"A\""
))

# Fig 4.1.1 — OAT line plots
cells.append(nbf.v4.new_code_cell(
    "oat = load_oat(OUTPUT_DIR, CLUSTER)\n"
    "params = oat[\"param\"].unique()\n"
    "fig, axes = plt.subplots(nrows=4, ncols=3, figsize=(11, 12), sharey=False)\n"
    "axes = axes.flatten()\n"
    "for ax, p in zip(axes, params):\n"
    "    sub = oat[oat[\"param\"] == p]\n"
    "    ax.plot(sub[\"value\"], sub[\"spearman\"], marker=\"o\", label=\"Spearman ρ\")\n"
    "    ax.axhline(0.95, ls=\"--\", color=\"grey\", lw=0.5)\n"
    "    ax.set_title(p)\n"
    "    ax.set_xlabel(\"value\")\n"
    "    ax.set_ylabel(\"ρ (rank stability)\")\n"
    "    ax.set_ylim(0, 1.05)\n"
    "for ax in axes[len(params):]:\n"
    "    ax.axis(\"off\")\n"
    "fig.suptitle(\"Fig 4.1.1 — OAT rank stability per parameter (Cluster A)\")\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_1_1_oat_rank_stability\")\n"
    "plt.show()"
))

# Fig 4.1.2 — Tornado
cells.append(nbf.v4.new_code_cell(
    "tor = load_tornado(OUTPUT_DIR, CLUSTER)\n"
    "tor[\"total\"] = tor[\"delta_low\"] + tor[\"delta_high\"]\n"
    "tor = tor.sort_values(\"total\")\n"
    "\n"
    "fig, ax = plt.subplots(figsize=(7, 6))\n"
    "ax.barh(tor[\"param\"], -tor[\"delta_low\"], color=\"#5b9bd5\", label=\"Δ low (−)\")\n"
    "ax.barh(tor[\"param\"], tor[\"delta_high\"], color=\"#ed7d31\", label=\"Δ high (+)\")\n"
    "ax.axvline(0, color=\"black\", lw=0.5)\n"
    "ax.set_title(\"Fig 4.1.2 — Tornado: |Δ mean composite| per parameter\")\n"
    "ax.set_xlabel(\"Δ scalar metric (mean score)\")\n"
    "ax.legend(loc=\"lower right\")\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_1_2_tornado\")\n"
    "plt.show()"
))

# Fig 4.1.3 — Sobol
cells.append(nbf.v4.new_code_cell(
    "sob = load_sobol(OUTPUT_DIR, CLUSTER).sort_values(\"st\", ascending=True)\n"
    "fig, ax = plt.subplots(figsize=(7, 6))\n"
    "ax.barh(sob[\"param\"], sob[\"s1\"], color=\"#5b9bd5\", label=\"S1 (first-order)\")\n"
    "ax.barh(sob[\"param\"], (sob[\"st\"] - sob[\"s1\"]).clip(lower=0), left=sob[\"s1\"],\n"
    "        color=\"#ed7d31\", label=\"ST − S1 (interactions)\")\n"
    "ax.set_xlim(0, 1)\n"
    "ax.set_xlabel(\"Sobol index\")\n"
    "ax.set_title(\"Fig 4.1.3 — Sobol S1 vs ST\")\n"
    "ax.legend()\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_1_3_sobol\")\n"
    "plt.show()"
))

# Fig 4.1.4 — Grid heatmap + best config
cells.append(nbf.v4.new_code_cell(
    "grid = load_grid(OUTPUT_DIR, CLUSTER)\n"
    "param_cols = [c for c in grid.columns if c not in {\"combo_id\", \"spearman\", \"kendall\", \"mean\", \"std\"}]\n"
    "print(\"Top-3 grid parameters:\", param_cols)\n"
    "print(\"\\nBest by spearman:\")\n"
    "print(grid.nlargest(5, \"spearman\")[param_cols + [\"spearman\", \"mean\"]])\n"
    "print(\"\\nBest by mean composite:\")\n"
    "print(grid.nlargest(5, \"mean\")[param_cols + [\"spearman\", \"mean\"]])\n"
    "\n"
    "if len(param_cols) == 3:\n"
    "    p1, p2, p3 = param_cols\n"
    "    median_p3 = grid[p3].median()\n"
    "    sub = grid[np.isclose(grid[p3], median_p3, rtol=0.01)]\n"
    "    pivot = sub.pivot_table(index=p1, columns=p2, values=\"mean\")\n"
    "    fig, ax = plt.subplots(figsize=(7, 5))\n"
    "    sns.heatmap(pivot, cmap=\"viridis\", annot=False, ax=ax)\n"
    "    ax.set_title(f\"Fig 4.1.4 — Mean composite over ({p1}, {p2}) at {p3}={median_p3:g}\")\n"
    "    fig.tight_layout()\n"
    "    save_figure(fig, \"4_1_4_grid_heatmap\")\n"
    "    plt.show()"
))

nb["cells"] = cells
nb["metadata"] = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python"},
}

with open("notebooks/01_cluster_a.ipynb", "w") as f:
    nbf.write(nb, f)
print("wrote notebooks/01_cluster_a.ipynb")
