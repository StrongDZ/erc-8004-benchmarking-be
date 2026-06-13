"""One-shot builder for notebooks/03_cluster_c.ipynb.

Run from scripts/sensitivity/:  .venv/bin/python build_notebook_03.py
Keeps the notebook source in code (easy to regenerate / diff) rather than
hand-editing JSON. Safe to delete after the .ipynb is committed.
"""

import nbformat as nbf

nb = nbf.v4.new_notebook()
SNAP_ID = "snap_20260613_185128"  # snapshot WITH quality signals populated

cells = []

cells.append(nbf.v4.new_markdown_cell(
    "# Chương 4.3 — Sensitivity Analysis của Cụm C: Quality Score → Edge Weight\n\n"
    "8 tham số: 5 Q-weights (sum=1), ReasoningLenFull, AttachmentCountFull, WiBase.\n\n"
    "Mô hình propagation mới KHÔNG còn nhánh reward/penalty (WiMin/WiMax/Eta/Kappa "
    "đã bị bỏ). Tín hiệu chất lượng chỉ tạo trọng số cạnh "
    "`wᵢ = WiBase + (1 - WiBase)·Q`, với `Q = Σ QWeightₖ·signalₖ ∈ [0,1]`. Cụm C đo "
    "**trọng số cạnh trung bình mỗi agent** — đại lượng đi vào power iteration ở Cụm D.\n\n"
    "Output từ `sensitivity bench --cluster=C`. Snapshot dùng ở đây "
    "(`{}`) đã được populate quality signals từ `feedbackParsed`.\n\n"
    "> **Tham số LIVE vs DORMANT trên dữ liệu chain-8453 (theo tornado):**\n"
    ">\n"
    "> - **LIVE:** `WiBase` (chi phối mạnh nhất — sàn + thang của wᵢ), "
    "`QWeightConfidence`, rồi tới các Q-weight còn lại.\n"
    "> - **DORMANT (≈0):** `ReasoningLenFull` và `AttachmentCountFull` — dữ liệu "
    "chain-8453 hầu như không có reasoning dài/attachment nên các điểm bão hoà này "
    "không bao giờ ràng buộc (một phát hiện thực, không phải lỗi).\n"
    ">\n"
    "> Tín hiệu chất lượng thật trong dữ liệu: `reasoningLen` (từ `comment`) và "
    "`hasRatingBreakdown` (từ `score`)."
    .format(SNAP_ID)
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
    "CLUSTER = \"C\""
))

# Fig 4.3.1 — OAT 11 subplots
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
    "fig.suptitle(\"Fig 4.3.1 — OAT rank stability per parameter (Cluster C)\")\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_3_1_oat\")\n"
    "plt.show()"
))

# Fig 4.3.2 — Tornado
cells.append(nbf.v4.new_code_cell(
    "tor = load_tornado(OUTPUT_DIR, CLUSTER)\n"
    "tor[\"total\"] = tor[\"delta_low\"] + tor[\"delta_high\"]\n"
    "tor = tor.sort_values(\"total\")\n"
    "fig, ax = plt.subplots(figsize=(7, 5.5))\n"
    "ax.barh(tor[\"param\"], -tor[\"delta_low\"], color=\"#5b9bd5\", label=\"Δ low (−)\")\n"
    "ax.barh(tor[\"param\"], tor[\"delta_high\"], color=\"#ed7d31\", label=\"Δ high (+)\")\n"
    "ax.axvline(0, color=\"black\", lw=0.5)\n"
    "ax.set_title(\"Fig 4.3.2 — Tornado: |Δ mean trust| theo tham số (Cluster C)\")\n"
    "ax.set_xlabel(\"Δ điểm trust trung bình\")\n"
    "ax.legend(loc=\"lower right\")\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_3_2_tornado\")\n"
    "plt.show()"
))

# Fig 4.3.3 — Q-weight default composition (stacked)
cells.append(nbf.v4.new_code_cell(
    "qweights = [\"QWeightReasoning\", \"QWeightAttachment\", \"QWeightBreakdown\",\n"
    "            \"QWeightPayment\", \"QWeightConfidence\"]\n"
    "q_default = [0.20, 0.25, 0.20, 0.15, 0.20]\n"
    "colors = [\"#1f77b4\", \"#ff7f0e\", \"#2ca02c\", \"#d62728\", \"#9467bd\"]\n"
    "fig, ax = plt.subplots(figsize=(7, 4))\n"
    "bottom = 0.0\n"
    "for w, val, col in zip(qweights, q_default, colors):\n"
    "    ax.bar([\"Default Q weights\"], val, bottom=bottom, color=col, label=w)\n"
    "    bottom += val\n"
    "ax.set_ylabel(\"Weight\")\n"
    "ax.set_title(\"Fig 4.3.3 — Q-weight default composition (Σ = 1)\")\n"
    "ax.legend(bbox_to_anchor=(1.02, 1), loc=\"upper left\", fontsize=8)\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_3_3_qweights_stacked\")\n"
    "plt.show()"
))

# Fig 4.3.4 — heatmap of mean trust over the two most-influential grid params
cells.append(nbf.v4.new_code_cell(
    "grid = load_grid(OUTPUT_DIR, CLUSTER)\n"
    "param_cols = [c for c in grid.columns if c not in {\"combo_id\", \"spearman\", \"kendall\", \"mean\", \"std\"}]\n"
    "print(\"Top-3 grid parameters:\", param_cols)\n"
    "# Choose the 2 axes with the widest mean-response spread; hold the 3rd at its median.\n"
    "spreads = {p: grid.groupby(p)[\"mean\"].mean().max() - grid.groupby(p)[\"mean\"].mean().min()\n"
    "           for p in param_cols}\n"
    "y_ax, x_ax = sorted(param_cols, key=lambda p: spreads[p], reverse=True)[:2]\n"
    "third = [p for p in param_cols if p not in (y_ax, x_ax)][0]\n"
    "median3 = grid[third].median()\n"
    "sub = grid[np.isclose(grid[third], median3, rtol=0.01)]\n"
    "pivot = sub.pivot_table(index=y_ax, columns=x_ax, values=\"mean\")\n"
    "fig, ax = plt.subplots(figsize=(6.5, 5))\n"
    "sns.heatmap(pivot, cmap=\"viridis\", ax=ax)\n"
    "ax.set_title(f\"Fig 4.3.4 — Mean trust over ({y_ax}, {x_ax}) at {third}={median3:g}\")\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_3_4_grid_heatmap\")\n"
    "plt.show()"
))

# Dominant-parameter summary
cells.append(nbf.v4.new_code_cell(
    "sob = load_sobol(OUTPUT_DIR, CLUSTER).sort_values(\"st\", ascending=False)\n"
    "print(\"Sobol total-order (ST) ranking — Cluster C:\")\n"
    "print(sob[[\"param\", \"s1\", \"st\"]].to_string(index=False))"
))

nb["cells"] = cells
nb["metadata"] = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python"},
}

with open("notebooks/03_cluster_c.ipynb", "w") as f:
    nbf.write(nb, f)
print("wrote notebooks/03_cluster_c.ipynb")
