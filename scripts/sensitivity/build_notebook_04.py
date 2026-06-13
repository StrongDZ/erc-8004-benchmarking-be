"""One-shot builder for notebooks/04_cluster_d.ipynb.

Run from scripts/sensitivity/:  .venv/bin/python build_notebook_04.py
Safe to delete after the .ipynb is committed.
"""

import nbformat as nbf

nb = nbf.v4.new_notebook()
SNAP_ID = "snap_20260613_185128"

cells = []

cells.append(nbf.v4.new_markdown_cell(
    "# Chương 4.4 — Sensitivity Analysis của Cụm D: EigenTrust Power Iteration + Teleport\n\n"
    "4 tham số: Alpha (damping), Epsilon (ngưỡng hội tụ), MaxIter, TeleportGamma "
    "(trộn reviewer-reliability với external on-chain score trong teleport prior).\n\n"
    "Output từ `sensitivity bench --cluster=D`. Vì power iteration là thuật toán lặp "
    "**xác định** theo (graph, Alpha), Sobol được bổ sung bằng **convergence study** "
    "(số vòng lặp tới hội tụ theo Alpha).\n\n"
    "> **Phát hiện chính:**\n"
    "> - **Alpha** chi phối mạnh nhất cả độ lớn điểm lẫn thời gian hội tụ; số vòng lặp "
    "bùng nổ khi α→1 (α=0.99 không hội tụ trong 100 vòng).\n"
    "> - **TeleportGamma** là tham số ảnh hưởng thứ nhì: vì ~92% ví có external "
    "on-chain score, việc dịch trọng số từ reviewer-reliability sang external score "
    "làm đổi thứ hạng đáng kể (Δ mean ≈ 2.5).\n"
    "> - **MaxIter** và **Epsilon** nhỏ → đã hội tụ hoàn toàn, gần như không ảnh hưởng "
    "thứ hạng. Default (MaxIter=50, Eps=1e-6) nằm trong vùng hội tụ."
))

cells.append(nbf.v4.new_code_cell(
    "import sys\n"
    "sys.path.insert(0, '..')\n"
    "from lib import setup_thesis_style, save_figure, load_oat, load_tornado\n"
    "\n"
    "import pandas as pd\n"
    "import matplotlib.pyplot as plt\n"
    "import seaborn as sns\n"
    "import numpy as np\n"
    "\n"
    "setup_thesis_style()\n"
    "\n"
    f"SNAP_ID = \"{SNAP_ID}\"\n"
    "OUTPUT_DIR = f\"../output/{SNAP_ID}\"\n"
    "CLUSTER = \"D\""
))

# Fig 4.4.1 — Alpha vs convergence
cells.append(nbf.v4.new_code_cell(
    "conv = pd.read_csv(f\"{OUTPUT_DIR}/cluster_D/convergence.csv\")\n"
    "MAXITER = 100  # cap used by the convergence run\n"
    "fig, ax = plt.subplots(figsize=(7, 4.5))\n"
    "ax.plot(conv[\"alpha\"], conv[\"iterations\"], marker=\"o\", color=\"#1f77b4\")\n"
    "ax.axhline(MAXITER, ls=\"--\", color=\"#d62728\", lw=0.8, label=f\"MaxIter cap = {MAXITER}\")\n"
    "ax.set_xlabel(\"Alpha (damping factor)\")\n"
    "ax.set_ylabel(\"Số vòng lặp tới hội tụ\")\n"
    "ax.set_title(\"Fig 4.4.1 — Power iteration: số vòng lặp hội tụ theo Alpha\")\n"
    "ax.legend(loc=\"upper left\")\n"
    "ax.grid(True, alpha=0.3)\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_4_1_alpha_convergence\")\n"
    "plt.show()"
))

# Fig 4.4.2 — OAT 4 subplots (rank stability + mean, twin axis)
cells.append(nbf.v4.new_code_cell(
    "oat = load_oat(OUTPUT_DIR, CLUSTER)\n"
    "params = oat[\"param\"].unique()\n"
    "fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(10, 7), sharey=False)\n"
    "axes = axes.flatten()\n"
    "for ax, p in zip(axes, params):\n"
    "    sub = oat[oat[\"param\"] == p]\n"
    "    ax.plot(sub[\"value\"], sub[\"spearman\"], marker=\"o\", color=\"#1f77b4\", label=\"Spearman ρ\")\n"
    "    ax2 = ax.twinx()\n"
    "    ax2.plot(sub[\"value\"], sub[\"mean\"], marker=\"s\", color=\"#ed7d31\", label=\"mean trust\")\n"
    "    ax.axhline(0.95, ls=\"--\", color=\"grey\", lw=0.5)\n"
    "    ax.set_title(p)\n"
    "    ax.set_xlabel(\"value\")\n"
    "    ax.set_ylabel(\"Spearman ρ\", color=\"#1f77b4\")\n"
    "    ax2.set_ylabel(\"mean trust\", color=\"#ed7d31\")\n"
    "    ax.set_ylim(0, 1.05)\n"
    "fig.suptitle(\"Fig 4.4.2 — OAT rank stability + mean trust (Cluster D)\")\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_4_2_oat\")\n"
    "plt.show()"
))

# Fig 4.4.3 — Tornado
cells.append(nbf.v4.new_code_cell(
    "tor = load_tornado(OUTPUT_DIR, CLUSTER)\n"
    "tor[\"total\"] = tor[\"delta_low\"] + tor[\"delta_high\"]\n"
    "tor = tor.sort_values(\"total\")\n"
    "fig, ax = plt.subplots(figsize=(7, 3.5))\n"
    "ax.barh(tor[\"param\"], -tor[\"delta_low\"], color=\"#5b9bd5\", label=\"Δ low (−)\")\n"
    "ax.barh(tor[\"param\"], tor[\"delta_high\"], color=\"#ed7d31\", label=\"Δ high (+)\")\n"
    "ax.axvline(0, color=\"black\", lw=0.5)\n"
    "ax.set_title(\"Fig 4.4.3 — Tornado: |Δ mean trust| theo tham số (Cluster D)\")\n"
    "ax.set_xlabel(\"Δ điểm trust trung bình\")\n"
    "ax.legend(loc=\"lower right\")\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_4_3_tornado\")\n"
    "plt.show()"
))

# Fig 4.4.4 — Alpha × TeleportGamma heatmap (rank stability)
cells.append(nbf.v4.new_code_cell(
    "grid = pd.read_csv(f\"{OUTPUT_DIR}/cluster_D/grid.csv\")\n"
    "param_cols = [c for c in grid.columns if c not in {\"combo_id\", \"spearman\", \"kendall\", \"mean\", \"std\"}]\n"
    "print(\"Top-3 grid parameters:\", param_cols)\n"
    "if \"Alpha\" in param_cols and \"TeleportGamma\" in param_cols:\n"
    "    third = [c for c in param_cols if c not in (\"Alpha\", \"TeleportGamma\")][0]\n"
    "    median3 = grid[third].median()\n"
    "    sub = grid[np.isclose(grid[third], median3, rtol=0.01)]\n"
    "    pivot = sub.pivot_table(index=\"Alpha\", columns=\"TeleportGamma\", values=\"spearman\")\n"
    "    fig, ax = plt.subplots(figsize=(7, 5))\n"
    "    sns.heatmap(pivot, cmap=\"viridis\", ax=ax)\n"
    "    ax.set_title(f\"Fig 4.4.4 — Spearman ρ over (Alpha, TeleportGamma) at {third}={median3:g}\")\n"
    "    fig.tight_layout()\n"
    "    save_figure(fig, \"4_4_4_alpha_teleportgamma_heatmap\")\n"
    "    plt.show()\n"
    "else:\n"
    "    print(f\"Alpha or TeleportGamma not in top-3 ({param_cols}); skipping heatmap\")"
))

nb["cells"] = cells
nb["metadata"] = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python"},
}

with open("notebooks/04_cluster_d.ipynb", "w") as f:
    nbf.write(nb, f)
print("wrote notebooks/04_cluster_d.ipynb")
