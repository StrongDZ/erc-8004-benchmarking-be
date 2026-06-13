"""One-shot builder for notebooks/00_overview.ipynb — master cross-cluster summary.

Run from scripts/sensitivity/:  .venv/bin/python build_notebook_00.py
Safe to delete after the .ipynb is committed.
"""

import nbformat as nbf

nb = nbf.v4.new_notebook()
SNAP_ID = "snap_20260613_185128"

cells = []

cells.append(nbf.v4.new_markdown_cell(
    "# Overview — Tổng hợp Sensitivity Analysis 4 cụm\n\n"
    "Bảng so sánh và tornado tổng hợp xuyên cụm. Mỗi cụm đã có notebook riêng (01–04).\n\n"
    "> **Lưu ý đo lường:** |Δ| thô của mỗi cụm nằm trên *thang điểm khác nhau* "
    "(A: thang reputation; B: composite 0–100; C: trust cộng dồn; D: trust scaled "
    "0–100), nên **không so sánh trực tiếp** được. Tornado tổng hợp dưới đây "
    "**chuẩn hoá theo từng cụm** (chia cho |Δ| lớn nhất trong cụm) → thể hiện "
    "*tầm quan trọng tương đối* của mỗi tham số *bên trong* cụm của nó.\n\n"
    f"Snapshot: `{SNAP_ID}` (cùng một snapshot cho cả 4 cụm)."
))

cells.append(nbf.v4.new_code_cell(
    "import sys\n"
    "sys.path.insert(0, '..')\n"
    "from lib import setup_thesis_style, save_figure, load_tornado\n"
    "import pandas as pd\n"
    "import matplotlib.pyplot as plt\n"
    "\n"
    "setup_thesis_style()\n"
    f"SNAP_ID = \"{SNAP_ID}\"\n"
    "OUTPUT_DIR = f\"../output/{SNAP_ID}\"\n"
    "\n"
    "clusters = [\"A\", \"B\", \"C\", \"D\"]\n"
    "tornados = {}\n"
    "for cl in clusters:\n"
    "    try:\n"
    "        tornados[cl] = load_tornado(OUTPUT_DIR, cl)\n"
    "    except FileNotFoundError:\n"
    "        print(f\"Cluster {cl}: tornado.csv not found\")"
))

# Combined normalized tornado
cells.append(nbf.v4.new_code_cell(
    "rows = []\n"
    "for cl, df in tornados.items():\n"
    "    sub = df.copy()\n"
    "    sub[\"total\"] = sub[\"delta_low\"] + sub[\"delta_high\"]\n"
    "    mx = sub[\"total\"].max()\n"
    "    sub[\"rel\"] = sub[\"total\"] / mx if mx > 0 else 0.0\n"
    "    sub[\"cluster\"] = cl\n"
    "    rows.append(sub)\n"
    "combined = pd.concat(rows, ignore_index=True)\n"
    "# Keep the top-4 relative params of each cluster for a readable chart.\n"
    "top = (combined.sort_values(\"rel\", ascending=False)\n"
    "                .groupby(\"cluster\", group_keys=False).head(4))\n"
    "top = top.sort_values([\"cluster\", \"rel\"])\n"
    "\n"
    "color_map = {\"A\": \"#1f77b4\", \"B\": \"#ff7f0e\", \"C\": \"#2ca02c\", \"D\": \"#d62728\"}\n"
    "fig, ax = plt.subplots(figsize=(8, 9))\n"
    "labels = top[\"param\"] + \"  [\" + top[\"cluster\"] + \"]\"\n"
    "ax.barh(labels, top[\"rel\"], color=[color_map[c] for c in top[\"cluster\"]])\n"
    "handles = [plt.Rectangle((0, 0), 1, 1, color=color_map[c]) for c in clusters if c in tornados]\n"
    "ax.legend(handles, [f\"Cluster {c}\" for c in clusters if c in tornados], loc=\"lower right\")\n"
    "ax.set_xlabel(\"Tầm quan trọng tương đối (|Δ| chuẩn hoá theo cụm)\")\n"
    "ax.set_title(\"Overview — Top-4 tham số nhạy nhất mỗi cụm (chuẩn hoá nội cụm)\")\n"
    "ax.set_xlim(0, 1.05)\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"overview_combined_tornado\")\n"
    "plt.show()"
))

# Summary table
cells.append(nbf.v4.new_code_cell(
    "names = {\"A\": \"Scoring Core\", \"B\": \"Composite Blend\",\n"
    "         \"C\": \"Quality+Propagation\", \"D\": \"Power Iteration\"}\n"
    "summary = []\n"
    "for cl in clusters:\n"
    "    if cl not in tornados:\n"
    "        continue\n"
    "    df = tornados[cl].copy()\n"
    "    df[\"total\"] = df[\"delta_low\"] + df[\"delta_high\"]\n"
    "    df = df.sort_values(\"total\", ascending=False)\n"
    "    top1 = df.iloc[0]\n"
    "    summary.append({\n"
    "        \"Cluster\": cl,\n"
    "        \"Tên cụm\": names[cl],\n"
    "        \"# params\": len(df),\n"
    "        \"Tham số nhạy nhất\": top1[\"param\"],\n"
    "        \"|Δ| (thô)\": round(top1[\"total\"], 4),\n"
    "    })\n"
    "print(pd.DataFrame(summary).to_string(index=False))"
))

nb["cells"] = cells
nb["metadata"] = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python"},
}

with open("notebooks/00_overview.ipynb", "w") as f:
    nbf.write(nb, f)
print("wrote notebooks/00_overview.ipynb")
