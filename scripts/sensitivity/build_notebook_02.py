"""One-shot builder for notebooks/02_cluster_b.ipynb.

Run from scripts/sensitivity/:  .venv/bin/python build_notebook_02.py
Keeps the notebook source in code (easy to regenerate / diff) rather than
hand-editing JSON. Safe to delete after the .ipynb is committed.
"""

import nbformat as nbf

nb = nbf.v4.new_notebook()
SNAP_ID = "snap_20260613_185128"

cells = []

cells.append(nbf.v4.new_markdown_cell(
    "# Chương 4.2 — Sensitivity Analysis của Cụm B: Composite Blend (v2)\n\n"
    "5 trọng số composite: w_reputation, w_adoption, w_services, w_publisher, "
    "w_compliance — ràng buộc sum=1.\n\n"
    "Output từ `sensitivity bench --cluster=B`. Đổi `SNAP_ID` ở cell dưới nếu chạy "
    "trên snapshot khác.\n\n"
    "> **Lưu ý:** Tier1Total/Tier2Total (trọng số tier của compliance) KHÔNG được "
    "khảo sát ở đây — chúng là input của ComputeComplianceScore (quyết định mức đóng "
    "góp của từng trường card *khi tính* điểm), không phải hệ số nhân hậu kỳ. Snapshot "
    "chỉ lưu ComplianceScore đã tính nên không thể tái lập trung thực; cần snapshot "
    "lưu raw ComplianceInput mới khảo sát được tier.\n\n"
    "> **Lưu ý phương pháp:** vì 5 trọng số chịu ràng buộc tổng = 1 (recompute "
    "chuẩn hoá lại sau mỗi lần đổi), chúng KHÔNG độc lập. Do đó chỉ số Sobol "
    "(`sobol.csv`) bị lệch — đôi khi cho ST < S1 vốn không thể xảy ra với input "
    "độc lập. Vì vậy cụm B dùng **Dirichlet simplex sampling** (lấy mẫu đều trên "
    "đơn hình trọng số) làm phương pháp chính, kèm OAT/Tornado cho từng tham số; "
    "Sobol chỉ để tham khảo."
))

cells.append(nbf.v4.new_code_cell(
    "import sys\n"
    "sys.path.insert(0, '..')\n"
    "from lib import setup_thesis_style, save_figure, load_oat, load_tornado, ternary_scatter\n"
    "\n"
    "import matplotlib.pyplot as plt\n"
    "import pandas as pd\n"
    "\n"
    "setup_thesis_style()\n"
    "\n"
    f"SNAP_ID = \"{SNAP_ID}\"  # snapshot ID from `sensitivity snapshot list`\n"
    "OUTPUT_DIR = f\"../output/{SNAP_ID}\"\n"
    "CLUSTER = \"B\""
))

# Fig 4.2.1 — Ternary plot of the weight simplex coloured by Spearman ρ
cells.append(nbf.v4.new_code_cell(
    "simplex = pd.read_csv(f\"{OUTPUT_DIR}/cluster_B/simplex.csv\")\n"
    "# 4-weight simplex → project to 3 corners (rep, svc, pub); w_compliance is the\n"
    "# dropped 4th dim. mpltern renormalises each point, so the 3 shown coords are relative.\n"
    "fig = ternary_scatter(\n"
    "    simplex,\n"
    "    t_col=\"w_reputation\",\n"
    "    l_col=\"w_services\",\n"
    "    r_col=\"w_publisher\",\n"
    "    color_col=\"spearman\",\n"
    "    title=\"Fig 4.2.1 — Spearman ρ trên đơn hình (w_rep, w_svc, w_pub)\",\n"
    ")\n"
    "save_figure(fig, \"4_2_1_ternary_spearman\")\n"
    "plt.show()"
))

# Fig 4.2.2 — Tornado
cells.append(nbf.v4.new_code_cell(
    "tor = load_tornado(OUTPUT_DIR, CLUSTER)\n"
    "tor[\"total\"] = tor[\"delta_low\"] + tor[\"delta_high\"]\n"
    "tor = tor.sort_values(\"total\")\n"
    "\n"
    "fig, ax = plt.subplots(figsize=(7, 4.5))\n"
    "ax.barh(tor[\"param\"], -tor[\"delta_low\"], color=\"#5b9bd5\", label=\"Δ low (−)\")\n"
    "ax.barh(tor[\"param\"], tor[\"delta_high\"], color=\"#ed7d31\", label=\"Δ high (+)\")\n"
    "ax.axvline(0, color=\"black\", lw=0.5)\n"
    "ax.set_title(\"Fig 4.2.2 — Tornado: |Δ mean composite| theo tham số\")\n"
    "ax.set_xlabel(\"Δ điểm composite trung bình\")\n"
    "ax.legend(loc=\"lower right\")\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_2_2_tornado\")\n"
    "plt.show()"
))

# Fig 4.2.3 — Per-weight OAT rank stability (5 composite weights)
cells.append(nbf.v4.new_code_cell(
    "oat = load_oat(OUTPUT_DIR, CLUSTER)\n"
    "fig, ax = plt.subplots(figsize=(7, 4.5))\n"
    "for p in oat[\"param\"].unique():\n"
    "    sub = oat[oat[\"param\"] == p]\n"
    "    ax.plot(sub[\"value\"], sub[\"spearman\"], marker=\"o\", label=p)\n"
    "ax.axhline(0.95, ls=\"--\", color=\"grey\", lw=0.5)\n"
    "ax.set_xlabel(\"Giá trị trọng số (trước chuẩn hoá)\")\n"
    "ax.set_ylabel(\"Spearman ρ (rank stability)\")\n"
    "ax.set_title(\"Fig 4.2.3 — OAT: độ ổn định thứ hạng theo từng trọng số composite\")\n"
    "ax.legend()\n"
    "fig.tight_layout()\n"
    "save_figure(fig, \"4_2_3_weight_rank_stability\")\n"
    "plt.show()"
))

# Best simplex configs
cells.append(nbf.v4.new_code_cell(
    "cols = [\"w_reputation\", \"w_services\", \"w_publisher\", \"w_compliance\", \"spearman\", \"mean\"]\n"
    "print(\"Top-10 cấu hình trọng số theo độ ổn định thứ hạng (Spearman ρ):\")\n"
    "print(simplex.nlargest(10, \"spearman\")[cols].to_string(index=False))\n"
    "print(\"\\nTop-10 theo điểm composite trung bình:\")\n"
    "print(simplex.nlargest(10, \"mean\")[cols].to_string(index=False))"
))

nb["cells"] = cells
nb["metadata"] = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python"},
}

with open("notebooks/02_cluster_b.ipynb", "w") as f:
    nbf.write(nb, f)
print("wrote notebooks/02_cluster_b.ipynb")
