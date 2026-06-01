"""loaders.py — CSV → DataFrame helpers for sensitivity output.

Paths are relative to the notebook directory. Pass output_dir explicitly to
override the default scripts/sensitivity/output/<snapshot_id>/cluster_X path.
"""

from pathlib import Path

import pandas as pd


def _path(output_dir, cluster: str, name: str) -> Path:
    return Path(output_dir) / f"cluster_{cluster}" / name


def load_oat(output_dir, cluster: str) -> pd.DataFrame:
    return pd.read_csv(_path(output_dir, cluster, "oat.csv"))


def load_tornado(output_dir, cluster: str) -> pd.DataFrame:
    return pd.read_csv(_path(output_dir, cluster, "tornado.csv"))


def load_sobol(output_dir, cluster: str) -> pd.DataFrame:
    return pd.read_csv(_path(output_dir, cluster, "sobol.csv"))


def load_grid(output_dir, cluster: str) -> pd.DataFrame:
    return pd.read_csv(_path(output_dir, cluster, "grid.csv"))
