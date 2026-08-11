"""Loaders for the operator-provided seed CSVs.

These are the two inputs the pipelines cannot derive for themselves: which fids
built miniapps, and which accounts count as "Arbitrum brand". Missing or
malformed files fail with a message that says exactly what to write and where,
rather than an empty result that looks like a real answer.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config.settings import DATA_DIR, SEEDS_DIR

logger = logging.getLogger(__name__)


class SeedMissingError(FileNotFoundError):
    pass


def _candidate_paths(name: str) -> list[Path]:
    """Look in seeds/ first, then data/<name>/ — the operator mentioned both."""
    return [
        SEEDS_DIR / f"{name}.csv",
        DATA_DIR / name / f"{name}.csv",
        DATA_DIR / f"{name}.csv",
    ]


def _load(name: str, required_columns: set[str], schema_hint: str) -> pd.DataFrame:
    for path in _candidate_paths(name):
        if path.exists():
            df = pd.read_csv(path)
            df.columns = [c.strip().lower() for c in df.columns]
            missing = required_columns - set(df.columns)
            if missing:
                raise ValueError(
                    f"{path} is missing column(s) {sorted(missing)}.\n"
                    f"Expected schema: {schema_hint}"
                )
            logger.info("loaded seed %s from %s (%d rows)", name, path, len(df))
            return df
    searched = "\n  ".join(str(p) for p in _candidate_paths(name))
    raise SeedMissingError(
        f"Seed file '{name}.csv' not found. Searched:\n  {searched}\n"
        f"Expected schema: {schema_hint}"
    )


def load_miniapp_builders() -> pd.DataFrame:
    """fid[,username] — Farcaster accounts that shipped an Arbitrum miniapp."""
    df = _load(
        "miniapp_builders",
        {"fid"},
        "fid[,username,app_name,app_url]",
    )
    df = df[pd.to_numeric(df["fid"], errors="coerce").notna()].copy()
    df["fid"] = df["fid"].astype(int)
    return df.drop_duplicates(subset=["fid"])


def load_brand_accounts() -> pd.DataFrame:
    """fid,name[,weight] — the Arbitrum-brand accounts engagement is measured against."""
    df = _load(
        "brand_accounts",
        {"fid"},
        "fid,name[,weight]  (weight defaults to 1.0)",
    )
    df = df[pd.to_numeric(df["fid"], errors="coerce").notna()].copy()
    df["fid"] = df["fid"].astype(int)
    if "weight" not in df.columns:
        df["weight"] = 1.0
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(1.0)
    if "name" not in df.columns:
        df["name"] = None
    return df.drop_duplicates(subset=["fid"])
