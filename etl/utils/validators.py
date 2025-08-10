# Lightweight dataframe validations used before loading to RAW.
from __future__ import annotations
import pandas as pd


def assert_non_null(df: pd.DataFrame, cols: list[str]) -> None:
    missing = {c: int(df[c].isna().sum()) for c in cols if c in df.columns}
    bad = {k: v for k, v in missing.items() if v > 0}
    if bad:
        raise ValueError(f"Non-null columns contain nulls: {bad}")


def assert_non_negative(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            bad = df[c].dropna().astype(float) < 0
            if bad.any():
                raise ValueError(f"Negative values found in column '{c}'")


def assert_unique(df: pd.DataFrame, cols: list[str]) -> None:
    if not cols:
        return
    if df.duplicated(subset=cols).any():
        dups = df[df.duplicated(subset=cols, keep=False)][cols].head(5)
        raise ValueError(f"Duplicate PK candidates for {cols}. Sample:\n{dups}")
