"""
CSV -> Snowflake RAW loader driven by a YAML contract.

Usage examples:
  # Use the contract's source_file under data/raw/
  python etl/load_to_snowflake.py --config etl/config/annual_production.yml

  # Override the CSV path (e.g., use the 2k sample)
  python etl/load_to_snowflake.py --config etl/config/annual_production.yml \
      --csv data/raw/oil-and-gas-annual-production-beginning-2001-1_sample2000.csv

  # Dry run: do everything except write to Snowflake
  python etl/load_to_snowflake.py --config etl/config/annual_production.yml --dry-run
"""

from __future__ import annotations
import argparse
import os
import pathlib
import yaml
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from utils.logging import get_logger
from utils import validators as vd
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())  # loads .env automatically if present

LOGGER = get_logger("etl.load_to_snowflake")


# ---------- helpers ----------
def load_config(cfg_path: str) -> dict:
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_conn():
    """Build a Snowflake connection from environment variables."""
    required = [
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {missing}")

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )


def normalize_strings(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    rename_map = cfg.get("rename_map", {}) or {}
    df = df.rename(columns=rename_map)

    # Normalize whitespace / case on string columns
    string_cols = set(cfg.get("string_columns", []) or [])
    if cfg.get("normalize", {}).get("strip_whitespace", False):
        for c in string_cols:
            if c in df.columns:
                df[c] = df[c].astype("string").str.strip()

    upper_cols = cfg.get("normalize", {}).get("uppercase", []) or []
    for c in upper_cols:
        if c in df.columns:
            df[c] = df[c].astype("string").str.upper()

    return df


def _coerce_numeric_series(s: pd.Series) -> pd.Series:
    """Return a numeric pandas Series after cleaning thousand separators.
    - Keeps NaN as NaN
    - Removes thousand separators (',')
    - Trims whitespace
    """
    # Work on strings to preserve raw content first
    s = s.astype("string")
    s = s.str.strip().str.replace(",", "", regex=False)
    # Empty strings -> NaN
    s = s.replace({"": pd.NA})
    # Coerce to numeric (float64), invalid parsing -> NaN
    s_num = pd.to_numeric(s, errors="coerce")
    return s_num


def _prefer_int64_or_float64(s_num: pd.Series) -> pd.Series:
    """If all non-null values are whole numbers, cast to pandas nullable Int64.
    Otherwise, cast to pandas nullable Float64.
    """
    non_null = s_num.dropna()
    if len(non_null) == 0:
        # All NaN -> keep as Int64 for consistency
        return s_num.astype("Int64")
    # Check if every value is an integer value (e.g., 0.0 -> 0)
    if (non_null % 1 == 0).all():
        return s_num.astype("Int64")
    return s_num.astype("Float64")


def cast_numeric(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Cast numeric columns using pandas nullable dtypes with safety checks.
    - integer_columns: prefer Int64 if truly integral, else Float64
    - numeric_columns: cast to Float64 unless integral
    """
    int_cols = cfg.get("integer_columns", []) or []
    num_cols = cfg.get("numeric_columns", []) or []

    # First pass: coerce both sets to numeric base
    for c in set(int_cols + num_cols):
        if c in df.columns:
            base = _coerce_numeric_series(df[c])
            df[c] = _prefer_int64_or_float64(base)

    return df


def apply_validations(df: pd.DataFrame, cfg: dict) -> None:
    """Apply lightweight validations declared in the YAML contract."""
    v = cfg.get("validations") or {}

    # Required non-null columns
    if v.get("non_null"):
        vd.assert_non_null(df, v["non_null"])

    # Non-negative numeric columns
    if v.get("non_negative_numeric"):
        vd.assert_non_negative(df, v["non_negative_numeric"])

    # Uniqueness on PK candidate
    pk = cfg.get("pk_candidate")
    if pk:
        enforce = bool(v.get("enforce_unique_in_raw", False))
        if enforce:
            vd.assert_unique(df, pk)
        else:
            dupes = df[df.duplicated(subset=pk, keep=False)]
            if len(dupes) > 0:
                LOGGER.warning(
                    "Found %d duplicate rows for PK candidate %s in RAW; not failing. Writing a sample.",
                    len(dupes),
                    pk,
                )
                outdir = pathlib.Path("docs/duplicates")
                outdir.mkdir(parents=True, exist_ok=True)
                (outdir / f"{cfg['raw_table'].lower()}_pk_dupes_sample.csv").write_text(
                    dupes.head(200).to_csv(index=False), encoding="utf-8"
                )


# ---------- main ----------
def main(args):
    cfg = load_config(args.config)
    raw_table = cfg["raw_table"].upper()
    delimiter = cfg.get("delimiter") or ","
    source_file = cfg["source_file"]
    csv_path = args.csv or str(pathlib.Path("data/raw") / source_file)

    # NA handling: combine defaults with custom tokens
    na_like = cfg.get("na_like_values", []) or []
    # Read all as strings to preserve leading zeros/codes, then cast numerics
    LOGGER.info("Reading CSV: %s", csv_path)
    df = pd.read_csv(
        csv_path,
        delimiter=delimiter,
        dtype=str,
        na_values=na_like,
        keep_default_na=True,
        low_memory=False,
    )

    LOGGER.info("Input columns: %s", list(df.columns))
    df = normalize_strings(df, cfg)
    df = cast_numeric(df, cfg)
    # Optional: log numeric dtypes chosen
    num_cols_all = set(
        (cfg.get("integer_columns") or []) + (cfg.get("numeric_columns") or [])
    )
    chosen = {c: str(df[c].dtype) for c in num_cols_all if c in df.columns}
    LOGGER.info("Numeric dtypes chosen: %s", chosen)

    LOGGER.info("Standardized columns: %s", list(df.columns))

    # Validations
    apply_validations(df, cfg)
    LOGGER.info("Validations passed. Rows: %d  Columns: %d", len(df), df.shape[1])

    if args.dry_run:
        LOGGER.info("[DRY RUN] Skipping upload to Snowflake.")
        return

    # Connect and write
    with get_conn() as conn:
        # Ensure we're in the right database/schema (SNOWFLAKE_SCHEMA is default RAW)
        cur = conn.cursor()
        target_db = os.getenv("SNOWFLAKE_DATABASE")
        target_schema = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
        cur.execute(f'USE DATABASE "{target_db}"')
        cur.execute(f'USE SCHEMA "{target_schema}"')
        cur.close()

        LOGGER.info("Loading into %s.%s.%s", target_db, target_schema, raw_table)
        # Idempotent load: overwrite table content with the CSV
        success, nchunks, nrows, _ = write_pandas(
            conn, df, raw_table, auto_create_table=True, overwrite=True
        )
        if not success:
            raise RuntimeError("write_pandas returned success=False")
        LOGGER.info("Upload complete: chunks=%s rows=%s", nchunks, nrows)


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Load CSV to Snowflake RAW using a YAML contract"
    )
    p.add_argument("--config", required=True, help="Path to YAML contract")
    p.add_argument("--csv", help="Override CSV path (default: data/raw/<source_file>)")
    p.add_argument("--dry-run", action="store_true", help="Skip Snowflake write")
    main(p.parse_args())
