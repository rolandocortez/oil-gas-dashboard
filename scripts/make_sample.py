#!/usr/bin/env python3
"""
Create a small sample (N data rows + header) from a large CSV.
"""
from __future__ import annotations
import argparse
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path")
    ap.add_argument("--out", default=None)
    ap.add_argument("--n", type=int, default=2000, help="number of data rows")
    args = ap.parse_args()

    out = args.out or args.csv_path.replace(".csv", f"_sample{args.n}.csv")
    df = pd.read_csv(args.csv_path, dtype=object, nrows=args.n, low_memory=False)
    df.to_csv(out, index=False)
    print(f"[INFO] Wrote sample to {out}")


if __name__ == "__main__":
    main()
