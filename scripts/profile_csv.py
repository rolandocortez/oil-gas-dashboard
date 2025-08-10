#!/usr/bin/env python3
"""
Quick CSV profiler:
- Detects delimiter if not provided.
- Prints header, row count (approx via chunking), and simple stats.
- Writes a Markdown summary into docs/profiles/<basename>.md
"""
from __future__ import annotations
import argparse
import csv
import pathlib
import pandas as pd


def sniff_delimiter(path: str, sample_size: int = 65536) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        sample = f.read(sample_size)
    try:
        dialect = csv.Sniffer().sniff(sample)
        return dialect.delimiter
    except Exception:
        return ","  # default fallback


def profile_csv(path: str, delimiter: str | None = None, n_preview: int = 10) -> dict:
    delim = delimiter or sniff_delimiter(path)
    # Read a small chunk with dtype=object to preserve leading zeros
    df = pd.read_csv(path, delimiter=delim, dtype=object, nrows=5000, low_memory=False)
    header = list(df.columns)
    preview = df.head(n_preview)

    # Approximate row count via chunking (efficient on large files)
    row_count = 0
    for chunk in pd.read_csv(
        path, delimiter=delim, dtype=object, chunksize=100_000, low_memory=False
    ):
        row_count += len(chunk)

    # Null counts for the sampled chunk (good enough to design cleaning rules)
    nulls = df.isna().sum().to_dict()

    return {
        "delimiter": delim,
        "header": header,
        "row_count": row_count,
        "nulls_sample": nulls,
        "preview": preview,
    }


def write_markdown_report(info: dict, path: str):
    md = [
        "# CSV Profile",
        f"- **Path:** `{path}`",
        f"- **Delimiter:** `{info['delimiter']}`",
        f"- **Approx. rows:** {info['row_count']}",
        f"- **Columns ({len(info['header'])}):**",
        "",
    ]
    for c in info["header"]:
        md.append(f"  - `{c}`")
    md.append("\n## Sample (first 10 rows)\n")
    md.append(info["preview"].to_markdown(index=False))

    # Nulls (sample)
    md.append("\n## Null count (in first 5k rows)")
    md.append("| column | nulls |")
    md.append("|---|---:|")
    for c, n in info["nulls_sample"].items():
        md.append(f"| {c} | {n} |")

    out_dir = pathlib.Path("docs/profiles")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / (pathlib.Path(path).stem + ".md")
    out_path.write_text("\n".join(md), encoding="utf-8")
    print(f"[INFO] Wrote profile to {out_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path", help="Path to the CSV file")
    ap.add_argument(
        "--delimiter", help="Force a delimiter (default: sniff or ',')", default=None
    )
    args = ap.parse_args()

    info = profile_csv(args.csv_path, args.delimiter)
    print("[INFO] Header:", info["header"])
    print("[INFO] Rows (approx):", info["row_count"])
    print("[INFO] Delimiter:", info["delimiter"])
    print("[INFO] Preview:")
    print(info["preview"].head(5).to_string(index=False))
    write_markdown_report(info, args.csv_path)


if __name__ == "__main__":
    main()
