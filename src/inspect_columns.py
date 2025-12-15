#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os
import pandas as pd

def read_any_table(path: str) -> pd.DataFrame:
    ext = (os.path.splitext(path)[1] or "").lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return pd.read_csv(path, dtype=str, sep=None, engine="python").fillna("")

def main():
    ap = argparse.ArgumentParser(description="List columns & sample rows for a file (CSV/XLSX).")
    ap.add_argument("--in", dest="inp", required=True, help="Path to CSV/XLSX")
    ap.add_argument("--sample", type=int, default=5, help="Show first N rows (default 5)")
    args = ap.parse_args()

    df = read_any_table(args.inp)
    print(f"File: {args.inp}")
    print(f"Rows: {len(df)}")
    print("\nColumns (exact):")
    for i, c in enumerate(df.columns, 1):
        print(f"  {i:>2}. {c}")

    print("\nColumns (lowercase):")
    for i, c in enumerate(df.columns, 1):
        print(f"  {i:>2}. {c.lower()}")

    n = min(args.sample, len(df))
    if n:
        print(f"\nSample (first {n} rows):")
        print(df.head(n).to_string(index=False))

if __name__ == "__main__":
    main()
