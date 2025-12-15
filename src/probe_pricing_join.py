#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, sys
import pandas as pd

ITEM_KEYS = ["item#", "item #", "item no", "item_no", "item number", "item"]
PART_KEYS = ["part#", "part #", "part no", "part_no", "part", "vendor product number"]
GEN_KEYS  = ["sku", "lookupnm"]

def read_any_table(path: str) -> pd.DataFrame:
    ext = (os.path.splitext(path)[1] or "").lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return pd.read_csv(path, dtype=str, sep=None, engine="python").fillna("")

def find_col_case_insensitive(dfr: pd.DataFrame, name: str):
    low = [c.lower() for c in dfr.columns]
    return dfr.columns[low.index(name.lower())] if name.lower() in low else None

def detect_pricing_join_col(dfp: pd.DataFrame, prefer: str = "auto",
                            override: str | None = None) -> str | None:
    if override:
        c = find_col_case_insensitive(dfp, override)
        if c: return c

    cols_lower = [c.lower() for c in dfp.columns]

    def first_hit(keys):
        for k in keys:
            if k in cols_lower:
                return dfp.columns[cols_lower.index(k)]
        return None

    if prefer == "item":
        return first_hit(ITEM_KEYS) or first_hit(PART_KEYS) or first_hit(GEN_KEYS)
    if prefer == "part":
        return first_hit(PART_KEYS) or first_hit(ITEM_KEYS) or first_hit(GEN_KEYS)
    return first_hit(ITEM_KEYS) or first_hit(PART_KEYS) or first_hit(GEN_KEYS)

def main():
    ap = argparse.ArgumentParser(description="Probe SAAMM↔Pricing join viability and report match stats.")
    ap.add_argument("--saamm", required=True, help="Path to SAAMM CSV/TXT")
    ap.add_argument("--pricing", required=True, help="Path to Pricing CSV/XLSX")
    ap.add_argument("--prefer", choices=["auto","item","part"], default="auto",
                    help="Prefer Item# or Part# when auto-detecting pricing join column (default auto).")
    ap.add_argument("--override", default=None,
                    help="Explicit pricing header to use for join (case-insensitive), e.g. 'Item#' or 'Part #'.")
    ap.add_argument("--max-show", type=int, default=15, help="Max examples to show for unmatched/dupes.")
    args = ap.parse_args()

    df  = read_any_table(args.saamm).fillna("")
    dfp = read_any_table(args.pricing).fillna("")

    if "lookupnm" not in df.columns:
        print("ERROR: SAAMM is missing 'lookupnm' column.", file=sys.stderr)
        sys.exit(2)

    pcol = detect_pricing_join_col(dfp, prefer=args.prefer, override=args.override)
    if not pcol:
        print("ERROR: Could not detect a suitable pricing join column. Try --override.", file=sys.stderr)
        print("Pricing columns:", list(dfp.columns))
        sys.exit(3)

    # Normalize keys
    df["_join_key"]  = df["lookupnm"].astype(str).str.strip().str.lower()
    dfp["_join_key"] = dfp[pcol].astype(str).str.strip().str.lower()

    # Stats
    rows_saamm = len(df)
    rows_pr    = len(dfp)
    nonempty_saamm = (df["_join_key"]!="").sum()
    nonempty_pr    = (dfp["_join_key"]!="").sum()

    # Duplicate pricing keys
    dupes = dfp["_join_key"].value_counts()
    pr_dupe_keys = dupes[dupes > 1]
    n_dupe_keys  = len(pr_dupe_keys)

    # Left join to measure match rate
    merged = df.merge(dfp[["_join_key"]], on="_join_key", how="left", indicator=True)
    matched = (merged["_merge"]=="both").sum()
    unmatched = rows_saamm - matched

    # Report
    print(f"SAAMM rows                : {rows_saamm}")
    print(f"Pricing rows              : {rows_pr}")
    print(f"Detected pricing join col : {pcol!r}")
    print(f"Non-empty SAAMM keys      : {nonempty_saamm}")
    print(f"Non-empty Pricing keys    : {nonempty_pr}")
    print(f"Matched rows              : {matched} ({matched/rows_saamm*100:.1f}%)")
    print(f"Unmatched rows            : {unmatched} ({unmatched/rows_saamm*100:.1f}%)")
    print(f"Pricing duplicate keys    : {n_dupe_keys}")

    # Show examples
    if unmatched:
        miss_keys = merged.loc[merged["_merge"]!="both", "_join_key"].drop_duplicates()
        print("\nExamples of SAAMM keys with no pricing match:")
        for i, k in enumerate(miss_keys.head(args.max_show), 1):
            print(f"  {i:>2}. {k!r}")

    if n_dupe_keys:
        print("\nTop pricing duplicate keys (key → count):")
        for i, (k, cnt) in enumerate(pr_dupe_keys.head(args.max_show).items(), 1):
            print(f"  {i:>2}. {k!r} → {cnt}")

    # Exit non-zero if we clearly have a join problem
    if matched == 0 or nonempty_pr == 0:
        sys.exit(4)
    sys.exit(0)

if __name__ == "__main__":
    main()
