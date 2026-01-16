#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
make_budget_report.py

Create a “budget” CSV with per-row lengths for descrip3 (and desc_src_www, if present),
plus headroom vs a 260-char cap. Output goes to the run’s log folder.

Usage:
  python src/make_budget_report.py \
    --line cdw \
    --in rep/final/SAAMM_desc3_cdw_en_20251007_1651.csv

Options:
  --cap 260   # (optional) override max length used to compute headroom
"""

from __future__ import annotations
import argparse, os, csv, datetime as dt
import pandas as pd

def ts_now() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M")

def read_any_table(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    # CSV/TXT
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        # delimiter sniff
        return pd.read_csv(path, dtype=str, sep=None, engine="python").fillna("")

def infer_logs_dir(in_path: str) -> str:
    """Prefer <repo>/rep/logs if 'rep' is in the path; else alongside input under 'logs'."""
    p = os.path.abspath(in_path)
    parts = p.split(os.sep)
    if "rep" in parts:
        base = os.sep.join(parts[:parts.index("rep")+1])  # includes 'rep'
        logs = os.path.join(base, "logs")
    else:
        logs = os.path.join(os.path.dirname(p), "logs")
    os.makedirs(logs, exist_ok=True)
    return logs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--line", required=True, help="3-char product line (e.g., bli, ils, cdw)")
    ap.add_argument("--in", dest="inp", required=True, help="Path to QA/FINAL CSV or XLSX")
    ap.add_argument("--cap", type=int, default=260, help="Max length for descrip3 (default 260)")
    args = ap.parse_args()

    src = os.path.abspath(args.inp)
    if not os.path.exists(src):
        raise SystemExit(f"Input not found: {src}")

    df = read_any_table(src)
    cols = [c.lower() for c in df.columns]
    # Canonicalize column names to access regardless of case
    def col(name: str) -> str:
        low = name.lower()
        if low in cols:
            return df.columns[cols.index(low)]
        # try a few common aliases for descrip3
        if low == "descrip3":
            for cand in ["descrip3", "Descrip3", "DESCRIP3"]:
                if cand in df.columns: return cand
        raise KeyError(name)

    try:
        c_d3 = col("descrip3")
    except KeyError:
        raise SystemExit("This file does not contain a 'descrip3' column. "
                         "Pass a QA/FINAL export with descrip3 present.")

    c_www = None
    if "desc_src_www" in [c.lower() for c in df.columns]:
        c_www = df.columns[[c.lower() for c in df.columns].index("desc_src_www")]

    # Compute lengths (including spaces)
    df["descrip3_len"] = df[c_d3].map(lambda x: len(x) if isinstance(x, str) else len(str(x)))
    df["headroom"] = args.cap - df["descrip3_len"]
    if c_www:
        df["desc_src_www_len"] = df[c_www].map(lambda x: len(x) if isinstance(x, str) else len(str(x)))

    # Compose output path in logs
    logs_dir = infer_logs_dir(src)
    stamp = ts_now()
    out_csv = os.path.join(logs_dir, f"budget_{args.line}_{stamp}.csv")
    out_txt = os.path.join(logs_dir, f"budget_{args.line}_{stamp}.txt")

    # Choose useful columns if present
    keep = []
    for k in ["rowpointer","lookupnm", c_d3, "descrip3_len", "headroom"]:
        if k in df.columns: keep.append(k)
    if c_www:
        for k in [c_www, "desc_src_www_len"]:
            if k in df.columns: keep.append(k)

    # Write CSV
    df[keep].to_csv(out_csv, index=False, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    # Summary stats to console & txt
    q = df["descrip3_len"].quantile([0.5, 0.9, 0.95, 0.99])
    trimmed = int((df["descrip3_len"] > args.cap).sum())
    summary = [
        f"source: {src}",
        f"rows: {len(df)}",
        f"trimmed(>{args.cap}): {trimmed}",
        f"median: {q.loc[0.5]:.0f}",
        f"p90: {q.loc[0.9]:.0f}",
        f"p95: {q.loc[0.95]:.0f}",
        f"p99: {q.loc[0.99]:.0f}",
        f"output csv: {out_csv}",
    ]
    print("\n".join(summary))
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write("\n".join(summary) + "\n")

if __name__ == "__main__":
    main()
