#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
saamm_desc3_update_v2_1.py

Version: 2.1.0

Brief:
- Reads a master pricing XLSX (prodline, lookupnm, prod, descrip_1, descrip_2).
- Reads one SAAMM tab-delimited extract OR a flat folder of extracts (NON-recursive).
- Auto-detects mmicspNNNN and LINE from each SAAMM filename.
- Builds a search-optimized descrip3 using decision logic:
  - includes product line token (trim to 5 chars)
  - token-based, normalized lowercase
  - includes lookupnm and size expansions (e.g., 1-1/2" -> "1 1 2 inch")
  - caps output at 234 chars (85% of 276)
- Updates output ONLY when descrip3 is blank OR differs from computed value.
- Sets user24="d" ONLY for updated rows.
- Writes:
  1) Upload-ready CSV: SAAMM_<LINE>_upd_mmicspNNNN_MMDDYYYY_HHMM.csv
  2) Budget log:       Budget_<LINE>_mmicspNNNN_MMDDYYYY_HHMM.txt
  3) Diff log:         Diff_log_<LINE>_mmicspNNNN_MMDDYYYY_HHMM.csv (only when original descrip3 not blank and changed)
- Safety Control:
  - If a file’s name cannot be parsed for mmicsp/LINE, it is SKIPPED and recorded in Skip_log_*.txt
"""

import argparse
import re
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd

TZ = ZoneInfo("America/New_York")

SCRIPT_NAME = "saamm_desc3_update_v2_1.py"
SCRIPT_VERSION = "2.1.0"

# ---- Decision parameters ----
DESC3_CAP = 234          # 85% of 276 (your requirement)
PRODLINE_MAXLEN = 5

# Required SAAMM columns and final output order (leave as-is except descrip3 + user24)
REQ_SAAMM_COLS = [
    "extractseqno", "prod", "source-desc-name", "attributegrp",
    "descrip1", "descrip2", "descrip3", "lookupnm", "user24", "rowpointer"
]

_ws_re = re.compile(r"\s+")
_sep_re = re.compile(r"[/,_\-]+")

def now_stamp():
    # MMDDYYYY_HHMM
    return datetime.now(TZ).strftime("%m%d%Y_%H%M")

def norm_lookupnm(x: str) -> str:
    s = str(x or "").strip().upper()
    s = _ws_re.sub(" ", s)
    return s

def norm_text(x: str) -> str:
    s = str(x or "").strip().lower()
    s = _sep_re.sub(" ", s)
    s = s.replace('"', " ")
    s = _ws_re.sub(" ", s).strip()
    return s

def norm_cmp(x: str) -> str:
    # compare old vs new stable (case + whitespace insensitive)
    return _ws_re.sub(" ", str(x or "").strip().lower())

def uniq_preserve(tokens):
    seen = set()
    out = []
    for t in tokens:
        if not t:
            continue
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out

# ---- Size expansions ----
mix_pat = re.compile(r"(?P<w>\d+)\s*-\s*(?P<n>\d+)\s*/\s*(?P<d>\d+)\s*\"?")
frac_pat = re.compile(r"(?<!\d)(?P<n>\d+)\s*/\s*(?P<d>\d+)\s*\"?")

def size_expansions(text_lc: str):
    extras = []

    for m in mix_pat.finditer(text_lc):
        w, n, d = m.group("w"), m.group("n"), m.group("d")
        extras += [f"{w}-{n}/{d}", f"{w} {n} {d}", f"{w} {n} {d} inch"]

    masked = mix_pat.sub(" ", text_lc)
    for m in frac_pat.finditer(masked):
        n, d = m.group("n"), m.group("d")
        extras += [f"{n}/{d}", f"{n} {d}", f"{n} {d} inch"]

    out = []
    for e in extras:
        out += norm_text(e).split()
    return out

# ---- Core decision logic for descrip3 ----
def build_desc3(prodline, lookupnm, prod, d1, d2):
    # include product line (trim to 5 chars)
    pl = norm_text(prodline)[:PRODLINE_MAXLEN] if prodline else ""

    base_text = " ".join([
        norm_text(prod),
        norm_text(d1),
        norm_text(d2),
        norm_text(lookupnm),
    ])

    tokens = []
    if pl:
        tokens += pl.split()

    tokens += base_text.split()
    tokens += size_expansions(base_text)

    tokens = uniq_preserve(tokens)

    out = " ".join(tokens).strip()
    if len(out) > DESC3_CAP:
        out = out[:DESC3_CAP].rstrip()

    return out

# ---- IO ----
def read_master_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    need = ["prodline", "lookupnm", "prod", "descrip_1", "descrip_2"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise SystemExit(f"Master XLSX missing columns: {missing}")
    return df

def read_saamm_tab(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False).fillna("")
    df.columns = [c.strip() for c in df.columns]  # defend against trailing spaces
    missing = [c for c in REQ_SAAMM_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"SAAMM file {path.name} missing columns: {missing}")
    return df

def try_parse_mmicsp_and_line(saamm_file: Path):
    """
    Safety-friendly parser.
    Returns (mmicsp_token, line_token) OR (None, None) if cannot parse.

    Supported filename styles (recommended: no spaces):
      mmicsp2367-SIA.txt
      mmicsp2367_SIA.txt
      mmicsp2367 - SIA.txt
      mmicsp2367-SIA-anything.txt (line extracted from 2nd token after separator)
    """
    stem = saamm_file.stem.strip()
    if not stem:
        return None, None

    # Normalize: " - " -> "-", "_" -> "-", collapse whitespace
    s = re.sub(r"\s*-\s*", "-", stem)
    s = s.replace("_", "-")
    s = _ws_re.sub(" ", s).strip()

    # Prefer dash-splitting
    if "-" in s:
        parts = [p for p in s.split("-") if p]
        mmicsp = parts[0].strip()
        if not mmicsp.lower().startswith("mmicsp"):
            return None, None

        line = ""
        for p in parts[1:]:
            p2 = re.sub(r"[^A-Za-z0-9]", "", p).upper()
            if 2 <= len(p2) <= 6:
                line = p2
                break

        if not line:
            return None, None

        return mmicsp, line

    # Fallback: "mmicsp2367 SIA"
    parts = s.split()
    if len(parts) >= 2 and parts[0].lower().startswith("mmicsp"):
        line = re.sub(r"[^A-Za-z0-9]", "", parts[1]).upper()
        if 2 <= len(line) <= 6:
            return parts[0], line

    return None, None

def write_budget_log(log_path: Path, out_df: pd.DataFrame):
    lengths = out_df["descrip3"].astype(str).apply(len)
    pct = (lengths / float(DESC3_CAP)) * 100.0
    top3 = sorted(set(lengths.tolist()), reverse=True)[:3]

    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"{SCRIPT_NAME} v{SCRIPT_VERSION}\n")
        f.write("DESC3 Budget Report\n")
        f.write(f"Cap (chars): {DESC3_CAP}\n\n")

        f.write("Top 3 unique descrip3 lengths (chars):\n")
        for i, v in enumerate(top3, start=1):
            f.write(f"  {i}. {v}\n")
        f.write("\n")

        f.write("Summary:\n")
        f.write(f"  Rows: {len(out_df):,}\n")
        f.write(f"  Max length: {int(lengths.max())}\n")
        f.write(f"  Avg length: {lengths.mean():.1f}\n")
        f.write(f"  Avg % used: {pct.mean():.1f}%\n")
        f.write(f"  Over cap (should be 0): {(lengths > DESC3_CAP).sum():,}\n\n")

        f.write("Length distribution snapshot (top 10 most common lengths):\n")
        vc = lengths.value_counts().head(10)
        for length_value, count in vc.items():
            f.write(f"  len={int(length_value):3d}  count={int(count):,}\n")

def write_diff_log(diff_csv_path: Path, df_in: pd.DataFrame, df_out: pd.DataFrame, mask_changed_nonblank):
    diff_df = pd.DataFrame({
        "lookupnm": df_in.loc[mask_changed_nonblank, "lookupnm"].astype(str),
        "descrip3_old": df_in.loc[mask_changed_nonblank, "descrip3"].astype(str),
        "descrip3_new": df_out.loc[mask_changed_nonblank, "descrip3"].astype(str),
    })
    diff_df.to_csv(diff_csv_path, index=False, encoding="utf-8-sig")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master-xlsx", required=True, help="Master XLSX with prodline/lookupnm/prod/descrip_1/descrip_2")
    ap.add_argument("--saamm-in", required=True, help="SAAMM tab file OR a folder (NON-recursive) containing many SAAMM tab files")
    ap.add_argument("--out-dir", required=True, help="Output folder for SAAMM_<LINE>_upd_mmicspNNNN_MMDDYYYY_HHMM.csv")
    ap.add_argument("--log-dir", required=True, help="Output folder for Budget_*.txt, Diff_log_*.csv, Skip_log_*.txt")
    args = ap.parse_args()

    master_path = Path(args.master_xlsx)
    saamm_in = Path(args.saamm_in)
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    log_dir = Path(args.log_dir); log_dir.mkdir(parents=True, exist_ok=True)

    stamp = now_stamp()

    # Skip log for this run (one file listing all skipped inputs + reason)
    skip_log_path = log_dir / f"Skip_log_{stamp}.txt"
    skipped = 0

    with open(skip_log_path, "w", encoding="utf-8") as slog:
        slog.write(f"{SCRIPT_NAME} v{SCRIPT_VERSION}\n")
        slog.write(f"Run stamp: {stamp}\n")
        slog.write("Skipped files (Safety Control):\n\n")

        # ---- Load master and build lookup -> desc3_new map ----
        mdf = read_master_xlsx(master_path)
        mdf["__lk"] = mdf["lookupnm"].map(norm_lookupnm)
        mdf["__desc3_new"] = mdf.apply(
            lambda r: build_desc3(r["prodline"], r["lookupnm"], r["prod"], r["descrip_1"], r["descrip_2"]),
            axis=1
        )
        desc3_map = dict(zip(mdf["__lk"], mdf["__desc3_new"]))

        # ---- Determine SAAMM files to process (NON-RECURSIVE) ----
        if saamm_in.is_dir():
            saamm_files = []
            for ext in ("*.txt", "*.tsv", "*.tab"):
                saamm_files.extend(list(saamm_in.glob(ext)))
            saamm_files = sorted(saamm_files)
            if not saamm_files:
                raise SystemExit(f"No SAAMM tab files (*.txt/*.tsv/*.tab) found in folder: {saamm_in}")
        else:
            saamm_files = [saamm_in]

        # ---- Process each SAAMM file ----
        for saamm_file in saamm_files:
            mmicsp_token, line_token = try_parse_mmicsp_and_line(saamm_file)
            if not mmicsp_token or not line_token:
                skipped += 1
                slog.write(f"- {saamm_file.name} :: cannot parse mmicsp/LINE from filename\n")
                continue

            try:
                df_in = read_saamm_tab(saamm_file)
            except Exception as e:
                skipped += 1
                slog.write(f"- {saamm_file.name} :: read/column error: {e}\n")
                continue

            df_in["__lk"] = df_in["lookupnm"].map(norm_lookupnm)
            df_in["__new"] = df_in["__lk"].map(desc3_map)

            has_new = df_in["__new"].notna() & (df_in["__new"].astype(str).str.len() > 0)
            cur_blank = df_in["descrip3"].astype(str).str.strip().eq("")
            cur_diff = df_in.apply(lambda r: norm_cmp(r["descrip3"]) != norm_cmp(r["__new"]), axis=1)

            # Update if blank OR differs (only if proposed exists)
            mask_update = has_new & (cur_blank | cur_diff)

            # Diff log: only where original descrip3 NOT blank AND changed
            mask_changed_nonblank = has_new & (~cur_blank) & cur_diff

            df_out = df_in.copy()
            df_out.loc[mask_update, "descrip3"] = df_out.loc[mask_update, "__new"]
            df_out.loc[mask_update, "user24"] = "d"

            out_name = f"SAAMM_{line_token}_upd_{mmicsp_token}_{stamp}.csv"
            out_path = out_dir / out_name
            df_out[REQ_SAAMM_COLS].to_csv(out_path, index=False, encoding="utf-8-sig")

            budget_path = log_dir / f"Budget_{line_token}_{mmicsp_token}_{stamp}.txt"
            diff_path = log_dir / f"Diff_log_{line_token}_{mmicsp_token}_{stamp}.csv"

            write_budget_log(budget_path, df_out)
            write_diff_log(diff_path, df_in, df_out, mask_changed_nonblank)

            blank_updates = int((has_new & cur_blank).sum())
            diff_updates = int((has_new & ~cur_blank & cur_diff).sum())
            total_updates = int(mask_update.sum())

            print(f"[OK] {saamm_file.name}")
            print(f"     Parsed : line={line_token}  mmicsp={mmicsp_token}")
            print(f"     Output : {out_path.name}")
            print(f"     Budget : {budget_path.name}")
            print(f"     Diff   : {diff_path.name}")
            print(f"     Updates: blank={blank_updates:,}  diff(nonblank)={diff_updates:,}  total={total_updates:,}  rows={len(df_in):,}")
            print("")

        slog.write(f"\nSkipped count: {skipped}\n")

    # If nothing was skipped, you still get a skip log (it documents “0 skipped”)
    if skipped == 0:
        print(f"[INFO] Safety Control: no files skipped. (See {skip_log_path.name})")
    else:
        print(f"[WARN] Safety Control: skipped {skipped} file(s). (See {skip_log_path.name})")

if __name__ == "__main__":
    main()
