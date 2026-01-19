#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd

TZ = ZoneInfo("America/New_York")

# ---- Decision parameters ----
DESC3_CAP = 234          # 85% of 276
PRODLINE_MAXLEN = 5

# Required SAAMM columns and final output order (leave as-is except descrip3 + user24)
REQ_SAAMM_COLS = [
    "extractseqno", "prod", "source-desc-name", "attributegrp",
    "descrip1", "descrip2", "descrip3", "lookupnm", "user24", "rowpointer"
]

_ws_re = re.compile(r"\s+")
_sep_re = re.compile(r"[/,_\-]+")

def now_stamp():
    # MMDDYYYY_HHMM as requested
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
    # used to compare old vs new in a stable way (case + whitespace insensitive)
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
# mixed number: 1-1/2" -> also add tokens like "1 1 2 inch"
mix_pat = re.compile(r"(?P<w>\d+)\s*-\s*(?P<n>\d+)\s*/\s*(?P<d>\d+)\s*\"?")
# fraction only: 1/2" -> add "1 2 inch"
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

    # add size expansions (critical for search success)
    tokens += size_expansions(base_text)

    tokens = uniq_preserve(tokens)
    out = " ".join(tokens).strip()

    # cap at 234 chars
    if len(out) > DESC3_CAP:
        out = out[:DESC3_CAP].rstrip()

    return out

def read_master_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    need = ["prodline", "lookupnm", "prod", "descrip_1", "descrip_2"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise SystemExit(f"Master XLSX missing columns: {missing}")
    return df

def read_saamm_tab(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False).fillna("")
    df.columns = [c.strip() for c in df.columns]  # protect against trailing spaces
    missing = [c for c in REQ_SAAMM_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"SAAMM file {path.name} missing columns: {missing}")
    return df

def derive_mmicsp_token(saamm_file: Path) -> str:
    # from "mmicsp2367 - SIA.txt" -> "mmicsp2367"
    # from "mmicsp2230 - AMD.txt" -> "mmicsp2230"
    stem = saamm_file.stem
    first = stem.split()[0]
    return first

def write_budget_log(log_path: Path, out_df: pd.DataFrame):
    # compute length and pct usage vs 234
    lengths = out_df["descrip3"].astype(str).apply(lambda s: len(s))
    pct = (lengths / float(DESC3_CAP)) * 100.0

    # top 3 unique lengths (largest)
    top3 = sorted(set(lengths.tolist()), reverse=True)[:3]

    # write a simple, readable TXT budget report
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"DESC3 Budget Report\n")
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

        # optional: include a short distribution snapshot
        f.write("Length distribution snapshot (top 10 most common lengths):\n")
        vc = lengths.value_counts().head(10)
        for length_value, count in vc.items():
            f.write(f"  len={int(length_value):3d}  count={int(count):,}\n")

def write_diff_log(diff_csv_path: Path, df_in: pd.DataFrame, df_out: pd.DataFrame, mask_changed_nonblank):
    # only where original descrip3 was NOT blank and it changed
    # required columns per your request:
    # original lookupnm, original descrip3, changed descrip3
    diff_df = pd.DataFrame({
        "lookupnm": df_in.loc[mask_changed_nonblank, "lookupnm"].astype(str),
        "descrip3_old": df_in.loc[mask_changed_nonblank, "descrip3"].astype(str),
        "descrip3_new": df_out.loc[mask_changed_nonblank, "descrip3"].astype(str),
    })
    diff_df.to_csv(diff_csv_path, index=False, encoding="utf-8-sig")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master-xlsx", required=True, help="Master XLSX (prodline, lookupnm, prod, descrip_1, descrip_2, descrip3)")
    ap.add_argument("--saamm-in", required=True, help="SAAMM tab-delimited file OR a folder containing many .txt extracts")
    ap.add_argument("--out-dir", required=True, help="Output folder for SAAMM_<LINE>_upd_mmicspXXXX_MMDDYYYY_HHMM.csv")
    ap.add_argument("--log-dir", required=True, help="Output folder for logs (budget + diff)")
    ap.add_argument("--line", required=True, help="Line code (e.g., SIA). Used in output filename.")
    args = ap.parse_args()

    master_path = Path(args.master_xlsx)
    saamm_in = Path(args.saamm_in)
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    log_dir = Path(args.log_dir); log_dir.mkdir(parents=True, exist_ok=True)
    line = args.line.strip().upper()

    stamp = now_stamp()

    # ---- Load master and build lookup -> desc3_new map ----
    mdf = read_master_xlsx(master_path)
    mdf["__lk"] = mdf["lookupnm"].map(norm_lookupnm)
    mdf["__desc3_new"] = mdf.apply(
        lambda r: build_desc3(r["prodline"], r["lookupnm"], r["prod"], r["descrip_1"], r["descrip_2"]),
        axis=1
    )
    desc3_map = dict(zip(mdf["__lk"], mdf["__desc3_new"]))

    # ---- Determine SAAMM files to process ----
    if saamm_in.is_dir():
        saamm_files = sorted(list(saamm_in.glob("*.txt")) + list(saamm_in.glob("*.tsv")) + list(saamm_in.glob("*.tab")))
        if not saamm_files:
            raise SystemExit(f"No tab-delimited SAAMM files found in folder: {saamm_in}")
    else:
        saamm_files = [saamm_in]

    # ---- Process each SAAMM file ----
    for saamm_file in saamm_files:
        df_in = read_saamm_tab(saamm_file)

        # normalize lookupnm and map to proposed descrip3
        df_in["__lk"] = df_in["lookupnm"].map(norm_lookupnm)
        df_in["__new"] = df_in["__lk"].map(desc3_map)

        # proposed exists
        has_new = df_in["__new"].notna() & (df_in["__new"].astype(str).str.len() > 0)

        # current blank?
        cur_blank = df_in["descrip3"].astype(str).str.strip().eq("")

        # differs? (case/space-insensitive)
        cur_diff = df_in.apply(lambda r: norm_cmp(r["descrip3"]) != norm_cmp(r["__new"]), axis=1)

        # FINAL mask: update if blank OR differs, and only if proposed exists
        mask_update = has_new & (cur_blank | cur_diff)

        # for Diff_log: only where original descrip3 NOT blank AND changed
        mask_changed_nonblank = has_new & (~cur_blank) & cur_diff

        # apply update
        df_out = df_in.copy()
        df_out.loc[mask_update, "descrip3"] = df_out.loc[mask_update, "__new"]
        df_out.loc[mask_update, "user24"] = "d"

        # output naming: SAAMM_<LINE>_upd_mmicspXXXX_MMDDYYYY_HHMM.csv
        mmicsp_token = derive_mmicsp_token(saamm_file)
        out_name = f"SAAMM_{line}_upd_{mmicsp_token}_{stamp}.csv"
        out_path = out_dir / out_name

        # write upload-ready CSV with required columns only
        df_out[REQ_SAAMM_COLS].to_csv(out_path, index=False, encoding="utf-8-sig")

        # logs
        budget_path = log_dir / f"Budget_{line}_{mmicsp_token}_{stamp}.txt"
        diff_path = log_dir / f"Diff_log_{line}_{mmicsp_token}_{stamp}.csv"

        write_budget_log(budget_path, df_out)
        write_diff_log(diff_path, df_in, df_out, mask_changed_nonblank)

        # console summary
        blank_updates = int((has_new & cur_blank).sum())
        diff_updates = int((has_new & ~cur_blank & cur_diff).sum())
        total_updates = int(mask_update.sum())

        print(f"[OK] {saamm_file.name}")
        print(f"     Output : {out_path.name}")
        print(f"     Budget : {budget_path.name}")
        print(f"     Diff   : {diff_path.name}")
        print(f"     Updates: blank={blank_updates:,}  diff(nonblank)={diff_updates:,}  total={total_updates:,}  rows={len(df_in):,}")
        print("")

if __name__ == "__main__":
    main()
