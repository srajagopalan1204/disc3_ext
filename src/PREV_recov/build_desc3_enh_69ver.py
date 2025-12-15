#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_desc3_enh.py    found at 69 

Here’s a complete drop-in src/build_desc3_enh.py that (a) normalizes spaces/slashes/hyphens, (b) applies glue case-insensitively to both descrip3 and desc_src_www, (c) expands safe synonyms/colors/materials, (d) preserves the SAAMM schema and adds desc_src_www, (e) enforces the 260-char cap, and (f) writes _en_ outputs.
Enhanced builder for SAAMM descrip3 + desc_src_www with:
  - robust normalization (NBSP/thin space, slashes, hyphens, tabs)
  - case-insensitive GLUE on both fields
  - safe synonyms/colors/materials expansions
  - 260-char cap for descrip3
  - desc_src_www excludes pline token and legacy part token
  - pricing join (lookupnm ↔ item/part)
  - optional row overrides
  - _en_ in filenames

Usage:
  python src/build_desc3_enh.py \
    --line bli \
    --saamm /path/to/SAAMM_BLI_Bline.csv \
    --pricing /path/to/Pricing_BLI_Price_sheet.xlsx \
    --out rep \
    --mode {dryrun,writefinal} \
    [--row-overrides controls/row_overrides_bli.csv]
"""

from __future__ import annotations
import argparse, csv, datetime as dt, json, os, re, sys, glob
from typing import Dict, List, Tuple, Iterable, Optional

import pandas as pd

# -----------------------
# Utilities
# -----------------------

def ts_now() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M")

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def read_any_table(path: str) -> pd.DataFrame:
    """Read CSV/TXT with unknown delimiter or an XLS/XLSX."""
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    # sniff delimiter for csv/txt
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return pd.read_csv(path, dtype=str, sep=None, engine="python").fillna("")

def write_csv(path: str, df: pd.DataFrame) -> None:
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

# -----------------------
# JSON5 light parser
# -----------------------

def load_json5(path: str) -> dict:
    """Load JSON5 by removing // and /* */ comments + trailing commas."""
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    # Remove // comments
    text = re.sub(r"//.*?$", "", text, flags=re.MULTILINE)
    # Remove /* ... */ comments
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    # Remove trailing commas before } or ]
    text = re.sub(r",\s*([}\]])", r"\1", text)
    return json.loads(text)

# -----------------------
# Normalization & tokenization
# -----------------------

WS_CHARS = r"\u00A0\u1680\u180E\u2000-\u200D\u202F\u205F\u2060\u3000"

def normalize_text(s: str) -> str:
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    # unify exotic spaces (NBSP/thin) + tabs -> space
    s = re.sub(fr"[{WS_CHARS}\t]+", " ", s)
    # slashes and punctuation that we want as separators
    s = s.replace("/", " ")
    # hyphens/underscores separate tokens for search (e.g., 1-1/4 -> "1 1 4")
    s = re.sub(r"[-_]+", " ", s)
    # collapse spaces, strip
    s = re.sub(r"\s+", " ", s).strip()
    # lower-case
    s = s.lower()
    return s

def tokenize(s: str) -> List[str]:
    s = normalize_text(s)
    if not s:
        return []
    return s.split(" ")

def detokenize(tokens: Iterable[str]) -> str:
    toks = [t for t in tokens if t]
    return " ".join(toks).strip()

# -----------------------
# Rules: synonyms & glue
# -----------------------

def build_syn_map(cfg: dict) -> Dict[str, str]:
    syn = {}
    # colors/materials can be applied as synonyms
    for section in ["colors", "materials", "pricing_synonyms", "synonyms"]:
        if section in cfg and isinstance(cfg[section], dict):
            for k, v in cfg[section].items():
                if not k: 
                    continue
                syn[k.strip().lower()] = str(v).strip().lower()
    return syn

def build_glue_map(cfg: dict) -> Dict[Tuple[str, str], str]:
    out = {}
    m = cfg.get("glue_bigrams", {}) or {}
    for k, v in m.items():
        k = (k or "").strip().lower()
        v = (v or "").strip().lower()
        parts = k.split()
        if len(parts) == 2:
            out[(parts[0], parts[1])] = v
    return out

def apply_synonyms(tokens: List[str], syn: Dict[str,str]) -> List[str]:
    if not syn:
        return tokens
    return [ syn.get(t, t) for t in tokens ]

def apply_glue(tokens: List[str], glue_pairs: Dict[Tuple[str,str], str]) -> List[str]:
    if not glue_pairs:
        return tokens
    out: List[str] = []
    i = 0
    n = len(tokens)
    while i < n:
        t0 = tokens[i]
        t1 = tokens[i+1] if i+1<n else None
        key = (t0, t1) if t1 is not None else None
        if key and key in glue_pairs:
            out.append(glue_pairs[key])
            i += 2
        else:
            out.append(t0)
            i += 1
    return out

# -----------------------
# Builder core
# -----------------------

def detect_pricing_part_column(dfp: pd.DataFrame) -> Optional[str]:
    candidates = [
        "item#", "item #", "item no", "item_no", "item number", "item",
        "part#", "part #", "part no", "part_no", "part", "sku", "lookupnm"
    ]
    cols = [c.lower() for c in dfp.columns]
    for c in candidates:
        if c in cols:
            # return actual cased column name
            return dfp.columns[cols.index(c)]
    return None

def uniq_preserve(seq: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for t in seq:
        if t not in seen:
            out.append(t); seen.add(t)
    return out

def cap_260(s: str) -> Tuple[str, bool]:
    if len(s) <= 260:
        return s, False
    return s[:260].rstrip(), True

def build_row_fields(row: pd.Series,
                     pricing_row: Optional[pd.Series],
                     cfg: dict,
                     syn_map: Dict[str,str],
                     glue_map: Dict[Tuple[str,str], str],
                     pline3: str) -> Tuple[str, str]:
    """
    Returns (descrip3_after, desc_src_www)
    """
    # Gather text sources
    fields_saamm = []
    for name in ["source-desc-name", "descrip1", "descrip2", "Item Description", "attributegrp"]:
        if name in row and str(row[name]).strip():
            fields_saamm.append(str(row[name]))
    # Pricing description candidates
    pricing_fields = []
    if pricing_row is not None:
        for name in ["Item Description", "Description", "Long Description", "desc", "desc1"]:
            if name in pricing_row and str(pricing_row[name]).strip():
                pricing_fields.append(str(pricing_row[name]))

    # Base strings
    s_saamm = " ".join(fields_saamm)
    s_pr = " ".join(pricing_fields)

    # Normalize -> tokens
    toks_saamm = tokenize(s_saamm)
    toks_pr    = tokenize(s_pr)

    # Merge token sources
    base_tokens = toks_saamm + toks_pr

    # Add helpful structural tokens
    # - prod (split by hyphen already normalized)
    prod = normalize_text(row.get("prod", "") or "")
    if prod:
        base_tokens = [prod] + base_tokens

    # Apply synonyms & glue to both streams
    tok = apply_synonyms(base_tokens, syn_map)
    tok = apply_glue(tok, glue_map)

    # descrip3 tokens start with pline token (legacy friendliness)
    descrip3_tokens = ([pline3] if pline3 else []) + tok

    # append legacy part at end
    lookupnm = normalize_text(row.get("lookupnm", "") or "")
    if lookupnm:
        descrip3_tokens = descrip3_tokens + [lookupnm]

    # desc_src_www: similar but exclude pline token and the legacy part token
    www_tokens = [t for t in tok if t]  # base + rules applied
    if pline3:
        www_tokens = [t for t in www_tokens if t != pline3]
    if lookupnm:
        www_tokens = [t for t in www_tokens if t != lookupnm]

    # de-dup but keep order
    descrip3_tokens = uniq_preserve(descrip3_tokens)
    www_tokens      = uniq_preserve(www_tokens)

    # join, cap
    descrip3_str, trimmed = cap_260(detokenize(descrip3_tokens))
    desc_src_www_str      = detokenize(www_tokens)

    return descrip3_str, desc_src_www_str

# -----------------------
# Main
# -----------------------

def run(args: argparse.Namespace) -> None:
    out_root = args.out
    ensure_dir(out_root)
    ensure_dir(os.path.join(out_root, "qa"))
    ensure_dir(os.path.join(out_root, "final"))
    ensure_dir(os.path.join(out_root, "logs"))
    ensure_dir(os.path.join(out_root, "manifest"))

    # Load config for this line
    cfg_path = os.path.join("configs", "lines", f"{args.line}.json5")
    if not os.path.exists(cfg_path):
        sys.exit(f"Config not found: {cfg_path}")
    cfg = load_json5(cfg_path)
    pline3 = (cfg.get("pline3") or args.line or "").strip().lower()

    syn_map  = build_syn_map(cfg)
    glue_map = build_glue_map(cfg)

    # Read inputs
    df = read_any_table(args.saamm)
    df = df.fillna("")
    # Ensure all base columns exist
    for col in ["descrip3", "lookupnm", "prod", "source-desc-name", "descrip1", "descrip2", "attributegrp"]:
        if col not in df.columns:
            # add missing
            df[col] = ""

    # pricing join (mainly for extra context; not mandatory)
    dfp = read_any_table(args.pricing)
    part_col = detect_pricing_part_column(dfp)
    if part_col is None:
        part_col = "lookupnm" if "lookupnm" in dfp.columns else None
    dfp = dfp.fillna("")
    if part_col:
        # normalized key to join
        dfp["_join_key"] = dfp[part_col].astype(str).str.strip().str.lower()
    else:
        dfp["_join_key"] = ""

    df["_join_key"] = df["lookupnm"].astype(str).str.strip().str.lower()

    dfp_slim = dfp[[c for c in dfp.columns if c != "_join_key"] + ["_join_key"]]

    left = df.merge(dfp_slim, on="_join_key", how="left", suffixes=("", "_pr"))

    # Build outputs per row
    d3_after = []
    www_list = []
    trimmed_flags = []

    for i, row in left.iterrows():
        # locate pricing row slice (not strictly required)
        pr_idx = None
        # N.B. after merge, multiple pricing columns may exist; we pass the whole row to builder
        descrip3_new, www_new = build_row_fields(row, row, cfg, syn_map, glue_map, pline3)
        d3_after.append(descrip3_new)
        www_list.append(www_new)
        trimmed_flags.append("yes" if len(descrip3_new) >= 260 else "no")

    left["descrip3"] = d3_after
    left["desc_src_www"] = www_list
    left["trimmed_flag"] = trimmed_flags

    # Optional: row overrides
    if args.row_overrides:
        ov = read_any_table(args.row_overrides)
        for c in ["rowpointer", "descrip3_override", "desc_src_www_override"]:
            if c not in ov.columns:
                ov[c] = ""
        ov = ov.fillna("")
        ov_map = { str(r["rowpointer"]).strip(): r for _, r in ov.iterrows() }
        def apply_override(irow):
            rp = str(irow.get("rowpointer","")).strip()
            if rp and rp in ov_map:
                rec = ov_map[rp]
                d3o = str(rec.get("descrip3_override","")).strip()
                wwo = str(rec.get("desc_src_www_override","")).strip()
                if d3o:
                    irow["descrip3"] = d3o
                if wwo:
                    irow["desc_src_www"] = wwo
            return irow
        left = left.apply(apply_override, axis=1)

    # Manifest / QA / Final
    stamp = ts_now()
    qa_path   = os.path.join(out_root, "qa",    f"QA_{args.line}_en_{stamp}.csv")
    final_path= os.path.join(out_root, "final", f"SAAMM_desc3_{args.line}_en_{stamp}.csv")
    log_path  = os.path.join(out_root, "logs",  f"run_{args.line}_en_{stamp}.log")
    mani_path = os.path.join(out_root, "manifest", f"manifest_{args.line}_en_{stamp}.json")

    # Build QA summary subset
    qa_cols = [c for c in ["rowpointer","lookupnm","prod","source-desc-name","descrip1","descrip2","attributegrp","descrip3","desc_src_www","trimmed_flag"] if c in left.columns]
    qa = left[qa_cols].copy()
    write_csv(qa_path, qa)

    # Log
    rows_in = len(df)
    trimmed_count = (left["trimmed_flag"]=="yes").sum()
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"[{stamp}] line={args.line} mode={args.mode}\n")
        f.write(f"rows_in={rows_in}\n")
        f.write(f"trimmed_descrip3={trimmed_count}\n")
        f.write(f"cfg_path={cfg_path}\n")
        f.write(f"pricing_part_col={part_col or ''}\n")

    # Manifest
    manifest = {
        "line": args.line,
        "timestamp": stamp,
        "saamm": os.path.abspath(args.saamm),
        "pricing": os.path.abspath(args.pricing),
        "cfg_path": os.path.abspath(cfg_path),
        "qa_csv": os.path.abspath(qa_path),
        "final_csv": os.path.abspath(final_path) if args.mode=="writefinal" else None,
        "row_overrides": os.path.abspath(args.row_overrides) if args.row_overrides else None,
        "counts": {
            "rows_in": rows_in,
            "trimmed": int(trimmed_count),
        }
    }
    with open(mani_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    if args.mode == "writefinal":
        # Keep the original SAAMM columns order (where possible) and append/replace fields
        # Rebuild a frame that mirrors the original input schema but with updated descrip3 and extra desc_src_www
        base_cols = list(df.columns)
        out_df = left.copy()
        # Guarantee columns existence
        for c in base_cols:
            if c not in out_df.columns:
                out_df[c] = ""
        # In the same order + append desc_src_www if not part of base
        ordered = base_cols.copy()
        if "desc_src_www" not in ordered:
            ordered.append("desc_src_www")
        if "trimmed_flag" not in ordered:
            ordered.append("trimmed_flag")
        write_csv(final_path, out_df[ordered])

    print(f"QA written: {qa_path}")
    if args.mode == "writefinal":
        print(f"FINAL written: {final_path}")
    print(f"LOG: {log_path}")
    print(f"MANIFEST: {mani_path}")

# -----------------------
# CLI
# -----------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--line", required=True, help="3-char product line code (e.g., bli)")
    ap.add_argument("--saamm", required=True, help="Path to SAAMM CSV/TXT")
    ap.add_argument("--pricing", required=True, help="Path to Pricing Excel/CSV")
    ap.add_argument("--out", required=True, help="Output folder root (rep/* will be created)")
    ap.add_argument("--mode", required=True, choices=["dryrun","writefinal"])
    ap.add_argument("--row-overrides", help="Optional CSV with rowpointer, descrip3_override, desc_src_www_override")
    args = ap.parse_args()
    run(args)

if __name__ == "__main__":
    main()
