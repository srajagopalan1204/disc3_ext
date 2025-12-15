#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_desc3_enh.py
83 
Here’s the updated src/build_desc3_enh.py with the new CLI controls to include/exclude the 3-char line token in each output field, plus stricter removal of stray line tokens coming from prod or other sources.

--pline-in-descrip3 {yes,no} → include the line token at the start of descrip3 (default: yes unless overridden by config include_pline_descrip3).

--pline-in-www {yes,no} → include the line token in desc_src_www (default: no unless overridden by config include_pline_www).

The builder now removes all occurrences of the line token from both streams first; if inclusion is enabled, it then adds a single line token in the right place (avoids “ils ils …” duplicates).

More robust normalization (slashes, hyphens, Unicode spaces) is kept.

Enhanced builder for SAAMM descrip3 + desc_src_www with:
  - robust normalization (NBSP/thin space, slashes, hyphens, tabs)
  - case-insensitive GLUE on both fields
  - safe synonyms/colors/materials expansions
  - 260-char cap for descrip3
  - explicit CLI to include/exclude the 3-char line token in each field:
      --pline-in-descrip3 {yes,no}
      --pline-in-www      {yes,no}
  - removal of duplicate line tokens coming from prod or other fields
  - pricing join (lookupnm ↔ item/part)
  - optional row overrides
  - _en_ in filenames

Usage:
  python src/build_desc3_enh.py \
    --line <pli> \
    --saamm /path/to/SAAMM_<PLI>.csv \
    --pricing /path/to/Pricing_<PLI>.xlsx \
    --out rep \
    --mode {dryrun,writefinal} \
    [--pline-in-descrip3 yes|no] \
    [--pline-in-www yes|no] \
    [--row-overrides controls/row_overrides_<pli>.csv]
"""

from __future__ import annotations
import argparse, csv, datetime as dt, json, os, re, sys
from typing import Dict, List, Tuple, Iterable, Optional

import pandas as pd

# -----------------------
# Small helpers
# -----------------------

def ts_now() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M")

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def read_any_table(path: str) -> pd.DataFrame:
    """Read CSV/TXT (delimiter-sniffed) or Excel."""
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return pd.read_csv(path, dtype=str, sep=None, engine="python").fillna("")

def write_csv(path: str, df: pd.DataFrame) -> None:
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

# -----------------------
# JSON5-light loader (tolerates comments & trailing commas)
# -----------------------

def load_json5(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    text = re.sub(r"//.*?$", "", text, flags=re.MULTILINE)     # // comments
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)      # /* ... */ comments
    text = re.sub(r",\s*([}\]])", r"\1", text)                  # trailing commas
    return json.loads(text)

# -----------------------
# Normalization & tokenization
# -----------------------

# Unicode space chars (NBSP, thin space, etc.)
WS_CHARS = r"\u00A0\u1680\u180E\u2000-\u200D\u202F\u205F\u2060\u3000"

def normalize_text(s: str) -> str:
    """Lowercase, unify whitespace, split punctuation that should be separators."""
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    # exotic spaces & tabs -> space
    s = re.sub(fr"[{WS_CHARS}\t]+", " ", s)
    # slashes & hyphens/underscores as separators
    s = s.replace("/", " ")
    s = re.sub(r"[-_]+", " ", s)
    # collapse spaces
    s = re.sub(r"\s+", " ", s).strip()
    s = s.lower()
    return s

def tokenize(s: str) -> List[str]:
    s = normalize_text(s)
    return s.split(" ") if s else []

def detokenize(tokens: Iterable[str]) -> str:
    toks = [t for t in tokens if t]
    return " ".join(toks).strip()

# -----------------------
# Rules: synonyms & glue
# -----------------------

def build_syn_map(cfg: dict) -> Dict[str, str]:
    syn = {}
    for section in ["colors", "materials", "pricing_synonyms", "synonyms"]:
        d = cfg.get(section) or {}
        if isinstance(d, dict):
            for k, v in d.items():
                if k is None: 
                    continue
                syn[str(k).strip().lower()] = str(v).strip().lower()
    return syn

def build_glue_map(cfg: dict) -> Dict[Tuple[str, str], str]:
    out = {}
    m = cfg.get("glue_bigrams", {}) or {}
    for k, v in m.items():
        key = str(k or "").strip().lower()
        val = str(v or "").strip().lower()
        parts = key.split()
        if len(parts) == 2:
            out[(parts[0], parts[1])] = val
    return out

def apply_synonyms(tokens: List[str], syn: Dict[str, str]) -> List[str]:
    if not syn:
        return tokens
    return [syn.get(t, t) for t in tokens]

def apply_glue(tokens: List[str], glue_pairs: Dict[Tuple[str, str], str]) -> List[str]:
    if not glue_pairs:
        return tokens
    out: List[str] = []
    i = 0
    n = len(tokens)
    while i < n:
        t0 = tokens[i]
        t1 = tokens[i+1] if i+1 < n else None
        if t1 is not None and (t0, t1) in glue_pairs:
            out.append(glue_pairs[(t0, t1)])
            i += 2
        else:
            out.append(t0)
            i += 1
    return out

def uniq_preserve(seq: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for t in seq:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out

def cap_260(s: str) -> Tuple[str, bool]:
    if len(s) <= 260:
        return s, False
    return s[:260].rstrip(), True

# -----------------------
# Builder core
# -----------------------

def detect_pricing_part_column(dfp: pd.DataFrame) -> Optional[str]:
    candidates = [
        "item#", "item #", "item no", "item_no", "item number", "item",
        "part#", "part #", "part no", "part_no", "part", "sku", "lookupnm"
    ]
    cols_lower = [c.lower() for c in dfp.columns]
    for c in candidates:
        if c in cols_lower:
            return dfp.columns[cols_lower.index(c)]
    return None

def build_row_fields(
    row: pd.Series,
    pricing_row: Optional[pd.Series],
    cfg: dict,
    syn_map: Dict[str, str],
    glue_map: Dict[Tuple[str, str], str],
    pline3: str,
    include_pline_d3: bool,
    include_pline_www: bool,
) -> Tuple[str, str]:
    """
    Returns (descrip3_after, desc_src_www)
    """

    # Collect SAAMM descriptive fields
    saam_strs = []
    for name in ["source-desc-name", "descrip1", "descrip2", "Item Description", "attributegrp"]:
        if name in row and str(row[name]).strip():
            saam_strs.append(str(row[name]))
    s_saamm = " ".join(saam_strs)

    # Pricing-side descriptions (optional enrichment)
    pricing_strs = []
    if pricing_row is not None:
        for name in ["Item Description", "Description", "Long Description", "desc", "desc1"]:
            if name in pricing_row and str(pricing_row[name]).strip():
                pricing_strs.append(str(pricing_row[name]))
    s_price = " ".join(pricing_strs)

    # Base tokens = SAAMM + Pricing
    toks_saamm = tokenize(s_saamm)
    toks_price = tokenize(s_price)
    base_tokens = toks_saamm + toks_price

    # Prepend prod (often carries structure), but normalize first
    prod_tok = tokenize(row.get("prod", "") or "")
    if prod_tok:
        base_tokens = prod_tok + base_tokens

    # Normalize with synonyms + glue
    tok = apply_synonyms(base_tokens, syn_map)
    tok = apply_glue(tok, glue_map)

    # Remove ANY existing occurrences of pline3 first (from prod, desc, etc.)
    if pline3:
        tok = [t for t in tok if t != pline3]

    # Build descrip3 stream
    descrip3_tokens = tok[:]
    if include_pline_d3 and pline3:
        # ensure exactly one instance at the front
        descrip3_tokens = [pline3] + descrip3_tokens

    # Append legacy part number (lookupnm) to descrip3
    lookupnm = normalize_text(row.get("lookupnm", "") or "")
    if lookupnm:
        descrip3_tokens = descrip3_tokens + [lookupnm]

    # Build desc_src_www stream (exclude line token unless explicitly requested)
    www_tokens = tok[:]
    if not include_pline_www and pline3:
        www_tokens = [t for t in www_tokens if t != pline3]

    # Always exclude legacy part from desc_src_www
    if lookupnm:
        www_tokens = [t for t in www_tokens if t != lookupnm]

    # De-duplicate while preserving order
    descrip3_tokens = uniq_preserve(descrip3_tokens)
    www_tokens      = uniq_preserve(www_tokens)

    # Join + cap
    descrip3_str, trimmed = cap_260(detokenize(descrip3_tokens))
    desc_src_www_str      = detokenize(www_tokens)

    return descrip3_str, desc_src_www_str

# -----------------------
# Main
# -----------------------

def run(args: argparse.Namespace) -> None:
    out_root = args.out
    for sub in ("qa", "final", "logs", "manifest"):
        ensure_dir(os.path.join(out_root, sub))

    # Load config
    cfg_path = os.path.join("configs", "lines", f"{args.line}.json5")
    if not os.path.exists(cfg_path):
        sys.exit(f"Config not found: {cfg_path}")
    cfg = load_json5(cfg_path)

    pline3 = (cfg.get("pline3") or args.line or "").strip().lower()

    # Effective include flags: CLI overrides config, else defaults
    include_pline_d3 = cfg.get("include_pline_descrip3", True)
    include_pline_www = cfg.get("include_pline_www", False)

    if args.pline_in_descrip3 is not None:
        include_pline_d3 = (args.pline_in_descrip3.lower() == "yes")
    if args.pline_in_www is not None:
        include_pline_www = (args.pline_in_www.lower() == "yes")

    syn_map  = build_syn_map(cfg)
    glue_map = build_glue_map(cfg)

    # Read inputs
    df = read_any_table(args.saamm).fillna("")
    # Ensure required columns exist (kept as text)
    for col in ["descrip3", "lookupnm", "prod", "source-desc-name", "descrip1", "descrip2", "attributegrp"]:
        if col not in df.columns:
            df[col] = ""

    # Read pricing and detect part key
    dfp = read_any_table(args.pricing).fillna("")
    part_col = detect_pricing_part_column(dfp)
    if part_col is None:
        part_col = "lookupnm" if "lookupnm" in dfp.columns else None

    if part_col:
        dfp["_join_key"] = dfp[part_col].astype(str).str.strip().str.lower()
    else:
        dfp["_join_key"] = ""

    df["_join_key"] = df["lookupnm"].astype(str).str.strip().str.lower()

    # Slim pricing to avoid column name explosions; keep for enrichment lookup
    dfp_slim = dfp[[c for c in dfp.columns if c != "_join_key"] + ["_join_key"]]

    left = df.merge(dfp_slim, on="_join_key", how="left", suffixes=("", "_pr"))

    # Build outputs
    d3_after, www_list, trimmed_flags = [], [], []

    for _, row in left.iterrows():
        d3_new, www_new = build_row_fields(
            row=row,
            pricing_row=row,
            cfg=cfg,
            syn_map=syn_map,
            glue_map=glue_map,
            pline3=pline3,
            include_pline_d3=include_pline_d3,
            include_pline_www=include_pline_www,
        )
        d3_after.append(d3_new)
        www_list.append(www_new)
        trimmed_flags.append("yes" if len(d3_new) >= 260 else "no")

    left["descrip3"] = d3_after
    left["desc_src_www"] = www_list
    left["trimmed_flag"] = trimmed_flags

    # Optional row overrides
    if args.row_overrides:
        ov = read_any_table(args.row_overrides).fillna("")
        for c in ["rowpointer", "descrip3_override", "desc_src_www_override"]:
            if c not in ov.columns:
                ov[c] = ""
        ov_map = { str(r["rowpointer"]).strip(): r for _, r in ov.iterrows() }

        def apply_override(irow):
            rp = str(irow.get("rowpointer", "")).strip()
            if rp and rp in ov_map:
                rec = ov_map[rp]
                d3o = str(rec.get("descrip3_override", "")).strip()
                wwo = str(rec.get("desc_src_www_override", "")).strip()
                if d3o:
                    irow["descrip3"] = d3o
                if wwo:
                    irow["desc_src_www"] = wwo
            return irow

        left = left.apply(apply_override, axis=1)

    # Paths
    stamp = ts_now()
    qa_path    = os.path.join(out_root, "qa",    f"QA_{args.line}_en_{stamp}.csv")
    final_path = os.path.join(out_root, "final", f"SAAMM_desc3_{args.line}_en_{stamp}.csv")
    log_path   = os.path.join(out_root, "logs",  f"run_{args.line}_en_{stamp}.log")
    mani_path  = os.path.join(out_root, "manifest", f"manifest_{args.line}_en_{stamp}.json")

    # QA slice
    qa_cols = [c for c in ["rowpointer","lookupnm","prod","source-desc-name","descrip1","descrip2","attributegrp","descrip3","desc_src_www","trimmed_flag"] if c in left.columns]
    write_csv(qa_path, left[qa_cols].copy())

    # Log
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"[{stamp}] line={args.line} mode={args.mode}\n")
        f.write(f"rows_in={len(df)}\n")
        f.write(f"trimmed_descrip3={(left['trimmed_flag']=='yes').sum()}\n")
        f.write(f"cfg_path={cfg_path}\n")
        f.write(f"pricing_part_col={part_col or ''}\n")
        f.write(f"include_pline_descrip3={include_pline_d3}\n")
        f.write(f"include_pline_www={include_pline_www}\n")

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
            "rows_in": int(len(df)),
            "trimmed": int((left['trimmed_flag']=='yes').sum()),
        },
        "flags": {
            "include_pline_descrip3": include_pline_d3,
            "include_pline_www": include_pline_www
        }
    }
    with open(mani_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    # Final (schema-preserving + extra columns)
    if args.mode == "writefinal":
        base_cols = list(df.columns)
        out_df = left.copy()
        for c in base_cols:
            if c not in out_df.columns:
                out_df[c] = ""
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
    ap.add_argument("--line", required=True, help="3-char product line code (e.g., bli, ils)")
    ap.add_argument("--saamm", required=True, help="Path to SAAMM CSV/TXT")
    ap.add_argument("--pricing", required=True, help="Path to Pricing Excel/CSV")
    ap.add_argument("--out", required=True, help="Output folder root (rep/* will be created)")
    ap.add_argument("--mode", required=True, choices=["dryrun","writefinal"])
    ap.add_argument("--row-overrides", help="Optional CSV with rowpointer, descrip3_override, desc_src_www_override")
    ap.add_argument("--pline-in-descrip3", choices=["yes","no"], default=None,
                    help="Include the 3-char line token at start of descrip3 (default from config or 'yes').")
    ap.add_argument("--pline-in-www", choices=["yes","no"], default=None,
                    help="Include the 3-char line token in desc_src_www (default from config or 'no').")
    args = ap.parse_args()
    run(args)

if __name__ == "__main__":
    main()
