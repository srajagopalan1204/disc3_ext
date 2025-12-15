#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_desc3_enh.py — Enhanced SAAMM → descrip3 (+ desc_src_www) builder

120 
It includes:

Parentheses stripping via config/CLI (extra_separators, --strip-parens yes)

Removal of runs of * (2+) via config/CLI (squelch_repeated_stars, --squelch-stars yes)

Global + per-line JSON5-light configs with precedence (CLI > line > global > defaults)

Pricing join auto-detect/override + pricing extra columns into {both|descrip3|www|none}

desc_src_www field

user24 included in QA when present

260-char cap for descrip3 with trimmed_flag

Row overrides support

Logs + manifest + QA/FINAL outputs

Add these to your configs (already sanitized JSON is best):

configs/global.json5:
{"squelch_repeated_stars": true, "extra_separators": ["(", ")"]}

(optional) configs/lines/<line>.json5 can override them.

Features
--------
- JSON5-light configs: global + per-line, merged with precedence (CLI > line > global > defaults)
- Normalization: lowercase, unicode-space collapse, '/', '-', '_' -> space
- Extra separators from config (e.g., '(' and ')') or CLI --strip-parens yes
- Remove runs of '*' (2+) as separators via config or CLI (--squelch-stars yes)
- Synonyms (colors/materials/synonyms/pricing_synonyms) & GLUE bigrams
- Include/exclude 3-char line token (pline3) in descrip3 or desc_src_www independently
- Append legacy part number (lookupnm) to end of descrip3 (not to desc_src_www)
- Pricing join auto-detect (Item#/Part#/SKU), explicit override supported
- Pricing “extra” columns route into {both|descrip3|www|none}
- 260-char cap on descrip3 with trimmed_flag
- Row overrides CSV support
- QA + FINAL outputs, per-run log + manifest

Usage
-----
python src/build_desc3_enh.py \
  --line cdw \
  --saamm data/SAAMM_CDW.csv \
  --pricing data/Pricing_CWD.xlsx \
  --pricing-extra-cols "Manufacturer Description, SCOTT DESCRIPTION" \
  --extra-into both \
  --pricing-join-prefer auto \
  --pline-in-descrip3 no \
  --pline-in-www no \
  --strip-parens yes \
  --squelch-stars yes \
  --out rep \
  --mode writefinal
"""

from __future__ import annotations
import argparse, csv, datetime as dt, json, os, re, sys
from typing import Dict, List, Tuple, Iterable, Optional

import pandas as pd

# ------------------------------
# Helpers
# ------------------------------

def ts_now() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M")

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def read_any_table(path: str) -> pd.DataFrame:
    ext = (os.path.splitext(path)[1] or "").lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        # Try Python engine with sniffed delimiter
        return pd.read_csv(path, dtype=str, sep=None, engine="python").fillna("")

def write_csv(path: str, df: pd.DataFrame) -> None:
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

# JSON5-light loader: strips comments & trailing commas; requires double-quoted keys/values afterward
def load_json5(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    # strip // line comments and /* block */ comments
    text = re.sub(r"//.*?$", "", text, flags=re.MULTILINE)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    # remove trailing commas
    text = re.sub(r",\s*([}\]])", r"\1", text)
    return json.loads(text) if text.strip() else {}

def merge_cfg(global_cfg: dict, line_cfg: dict) -> dict:
    """Shallow-merge line over global. Merge extra_separators arrays."""
    out = dict(global_cfg or {})
    line_cfg = line_cfg or {}
    for k, v in line_cfg.items():
        if k == "extra_separators":
            base = out.get("extra_separators", []) or []
            seen = set(base)
            merged = list(base)
            for it in (v or []):
                if it not in seen:
                    merged.append(it); seen.add(it)
            out["extra_separators"] = merged
        else:
            out[k] = v
    return out

# ------------------------------
# Normalization & tokenization
# ------------------------------

# a range of exotic whitespace chars to normalize to plain space
WS_CHARS = r"\u00A0\u1680\u180E\u2000-\u200D\u202F\u205F\u2060\u3000"

def normalize_text(
    s: str,
    extra_seps: Optional[List[str]] = None,
    squelch_stars: bool = False,
) -> str:
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    # exotic whitespace -> space
    s = re.sub(fr"[{WS_CHARS}\t]+", " ", s)
    # slashes, hyphens, underscores -> space
    s = s.replace("/", " ")
    s = re.sub(r"[-_]+", " ", s)
    # runs of '*' (2+) -> space
    if squelch_stars:
        s = re.sub(r"\*{2,}", " ", s)
    # extra separators (e.g., '(' and ')') -> space
    if extra_seps:
        patt = "[" + re.escape("".join(extra_seps)) + "]"
        s = re.sub(patt, " ", s)
    # collapse spaces & lowercase
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def tokenize(
    s: str,
    extra_seps: Optional[List[str]] = None,
    squelch_stars: bool = False,
) -> List[str]:
    s = normalize_text(s, extra_seps=extra_seps, squelch_stars=squelch_stars)
    return s.split(" ") if s else []

# ------------------------------
# Synonyms & glue
# ------------------------------

def build_syn_map(cfg: dict) -> Dict[str, str]:
    syn = {}
    for section in ["colors", "materials", "pricing_synonyms", "synonyms"]:
        d = cfg.get(section) or {}
        if isinstance(d, dict):
            for k, v in d.items():
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
    # support multi-token expansions (e.g., "pc" -> "pc pieces")
    out: List[str] = []
    for t in tokens:
        rep = syn.get(t, t)
        out.extend(rep.split())  # split to multiple tokens if needed
    return out

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
            out.append(t); seen.add(t)
    return out

def cap_len(s: str, cap: int) -> Tuple[str, bool]:
    if len(s) <= cap:
        return s, False
    return s[:cap].rstrip(), True

# ------------------------------
# Pricing join & extras
# ------------------------------

ITEM_KEYS = ["item#", "item #", "item no", "item_no", "item number", "item"]
PART_KEYS = ["part#", "part #", "part no", "part_no", "part", "vendor product number"]
GEN_KEYS  = ["sku", "lookupnm"]

def find_col_case_insensitive(dfr: pd.DataFrame, name: str) -> Optional[str]:
    low = [c.lower() for c in dfr.columns]
    try:
        idx = low.index(name.lower())
        return dfr.columns[idx]
    except ValueError:
        return None

def detect_pricing_join_col(dfp: pd.DataFrame, prefer: str = "auto",
                            override: Optional[str] = None) -> Optional[str]:
    if override:
        c = find_col_case_insensitive(dfp, override)
        if c: return c

    cols_lower = [c.lower() for c in dfp.columns]

    def first_hit(keys: List[str]) -> Optional[str]:
        for k in keys:
            if k in cols_lower:
                return dfp.columns[cols_lower.index(k)]
        return None

    if prefer == "item":
        return first_hit(ITEM_KEYS) or first_hit(PART_KEYS) or first_hit(GEN_KEYS)
    if prefer == "part":
        return first_hit(PART_KEYS) or first_hit(ITEM_KEYS) or first_hit(GEN_KEYS)
    return first_hit(ITEM_KEYS) or first_hit(PART_KEYS) or first_hit(GEN_KEYS)

def resolve_extra_cols(dfp: pd.DataFrame, extra_cols_csv: Optional[str]) -> List[str]:
    if not extra_cols_csv:
        return []
    wanted = [c.strip() for c in extra_cols_csv.split(",") if c.strip()]
    resolved: List[str] = []
    for w in wanted:
        c = find_col_case_insensitive(dfp, w)
        if c:
            resolved.append(c)
    # de-dup preserve order
    seen = set(); out=[]
    for c in resolved:
        if c not in seen:
            out.append(c); seen.add(c)
    return out

# ------------------------------
# Build row fields
# ------------------------------

def detokenize(tokens: Iterable[str]) -> str:
    toks = [t for t in tokens if t]
    return " ".join(toks).strip()

def build_row_fields(
    row: pd.Series,
    pricing_row: Optional[pd.Series],
    cfg: dict,
    syn_map: Dict[str, str],
    glue_map: Dict[Tuple[str, str], str],
    pline3: str,
    include_pline_d3: bool,
    include_pline_www: bool,
    extra_cols: List[str],
    extra_into: str,
    descrip3_cap: int,
    extra_seps: Optional[List[str]],
    squelch_stars: bool,
) -> Tuple[str, str, bool]:
    # SAAMM descriptive fields
    saam_strs = []
    for name in ["source-desc-name", "descrip1", "descrip2", "attributegrp"]:
        if name in row and str(row[name]).strip():
            saam_strs.append(str(row[name]))
    s_saamm = " ".join(saam_strs)

    # Pricing enrichment strings
    pricing_strs = []
    if pricing_row is not None:
        for name in ["Item Description", "Description", "Long Description", "desc", "desc1"]:
            if name in pricing_row and str(pricing_row[name]).strip():
                pricing_strs.append(str(pricing_row[name]))
        for name in extra_cols:
            if name in pricing_row and str(pricing_row[name]).strip():
                pricing_strs.append(str(pricing_row[name]))

    # Tokenize
    toks_saamm = tokenize(s_saamm, extra_seps=extra_seps, squelch_stars=squelch_stars)
    toks_price = tokenize(" ".join(pricing_strs), extra_seps=extra_seps, squelch_stars=squelch_stars) if pricing_strs else []
    prod_tok   = tokenize(row.get("prod", "") or "", extra_seps=extra_seps, squelch_stars=squelch_stars)

    base_tokens = prod_tok + toks_saamm + toks_price

    # Synonyms + Glue
    tok = apply_synonyms(base_tokens, syn_map)
    tok = apply_glue(tok, glue_map)

    # Remove pline token before optionally re-adding
    if pline3:
        tok = [t for t in tok if t != pline3]

    # descrip3 tokens
    descrip3_tokens = tok[:]
    if include_pline_d3 and pline3:
        descrip3_tokens = [pline3] + descrip3_tokens

    # lookupnm (legacy) at end of descrip3
    lookupnm_raw = row.get("lookupnm", "") or ""
    lookupnm = normalize_text(lookupnm_raw, extra_seps=extra_seps, squelch_stars=squelch_stars)
    if lookupnm:
        descrip3_tokens = descrip3_tokens + [lookupnm]

    # desc_src_www tokens
    www_tokens = tok[:]
    if not include_pline_www and pline3:
        www_tokens = [t for t in www_tokens if t != pline3]
    if lookupnm:
        www_tokens = [t for t in www_tokens if t != lookupnm]

    # Handle extra_into by subtracting "extras-only" tokens where needed
    if extra_cols and extra_into in ("descrip3", "www", "none"):
        std_pr_descs = []
        if pricing_row is not None:
            for name in ["Item Description", "Description", "Long Description", "desc", "desc1"]:
                if name in pricing_row and str(pricing_row[name]).strip():
                    std_pr_descs.append(str(pricing_row[name]))
        base_no_extra = toks_saamm + tokenize(" ".join(std_pr_descs), extra_seps=extra_seps, squelch_stars=squelch_stars) if pricing_row is not None else toks_saamm
        base_no_extra = apply_glue(apply_synonyms(base_no_extra, syn_map), glue_map)
        if pline3:
            base_no_extra = [t for t in base_no_extra if t != pline3]

        extras_only = []
        if pricing_row is not None:
            for name in extra_cols:
                if name in pricing_row and str(pricing_row[name]).strip():
                    extras_only += tokenize(str(pricing_row[name]), extra_seps=extra_seps, squelch_stars=squelch_stars)
        extras_only = apply_glue(apply_synonyms(extras_only, syn_map), glue_map)

        from collections import Counter
        def remove_multiset(stream: List[str], to_remove: List[str]) -> List[str]:
            if not to_remove:
                return stream
            rem = Counter(to_remove)
            out = []
            for t in stream:
                if rem.get(t, 0) > 0:
                    rem[t] -= 1
                else:
                    out.append(t)
            return out

        if extra_into == "descrip3":
            www_tokens = remove_multiset(www_tokens, extras_only)
        elif extra_into == "www":
            descrip3_tokens = remove_multiset(descrip3_tokens, extras_only)
        elif extra_into == "none":
            descrip3_tokens = remove_multiset(descrip3_tokens, extras_only)
            www_tokens      = remove_multiset(www_tokens, extras_only)

    # de-dup & detokenize
    descrip3_tokens = uniq_preserve(descrip3_tokens)
    www_tokens      = uniq_preserve(www_tokens)

    d3_str  = detokenize(descrip3_tokens)
    d3_str, trimmed = cap_len(d3_str, descrip3_cap)
    www_str = detokenize(www_tokens)

    return d3_str, www_str, trimmed

# ------------------------------
# Main runner
# ------------------------------

def run(args: argparse.Namespace) -> None:
    out_root = args.out
    for sub in ("qa", "final", "logs", "manifest"):
        ensure_dir(os.path.join(out_root, sub))

    # Load configs
    global_cfg_path = os.path.join("configs", "global.json5")
    global_cfg = load_json5(global_cfg_path) if os.path.exists(global_cfg_path) else {}

    line_cfg_path = os.path.join("configs", "lines", f"{args.line}.json5")
    if not os.path.exists(line_cfg_path):
        sys.exit(f"Config not found: {line_cfg_path}")
    line_cfg = load_json5(line_cfg_path)

    # Merge (line over global)
    cfg = merge_cfg(global_cfg, line_cfg)

    # Effective flags (config with CLI overrides)
    pline3 = (cfg.get("pline3") or args.line or "").strip().lower()

    include_pline_d3  = cfg.get("include_pline_descrip3", True)
    include_pline_www = cfg.get("include_pline_www", False)
    if args.pline_in_descrip3 is not None:
        include_pline_d3 = (args.pline_in_descrip3.lower() == "yes")
    if args.pline_in_www is not None:
        include_pline_www = (args.pline_in_www.lower() == "yes")

    extra_seps = list(cfg.get("extra_separators", [])) if cfg.get("extra_separators") else []
    if args.strip_parens == "yes":
        for ch in ("(", ")"):
            if ch not in extra_seps:
                extra_seps.append(ch)

    squelch_stars = bool(cfg.get("squelch_repeated_stars", False))
    if args.squelch_stars is not None:
        squelch_stars = (args.squelch_stars.lower() == "yes")

    descrip3_cap = int(args.descrip3_cap)

    # Synonyms + Glue maps from merged cfg
    syn_map  = build_syn_map(cfg)
    glue_map = build_glue_map(cfg)

    # SAAMM (preserve leading zeros by dtype=str)
    df = read_any_table(args.saamm).fillna("")
    for col in ["descrip3", "lookupnm", "prod", "source-desc-name", "descrip1", "descrip2", "attributegrp", "user24"]:
        if col not in df.columns:
            df[col] = ""

    # Pricing
    dfp = read_any_table(args.pricing).fillna("")
    join_col = detect_pricing_join_col(dfp, prefer=args.pricing_join_prefer, override=args.pricing_join_override)
    if not join_col:
        sys.exit("Could not detect a pricing join column. Use --pricing-join-override to specify the header (e.g., 'Item#' or 'Part #').")

    # Keys (normalized lowercase, stripped)
    df["_join_key"]  = df["lookupnm"].astype(str).str.strip().str.lower()
    dfp["_join_key"] = dfp[join_col].astype(str).str.strip().str.lower()

    # Resolve pricing extra columns
    extra_cols = resolve_extra_cols(dfp, args.pricing_extra_cols)

    # Merge
    left = df.merge(dfp, on="_join_key", how="left", suffixes=("", "_pr"))

    # Build outputs row-wise
    d3_list, www_list, trim_list = [], [], []
    for _, row in left.iterrows():
        d3_new, www_new, trimmed = build_row_fields(
            row=row,
            pricing_row=row,  # merged row contains pricing fields
            cfg=cfg,
            syn_map=syn_map,
            glue_map=glue_map,
            pline3=pline3,
            include_pline_d3=include_pline_d3,
            include_pline_www=include_pline_www,
            extra_cols=extra_cols,
            extra_into=args.extra_into,
            descrip3_cap=descrip3_cap,
            extra_seps=extra_seps,
            squelch_stars=squelch_stars,
        )
        d3_list.append(d3_new)
        www_list.append(www_new)
        trim_list.append("yes" if trimmed else "no")

    left["descrip3"] = d3_list
    left["desc_src_www"] = www_list
    left["trimmed_flag"] = trim_list

    # Optional row overrides
    if args.row_overrides:
        ov = read_any_table(args.row_overrides).fillna("")
        for c in ["rowpointer", "descrip3_override", "desc_src_www_override"]:
            if c not in ov.columns:
                ov[c] = ""
        ov_map = { str(r["rowpointer"]).strip(): r for _, r in ov.iterrows() }

        def apply_override(irow: pd.Series) -> pd.Series:
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

    # QA (include user24 when present)
    qa_cols = [c for c in [
        "rowpointer","lookupnm","user24","prod","source-desc-name",
        "descrip1","descrip2","attributegrp","descrip3","desc_src_www","trimmed_flag"
    ] if c in left.columns]
    write_csv(qa_path, left[qa_cols].copy())

    # Log
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"[{stamp}] line={args.line} mode={args.mode}\n")
        f.write(f"rows_in={len(df)}\n")
        f.write(f"trimmed_descrip3={(left['trimmed_flag']=='yes').sum()}\n")
        f.write(f"cfg_global={'configs/global.json5' if os.path.exists(global_cfg_path) else 'None'}\n")
        f.write(f"cfg_line={line_cfg_path}\n")
        f.write(f"pricing_join_col={join_col}\n")
        f.write(f"extra_cols={extra_cols}\n")
        f.write(f"include_pline_descrip3={include_pline_d3}\n")
        f.write(f"include_pline_www={include_pline_www}\n")
        f.write(f"descrip3_cap={descrip3_cap}\n")
        f.write(f"extra_separators={extra_seps}\n")
        f.write(f"squelch_repeated_stars={squelch_stars}\n")

    # Manifest
    manifest = {
        "line": args.line,
        "timestamp": stamp,
        "saamm": os.path.abspath(args.saamm),
        "pricing": os.path.abspath(args.pricing),
        "cfg_global": os.path.abspath(global_cfg_path) if os.path.exists(global_cfg_path) else None,
        "cfg_line": os.path.abspath(line_cfg_path),
        "qa_csv": os.path.abspath(qa_path),
        "final_csv": os.path.abspath(final_path) if args.mode=="writefinal" else None,
        "row_overrides": os.path.abspath(args.row_overrides) if args.row_overrides else None,
        "counts": {
            "rows_in": int(len(df)),
            "trimmed": int((left['trimmed_flag']=='yes').sum()),
        },
        "flags": {
            "include_pline_descrip3": include_pline_d3,
            "include_pline_www": include_pline_www,
            "extra_cols": extra_cols,
            "extra_into": args.extra_into,
            "pricing_join_col": join_col,
            "extra_separators": extra_seps,
            "squelch_repeated_stars": squelch_stars,
        }
    }
    with open(mani_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    # FINAL (SAAMM original columns + new fields)
    if args.mode == "writefinal":
        base_cols = list(df.columns)
        out_df = left.copy()
        for c in base_cols:
            if c not in out_df.columns:
                out_df[c] = ""
        ordered = base_cols.copy()
        if "desc_src_www" not in ordered: ordered.append("desc_src_www")
        if "trimmed_flag" not in ordered: ordered.append("trimmed_flag")
        write_csv(final_path, out_df[ordered])

    print(f"QA written: {qa_path}")
    if args.mode == "writefinal":
        print(f"FINAL written: {final_path}")
    print(f"LOG: {log_path}")
    print(f"MANIFEST: {mani_path}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--line", required=True, help="3-char product line (e.g., bli, ils, cdw)")
    ap.add_argument("--saamm", required=True, help="Path to SAAMM CSV/TXT")
    ap.add_argument("--pricing", required=True, help="Path to Pricing Excel/CSV")
    ap.add_argument("--out", required=True, help="Output folder root (rep/* will be created)")
    ap.add_argument("--mode", required=True, choices=["dryrun","writefinal"])

    # Pricing join & extras
    ap.add_argument("--pricing-join-prefer", choices=["auto","item","part"], default="auto",
                    help="Prefer Item# vs Part# when auto-detecting pricing join column.")
    ap.add_argument("--pricing-join-override", default=None,
                    help="Exact pricing column header to use for the join (case-insensitive).")
    ap.add_argument("--pricing-extra-cols", default=None,
                    help='Comma-separated pricing columns to include (e.g., "Manufacturer Description, SCOTT DESCRIPTION").')
    ap.add_argument("--extra-into", choices=["both","descrip3","www","none"], default="both",
                    help="Where to include extra pricing fields (default: both).")

    # Line token placement
    ap.add_argument("--pline-in-descrip3", choices=["yes","no"], default=None,
                    help="Include pline3 token at start of descrip3 (default from config).")
    ap.add_argument("--pline-in-www", choices=["yes","no"], default=None,
                    help="Include pline3 token in desc_src_www (default from config).")

    # Normalization toggles (override config)
    ap.add_argument("--strip-parens", choices=["yes","no"], default=None,
                    help="If yes, add '(' and ')' to separators for this run.")
    ap.add_argument("--squelch-stars", choices=["yes","no"], default=None,
                    help="If yes, treat runs of '*' (2+) as separators this run.")

    # Misc
    ap.add_argument("--descrip3-cap", type=int, default=260, help="Max length for descrip3 (default 260).")
    ap.add_argument("--row-overrides", default=None,
                    help="Optional CSV: rowpointer, descrip3_override, desc_src_www_override")

    args = ap.parse_args()
    run(args)

if __name__ == "__main__":
    main()
