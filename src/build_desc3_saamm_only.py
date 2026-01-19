#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_desc3_saamm_only.py — SAAMM-only → descrip3 (+ desc_src_www) builder

Based on build_desc3_enh.py, but modified to NOT require pricing.
- Builds descrip3 and desc_src_www solely from SAAMM columns:
    prod + source-desc-name + descrip1 + descrip2 + attributegrp
- Keeps: configs (global + per-line), synonyms, glue_bigrams, drop_tokens,
         pline3 include/exclude, lookupnm append (descrip3 only),
         260 cap + trimmed_flag, QA + FINAL + logs + manifest, feedback column.

Outputs:
  <out>/qa/QA_<line>_en_<stamp>.csv
  <out>/final/SAAMM_desc3_<line>_en_<stamp>.csv   (when --mode writefinal)
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

# JSON5-light loader: strips comments & trailing commas; requires valid JSON afterward
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
    out: List[str] = []
    for t in tokens:
        rep = syn.get(t, t)
        out.extend(rep.split())  # support multi-token expansions
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

def detokenize(tokens: Iterable[str]) -> str:
    toks = [t for t in tokens if t]
    return " ".join(toks).strip()

def _read_drop_tokens(cfg: dict) -> set:
    raw = cfg.get("drop_tokens", []) or []
    out = set()
    if isinstance(raw, (list, tuple)):
        for x in raw:
            s = str(x).strip().lower()
            if s:
                out.add(s)
    return out

# ------------------------------
# Build row fields (SAAMM-only)
# ------------------------------

def build_row_fields_saamm_only(
    row: pd.Series,
    cfg: dict,
    syn_map: Dict[str, str],
    glue_map: Dict[Tuple[str, str], str],
    pline3: str,
    include_pline_d3: bool,
    include_pline_www: bool,
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

    # Tokenize: prod + SAAMM fields
    prod_tok   = tokenize(row.get("prod", "") or "", extra_seps=extra_seps, squelch_stars=squelch_stars)
    toks_saamm = tokenize(s_saamm, extra_seps=extra_seps, squelch_stars=squelch_stars)

    tok = prod_tok + toks_saamm

    # Synonyms + Glue
    tok = apply_synonyms(tok, syn_map)
    tok = apply_glue(tok, glue_map)

    # DROP TOKENS
    drop_set = _read_drop_tokens(cfg)
    if drop_set:
        tok = [t for t in tok if t not in drop_set]

    # Remove pline token before optionally re-adding
    if pline3:
        tok = [t for t in tok if t != pline3]

    # descrip3 tokens
    descrip3_tokens = tok[:]
    if include_pline_d3 and pline3:
        descrip3_tokens = [pline3] + descrip3_tokens

    # lookupnm (legacy) at end of descrip3 (not in www)
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

    # DROP TOKENS post-filter (belt+suspenders)
    if drop_set:
        descrip3_tokens = [t for t in descrip3_tokens if t not in drop_set]
        www_tokens      = [t for t in www_tokens if t not in drop_set]

    # De-dup & detokenize
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

    cfg = merge_cfg(global_cfg, line_cfg)

    # Effective flags
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

    syn_map  = build_syn_map(cfg)
    glue_map = build_glue_map(cfg)

    # SAAMM
    df = read_any_table(args.saamm).fillna("")
    for col in ["descrip3", "lookupnm", "prod", "source-desc-name", "descrip1", "descrip2", "attributegrp", "user24"]:
        if col not in df.columns:
            df[col] = ""

    # Build outputs row-wise
    d3_list, www_list, trim_list = [], [], []
    for _, row in df.iterrows():
        d3_new, www_new, trimmed = build_row_fields_saamm_only(
            row=row,
            cfg=cfg,
            syn_map=syn_map,
            glue_map=glue_map,
            pline3=pline3,
            include_pline_d3=include_pline_d3,
            include_pline_www=include_pline_www,
            descrip3_cap=descrip3_cap,
            extra_seps=extra_seps,
            squelch_stars=squelch_stars,
        )
        d3_list.append(d3_new)
        www_list.append(www_new)
        trim_list.append("yes" if trimmed else "no")

    out_df = df.copy()
    out_df["descrip3"] = d3_list
    out_df["desc_src_www"] = www_list
    out_df["trimmed_flag"] = trim_list
    out_df["feedback"] = ""  # always present

    # Paths
    stamp = ts_now()
    qa_path    = os.path.join(out_root, "qa",    f"QA_{args.line}_en_{stamp}.csv")
    final_path = os.path.join(out_root, "final", f"SAAMM_desc3_{args.line}_en_{stamp}.csv")
    log_path   = os.path.join(out_root, "logs",  f"run_{args.line}_en_{stamp}.log")
    mani_path  = os.path.join(out_root, "manifest", f"manifest_{args.line}_en_{stamp}.json")

    # QA
    qa_cols = [c for c in [
        "rowpointer","lookupnm","user24","prod","source-desc-name",
        "descrip1","descrip2","attributegrp","descrip3","desc_src_www","trimmed_flag","feedback"
    ] if c in out_df.columns]
    write_csv(qa_path, out_df[qa_cols].copy())

    # Log
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"[{stamp}] line={args.line} mode={args.mode} (SAAMM-only)\n")
        f.write(f"rows_in={len(df)}\n")
        f.write(f"trimmed_descrip3={(out_df['trimmed_flag']=='yes').sum()}\n")
        f.write(f"cfg_global={'configs/global.json5' if os.path.exists(global_cfg_path) else 'None'}\n")
        f.write(f"cfg_line={line_cfg_path}\n")
        f.write(f"include_pline_descrip3={include_pline_d3}\n")
        f.write(f"include_pline_www={include_pline_www}\n")
        f.write(f"descrip3_cap={descrip3_cap}\n")
        f.write(f"extra_separators={extra_seps}\n")
        f.write(f"squelch_repeated_stars={squelch_stars}\n")
        f.write(f"drop_tokens={cfg.get('drop_tokens', [])}\n")

    # Manifest
    manifest = {
        "line": args.line,
        "timestamp": stamp,
        "saamm": os.path.abspath(args.saamm),
        "cfg_global": os.path.abspath(global_cfg_path) if os.path.exists(global_cfg_path) else None,
        "cfg_line": os.path.abspath(line_cfg_path),
        "qa_csv": os.path.abspath(qa_path),
        "final_csv": os.path.abspath(final_path) if args.mode=="writefinal" else None,
        "counts": {
            "rows_in": int(len(df)),
            "trimmed": int((out_df['trimmed_flag']=='yes').sum()),
        },
        "flags": {
            "include_pline_descrip3": include_pline_d3,
            "include_pline_www": include_pline_www,
            "extra_separators": extra_seps,
            "squelch_repeated_stars": squelch_stars,
            "drop_tokens": cfg.get("drop_tokens", []),
        }
    }
    with open(mani_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    # FINAL (original SAAMM columns + new fields appended)
    if args.mode == "writefinal":
        base_cols = list(df.columns)
        ordered = base_cols.copy()
        if "desc_src_www" not in ordered: ordered.append("desc_src_www")
        if "trimmed_flag" not in ordered: ordered.append("trimmed_flag")
        if "feedback" not in ordered: ordered.append("feedback")
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
    ap.add_argument("--out", required=True, help="Output folder root (qa/final/logs/manifest will be created)")
    ap.add_argument("--mode", required=True, choices=["dryrun","writefinal"])

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

    args = ap.parse_args()
    run(args)

if __name__ == "__main__":
    main()
