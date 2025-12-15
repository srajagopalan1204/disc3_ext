#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_desc3_enh.py number 22 

Enhanced variant of build_desc3.py:
- Adds a new column `desc_src_www` built from the same token packs that feed `descrip3`,
  but EXCLUDES:
    * the 3-char product line token (pline3), and
    * the legacy token derived from lookupnm.
- Applies glue/synonyms/expansions identically to descrip3.
- Writes filenames with `_en_` tag (e.g., QA_bli_en_YYYYMMDD_HHMM.csv).
- Final export now includes BOTH `descrip3` and `desc_src_www`.

CLI:
  python src/build_desc3_enh.py --line <line> --saamm <saamm.csv|.txt> \
    --pricing <pricing.xlsx> --out <rep_dir> --mode {dryrun,writefinal}
"""

import argparse
import os
import json
import re
from datetime import datetime, timezone, timedelta
import pandas as pd

# Pipeline helpers (present in your repo)
from io_schemas import HEADER_LOCK
from token_packs import (
    tokens_from_prod, detect_family, family_expansions, color_from_tokens,
    material_from_text, footage_and_uom, build_legacy_token, clean_token,
    compose_desc3, de_dupe_keep_order, apply_glue_bigrams, apply_synonyms
)
from qa_metrics import qa_columns, compute_budget_metrics

NY_TZ = timezone(timedelta(hours=-4))


# -------- I/O helpers --------
def load_json5(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.loads(f.read())

def read_saamm(path: str) -> pd.DataFrame:
    for sep in [",", "|", "\t"]:
        try:
            df = pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False, na_filter=False)
            if set(HEADER_LOCK).issubset(df.columns):
                return df
        except Exception:
            pass
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False, engine="python")

def read_pricing(path: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    sheet = xls.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet, dtype=str, engine="openpyxl")
    return df.fillna("")

def verify_schema(df: pd.DataFrame) -> None:
    missing = [c for c in HEADER_LOCK if c not in df.columns]
    if missing:
        raise ValueError(f"SAAMM missing required columns: {missing}")


# -------- utilities --------
def derive_pline3(prod: str, cfg_line: dict) -> str:
    pl = (cfg_line or {}).get("pline3", "")
    if pl:
        return pl.lower()
    head = (prod or "").split(" ", 1)[0]
    if head.isalpha() and len(head) >= 3:
        return head[:3].lower()
    return ""

def strip_pline_prefix(s: str, pline3: str) -> str:
    if not pline3:
        return str(s or "")
    return re.sub(rf"^\s*{re.escape(pline3)}[\/\-\s]+", "", str(s or ""), flags=re.IGNORECASE)

def norm_key(x: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]", "", str(x or ""))
    if s.isdigit():
        s = s.lstrip("0") or "0"
    return s.upper()


# -------- core builder (enhanced) --------
def build_desc3_for_row_enh(row: dict, cfg_line: dict):
    """
    Returns:
      desc3 (str), desc_src_www (str), pline3 (str), trimmed (bool)

    desc_src_www uses the same packs as descrip3 but EXCLUDES:
      - product-line 3-char token (pline3)
      - legacy token derived from lookupnm
    """
    budget = 260

    prod     = row.get("prod", "") or ""
    d1       = row.get("descrip1", "") or ""
    d2       = row.get("descrip2", "") or ""
    lookupnm = row.get("lookupnm", "") or ""

    colors       = (cfg_line.get("colors") or {})
    materials    = (cfg_line.get("materials") or {})
    uom_terms    = (cfg_line.get("uom_terms") or [])
    synonyms_map = {**(cfg_line.get("synonyms") or {}), **(cfg_line.get("pricing_synonyms") or {})}
    glue_map     = (cfg_line.get("glue_bigrams") or {})

    # CORE from prod (with glue)
    core_tokens = apply_glue_bigrams(tokens_from_prod(prod), glue_map)

    # PHRASE from descrip1/2 (with glue)
    phrase_raw = f"{d1} {d2}".strip()
    phrase_tokens = [clean_token(t) for t in phrase_raw.split() if t]
    phrase_tokens = apply_glue_bigrams(phrase_tokens, glue_map)

    # Families, color/materials, UOM/specs
    fams = detect_family(core_tokens)
    color_pairs = color_from_tokens(core_tokens + phrase_tokens, colors)
    nums, uoms = footage_and_uom(core_tokens + phrase_tokens, uom_terms)

    specs = []
    for t in phrase_tokens + core_tokens:
        tl = t.lower()
        if re.fullmatch(r"\d+v", tl):  # e.g., 600v
            specs.append(tl)
    specs = de_dupe_keep_order(specs)

    # pline and legacy
    pline3 = derive_pline3(prod, cfg_line)
    pline_pack = [pline3] if pline3 else []
    legacy = build_legacy_token(lookupnm, prod)
    legacy_pack = [legacy] if legacy else []

    # synonyms/expansions
    syn = []
    for ccode, cname in color_pairs:
        syn += [ccode, cname]
    for mcode, mname in material_from_text(" ".join(phrase_tokens + core_tokens), materials):
        syn += [mcode, mname]
    syn += apply_synonyms(core_tokens + phrase_tokens, synonyms_map)

    expand = []
    for fam in fams:
        expand += family_expansions(fam)

    # Compose descrip3 (unchanged)
    packs = [
        core_tokens,    # A
        pline_pack,     # A.1
        phrase_tokens,  # B
        specs,          # C
        syn,            # D
        expand,         # E
        legacy_pack     # F
    ]
    desc3, trimmed, _ = compose_desc3(packs, budget=budget)

    # desc_src_www (exclude pline + legacy)
    www_tokens = de_dupe_keep_order(core_tokens + phrase_tokens + specs + syn + expand)
    desc_src_www = " ".join([t for t in www_tokens if t])

    return desc3, desc_src_www, pline3, trimmed


# -------- pricing integration (unchanged) --------
def pick_pricing_part_column(pricing_df: pd.DataFrame, cfg_line: dict) -> str | None:
    override = (cfg_line or {}).get("pricing_part_header")
    if override and override in pricing_df.columns:
        return override
    aliases = {
        "part#", "part", "partnumber", "partnum", "partno",
        "sku", "catalog", "catalogno",
        "item", "item#", "itemno", "itemnumber", "itemid", "itemcode"
    }
    for c in pricing_df.columns:
        key = c.strip().lower().replace(" ", "")
        if key in aliases:
            return c
    return None

def integrate_pricing(saamm_df: pd.DataFrame, pricing_df: pd.DataFrame, cfg_line: dict) -> pd.DataFrame:
    pricing = pricing_df.copy()
    part_col = pick_pricing_part_column(pricing, cfg_line)

    if not part_col:
        out = saamm_df.copy()
        out["match_status"] = "price_unmatched"
        for col in ["PARTNUM", "Description", "Um", "Per"]:
            if col not in out.columns:
                out[col] = ""
        out["join_path"] = ""
        return out

    pricing["price_key"] = pricing[part_col].fillna("").astype(str).apply(norm_key)
    pricing = pricing.rename(columns={part_col: "PARTNUM"}).fillna("")

    pline3 = (cfg_line or {}).get("pline3", "")

    sa = saamm_df.copy()
    sa["lookup_key"]       = sa["lookupnm"].fillna("").astype(str).apply(norm_key)
    sa["prod_key"]         = sa["prod"].fillna("").astype(str).apply(norm_key)
    sa["prod_key_nopline"] = sa["prod"].fillna("").astype(str).apply(lambda s: norm_key(strip_pline_prefix(s, pline3)))

    core_cols = ["price_key", "PARTNUM", "Description", "Um", "Per"]

    # Primary: lookupnm → Pricing
    base = sa.merge(
        pricing[core_cols].drop_duplicates(),
        left_on="lookup_key", right_on="price_key",
        how="left", indicator=True, suffixes=("", "_p")
    )
    base["match_status"] = base["_merge"].map({"left_only": "price_unmatched", "both": "price_exact", "right_only": "price_unmatched"})
    base["join_path"] = base["match_status"].map({"price_exact": "lookupnm", "price_unmatched": ""})

    # Fallback 1: prod → Pricing
    m1 = base["match_status"].eq("price_unmatched")
    if m1.any():
        fb1 = sa.merge(
            pricing[core_cols].drop_duplicates(),
            left_on="prod_key", right_on="price_key",
            how="left", indicator=True, suffixes=("", "_pf1")
        )
        got = fb1["_merge"].eq("both")
        for col in core_cols:
            base.loc[m1 & got, col] = fb1.loc[m1 & got, col]
        base.loc[m1 & got, "match_status"] = "price_fallback"
        base.loc[m1 & got, "join_path"] = "prod"

    # Fallback 2: prod without pline prefix
    m2 = base["match_status"].eq("price_unmatched")
    if m2.any():
        fb2 = sa.merge(
            pricing[core_cols].drop_duplicates(),
            left_on="prod_key_nopline", right_on="price_key",
            how="left", indicator=True, suffixes=("", "_pf2")
        )
        got2 = fb2["_merge"].eq("both")
        for col in core_cols:
            base.loc[m2 & got2, col] = fb2.loc[m2 & got2, col]
        base.loc[m2 & got2, "match_status"] = "price_fallback"
        base.loc[m2 & got2, "join_path"] = "prod_nopline"

    return base


# -------- main --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--line", required=True, help="3-char product line code (e.g., bli)")
    ap.add_argument("--saamm", required=True, help="Path to SAAMM CSV/TXT")
    ap.add_argument("--pricing", required=True, help="Path to Pricing Excel")
    ap.add_argument("--out", required=True, help="Output folder root (rep/* will be created)")
    ap.add_argument("--mode", required=True, choices=["dryrun", "writefinal"])
    args = ap.parse_args()

    # Load configs
    cfg_global = load_json5("configs/global/thresholds.json5")
    cfg_line   = load_json5(f"configs/lines/{args.line}.json5")

    # File tags
    ts = datetime.now(NY_TZ).strftime("%Y%m%d_%H%M")
    variant = "en"
    tag = f"{variant}_{ts}"

    out_final    = os.path.join(args.out, "final",    f"SAAMM_desc3_{args.line}_{tag}.csv")
    out_qa       = os.path.join(args.out, "qa",       f"QA_{args.line}_{tag}.csv")
    out_log      = os.path.join(args.out, "logs",     f"run_{args.line}_{tag}.log")
    out_manifest = os.path.join(args.out, "manifest", f"manifest_{args.line}_{tag}.json")

    for p in (out_final, out_qa, out_log, out_manifest):
        os.makedirs(os.path.dirname(p), exist_ok=True)

    # Read inputs
    sa = read_saamm(args.saamm)
    verify_schema(sa)

    if cfg_global.get("inputs", {}).get("pricing", True):
        pr = read_pricing(args.pricing)
        merged = integrate_pricing(sa, pr, cfg_line)
    else:
        merged = sa.copy()
        merged["match_status"] = "price_unmatched"
        for col in ("PARTNUM", "Description", "Um", "Per"):
            if col not in merged.columns:
                merged[col] = ""
        merged["join_path"] = ""

    # Build rows
    rows = []
    for _, row in merged.iterrows():
        before = (row.get("descrip3", "") or "")
        desc3, desc_src_www, pline3, trimmed = build_desc3_for_row_enh(row, cfg_line)
        ms = str(row.get("match_status", ""))
        desc_text = row.get("Description", "")
        desc_nonempty = isinstance(desc_text, str) and desc_text.strip() != ""
        source_used = "pricing" if (ms in ("price_exact", "price_fallback") and desc_nonempty) else ""
        rows.append({
            **row.to_dict(),
            "pline3": pline3,
            "descrip3_before": before,
            "descrip3_after": desc3,
            "desc_src_www": desc_src_www,   # <-- NEW
            "trimmed_flag": "yes" if trimmed else "no",
            "source_used": source_used
        })
    df = pd.DataFrame(rows)

    # QA metrics
    df = qa_columns(df)
    df = compute_budget_metrics(df, budget=260)

    # Write QA
    df.to_csv(out_qa, index=False, lineterminator="\r\n")

    # Log
    with open(out_log, "w", encoding="utf-8") as f:
        f.write(f"[{ts}] line={args.line} mode={args.mode} variant={variant}\n")
        vc = df["match_status"].value_counts(dropna=False)
        f.write("match_status:\n" + vc.to_string() + "\n")
        if "join_path" in df.columns:
            f.write("join_path:\n" + df["join_path"].value_counts(dropna=False).to_string() + "\n")
        rows_with_pricing = int((df["source_used"] == "pricing").sum())
        f.write(f"rows_in={len(sa)} rows_with_pricing={rows_with_pricing}\n")
        L = pd.to_numeric(df["descrip3_len_after"], errors="coerce")
        f.write("len_after_p50={:.0f} p90={:.0f} p95={:.0f} max={:.0f}\n".format(
            L.quantile(0.5), L.quantile(0.9), L.quantile(0.95), L.max()
        ))
        trim_pct = 100.0 * (df["trimmed_flag"] == "yes").mean()
        f.write("trimmed_rows_pct={:.1f}%\n".format(trim_pct))

    # Manifest
    manifest = {
        "timestamp": ts,
        "variant": variant,
        "line": args.line,
        "inputs": {
            "saamm_path": args.saamm,
            "pricing_path": args.pricing if cfg_global.get('inputs', {}).get('pricing', True) else None
        },
        "configs": {
            "global": "configs/global/thresholds.json5",
            "line": f"configs/lines/{args.line}.json5"
        }
    }
    with open(out_manifest, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    # Final export: SAAMM schema + desc_src_www appended
    if args.mode == "writefinal":
        final = sa.copy()
        final["descrip3"] = df["descrip3_after"]
        final["desc_src_www"] = df["desc_src_www"]  # <-- include in final
        final.to_csv(out_final, index=False, lineterminator="\r\n")
        print(out_final)
    else:
        print(out_qa)


if __name__ == "__main__":
    main()
