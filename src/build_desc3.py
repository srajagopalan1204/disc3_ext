import argparse, os, json, re, sys, pandas as pd
from datetime import datetime, timezone, timedelta

from io_schemas import HEADER_LOCK
from join_utils import norm_part, derive_pline3_from_config, from_pricing_join_key
from token_packs import tokens_from_prod, detect_family, family_expansions, color_from_tokens, material_from_text, footage_and_uom, build_legacy_token, clean_token, compose_desc3, de_dupe_keep_order
from qa_metrics import qa_columns, compute_budget_metrics

NY_TZ = timezone(timedelta(hours=-4))

def load_json5(path):
    with open(path, "r", encoding="utf-8") as f:
        txt = f.read()
    return json.loads(txt)

def read_saamm(path):
    for sep in [",", "|", "\t"]:
        try:
            df = pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False, na_filter=False)
            if set(HEADER_LOCK).issubset(df.columns):
                return df
        except Exception:
            pass
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False, engine="python")
    return df

def read_pricing(path):
    xls = pd.ExcelFile(path)
    sheet = xls.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet, dtype=str)
    df = df.fillna("")
    return df

def verify_schema(df):
    missing = [c for c in HEADER_LOCK if c not in df.columns]
    if missing:
        raise ValueError(f"SAAMM missing required columns: {missing}")
    return True

def derive_pline3(prod, cfg_line):
    pl = derive_pline3_from_config(cfg_line)
    if pl:
        return pl
    head = (prod or "").split(" ",1)[0]
    if head.isalpha() and len(head)>=3:
        return head[:3].lower()
    return ""

def build_desc3_for_row(row, cfg_line):
    budget = 260
    prod = row.get("prod","")
    d1 = row.get("descrip1","")
    d2 = row.get("descrip2","")
    lookupnm = row.get("lookupnm","")

    colors = (cfg_line.get("colors") or {})
    materials = (cfg_line.get("materials") or {})
    uom_terms = (cfg_line.get("uom_terms") or [])
    families_cfg = (cfg_line.get("families") or {})
    pricing_syn = (cfg_line.get("pricing_synonyms") or {})

    core_tokens = tokens_from_prod(prod)
    fams = detect_family(core_tokens)

    color_pairs = color_from_tokens(core_tokens, colors)
    nums, uoms = footage_and_uom(core_tokens, uom_terms)

    pline3 = derive_pline3(prod, cfg_line)
    pline_pack = [pline3] if pline3 else []

    phrase_raw = f"{d1} {d2}".strip()
    phrase_tokens = [clean_token(t) for t in phrase_raw.split()]
    phrase_tokens = [t for t in phrase_tokens if t]

    specs = []
    for t in phrase_tokens + core_tokens:
        if re.fullmatch(r"\d+v", t.lower()):
            specs.append(t.lower())
    specs = de_dupe_keep_order(specs)

    syn = []
    for ccode, cname in color_pairs:
        syn += [ccode, cname]
    mats = material_from_text(" ".join(phrase_tokens + core_tokens), materials)
    for mcode, mname in mats:
        syn += [mcode, mname]

    expand = []
    for fam in fams:
        expand += family_expansions(fam)

    legacy = build_legacy_token(lookupnm, prod)
    legacy_pack = [legacy] if legacy else []

    packs = [core_tokens, pline_pack, phrase_tokens, specs, syn, expand, legacy_pack]
    desc3, trimmed, _ = compose_desc3(packs, budget=budget)
    return desc3, pline3, trimmed

def integrate_pricing(saamm_df, pricing_df, cfg_line):
    pricing = pricing_df.copy()
    part_col = None
    for c in pricing.columns:
        if c.strip().lower().replace(" ", "") in ("part#", "part", "partnumber", "partnum", "partno"):
            part_col = c; break
    if not part_col:
        for c in pricing.columns:
            if "part" in c.lower():
                part_col = c; break
    if not part_col:
        saamm_df["match_status"] = "price_unmatched"
        saamm_df["source_used"] = ""
        return saamm_df

    def norm_part_local(x): return re.sub(r"[^A-Za-z0-9]", "", str(x or "")).upper()
    pricing["price_key"] = pricing[part_col].fillna("").astype(str).apply(norm_part_local)
    pricing = pricing.rename(columns={part_col: "PARTNUM"})
    pricing = pricing.fillna("")

    sa = saamm_df.copy()
    sa["lookup_key"] = sa["lookupnm"].fillna("").astype(str).apply(norm_part_local)
    sa["prod_key"] = sa["prod"].fillna("").astype(str).apply(norm_part_local)

    left = sa.merge(pricing[["price_key","PARTNUM","Description","Um","Per"]].drop_duplicates(),
                    left_on="lookup_key", right_on="price_key", how="left", indicator=True, suffixes=("","_p"))
    left["match_status"] = left["_merge"].map({"left_only":"price_unmatched","both":"price_exact","right_only":"price_unmatched"})

    fallback_mask = left["match_status"] == "price_unmatched"
    if fallback_mask.any():
        fb = sa.merge(pricing[["price_key","PARTNUM","Description","Um","Per"]].drop_duplicates(),
                      left_on="prod_key", right_on="price_key", how="left", indicator=True, suffixes=("","_pf"))
        for col in ["PARTNUM","Description","Um","Per","price_key"]:
            left.loc[fallback_mask, col] = fb.loc[fallback_mask, col]
        left.loc[fallback_mask, "match_status"] = fb["_merge"].map({"left_only":"price_unmatched","both":"price_fallback","right_only":"price_unmatched"})[fallback_mask]

    return left

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--line", required=True)
    ap.add_argument("--saamm", required=True)
    ap.add_argument("--pricing", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--mode", required=True, choices=["dryrun","writefinal"])
    args = ap.parse_args()

    cfg_global = load_json5("configs/global/thresholds.json5")
    cfg_line = load_json5(f"configs/lines/{args.line}.json5")
    cfg_patterns = load_json5(f"configs/lines/{args.line}_patterns.json5")
    cfg_line_thresholds = load_json5(f"configs/lines/{args.line}_thresholds.json5")

    ts = datetime.now(NY_TZ).strftime("%Y%m%d_%H%M")
    out_final = os.path.join(args.out, "final", f"SAAMM_desc3_{args.line}_{ts}.csv")
    out_qa = os.path.join(args.out, "qa", f"QA_{args.line}_{ts}.csv")
    out_log = os.path.join(args.out, "logs", f"run_{args.line}_{ts}.log")
    out_manifest = os.path.join(args.out, "manifest", f"manifest_{args.line}_{ts}.json")

    for p in [out_final, out_qa, out_log, out_manifest]:
        os.makedirs(os.path.dirname(p), exist_ok=True)

    sa = read_saamm(args.saamm)
    verify_schema(sa)

    if cfg_global.get("inputs",{}).get("pricing", True):
        pr = read_pricing(args.pricing)
        merged = integrate_pricing(sa, pr, cfg_line)
    else:
        merged = sa.copy()
        merged["match_status"] = "price_unmatched"
        for col in ["PARTNUM","Description","Um","Per"]:
            if col not in merged.columns:
                merged[col] = ""

    rows = []
    for _, row in merged.iterrows():
        before = (row.get("descrip3","") or "")
        desc3, pline3, trimmed = build_desc3_for_row(row, cfg_line)
        rows.append({
            **row.to_dict(),
            "pline3": pline3,
            "descrip3_before": before,
            "descrip3_after": desc3,
            "trimmed_flag": "yes" if trimmed else "no",
            "source_used": "pricing" if row.get("match_status","").startswith("price_") and row.get("Description","") else ""
        })
    df = pd.DataFrame(rows)

    df = qa_columns(df)
    df = compute_budget_metrics(df, budget=260)
    df.to_csv(out_qa, index=False)

    with open(out_log, "w", encoding="utf-8") as f:
        f.write(f"[{ts}] line={args.line} mode={args.mode}
")
        f.write(f"rows_in={len(sa)} rows_with_pricing={len(df[df['source_used']=='pricing'])}
")

    manifest = {
      "timestamp": ts,
      "line": args.line,
      "inputs": {
        "saamm_path": args.saamm,
        "pricing_path": args.pricing if cfg_global.get('inputs',{}).get('pricing', True) else None
      },
      "configs": {
        "global": "configs/global/thresholds.json5",
        "line": f"configs/lines/{args.line}.json5",
        "patterns": f"configs/lines/{args.line}_patterns.json5",
        "line_thresholds": f"configs/lines/{args.line}_thresholds.json5",
        "schema_lock": "configs/global/schema_lock.json"
      }
    }
    with open(out_manifest, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    if args.mode == "writefinal":
        final = sa.copy()
        final["descrip3"] = df["descrip3_after"]
        final.to_csv(out_final, index=False)
        print(out_final)
    else:
        print(out_qa)

if __name__ == "__main__":
    main()
