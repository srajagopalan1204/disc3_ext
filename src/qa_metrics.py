import pandas as pd

def qa_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "match_status","join_confidence","pline3","pline3_source",
        "descrip3_before","descrip3_after","descrip3_len_before","descrip3_len_after",
        "pct_of_budget_used","headroom_chars","trimmed_flag","chars_trimmed","tokens_trimmed_count",
        "trims_by_pack","trim_trace","mandatory_preserved","rule_hint","impact","warnings","source_used"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

def compute_budget_metrics(df: pd.DataFrame, budget=260):
    after = df["descrip3_after"].fillna("").astype(str).str.len()
    before = df["descrip3_before"].fillna("").astype(str).str.len()
    df["descrip3_len_after"] = after
    df["descrip3_len_before"] = before
    df["pct_of_budget_used"] = (after / budget * 100).round(1).astype(str) + "%"
    df["headroom_chars"] = (budget - after).astype(int).astype(str)
    df["trimmed_flag"] = (after < before).map({True:"yes", False:"no"})
    df["chars_trimmed"] = (before - after).astype(int).astype(str)
    return df
