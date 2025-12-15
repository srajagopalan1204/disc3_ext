import re

def norm_part(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"[ /\-\.]", "", s).upper()

def derive_pline3_from_config(cfg_line: dict) -> str:
    return (cfg_line or {}).get("pline3", "").lower()

def from_pricing_join_key(part_col_value: str) -> str:
    return norm_part(part_col_value)
