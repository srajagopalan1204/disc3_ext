import re

ALNUM = re.compile(r"[^a-z0-9\.\s]")  # allow dot for metric tokens

def is_metric_thread(token: str) -> bool:
    # keep dotted forms like m3.5, m6.0; lowercased
    return bool(re.fullmatch(r"m\d+(?:\.\d+)?", token))

def clean_token(t: str) -> str:
    t = (t or "").lower()
    # keep dot only for metric-thread tokens; remove otherwise
    if not is_metric_thread(t):
        t = t.replace(".", " ")
    t = ALNUM.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def tokens_from_prod(prod: str):
    # Split product code by spaces and slashes, keep meaningful tokens.
    prod = (prod or "")
    raw = re.split(r"[ /]", prod)
    toks = []
    for r in raw:
        r = clean_token(r)
        if not r:
            continue
        toks.extend(r.split())
    return toks

def detect_family(tokens):
    families = {"thhn":"thhn","mtw":"mtw","xhhw-2":"xhhw-2"}
    found = []
    for t in tokens:
        if t in families:
            found.append(t)
        if t == "xhhw" or t == "xhhw2":
            found.append("xhhw-2")
    return list(dict.fromkeys(found))

def family_expansions(fam: str):
    if fam == "xhhw-2":
        return ["xhhw 2", "xhhw2"]
    return []

def color_from_tokens(tokens, color_map: dict):
    hits = []
    for t in tokens:
        tt = t.lower()
        if tt in color_map:
            hits.append((tt, color_map[tt]))
    return list(dict.fromkeys(hits))

def material_from_text(text: str, materials: dict):
    text = (text or "").lower()
    hits = []
    words = set(text.split())
    for k,v in materials.items():
        if (k in words) or (v in words):
            hits.append((k,v))
    return list(dict.fromkeys(hits))

def footage_and_uom(tokens, uom_terms):
    nums = []
    uoms = []
    for t in tokens:
        if t.isdigit():
            nums.append(t)
        elif t.lower() in uom_terms:
            uoms.append(t.lower())
    return nums, uoms

def build_legacy_token(lookupnm: str, prod: str):
    base = lookupnm if (lookupnm or "").strip() else prod
    base = re.sub(r"[^A-Za-z0-9]", "", base)
    return base.lower()

def de_dupe_keep_order(seq):
    seen = set()
    out = []
    for s in seq:
        if s not in seen and s:
            seen.add(s)
            out.append(s)
    return out

def compose_desc3(packs, budget=260):
    keep = []
    for p in packs:
        if not p: continue
        keep += p
    keep = de_dupe_keep_order(keep)
    legacy = keep[-1] if keep else ""
    body = keep[:-1]
    s = " ".join(body + [legacy]).strip()
    if len(s) <= budget:
        return s, False, []
    return s[:budget], True, []  # placeholder trim (detailed trim happens upstream)
