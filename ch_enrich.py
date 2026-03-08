"""
ch_enrich.py — Enrich company list with directors, PSC, succession & acquisition scores
Reads filtered_companies.json, writes enriched_companies.json
"""

import requests
import json
import time
import os
from datetime import datetime
from collections import Counter

import config as cfg


BASE = "https://api.company-information.service.gov.uk"
AUTH = None


def load_api_key():
    key_file = os.path.join(os.path.dirname(__file__), ".ch_api_key")
    if os.path.exists(key_file):
        with open(key_file) as f:
            for line in f:
                if "=" in line:
                    return line.strip().split("=", 1)[1]
    key = os.environ.get("COMPANIES_HOUSE_API_KEY")
    if key:
        return key
    raise RuntimeError("No Companies House API key found.")


def get(path, retries=3):
    for _ in range(retries):
        try:
            r = requests.get(f"{BASE}{path}", auth=AUTH, timeout=10)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(2)
        except requests.RequestException:
            time.sleep(1)
    return {}


# ── Director helpers ──────────────────────────────────────────────────────────

def calc_age(dob: dict) -> int | None:
    year = dob.get("year")
    if not year:
        return None
    month = dob.get("month", 6)
    return 2025 - year - (1 if month > 6 else 0)


def years_since(date_str: str) -> float:
    if not date_str:
        return 0
    try:
        start = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return (datetime.now() - start).days / 365.25
    except ValueError:
        return 0


def get_directors(company_number: str) -> list[dict]:
    data = get(f"/company/{company_number}/officers?items_per_page=100")
    active_roles = {"director", "corporate-director", "llp-member", "llp-designated-member"}
    directors = []
    for o in data.get("items", []):
        if o.get("officer_role") in active_roles and not o.get("resigned_on"):
            dob = o.get("date_of_birth", {})
            directors.append({
                "name":       o.get("name", ""),
                "role":       o.get("officer_role", ""),
                "age":        calc_age(dob),
                "appointed":  o.get("appointed_on", ""),
                "years_active": round(years_since(o.get("appointed_on", "")), 1),
                "nationality":  o.get("nationality", ""),
                "occupation":   o.get("occupation", ""),
            })
    return directors


def get_psc(company_number: str) -> list[dict]:
    data = get(f"/company/{company_number}/persons-with-significant-control?items_per_page=50")
    result = []
    for p in data.get("items", []):
        if not p.get("ceased_on"):
            result.append({
                "name":     p.get("name", ""),
                "kind":     p.get("kind", ""),
                "natures":  p.get("natures_of_control", []),
                "country":  p.get("country_of_residence", ""),
            })
    return result


# ── PE detection ──────────────────────────────────────────────────────────────

PE_INDICATORS = [
    "limited partnership", "l.p.", "llp", "holdings", "investment",
    "equity", "capital", "fund", "partners", "venture", "finance",
    "asset management",
]
CORPORATE_PSC_KINDS = {
    "corporate-entity-person-with-significant-control",
    "legal-person-person-with-significant-control",
}

def is_pe_backed(psc_list: list[dict]) -> bool:
    return any(
        p.get("kind") in CORPORATE_PSC_KINDS
        and any(pi in (p.get("name") or "").lower() for pi in PE_INDICATORS)
        for p in psc_list
    )


# ── Family / owner-managed detection ─────────────────────────────────────────

def detect_family(company_name: str, directors: list[dict]) -> dict:
    surnames = []
    for d in directors:
        parts = d["name"].split(",")[0].strip().split()
        if parts:
            surnames.append(parts[-1].lower())
    counts = Counter(surnames)
    shared = [s for s, n in counts.items() if n > 1 and len(s) > 2]
    surname_in_name = any(s in company_name.lower() for s in surnames if len(s) > 3)
    return {
        "is_family":      bool(shared) or surname_in_name,
        "is_owner_managed": len(directors) <= 3,
        "shared_surnames":  shared,
        "surname_in_name":  surname_in_name,
    }


# ── Succession scoring ────────────────────────────────────────────────────────

def succession_score(directors: list[dict]) -> dict:
    ages = [d["age"] for d in directors if d["age"]]
    max_age = max(ages, default=0)
    avg_age = sum(ages) / len(ages) if ages else 0
    dir_count = len(directors)

    # Component 1: founder age (0–34)
    if   max_age >= 70: age_sc = 34
    elif max_age >= 65: age_sc = 28
    elif max_age >= 60: age_sc = 22
    elif max_age >= 55: age_sc = 15
    elif max_age >= 45: age_sc = 8
    elif max_age > 0:   age_sc = 4
    else:               age_sc = 0

    # Component 2: director count (0–33)
    if   dir_count == 0: dir_sc = 33
    elif dir_count == 1: dir_sc = 30
    elif dir_count == 2: dir_sc = 20
    elif dir_count == 3: dir_sc = 10
    else:                dir_sc = 5

    # Component 3: age distribution (0–33)
    if not ages:
        dist_sc = 15
    elif all(a >= 55 for a in ages):        dist_sc = 33
    elif avg_age >= 58:                     dist_sc = 25
    elif avg_age >= 52:                     dist_sc = 18
    elif any(a < 45 for a in ages):         dist_sc = 8
    else:                                   dist_sc = 12

    total = age_sc + dir_sc + dist_sc
    return {
        "total":     total,
        "age_score": age_sc,
        "dir_score": dir_sc,
        "dist_score": dist_sc,
        "max_age":   max_age,
        "avg_age":   round(avg_age, 1),
    }


# ── Acquisition scoring ───────────────────────────────────────────────────────

def acquisition_score(company_age: int, succ: dict, pe_backed: bool) -> dict:
    w = cfg.SCORE_WEIGHTS

    # Scale fit — use company age as maturity proxy
    if   company_age >= 20: scale = w["scale_fit"]
    elif company_age >= 15: scale = int(w["scale_fit"] * 0.85)
    elif company_age >= 10: scale = int(w["scale_fit"] * 0.65)
    elif company_age >= 5:  scale = int(w["scale_fit"] * 0.40)
    else:                   scale = int(w["scale_fit"] * 0.15)

    # Founder retirement — map from age score
    founder_ret = min(w["founder_retirement"],
                      int(succ["age_score"] * w["founder_retirement"] / 34))

    # Succession weakness — map from succession total
    succ_dim = min(w["succession_weakness"],
                   int(succ["total"] * w["succession_weakness"] / 100))

    # Independence
    independence = 0 if pe_backed else w["independence"]

    # Sector fragmentation — fixed for this sector config
    fragmentation = int(w["sector_fragmentation"] * 0.8)

    # Operational signals — company age proxy
    ops = w["operational_signals"] if company_age >= 10 else int(w["operational_signals"] * 0.5)

    total = scale + founder_ret + succ_dim + independence + fragmentation + ops
    return {
        "total":             total,
        "scale":             scale,
        "founder_retirement": founder_ret,
        "succession":        succ_dim,
        "independence":      independence,
        "fragmentation":     fragmentation,
        "ops":               ops,
    }


def grade(score: int) -> str:
    if score >= 85: return "A+"
    if score >= 80: return "A"
    if score >= 75: return "B+"
    if score >= 70: return "B"
    if score >= 60: return "C"
    return "D"


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    global AUTH
    AUTH = (load_api_key(), "")

    filtered_path = os.path.join(cfg.OUTPUT_DIR, cfg.FILTERED_JSON)
    with open(filtered_path) as f:
        companies = json.load(f)

    print(f"\nEnriching {len(companies)} companies via Companies House API...")

    enriched = []
    for i, c in enumerate(companies):
        num = c["company_number"]
        if i % 25 == 0:
            print(f"  [{i+1}/{len(companies)}] processing...")

        directors = get_directors(num)
        time.sleep(0.05)
        psc       = get_psc(num)
        time.sleep(0.05)

        pe  = is_pe_backed(psc)
        fam = detect_family(c["company_name"], directors)
        ss  = succession_score(directors)

        incorp_year  = int(c["date_of_creation"][:4]) if c.get("date_of_creation") else 0
        company_age  = 2025 - incorp_year if incorp_year else 0
        acq          = acquisition_score(company_age, ss, pe)

        enriched.append({
            **c,
            "company_age_years":  company_age,
            "directors":          directors,
            "director_count":     len(directors),
            "psc":                psc,
            "pe_backed":          pe,
            **fam,
            "succession":         ss,
            "acquisition_score":  acq["total"],
            "acquisition_grade":  grade(acq["total"]),
            "acq_components":     acq,
        })
        time.sleep(0.05)

    enriched.sort(key=lambda x: x["acquisition_score"], reverse=True)

    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(out_path, "w") as f:
        json.dump(enriched, f, indent=2)

    print(f"\nDone. {len(enriched)} companies saved → {out_path}")
    return enriched


if __name__ == "__main__":
    run()
