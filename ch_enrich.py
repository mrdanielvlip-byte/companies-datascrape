"""
ch_enrich.py — Company enrichment: directors, PSC, succession & acquisition scoring

Acquisition scoring model (per institutional PE spec):
  Scale & Financial     30%  — company maturity, estimated size, balance sheet
  Market Attractiveness 20%  — sector fragmentation, B2B nature, growth signals
  Ownership & Succession30%  — PE independence, founder age, succession risk
  Dealability Signals   20%  — debt changes, governance hires, restructuring

Score range: 0–100
  80–100  Prime acquisition target
  65–79   High priority
  50–64   Medium priority
  < 50    Intelligence record only

All data points carry a reliability tier:
  Tier 1 — Official regulatory / registry data (Companies House)
  Tier 2 — Structured industry datasets
  Tier 3 — Verified corporate websites
  Tier 4 — Derived estimates
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
                    return line.strip().split("=", 1)[1].strip()
    return os.environ.get("COMPANIES_HOUSE_API_KEY", "")


def get(path, retries=3):
    from api_keys import get_auth
    for _ in range(retries):
        try:
            r = requests.get(f"{BASE}{path}", auth=get_auth(), timeout=10)
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
    active_roles = {
        "director", "corporate-director",
        "llp-member", "llp-designated-member",
    }
    directors = []
    for o in data.get("items", []):
        if o.get("officer_role") in active_roles and not o.get("resigned_on"):
            dob = o.get("date_of_birth", {})
            directors.append({
                "name":         o.get("name", ""),
                "role":         o.get("officer_role", ""),
                "age":          calc_age(dob),
                "dob_year":     dob.get("year"),
                "dob_month":    dob.get("month"),
                "appointed":    o.get("appointed_on", ""),
                "years_active": round(years_since(o.get("appointed_on", "")), 1),
                "nationality":  o.get("nationality", ""),
                "occupation":   o.get("occupation", ""),
                "data_tier":    "Tier 1 — Companies House officers register",
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
                "data_tier": "Tier 1 — Companies House PSC register",
            })
    return result


# ── Ownership & PE detection ──────────────────────────────────────────────────

# Strong PE indicators — names that very likely indicate PE/VC ownership
PE_STRONG = [
    "private equity", "buyout", "venture capital", "leveraged",
    "mezzanine", "growth equity", "secondary fund",
]
# Moderate PE indicators — could be PE, could be a family holding company
PE_MODERATE = [
    "limited partnership", "l.p.", " lp,", " lp ", "fund", "capital partners",
    "equity partners", "investment partners", "venture partners",
]
# Weak indicators — often PE but also common in non-PE corporate structures
PE_WEAK = [
    "holdings", "investment", "equity", "capital", "partners",
    "finance", "asset management",
]
# Corporate entity PSC kinds (not an individual person)
CORPORATE_PSC_KINDS = {
    "corporate-entity-person-with-significant-control",
    "legal-person-person-with-significant-control",
}
# OCR text patterns that suggest PE/group ownership
PE_OCR_PATTERNS = [
    "private equity", "venture capital", "buyout", "portfolio company",
    "acquired by", "acquisition by", "backed by", "owned by",
    "subsidiary of", "wholly owned subsidiary", "parent company",
    "ultimate parent", "group undertaking", "controlling party",
    "immediate parent", "ultimate controlling",
]


def is_pe_backed(psc_list: list[dict]) -> bool:
    """Legacy compat — simple boolean check."""
    ownership = analyse_ownership(psc_list)
    return ownership["pe_likelihood"] in ("High", "Medium")


def analyse_ownership(psc_list: list[dict], ocr_text: str = "") -> dict:
    """
    Deep ownership analysis:
      1. Identify corporate entity owners from PSC register
      2. Look up the holding company on Companies House (check ITS PSC too)
      3. Score PE likelihood from name patterns + holding company structure
      4. Scan OCR text for ownership/PE mentions

    Returns dict with:
      corporate_owner        — True/False
      owner_name             — name of the corporate entity (or None)
      owner_company_number   — CH number of holding co if found
      pe_likelihood          — "High" / "Medium" / "Low" / "None"
      pe_signals             — list of reasons for the PE score
      owner_psc_chain        — PSC of the holding company (if looked up)
    """
    result = {
        "corporate_owner":       False,
        "owner_name":            None,
        "owner_company_number":  None,
        "pe_likelihood":         "None",
        "pe_signals":            [],
        "owner_psc_chain":       [],
    }

    # ── Step 1: Find corporate entity owners in PSC ──────────────────────────
    corp_owners = [
        p for p in psc_list
        if p.get("kind") in CORPORATE_PSC_KINDS
    ]

    if not corp_owners:
        # Check OCR for ownership clues even with no corporate PSC
        if ocr_text:
            _scan_ocr_for_pe(ocr_text, result)
        return result

    # Take the first (usually primary) corporate owner
    owner = corp_owners[0]
    owner_name = owner.get("name", "").strip()
    result["corporate_owner"] = True
    result["owner_name"] = owner_name

    signals = []
    name_lower = owner_name.lower()

    # ── Step 2: Score PE likelihood from owner name ──────────────────────────
    for pat in PE_STRONG:
        if pat in name_lower:
            signals.append(f"Strong: '{pat}' in owner name")

    for pat in PE_MODERATE:
        if pat in name_lower:
            signals.append(f"Moderate: '{pat}' in owner name")

    for pat in PE_WEAK:
        if pat in name_lower:
            signals.append(f"Weak: '{pat}' in owner name")

    # ── Step 3: Look up holding company on Companies House ───────────────────
    try:
        # Search CH for the holding company by exact name
        search_q = owner_name.replace(" LIMITED", "").replace(" LTD", "").strip()
        search_data = get(f"/search/companies?q={requests.utils.quote(search_q)}&items_per_page=5")
        matches = search_data.get("items", [])

        # Try to find an exact or close match
        holding_co = None
        for m in matches:
            m_name = (m.get("title") or "").upper()
            if m_name == owner_name.upper() or m_name.replace("LIMITED", "LTD") == owner_name.upper().replace("LIMITED", "LTD"):
                holding_co = m
                break

        if holding_co:
            hc_number = holding_co.get("company_number", "")
            result["owner_company_number"] = hc_number

            # Get PSC of the holding company — check for PE one level up
            hc_psc_data = get(f"/company/{hc_number}/persons-with-significant-control?items_per_page=50")
            hc_psc = [p for p in hc_psc_data.get("items", []) if not p.get("ceased_on")]
            result["owner_psc_chain"] = [
                {"name": p.get("name", ""), "kind": p.get("kind", "")}
                for p in hc_psc[:5]
            ]

            # Check if the holding company is itself owned by a PE entity
            for p in hc_psc:
                if p.get("kind") in CORPORATE_PSC_KINDS:
                    pn = (p.get("name") or "").lower()
                    for pat in PE_STRONG + PE_MODERATE:
                        if pat in pn:
                            signals.append(f"Upstream: '{pat}' in holding co PSC: {p.get('name', '')}")

            # Check holding company SIC codes for investment/holding patterns
            hc_profile = get(f"/company/{hc_number}")
            hc_sics = hc_profile.get("sic_codes", [])
            # 64205 = Activities of financial services holding companies
            # 64209 = Activities of other holding companies
            # 64301 = Activities of venture and development capital companies
            # 64302 = Activities of open-ended investment companies
            pe_sics = {"64205", "64209", "64301", "64302", "64303", "64910", "66300"}
            if set(hc_sics) & pe_sics:
                signals.append(f"Holding co SIC: {', '.join(set(hc_sics) & pe_sics)}")
            time.sleep(0.1)

    except Exception:
        pass   # holding company lookup failed — proceed with name-only analysis

    # ── Step 4: Scan OCR text for ownership/PE clues ─────────────────────────
    if ocr_text:
        _scan_ocr_for_pe(ocr_text, result, signals)

    # ── Compute final PE likelihood ──────────────────────────────────────────
    result["pe_signals"] = signals
    strong_count   = sum(1 for s in signals if s.startswith("Strong") or s.startswith("Upstream"))
    moderate_count = sum(1 for s in signals if s.startswith("Moderate") or s.startswith("Holding"))
    weak_count     = sum(1 for s in signals if s.startswith("Weak"))
    ocr_count      = sum(1 for s in signals if s.startswith("OCR"))

    if strong_count >= 1 or (moderate_count >= 2):
        result["pe_likelihood"] = "High"
    elif moderate_count >= 1 or (weak_count >= 2) or (ocr_count >= 2):
        result["pe_likelihood"] = "Medium"
    elif weak_count >= 1 or ocr_count >= 1:
        result["pe_likelihood"] = "Low"
    else:
        result["pe_likelihood"] = "None"

    return result


def _scan_ocr_for_pe(ocr_text: str, result: dict, signals: list | None = None):
    """Scan OCR text for PE/ownership clues and add to signals."""
    if signals is None:
        signals = result.get("pe_signals", [])
    text_lower = ocr_text.lower()
    for pat in PE_OCR_PATTERNS:
        if pat in text_lower:
            signals.append(f"OCR: '{pat}' found in accounts text")
    result["pe_signals"] = signals


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
    long_tenures = [d for d in directors if d.get("years_active", 0) >= 15]
    return {
        "is_family":         bool(shared) or surname_in_name,
        "is_owner_managed":  len(directors) <= 3,
        "shared_surnames":   shared,
        "surname_in_name":   surname_in_name,
        "long_tenure_count": len(long_tenures),
        "data_tier":         "Tier 1 — Companies House officers register",
    }


# ── Succession analysis ───────────────────────────────────────────────────────

GOVERNANCE_ROLES = {
    "chief financial", "cfo", "finance director", "chief operating",
    "coo", "managing director", "independent director", "non-executive",
}

def succession_score(directors: list[dict]) -> dict:
    """
    Succession Risk = Founder Age Score + Director Count Score + Age Distribution Score
    Max: 100 (highest risk = best acquisition signal)

    Also checks for governance roles (COO, MD) that reduce succession risk.
    """
    ages      = [d["age"] for d in directors if d["age"]]
    max_age   = max(ages, default=0)
    avg_age   = sum(ages) / len(ages) if ages else 0
    dir_count = len(directors)

    # Governance role check — reduce risk if proper management structure exists
    has_governance = any(
        any(role in (d.get("occupation") or "").lower() for role in GOVERNANCE_ROLES)
        for d in directors
    )
    governance_penalty = -10 if has_governance else 0

    # Component 1: Founder age (0–34)
    if   max_age >= 70: age_sc = 34
    elif max_age >= 65: age_sc = 28
    elif max_age >= 60: age_sc = 22
    elif max_age >= 55: age_sc = 15
    elif max_age >= 45: age_sc = 8
    elif max_age > 0:   age_sc = 4
    else:               age_sc = 0

    # Component 2: Director count (0–33)
    # Single director = maximum key-person risk
    if   dir_count == 0: dir_sc = 33
    elif dir_count == 1: dir_sc = 30
    elif dir_count == 2: dir_sc = 20
    elif dir_count == 3: dir_sc = 10
    else:                dir_sc = 5

    # Component 3: Age distribution (0–33)
    if not ages:
        dist_sc = 15
    elif all(a >= 55 for a in ages):     dist_sc = 33
    elif avg_age >= 58:                  dist_sc = 25
    elif avg_age >= 52:                  dist_sc = 18
    elif any(a < 45 for a in ages):      dist_sc = 8
    else:                                dist_sc = 12

    total = max(0, age_sc + dir_sc + dist_sc + governance_penalty)

    return {
        "total":             total,
        "age_score":         age_sc,
        "dir_score":         dir_sc,
        "dist_score":        dist_sc,
        "governance_penalty":governance_penalty,
        "has_governance":    has_governance,
        "max_age":           max_age,
        "avg_age":           round(avg_age, 1),
        "formula":           "Founder Age Score + Director Count Score + Age Distribution Score",
        "data_tier":         "Tier 1 — Companies House officers register",
    }


# ── Dealability signals ───────────────────────────────────────────────────────

def dealability_score(company_number: str, directors: list[dict],
                       charges: dict) -> dict:
    """
    Dealability Signals score (0–20):
    • Corporate restructuring (new HoldCo, share transfers)   +5
    • Governance hires (CFO, MD, NED)                         +5
    • PSC / ownership changes                                  +4
    • Active filing history                                    +3
    • Clean charge register (no excessive debt)               +3

    Debt Growth = (Current Debt − Previous Debt) / Previous Debt
    Data tier: Tier 1
    """
    filing_history = get(f"/company/{company_number}/filing-history?items_per_page=25")
    filings        = filing_history.get("items", [])
    score = 0
    signals = []

    # Corporate restructuring
    restructure_kws = ["holding", "group", "reorganis", "transfer of shares",
                       "subdivision", "consolidation"]
    for f in filings:
        desc = f.get("description", "").lower()
        if any(kw in desc for kw in restructure_kws):
            signals.append({"type": "Corporate restructuring", "detail": f.get("description",""), "date": f.get("date",""), "tier": "Tier 1"})
            score += 5
            break

    # Governance hires
    gov_roles = ["chief financial", "cfo", "managing director", "finance director",
                 "non-executive", "independent director", "chief operating"]
    for d in directors:
        occ = (d.get("occupation") or "").lower()
        if any(role in occ for role in gov_roles):
            signals.append({"type": "Governance hire", "detail": f"{d['name']} — {d.get('occupation','')}", "date": d.get("appointed",""), "tier": "Tier 1"})
            score += 5
            break  # one governance signal sufficient

    # PSC changes
    psc_filings = [f for f in filings if "psc" in f.get("description","").lower()
                   or "significant-control" in f.get("links",{}).get("self","")]
    if psc_filings:
        signals.append({"type": "PSC/ownership change", "detail": f"{len(psc_filings)} PSC filing(s)", "date": psc_filings[0].get("date",""), "tier": "Tier 1"})
        score += 4

    # Active filing history
    if len(filings) >= 3:
        score += 3

    # Clean charge register — limited debt = cleaner deal structure
    outstanding = charges.get("outstanding_charges", 0)
    if outstanding == 0:
        signals.append({"type": "Clean charge register", "detail": "No outstanding charges", "tier": "Tier 1"})
        score += 3
    elif outstanding <= 2:
        score += 1

    return {
        "score":        min(score, 20),
        "signals":      signals,
        "signal_count": len(signals),
        "data_tier":    "Tier 1 — Companies House",
        "formula":      "Restructuring(5) + Governance(5) + PSC Change(4) + Active Filings(3) + Clean Charges(3)",
    }


# ── Registered charges ────────────────────────────────────────────────────────

def get_charges(company_number: str) -> dict:
    data  = get(f"/company/{company_number}/charges?items_per_page=25")
    items = data.get("items", [])
    outstanding = [c for c in items if c.get("status") == "outstanding"]
    satisfied   = [c for c in items if c.get("status") == "satisfied"]
    details = [
        {
            "created": c.get("created_on",""),
            "type":    c.get("classification",{}).get("description",""),
            "persons": [p.get("name","") for p in c.get("persons_entitled",[])],
        }
        for c in outstanding[:5]
    ]
    return {
        "total_charges":       len(items),
        "outstanding_charges": len(outstanding),
        "satisfied_charges":   len(satisfied),
        "charge_details":      details,
        "has_debt":            len(outstanding) > 0,
        "data_tier":           "Tier 1 — Companies House charges register",
    }


# ── Acquisition scoring (4-dimension institutional model) ─────────────────────

def acquisition_score(company_age: int, succ: dict, pe_backed: bool,
                       dealability: dict, charges: dict) -> dict:
    """
    Acquisition Score =
      (Scale & Financial    × 0.30)
    + (Market Attractiveness × 0.20)
    + (Ownership & Succession× 0.30)
    + (Dealability Signals   × 0.20)

    Each dimension scored 0–100, then weighted.
    Final score: 0–100
    """

    # Dimension 1: Scale & Financial (0–100 → weighted to 30)
    # Uses company age as maturity proxy; financial data enriched separately
    if   company_age >= 25: scale_raw = 90
    elif company_age >= 20: scale_raw = 80
    elif company_age >= 15: scale_raw = 65
    elif company_age >= 10: scale_raw = 50
    elif company_age >= 5:  scale_raw = 30
    else:                   scale_raw = 15
    scale = round(scale_raw * 0.30)

    # Dimension 2: Market Attractiveness (0–100 → weighted to 20)
    # Fixed fragmentation signal + B2B sector assumption from config
    frag_score = getattr(cfg, "MARKET_ATTRACTIVENESS_SCORE", 75)
    market = round(frag_score * 0.20)

    # Dimension 3: Ownership & Succession (0–100 → weighted to 30)
    # Combines PE independence + succession risk score
    independence_pts = 0 if pe_backed else 40   # 40/100 for independence
    succession_pts   = succ.get("total", 0)     # already 0–100
    ownership_raw    = (independence_pts + succession_pts) / 1.4  # normalise to 100
    ownership        = round(min(ownership_raw, 100) * 0.30)

    # Dimension 4: Dealability Signals (0–20 → weighted to 20)
    deal_raw  = dealability.get("score", 0)  # already 0–20
    deal_score = round((deal_raw / 20) * 100 * 0.20)

    total = scale + market + ownership + deal_score

    return {
        "total":              min(total, 100),
        "scale_financial":    scale,
        "market_attractiveness": market,
        "ownership_succession":  ownership,
        "dealability":        deal_score,
        "formula":            "Scale(×0.30) + Market(×0.20) + Ownership(×0.30) + Dealability(×0.20)",
        "data_tier":          "Tier 1–4 blended",
    }


def grade(score: int) -> str:
    """
    Score interpretation per institutional PE spec:
    80–100  Prime acquisition target
    65–79   High priority
    50–64   Medium priority
    < 50    Intelligence record only
    """
    if score >= 80: return "Prime"
    if score >= 65: return "High"
    if score >= 50: return "Medium"
    return "Intelligence Only"


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    global AUTH
    from api_keys import init as _init_keys, get_single_key
    _init_keys()
    AUTH = (get_single_key(), "")  # fallback for any code using AUTH directly

    filtered_path = os.path.join(cfg.OUTPUT_DIR, cfg.FILTERED_JSON)
    with open(filtered_path) as f:
        companies = json.load(f)

    print(f"\nEnriching {len(companies)} companies via Companies House API...")

    enriched = []
    for i, c in enumerate(companies):
        num = c["company_number"]
        if i % 25 == 0:
            print(f"  [{i+1}/{len(companies)}] processing...")

        directors = get_directors(num);    time.sleep(0.05)
        psc       = get_psc(num);          time.sleep(0.05)
        charges   = get_charges(num);      time.sleep(0.05)

        # Deep ownership analysis — checks holding company + PE patterns
        ownership = analyse_ownership(psc)
        pe  = ownership["pe_likelihood"] in ("High", "Medium")
        fam = detect_family(c["company_name"], directors)
        ss  = succession_score(directors)

        incorp_year  = int(c["date_of_creation"][:4]) if c.get("date_of_creation") else 0
        company_age  = 2025 - incorp_year if incorp_year else 0

        deal         = dealability_score(num, directors, charges)
        time.sleep(0.05)

        acq          = acquisition_score(company_age, ss, pe, deal, charges)

        enriched.append({
            **c,
            "company_age_years":  company_age,
            "directors":          directors,
            "director_count":     len(directors),
            "psc":                psc,
            "pe_backed":          pe,
            "ownership":          ownership,
            "corporate_owner":    ownership["corporate_owner"],
            "owner_name":         ownership["owner_name"],
            "pe_likelihood":      ownership["pe_likelihood"],
            "pe_signals":         ownership["pe_signals"],
            **fam,
            "succession":         ss,
            "charges":            charges,
            "dealability":        deal,
            "acquisition_score":  acq["total"],
            "acquisition_grade":  grade(acq["total"]),
            "acq_components":     acq,
            "data_tier":          "Tier 1 — Companies House",
        })
        time.sleep(0.05)

    enriched.sort(key=lambda x: x["acquisition_score"], reverse=True)

    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(out_path, "w") as f:
        json.dump(enriched, f, indent=2)

    print(f"\nDone. {len(enriched)} companies saved → {out_path}")

    # Summary
    prime  = sum(1 for c in enriched if c["acquisition_score"] >= 80)
    high   = sum(1 for c in enriched if 65 <= c["acquisition_score"] < 80)
    medium = sum(1 for c in enriched if 50 <= c["acquisition_score"] < 65)
    intel  = sum(1 for c in enriched if c["acquisition_score"] < 50)
    print(f"  Prime (80+): {prime}  |  High (65-79): {high}  |  Medium (50-64): {medium}  |  Intel only (<50): {intel}")

    return enriched


if __name__ == "__main__":
    run()
