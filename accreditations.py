"""
accreditations.py — Regulatory Accreditation & Register Lookups

Checks whether target companies hold key UK regulatory accreditations
and appear on public professional registers. Accreditations are a strong
indicator of quality, market positioning, and defensible revenue.

Sources (all free, no auth required):
  1. CQC API        — Care Quality Commission (health/social care)
     https://api.cqc.org.uk/public/v1/providers/search
  2. Env Agency API — Environment Agency (waste/environmental permits)
     https://environment.data.gov.uk/public-register/view/search-waste-operations
  3. ICO Register   — Information Commissioner's Office (data protection)
     https://ico.org.uk/ESDWebPages/Search (HTML scrape)
  4. Website text   — Detect accreditation keywords (ISO, CHAS, Gas Safe, etc.)
     from digital_health.py accreditations_on_site field

Output per company:
  • cqc_registered         — bool + registration number + status
  • environment_permitted  — bool + permit details
  • ico_registered         — bool (data controller)
  • accreditations         — list of detected accreditations
  • accreditation_score    — 0–25 quality signal score
  • data_tier              — Tier 1 / Tier 2

Sector relevance:
  • CQC       — healthcare, social care, domiciliary care
  • EA permit — waste management, hazardous materials, skip hire
  • ICO       — any data-handling company (broad signal)
  • ISO / trade accreditations — construction, engineering, IT, professional
"""

import requests
import json
import time
import os
import re
from urllib.parse import urlencode

import config as cfg

CQC_BASE  = "https://api.cqc.org.uk/public/v1"
EA_BASE   = "https://environment.data.gov.uk"
ICO_BASE  = "https://ico.org.uk/ESDWebPages/Search"

HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "PE-Research-Bot/1.0",
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _clean_name(name: str) -> str:
    return (name
            .replace(" LIMITED", "").replace(" LTD", "")
            .replace(" LLP", "").replace("  ", " ").strip())


def _get(url: str, params: dict = None, timeout: int = 12) -> dict | list:
    try:
        r = SESSION.get(url, params=params, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


# ── CQC Provider Search ───────────────────────────────────────────────────────

def check_cqc(company_name: str) -> dict:
    """
    Search CQC provider register for registered care providers.
    Relevant sectors: health, social care, domiciliary care, dentistry, GP.
    """
    clean = _clean_name(company_name)
    data  = _get(f"{CQC_BASE}/providers/search", params={"providerName": clean, "pageSize": 5})

    providers = []
    if isinstance(data, dict):
        providers = data.get("providers", [])
    elif isinstance(data, list):
        providers = data

    if not providers:
        return {"cqc_registered": False, "data_tier": "Tier 1 — CQC register"}

    # Take closest name match
    for p in providers:
        p_name = p.get("name", "").lower()
        if _name_match(clean, p_name):
            return {
                "cqc_registered":    True,
                "cqc_provider_id":   p.get("providerId", ""),
                "cqc_name":          p.get("name", ""),
                "cqc_status":        p.get("registrationStatus", ""),
                "cqc_type":          p.get("type", ""),
                "cqc_inspections":   p.get("numberOfLocations", 0),
                "cqc_overall_rating":p.get("currentRatings", {}).get("overall", {}).get("rating", ""),
                "data_tier":         "Tier 1 — CQC public register",
            }

    return {"cqc_registered": False, "data_tier": "Tier 1 — CQC register"}


# ── Environment Agency Permit Search ─────────────────────────────────────────

def check_env_agency(company_name: str) -> dict:
    """
    Search Environment Agency waste operations permit register.
    Relevant sectors: waste management, skip hire, recycling, remediation.
    """
    clean = _clean_name(company_name)

    # EA open data API — waste operations
    data = _get(
        f"{EA_BASE}/public-register/view/search-waste-operations.json",
        params={"_search": clean, "pageSize": 5}
    )

    results = data.get("items", []) if isinstance(data, dict) else []

    for item in results:
        holder = item.get("permitHolder", item.get("operatorName", ""))
        if _name_match(clean, holder.lower()):
            return {
                "env_permitted":    True,
                "permit_number":    item.get("permitNumber", item.get("permitId", "")),
                "permit_type":      item.get("permitType", item.get("activityDescription", "")),
                "permit_status":    item.get("permitStatus", ""),
                "permit_holder":    holder,
                "data_tier":        "Tier 1 — Environment Agency public register",
            }

    return {"env_permitted": False, "data_tier": "Tier 1 — Environment Agency register"}


# ── ICO Registration Check ────────────────────────────────────────────────────

def check_ico(company_name: str) -> dict:
    """
    Check ICO data protection register.
    Almost all data-handling companies should be registered.
    Absence can signal compliance weakness.
    """
    clean = _clean_name(company_name)
    params = {
        "SearchType": "Organisation",
        "SearchText": clean,
        "SubmitButton": "Search",
    }

    try:
        r = SESSION.get(ICO_BASE, params=params, timeout=12,
                        headers={"Accept": "text/html"})
        if r.status_code == 200:
            # Look for registration number pattern in HTML
            rn_match = re.search(r"Z\d{6,8}", r.text)
            name_match = clean.lower()[:10] in r.text.lower()
            if rn_match and name_match:
                return {
                    "ico_registered":  True,
                    "ico_reg_number":  rn_match.group(0),
                    "data_tier":       "Tier 1 — ICO public register",
                }
    except Exception:
        pass

    return {"ico_registered": False, "data_tier": "Tier 1 — ICO register"}


# ── Accreditation scoring ─────────────────────────────────────────────────────

# Weighted accreditation values — higher = stronger quality signal
ACCREDITATION_WEIGHTS = {
    "ISO 9001":      5,
    "ISO 14001":     4,
    "ISO 27001":     5,
    "ISO 45001":     4,
    "UKAS":          5,
    "CHAS":          3,
    "Safe Contractor": 3,
    "Constructionline": 3,
    "Gas Safe":      4,
    "NICEIC":        4,
    "NAPIT":         4,
    "ELECSA":        4,
    "CQC":           5,
    "Ofsted":        5,
    "FCA":           4,
    "Environment Permit": 4,
    "ICO":           2,
}

def _name_match(query: str, target: str) -> bool:
    words = [w for w in query.lower().split() if len(w) > 3]
    if not words:
        return False
    hits = sum(1 for w in words if w in target)
    return hits / len(words) >= 0.5


def score_accreditations(
    cqc:        dict,
    env:        dict,
    ico:        dict,
    site_accreds: list[str],
) -> dict:
    """
    Compute accreditation score (0–25) based on detected accreditations.
    Sources: CQC register + EA permit + ICO + website text.

    Higher score = more regulated / quality-certified business
    Interpretation in PE context:
      • Higher certification barrier → more defensible market position
      • Buyer inherits certified status → valuation premium
    """
    score   = 0
    found   = []

    if cqc.get("cqc_registered"):
        score += ACCREDITATION_WEIGHTS["CQC"]
        found.append("CQC Registered")

    if env.get("env_permitted"):
        score += ACCREDITATION_WEIGHTS["Environment Permit"]
        found.append(f"EA Permit: {env.get('permit_type','')[:40]}")

    if ico.get("ico_registered"):
        score += ACCREDITATION_WEIGHTS["ICO"]
        found.append(f"ICO Registered ({ico.get('ico_reg_number','')})")

    # Website-detected accreditations
    for a in site_accreds:
        for key, weight in ACCREDITATION_WEIGHTS.items():
            if key.lower() in a.lower() and key not in " ".join(found):
                score += weight
                found.append(a)
                break

    # Cap score
    score = min(score, 25)

    if   score >= 18: band = "Highly Regulated"
    elif score >= 12: band = "Well Certified"
    elif score >= 6:  band = "Some Accreditation"
    else:             band = "Minimal Certification"

    return {
        "accreditation_score": score,
        "accreditation_band":  band,
        "accreditations":      found,
        "accreditation_count": len(found),
        "data_tier":           "Tier 1–3 blended",
    }


# ── Main enrichment function ──────────────────────────────────────────────────

def enrich_accreditations(company: dict) -> dict:
    """Full accreditation enrichment for one company."""
    name     = company.get("company_name", "")
    sic_list = [str(s) for s in company.get("sic_codes", [])]

    # Determine which checks are sector-relevant
    is_health  = any(s.startswith("86") or s.startswith("87") or s.startswith("88") for s in sic_list)
    is_waste   = any(s.startswith("38") for s in sic_list)

    # CQC — health/care sectors
    if is_health:
        cqc = check_cqc(name)
        time.sleep(0.3)
    else:
        cqc = {"cqc_registered": False, "data_tier": "N/A — not a health sector company"}

    # Environment Agency — waste sectors
    if is_waste:
        env = check_env_agency(name)
        time.sleep(0.3)
    else:
        env = {"env_permitted": False, "data_tier": "N/A — not a waste/environmental company"}

    # ICO — all companies (broad signal)
    ico = check_ico(name)
    time.sleep(0.3)

    # Site accreditations from digital_health module (already enriched)
    site_accreds = company.get("digital_health", {}).get("accreditations_on_site", [])

    scoring = score_accreditations(cqc, env, ico, site_accreds)

    return {
        "cqc":          cqc,
        "env_agency":   env,
        "ico":          ico,
        **scoring,
    }


def run():
    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(enriched_path) as f:
        companies = json.load(f)

    top_n     = getattr(cfg, "ACCREDITATIONS_TOP_N", 75)
    to_enrich = companies[:top_n]
    skipped   = len(companies) - top_n

    print(f"\nAccreditation enrichment for top {len(to_enrich)} companies"
          f" ({skipped} skipped)...")

    accred_count = 0
    for i, c in enumerate(to_enrich):
        if i % 15 == 0:
            print(f"  [{i+1}/{len(to_enrich)}] processing {c['company_name'][:40]}...")
        result = enrich_accreditations(c)
        c["accreditations"] = result
        if result.get("accreditation_count", 0) > 0:
            accred_count += 1

    for c in companies[top_n:]:
        c["accreditations"] = {
            "accreditation_score": None,
            "accreditation_band": "Not assessed",
            "accreditations": [],
            "data_tier": "N/A",
        }

    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(out_path, "w") as f:
        json.dump(companies, f, indent=2)

    print(f"\nAccreditation enrichment complete → {out_path}")
    print(f"  {accred_count} / {len(to_enrich)} companies have detected accreditations")
    return companies


if __name__ == "__main__":
    run()
