"""
accreditations.py — Regulatory Accreditation & Register Lookups

Checks whether target companies hold key UK regulatory accreditations
and appear on public professional registers. Accreditations are a strong
indicator of quality, market positioning, and defensible revenue.

Sources:
  1. CQC API        — Care Quality Commission (health/social care)
  2. Env Agency     — EA Waste Operations + Waste Carriers registers (HTML)
  3. ICO Register   — Information Commissioner's Office (data protection)
  4. FCA Register   — FCA Authorised Firms (financial services, key required)
  5. Ofsted         — Registered providers (education / children's care)
  6. SIA            — Security Industry Authority Approved Contractors
  7. Website text   — ISO, CHAS, Gas Safe, NICEIC, etc. from digital_health

These checks are performed via reg_sources.py (verification mode) which
handles all HTTP calls, name matching, and error handling consistently.

Output per company:
  • registrations       — dict of {register_key: result} from reg_sources
  • regulatory_score    — 0–25 from reg_sources.score_registrations()
  • regulatory_band     — "Highly Regulated" / "Well Regulated" / etc.
  • confirmed_regs      — list of confirmed register entries
  • accreditation_score — 0–25 from website keyword detection
  • accreditation_band  — "Highly Certified" / "Well Certified" / etc.
  • accreditations      — list of detected accreditations (keyword)
  • combined_score      — 0–50 (regulatory + accreditation)
  • data_tier           — "Tier 1–3 blended"
"""

import json
import os
import time
import re

import config as cfg

try:
    from reg_sources import verify_all, score_registrations
    HAS_REG_SOURCES = True
except ImportError:
    HAS_REG_SOURCES = False


# ── Accreditation keyword scoring ──────────────────────────────────────────

ACCREDITATION_WEIGHTS = {
    "ISO 9001":          5,
    "ISO 14001":         4,
    "ISO 27001":         5,
    "ISO 45001":         4,
    "UKAS":              5,
    "CHAS":              3,
    "Safe Contractor":   3,
    "Constructionline":  3,
    "Gas Safe":          4,
    "NICEIC":            4,
    "NAPIT":             4,
    "ELECSA":            4,
    "BAFE":              4,
    "NSI":               3,
    "FIRAS":             3,
    "CQC":               5,
    "Ofsted":            5,
    "FCA":               4,
    "Environment Permit":4,
    "ICO":               2,
    "TrustMark":         3,
    "Which? Trusted":    3,
    "Cyber Essentials":  3,
}


def _name_match(query: str, target: str) -> bool:
    q = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC|AND|THE|OF|&)\b", "", query.upper()).strip()
    t = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC|AND|THE|OF|&)\b", "", target.upper()).strip()
    words = [w for w in q.split() if len(w) > 3]
    if not words:
        return False
    hits = sum(1 for w in words if w in t)
    return hits / len(words) >= 0.5


def score_website_accreditations(site_accreds: list) -> dict:
    """
    Score accreditations detected from website keyword scanning
    (populated by digital_health.py). Returns 0–25 score.
    """
    score = 0
    found = []

    for a in site_accreds:
        for key, weight in ACCREDITATION_WEIGHTS.items():
            if key.lower() in a.lower() and key not in " ".join(found):
                score += weight
                found.append(a)
                break

    score = min(score, 25)

    if   score >= 18: band = "Highly Certified"
    elif score >= 12: band = "Well Certified"
    elif score >= 6:  band = "Some Accreditation"
    else:             band = "Minimal Certification"

    return {
        "accreditation_score": score,
        "accreditation_band":  band,
        "accreditations":      found,
        "accreditation_count": len(found),
    }


# ── Main enrichment function ────────────────────────────────────────────────

def enrich_accreditations(company: dict) -> dict:
    """
    Full accreditation & regulatory register enrichment for one company.

    Step 1: Run all applicable register checks via reg_sources.verify_all()
    Step 2: Score website-detected accreditation keywords
    Step 3: Merge and combine scores
    """

    # ── Step 1: Register checks via reg_sources ──────────────────────────────
    if HAS_REG_SOURCES:
        reg_results = verify_all(company)
        reg_scoring = score_registrations(reg_results)
    else:
        reg_results = {}
        reg_scoring = {
            "regulatory_score": 0,
            "regulatory_band":  "Not assessed",
            "confirmed_regs":   [],
            "reg_count":        0,
        }

    # ── Step 2: Website accreditation keywords ───────────────────────────────
    site_accreds = company.get("digital_health", {}).get("accreditations_on_site", [])

    # Supplement site_accreds with any register confirmations (avoid double-count)
    confirmed_names = [r for r in reg_scoring.get("confirmed_regs", [])]

    # Add register-based virtual accreditations to site list if not already there
    reg_accred_map = {
        "EA_WASTE":    "Environment Permit",
        "EA_CARRIERS": "Environment Permit",
        "CQC":         "CQC",
        "FCA":         "FCA",
        "OFSTED":      "Ofsted",
        "ICO":         "ICO",
        "SIA":         "SIA ACS",
    }
    extra_accreds = []
    for reg_key, accred_name in reg_accred_map.items():
        r = reg_results.get(reg_key, {})
        if r.get("found") and accred_name not in " ".join(site_accreds):
            extra_accreds.append(accred_name)

    all_site_accreds = list(set(site_accreds + extra_accreds))
    accred_scoring   = score_website_accreditations(all_site_accreds)

    # ── Step 3: Combined score (cap at 50) ───────────────────────────────────
    combined = min(
        reg_scoring.get("regulatory_score", 0) +
        accred_scoring.get("accreditation_score", 0),
        50
    )

    if   combined >= 35: combined_band = "Premium — Highly Regulated & Certified"
    elif combined >= 25: combined_band = "Strong — Regulated or Well Certified"
    elif combined >= 15: combined_band = "Moderate — Some Regulation/Certification"
    else:                combined_band = "Low — Limited Verifiable Credentials"

    return {
        # Register verification results
        "registrations":    reg_results,
        "regulatory_score": reg_scoring.get("regulatory_score", 0),
        "regulatory_band":  reg_scoring.get("regulatory_band", "Not assessed"),
        "confirmed_regs":   reg_scoring.get("confirmed_regs", []),
        "reg_count":        reg_scoring.get("reg_count", 0),

        # Website-detected accreditation keywords
        "accreditation_score": accred_scoring["accreditation_score"],
        "accreditation_band":  accred_scoring["accreditation_band"],
        "accreditations":      accred_scoring["accreditations"],
        "accreditation_count": accred_scoring["accreditation_count"],

        # Combined
        "combined_score": combined,
        "combined_band":  combined_band,
        "data_tier":      "Tier 1–3 blended",
    }


# ── Pipeline runner ──────────────────────────────────────────────────────────

def run():
    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(enriched_path) as f:
        companies = json.load(f)

    top_n     = getattr(cfg, "ACCREDITATIONS_TOP_N", 75)
    to_enrich = companies[:top_n]
    skipped   = len(companies) - top_n

    print(f"\nAccreditation & regulatory enrichment for top {len(to_enrich)} companies"
          f" ({skipped} skipped)...")

    if not HAS_REG_SOURCES:
        print("  ⚠️  reg_sources.py not available — website keyword detection only")

    accred_count = 0
    reg_count    = 0

    for i, c in enumerate(to_enrich):
        if i % 15 == 0:
            print(f"  [{i+1}/{len(to_enrich)}] {c['company_name'][:45]}...")
        result = enrich_accreditations(c)
        c["accreditations"] = result
        if result.get("accreditation_count", 0) > 0:
            accred_count += 1
        if result.get("reg_count", 0) > 0:
            reg_count += 1

    for c in companies[top_n:]:
        c["accreditations"] = {
            "registrations":      {},
            "regulatory_score":   None,
            "regulatory_band":    "Not assessed",
            "confirmed_regs":     [],
            "reg_count":          0,
            "accreditation_score":None,
            "accreditation_band": "Not assessed",
            "accreditations":     [],
            "accreditation_count":0,
            "combined_score":     None,
            "combined_band":      "Not assessed",
            "data_tier":          "N/A",
        }

    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(out_path, "w") as f:
        json.dump(companies, f, indent=2)

    print(f"\nAccreditation enrichment complete → {out_path}")
    print(f"  Register confirmed: {reg_count} / {len(to_enrich)}")
    print(f"  Site keywords:      {accred_count} / {len(to_enrich)}")
    return companies


if __name__ == "__main__":
    run()
