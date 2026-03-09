"""
competitor_map.py — Geographic & Operational Competitor Mapping

For each company in the enriched dataset, identifies the 10 closest
competitors in the same sector by geographic proximity and operational overlap.

Proximity scoring:
  Exact postcode district match (AA11)  → distance score 100
  Same postcode area (AA)               → distance score 70
  Adjacent area (educated guess)        → distance score 40
  National (no geographic overlap)      → distance score 10

Competitor dimensions returned per match:
  - competitor_name
  - company_number
  - postcode
  - distance_band (Local / Regional / National)
  - sic_codes
  - accounts_type
  - estimated_revenue_gbp (from enriched JSON or estimated from employees)
  - employee_band (from CH profile)
  - is_pe_backed (charges outstanding >= 2 + age < 10 or large)
  - is_group_owned (from CH group/medium/large accounts type)
  - acquisition_fit (High / Medium / Low — size-adjacent for bolt-on)
  - sell_intent_score (if available from enriched data)

PE Exit Signal Detection:
  Companies with >= 2 charges AND filing_type in {group, medium, large}
  are likely PE-backed or institutional. These are flagged.

Output:
  Each company dict gets:
    competitor_map: [list of up to 10 closest competitors]
    pe_backed_competitors: [subset with PE/group signals]
    competitor_count_local: int (within same postcode district)
    competitor_count_regional: int (within same postcode area)
    competitor_market_concentration: float (HHI proxy — top 3 share of local market)
    fragmentation_score: 1–10 (10 = highly fragmented)
"""

import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Optional

import config as cfg

DATA_DIR = Path(__file__).parent / "data"
DB_PATH  = DATA_DIR / "companies_house.db"

# SIC codes associated with lift maintenance and adjacent verticals
LIFT_SICS = {
    "43999",  # other specialised construction
    "33120",  # repair of machinery
    "33190",  # repair of other equipment
    "28220",  # lifting/handling equipment manufacture
    "43210",  # electrical installation
    "43290",  # other installation activities
    "81100",  # combined facilities management
    "81290",  # other building and industrial cleaning
    "71122",  # engineering related consulting
    "74909",  # other professional technical
}

# Revenue bands by accounts type (proxy where no OCR data available)
ACCOUNTS_REV_PROXY = {
    "large":                  50_000_000,
    "group":                  40_000_000,
    "medium":                 15_000_000,
    "full":                    8_000_000,
    "small-full":              3_000_000,
    "small":                   2_000_000,
    "total-exemption-full":    1_500_000,
    "total-exemption-small":     800_000,
    "unaudited-abridged":        600_000,
    "micro-entity":              250_000,
    "dormant":                         0,
}


# ── Postcode utilities ─────────────────────────────────────────────────────────

def postcode_district(pc: str) -> str:
    """Extract postcode district, e.g. 'SW1A 1AA' → 'SW1A'."""
    pc = (pc or "").strip().upper()
    # UK postcode: 1–2 letters + 1–2 digits [+ optional letter] SPACE 1digit 2letters
    m = re.match(r'^([A-Z]{1,2}\d{1,2}[A-Z]?)', pc)
    return m.group(1) if m else ""


def postcode_area(pc: str) -> str:
    """Extract postcode area, e.g. 'SW1A 1AA' → 'SW'."""
    pc = (pc or "").strip().upper()
    m = re.match(r'^([A-Z]{1,2})', pc)
    return m.group(1) if m else ""


def proximity_band(pc_a: str, pc_b: str) -> tuple[str, int]:
    """
    Return (band_label, score) for two postcodes.
    Score used for competitor ranking (higher = closer).
    """
    if not pc_a or not pc_b:
        return ("National", 10)

    dist_a = postcode_district(pc_a)
    dist_b = postcode_district(pc_b)
    area_a = postcode_area(pc_a)
    area_b = postcode_area(pc_b)

    if dist_a and dist_a == dist_b:
        return ("Local", 100)
    if area_a and area_a == area_b:
        return ("Regional", 70)

    # Adjacent areas: try to identify nearby areas
    # UK postcode areas are roughly geographic — we use simple heuristics
    # (Not full lat-lon — would need ONS lookup; this is a good enough proxy)
    adjacent = _adjacent_areas(area_a)
    if area_b in adjacent:
        return ("Adjacent Region", 40)

    return ("National", 10)


# ── Approximate postcode area adjacency map (England/Wales/Scotland) ──────────

_ADJACENCY: dict[str, set[str]] = {
    # London clusters
    "E":   {"EC", "N", "NW", "SE", "SW", "W", "WC"},
    "EC":  {"E", "N", "SE", "SW", "W", "WC"},
    "N":   {"E", "EC", "EN", "IG", "NW", "W", "WC"},
    "NW":  {"E", "EC", "EN", "N", "W", "WC", "HA", "UB"},
    "SE":  {"E", "EC", "SW", "BR", "DA", "TN"},
    "SW":  {"E", "EC", "SE", "W", "WC", "KT", "SM", "CR"},
    "W":   {"EC", "N", "NW", "SW", "WC", "HA", "TW", "UB"},
    "WC":  {"E", "EC", "N", "NW", "SE", "SW", "W"},
    # South East
    "BR":  {"CR", "DA", "SE", "TN"},
    "CR":  {"BR", "KT", "SE", "SM", "SW"},
    "DA":  {"BR", "ME", "SE", "TN"},
    "EN":  {"N", "NW", "SG", "WD"},
    "HA":  {"NW", "UB", "W", "WD"},
    "KT":  {"CR", "RH", "SM", "SW", "TW"},
    "ME":  {"CT", "DA", "TN"},
    "RH":  {"BN", "CR", "GU", "KT", "TN"},
    "SM":  {"CR", "KT", "SW"},
    "TN":  {"BR", "DA", "ME", "RH"},
    "TW":  {"KT", "SL", "UB", "W"},
    "UB":  {"HA", "SL", "TW", "W"},
    "SL":  {"GU", "HP", "RG", "TW", "UB"},
    "GU":  {"KT", "RG", "RH", "SL"},
    "RG":  {"GU", "HP", "OX", "SL", "SO"},
    "OX":  {"CV", "HP", "MK", "NN", "RG", "SP"},
    "HP":  {"LU", "MK", "OX", "SL", "WD"},
    "WD":  {"EN", "HA", "HP", "LU"},
    "LU":  {"AL", "HP", "MK", "SG", "WD"},
    "AL":  {"EN", "LU", "SG"},
    "SG":  {"AL", "CB", "EN", "LU", "PE"},
    "CB":  {"CM", "IP", "PE", "SG"},
    "CM":  {"CB", "CO", "IP", "RM", "SS"},
    "CO":  {"CB", "CM", "IP", "NR"},
    "IP":  {"CB", "CM", "CO", "NR", "PE"},
    "NR":  {"CO", "IP", "PE"},
    "PE":  {"CB", "IP", "LE", "MK", "NN", "NR", "SG"},
    "MK":  {"HP", "LU", "NN", "OX", "PE", "SG"},
    "NN":  {"CV", "LE", "MK", "OX", "PE"},
    "LE":  {"CV", "DE", "NN", "PE", "NG"},
    "DE":  {"LE", "NG", "S", "ST", "WS"},
    "NG":  {"DE", "DN", "LE", "LN", "S", "SK"},
    "DN":  {"HU", "LN", "NG", "S", "WF", "YO"},
    "HU":  {"DN", "LN", "YO"},
    "LN":  {"DN", "HU", "NG", "PE"},
    "S":   {"DE", "DN", "HD", "NG", "SK", "WF"},
    "SK":  {"CH", "CW", "DE", "NG", "S", "ST"},
    "ST":  {"B", "CW", "DE", "SK", "TF", "WS", "WV"},
    "TF":  {"DY", "SY", "ST", "WV"},
    "WV":  {"B", "DY", "ST", "TF", "WS"},
    "WS":  {"B", "DE", "ST", "WV"},
    "B":   {"CV", "DY", "ST", "WS", "WV"},
    "CV":  {"B", "LE", "NN", "OX"},
    "DY":  {"B", "TF", "WR", "WS", "WV"},
    "WR":  {"DY", "GL", "HR", "SY"},
    "HR":  {"GL", "LD", "SY", "WR"},
    "GL":  {"BS", "HR", "NP", "OX", "WR"},
    "BS":  {"BA", "GL", "NP", "SN", "TA"},
    "BA":  {"BS", "SP", "SN", "TA"},
    "SN":  {"BA", "BS", "GL", "OX", "RG", "SP"},
    "SP":  {"BA", "BH", "DT", "SN", "SO"},
    "SO":  {"BH", "GU", "PO", "RG", "SP"},
    "PO":  {"BN", "GU", "RH", "SO"},
    "BN":  {"CR", "PO", "RH", "TN"},
    "BH":  {"DT", "SO", "SP"},
    "DT":  {"BA", "BH", "EX", "TA"},
    "TA":  {"BA", "BS", "DT", "EX"},
    "EX":  {"DT", "PL", "TA", "TQ"},
    "PL":  {"EX", "TQ"},
    "TQ":  {"EX", "PL"},
    # Midlands / North
    "CH":  {"CW", "LL", "SK", "WA"},
    "CW":  {"CH", "SK", "ST", "WA"},
    "WA":  {"CH", "CW", "L", "M", "SK", "WN"},
    "L":   {"CH", "PR", "WA", "WN"},
    "WN":  {"L", "M", "PR", "WA"},
    "M":   {"BB", "BL", "OL", "PR", "SK", "WA", "WN"},
    "BL":  {"BB", "FY", "M", "OL", "PR"},
    "OL":  {"BL", "HD", "HX", "M", "SK"},
    "BB":  {"BL", "BD", "FY", "LA", "M", "PR"},
    "PR":  {"BB", "BL", "FY", "L", "WN"},
    "FY":  {"BB", "BL", "LA", "PR"},
    "LA":  {"BB", "CA", "FY", "LS"},
    "BD":  {"BB", "HD", "HX", "LS"},
    "LS":  {"BD", "HG", "HX", "WF"},
    "HD":  {"BD", "HX", "OL", "S", "WF"},
    "HX":  {"BD", "HD", "LS", "OL"},
    "WF":  {"DN", "HD", "LS", "S"},
    "HG":  {"BD", "DL", "LS", "TS", "YO"},
    "YO":  {"DN", "HG", "HU", "LS", "TS"},
    "TS":  {"DL", "HG", "SR", "YO"},
    "SR":  {"DH", "NE", "TS"},
    "DL":  {"DH", "HG", "TS"},
    "DH":  {"DL", "NE", "SR"},
    "NE":  {"DH", "SR", "TD"},
    "TD":  {"EH", "ML", "NE"},
    # Scotland
    "EH":  {"FK", "KY", "ML", "TD"},
    "ML":  {"EH", "G", "KA", "TD"},
    "G":   {"KA", "ML", "PA"},
    "PA":  {"G", "KA"},
    "KA":  {"G", "ML", "PA"},
    "FK":  {"EH", "G", "KY", "PH", "ML"},
    "KY":  {"DD", "EH", "FK"},
    "DD":  {"AB", "KY", "PH"},
    "PH":  {"DD", "FK", "PH"},
    "AB":  {"DD", "IV"},
    "IV":  {"AB", "PH"},
    # Wales
    "CF":  {"NP", "SA", "SA"},
    "NP":  {"CF", "GL", "HR"},
    "SA":  {"CF", "LD", "SY"},
    "LD":  {"HR", "SA", "SY"},
    "SY":  {"CH", "HR", "LD", "LL", "SA", "TF", "WR"},
    "LL":  {"CH", "SY"},
    "CA":  {"DG", "LA", "TD"},
    "DG":  {"CA", "EH", "TD"},
}


def _adjacent_areas(area: str) -> set[str]:
    return _ADJACENCY.get(area, set())


# ── PE / Group ownership signals ──────────────────────────────────────────────

def _is_pe_backed(company: dict) -> bool:
    """
    Heuristic: PE-backed if has outstanding charges AND
    accounts type is group/medium/large or company is young (<10y) but large.
    """
    charges = company.get("outstanding_charges", 0) or 0
    if isinstance(charges, dict):
        charges = charges.get("outstanding_charges", 0) or 0
    acct = (company.get("accounts_type") or
            (company.get("bs") or {}).get("accounts_type") or "").lower()
    age  = company.get("company_age_years") or 0

    if charges >= 2 and acct in ("group", "medium", "large"):
        return True
    if charges >= 1 and age < 10 and acct in ("group", "medium"):
        return True
    return False


def _is_group_owned(company: dict) -> bool:
    """Accounts type group = part of a corporate group."""
    acct = (company.get("accounts_type") or
            (company.get("bs") or {}).get("accounts_type") or "").lower()
    return acct == "group"


# ── Revenue proxy ──────────────────────────────────────────────────────────────

def _rev_proxy(company: dict) -> float:
    """
    Best available revenue estimate for a competitor:
      1. Actual OCR turnover
      2. Revenue estimate from revenue_estimate.py
      3. Accounts-type proxy
    """
    # Actual
    actual = company.get("rev_actual") or company.get("turnover")
    if actual and actual > 0:
        return float(actual)

    # Estimate
    est = (company.get("revenue_estimate") or {}).get("revenue_mid")
    if est and est > 0:
        return float(est)

    # Proxy from accounts type
    acct = (company.get("accounts_type") or
            (company.get("bs") or {}).get("accounts_type") or "").lower()
    return float(ACCOUNTS_REV_PROXY.get(acct, 1_000_000))


# ── Acquisition fit ───────────────────────────────────────────────────────────

def _acquisition_fit(target_rev: float, competitor_rev: float) -> str:
    """
    Classify competitor acquisition fit relative to the platform target:
      High   — 20–80% of target size  (bolt-on sweet spot)
      Medium — 10–20% or 80–150%      (strategic fit possible)
      Low    — very small or very large
    """
    if target_rev <= 0:
        return "Unknown"
    ratio = competitor_rev / target_rev
    if 0.20 <= ratio <= 0.80:
        return "High"
    if 0.10 <= ratio < 0.20 or 0.80 < ratio <= 1.50:
        return "Medium"
    return "Low"


# ── Fragmentation score ───────────────────────────────────────────────────────

def _fragmentation_score(competitors: list[dict], target_rev: float) -> float:
    """
    Fragmentation score 1–10 (higher = more fragmented).

    Proxy: HHI-style measure.
    If many local competitors are small relative to target → high fragmentation.
    """
    if not competitors:
        return 7.0  # default — assume fragmented sector

    local_comps = [c for c in competitors if c.get("distance_band") in ("Local", "Regional")]
    if not local_comps:
        return 6.0

    # Sum of (rev / total) squared — modified HHI
    total = sum(c.get("estimated_revenue_gbp", 1_000_000) for c in local_comps) + (target_rev or 1_000_000)
    shares = [(c.get("estimated_revenue_gbp", 1_000_000) / total) ** 2 for c in local_comps]
    hhi = sum(shares)

    # HHI 0–1: near 0 = fragmented, near 1 = concentrated
    # Invert and scale to 1–10
    frag = round(max(1.0, min(10.0, (1 - hhi) * 10 + 1)), 1)
    return frag


# ── Main competitor mapping function ─────────────────────────────────────────

def build_competitor_map(
    target: dict,
    all_companies: list[dict],
    index_by_number: dict[str, dict],
    top_n: int = 10,
) -> dict:
    """
    For a single target company, find the top_n closest competitors.

    Returns a dict with:
      competitor_map: list[dict]
      pe_backed_competitors: list[str]  (company names)
      group_owned_competitors: list[str]
      competitor_count_local: int
      competitor_count_regional: int
      fragmentation_score: float
      largest_local_competitor: str | None
    """
    target_num = target.get("company_number", "")
    target_pc  = target.get("postcode") or target.get("registered_office_address", {}).get("postal_code", "")
    target_rev = _rev_proxy(target)

    scored: list[dict] = []

    for comp in all_companies:
        if comp.get("company_number") == target_num:
            continue  # skip self

        comp_pc = comp.get("postcode") or comp.get("registered_office_address", {}).get("postal_code", "")
        band, score = proximity_band(target_pc, comp_pc)

        comp_rev  = _rev_proxy(comp)
        acq_fit   = _acquisition_fit(target_rev, comp_rev)
        pe_flag   = _is_pe_backed(comp)
        grp_flag  = _is_group_owned(comp)

        # Boost score for PE/group signals (noteworthy for exit analysis)
        analysis_score = score
        if pe_flag or grp_flag:
            analysis_score += 5

        # Boost for acquisition fit
        if acq_fit == "High":
            analysis_score += 8
        elif acq_fit == "Medium":
            analysis_score += 3

        scored.append({
            "company_name":           comp.get("company_name", ""),
            "company_number":         comp.get("company_number", ""),
            "postcode":               comp_pc,
            "town":                   comp.get("registered_office_address", {}).get("locality") or comp.get("address_town", ""),
            "distance_band":          band,
            "proximity_score":        score,
            "analysis_score":         analysis_score,
            "sic_codes":              _sic_list(comp),
            "accounts_type":          (comp.get("accounts_type") or (comp.get("bs") or {}).get("accounts_type") or ""),
            "estimated_revenue_gbp":  round(comp_rev),
            "is_pe_backed":           pe_flag,
            "is_group_owned":         grp_flag,
            "acquisition_fit":        acq_fit,
            "sell_intent_score":      (comp.get("sell_intent") or {}).get("sell_intent_score"),
            "sell_intent_band":       (comp.get("sell_intent") or {}).get("sell_intent_band"),
        })

    # Sort: analysis_score desc, then revenue desc
    scored.sort(key=lambda x: (-x["analysis_score"], -x["estimated_revenue_gbp"]))
    top = scored[:top_n]

    # Summary stats
    local_count    = sum(1 for c in scored if c["distance_band"] == "Local")
    regional_count = sum(1 for c in scored if c["distance_band"] in ("Local", "Regional", "Adjacent Region"))
    pe_names       = [c["company_name"] for c in top if c["is_pe_backed"]]
    grp_names      = [c["company_name"] for c in top if c["is_group_owned"]]

    # Largest local competitor
    local_comps = [c for c in scored if c["distance_band"] in ("Local", "Regional")]
    largest_local = max(local_comps, key=lambda x: x["estimated_revenue_gbp"])["company_name"] if local_comps else None

    frag_score = _fragmentation_score(scored[:20], target_rev)

    return {
        "competitor_map":              top,
        "pe_backed_competitors":       pe_names,
        "group_owned_competitors":     grp_names,
        "competitor_count_local":      local_count,
        "competitor_count_regional":   regional_count,
        "fragmentation_score":         frag_score,
        "largest_local_competitor":    largest_local,
        "total_sector_competitors":    len(all_companies) - 1,
        "data_tier":                   "Tier 1 — Companies House sector DB + Tier 4 proximity heuristics",
    }


def _sic_list(company: dict) -> list[str]:
    sics = []
    for k in ("sic1", "sic2", "sic3", "sic4"):
        v = company.get(k)
        if v:
            sics.append(str(v))
    return sics


# ── Batch run ─────────────────────────────────────────────────────────────────

def run(companies: Optional[list[dict]] = None, enriched_path: Optional[str] = None) -> list[dict]:
    """
    Add competitor_map to each company in enriched list.

    Args:
        companies:      pre-loaded list (optional; if None, loads from enriched_path)
        enriched_path:  path to enriched JSON (defaults to cfg.OUTPUT_DIR/cfg.ENRICHED_JSON)

    Returns updated list.
    """
    if companies is None:
        if enriched_path is None:
            enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
        with open(enriched_path) as f:
            companies = json.load(f)

    print(f"\nCompetitor mapping for {len(companies)} companies...")

    # Build an index for fast lookup
    index_by_number = {c["company_number"]: c for c in companies}

    for i, company in enumerate(companies):
        if i % 100 == 0:
            print(f"  [{i+1}/{len(companies)}] mapping competitors...")
        company["competitor_analysis"] = build_competitor_map(
            target=company,
            all_companies=companies,
            index_by_number=index_by_number,
            top_n=10,
        )

    # Write back
    if enriched_path:
        with open(enriched_path, "w") as f:
            json.dump(companies, f, indent=2)
        print(f"  Competitor maps written → {enriched_path}")

    # Summary
    pe_signals = sum(
        1 for c in companies
        if (c.get("competitor_analysis") or {}).get("pe_backed_competitors")
    )
    high_frag = sum(
        1 for c in companies
        if (c.get("competitor_analysis") or {}).get("fragmentation_score", 0) >= 7.0
    )
    print(f"  Companies with PE-backed competitors: {pe_signals}")
    print(f"  Companies in highly fragmented markets (≥7.0): {high_frag}")

    return companies


if __name__ == "__main__":
    run()
