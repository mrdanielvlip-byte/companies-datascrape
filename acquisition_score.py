"""
acquisition_score.py — PE Acquisition Attractiveness Scoring Engine

Produces a multi-factor Acquisition Attractiveness Score (0–100) for each
target company based on five institutional PE evaluation dimensions:

  1. Market Fragmentation    (0–20)  — how fragmented the local market is
  2. Recurring Revenue       (0–20)  — proxy for contract/maintenance base
  3. Operational Improvement (0–20)  — scope for margin expansion (professionalise)
  4. Bolt-on Potential       (0–20)  — nearby targets available for roll-up
  5. Exit Attractiveness     (0–20)  — PE/strategic buyer appetite + sector multiples

Score bands:
  80–100  Tier 1 — Prime platform target
  65–79   Tier 2 — Strong acquisition candidate
  50–64   Tier 3 — Quality bolt-on
  35–49   Tier 4 — Watch list / monitor
  < 35    Out of scope — intelligence only

Each dimension carries a 0–20 scale with narrative rationale and sub-scores.

Data used:
  Tier 1: Companies House filings (accounts type, filing history, charges)
  Tier 2: CH officers register (director count, tenure)
  Tier 4: Revenue estimates, competitor map, sector benchmarks
"""

import json
import os
from typing import Optional

import config as cfg

# ── Sector constants (Lift Maintenance UK) ─────────────────────────────────────
# These drive scoring calibration. Update via configs/lift_maintenance.py

SECTOR = {
    "name":             "Lift Maintenance & Installation UK",
    "ebitda_multiple":  8.5,    # Typical EBITDA multiple (EV/EBITDA) at exit
    "recurring_pct":    0.80,   # % of sector revenue typically recurring (maintenance contracts)
    "market_size_gbp":  2_500_000_000,   # ~£2.5bn UK addressable market
    "top5_share":       0.25,   # top 5 players hold ~25% of market → highly fragmented
    "b2b_pct":          0.97,   # essentially all B2B
    "growth_rate":      0.04,   # ~4% CAGR (lifts installed base + ageing)
    "pe_interest_score":   8,   # 1–10 from master prompt context
    "regulatory_moat":     7,   # LOLER, BS EN 81, BSRIA — high barrier
}

# Target revenue sweet spot for a PE platform deal
PLATFORM_REV_MIN  =  5_000_000
PLATFORM_REV_MAX  = 30_000_000
BOLT_ON_REV_MIN   =    500_000
BOLT_ON_REV_MAX   =  5_000_000


# ── 1. Market Fragmentation (0–20) ────────────────────────────────────────────

def score_fragmentation(company: dict) -> dict:
    """
    High fragmentation = strong roll-up thesis.

    Sub-components:
      a. Sector-level fragmentation score (fixed for this sector)   0–10
      b. Local competitor count (more small rivals = better roll-up) 0–10
    """
    comp_analysis = company.get("competitor_analysis") or {}
    frag_score    = comp_analysis.get("fragmentation_score", 7.0)  # 1–10
    local_count   = comp_analysis.get("competitor_count_local", 0)
    regional_count= comp_analysis.get("competitor_count_regional", 0)
    total_count   = comp_analysis.get("total_sector_competitors", 1000)

    signals = []
    detail  = {}

    # a. Sector-level: HHI-derived fragmentation score (1–10)
    sector_a = min(10, round(frag_score))
    if frag_score >= 8:
        signals.append(f"Highly fragmented local market (fragmentation index {frag_score:.1f}/10)")
    elif frag_score >= 6:
        signals.append(f"Moderately fragmented local market (index {frag_score:.1f}/10)")

    # b. Local competitor density
    if   local_count >= 20: local_b = 10; signals.append(f"{local_count} local competitors — dense clustering, strong roll-up opportunity")
    elif local_count >= 10: local_b =  8; signals.append(f"{local_count} local competitors — good consolidation potential")
    elif local_count >=  5: local_b =  6
    elif local_count >=  2: local_b =  4
    else:                   local_b =  2; signals.append("Few local competitors — limited local roll-up; national strategy needed")

    detail = {
        "sector_frag_score": sector_a,
        "local_density_score": local_b,
        "local_competitors": local_count,
        "regional_competitors": regional_count,
        "total_sector": total_count,
        "fragmentation_index": frag_score,
    }
    total = sector_a + local_b

    return {"score": min(20, total), "signals": signals, "detail": detail,
            "data_tier": "Tier 1 CH + Tier 4 sector DB"}


# ── 2. Recurring Revenue (0–20) ───────────────────────────────────────────────

def score_recurring_revenue(company: dict) -> dict:
    """
    Recurring revenue proxy for lift maintenance:
      - Accounts type (small/full indicates real business, not project-only)
      - Staff costs / turnover ratio (high staff % = contract-dense)
      - Years in operation (long-standing = contracted client base)
      - Absence of construction SIC (pure maintenance > project)
      - Outstanding contracts / accreditations (from sector data)

    Lift maintenance is inherently ~80% recurring (LOLER inspections, PPM).
    We adjust down for pure-installer profiles (low tenure, project SIC).
    """
    sic1 = str(company.get("sic1") or "")
    sic2 = str(company.get("sic2") or "")
    all_sics = {sic1, sic2}
    age = company.get("company_age_years") or 0
    acct = (company.get("accounts_type") or
            (company.get("bs") or {}).get("accounts_type") or "").lower()

    staff_costs = (company.get("staff_costs") or
                   (company.get("financials") or {}).get("staff_costs"))
    turnover = (company.get("rev_actual") or
                (company.get("revenue_estimate") or {}).get("revenue_mid"))
    staff_ratio = (staff_costs / turnover) if (staff_costs and turnover and turnover > 0) else None

    signals = []
    detail  = {}
    score   = 0

    # Base: sector default (maintenance = recurring by nature)
    base = 10  # lift maintenance inherently recurring
    signals.append(f"Sector baseline: {SECTOR['recurring_pct']*100:.0f}% recurring contract revenue typical for lift maintenance")

    # Adjust for SIC profile: pure maintenance SICs get bonus
    maintenance_sics = {"33120", "33190", "43999"}
    installation_sics= {"43210", "43290", "43999", "28220"}
    is_pure_maintenance = any(s in maintenance_sics for s in all_sics)
    is_installer        = any(s in installation_sics for s in all_sics)

    if is_pure_maintenance and not is_installer:
        score += 4
        signals.append("SIC profile: pure maintenance — highest recurring revenue likelihood")
    elif is_pure_maintenance:
        score += 2
        signals.append("SIC profile: maintenance + installation mix")
    else:
        score += 1

    # Adjust for company age (long-standing = established contract base)
    if   age >= 20: score += 4; signals.append(f"Age {age} yrs — established maintenance contract base")
    elif age >= 15: score += 3; signals.append(f"Age {age} yrs — mature client base")
    elif age >= 10: score += 2
    elif age >=  5: score += 1

    # Adjust for staff/revenue ratio
    if staff_ratio is not None:
        if 0.40 <= staff_ratio <= 0.65:
            score += 2; signals.append(f"Staff/revenue ratio {staff_ratio:.0%} — in range for contract-dense model")
        elif staff_ratio > 0.65:
            score += 1

    total = min(20, base + score)
    detail = {
        "base": base,
        "sic_score": score,
        "age": age,
        "staff_ratio": round(staff_ratio, 2) if staff_ratio else None,
        "is_pure_maintenance": is_pure_maintenance,
    }

    return {"score": total, "signals": signals, "detail": detail,
            "data_tier": "Tier 1 CH + Tier 4 revenue model"}


# ── 3. Operational Improvement (0–20) ────────────────────────────────────────

def score_operational_improvement(company: dict) -> dict:
    """
    Scope for margin expansion / operational professionalisation post-acquisition.

    Indicators of improvement potential:
      - Solo / duo management (no governance layer) → +ops improvement
      - No professional directors (FD/MD/COO absent) → +leverage
      - Late filings → disorganised → systematic improvement possible
      - Low digital footprint → website/digital transformation value
      - Small, long-standing company → not maximising service pricing

    Higher score = MORE improvement potential (better for PE, not worse).
    """
    directors = company.get("directors", [])
    n_dirs    = len(directors)
    sell_intent = company.get("sell_intent") or {}
    struct_score = (sell_intent.get("components") or {}).get("business_structure") or {}
    ops_stress   = (sell_intent.get("components") or {}).get("operational_stress") or {}

    signals = []
    detail  = {}
    score   = 0

    # a. Governance gap (1–7)
    if struct_score.get("has_governance") == False:
        score += 7
        signals.append("No FD/CFO/MD/NED — significant professionalisation upside post-acquisition")
    elif n_dirs <= 2:
        score += 5
        signals.append("Solo/duo management — substantial operational leverage available")
    elif n_dirs <= 4:
        score += 3
    else:
        score += 1

    # b. Late filings / operational stress signal (0–5)
    late_filings = ops_stress.get("late_filings", 0) or 0
    if late_filings >= 2:
        score += 5
        signals.append("Repeated late filings — operational discipline opportunity")
    elif late_filings == 1:
        score += 3
        signals.append("Late filing on record — systems improvement lever")

    # c. No digital / website presence (0–4)
    website = company.get("website") or ""
    has_web = bool(website and website not in ("", "n/a", "N/A", "unknown"))
    if not has_web:
        score += 4
        signals.append("No confirmed website — digital transformation upside")
    else:
        score += 1

    # d. Revenue per employee proxy (0–4) — below benchmark = pricing improvement opportunity
    rev      = (company.get("rev_actual") or
                (company.get("revenue_estimate") or {}).get("revenue_mid") or 0)
    emp_band = company.get("employees_est_band") or ""
    # Try to get midpoint from band string
    emp_mid  = _parse_emp_band_mid(emp_band)
    if emp_mid and emp_mid > 0 and rev > 0:
        rpe = rev / emp_mid
        if rpe < 100_000:
            score += 4
            signals.append(f"Revenue/employee £{rpe:,.0f} — below sector benchmark (£120-200k), pricing power gap")
        elif rpe < 130_000:
            score += 2

    detail = {
        "governance_gap": struct_score.get("has_governance") == False,
        "director_count": n_dirs,
        "late_filings": late_filings,
        "has_website": has_web,
        "rev_per_employee": round(rev / emp_mid, 0) if (emp_mid and emp_mid > 0 and rev > 0) else None,
    }

    return {"score": min(20, score), "signals": signals, "detail": detail,
            "data_tier": "Tier 1 CH + Tier 4 operational heuristics"}


def _parse_emp_band_mid(band: str) -> Optional[float]:
    """Parse '10-19' → 14.5, '50-99' → 74.5, '1-9' → 5.0, etc."""
    import re
    band = str(band or "")
    m = re.match(r'(\d+)\s*[-–]\s*(\d+)', band)
    if m:
        return (int(m.group(1)) + int(m.group(2))) / 2.0
    m2 = re.match(r'(\d+)\+', band)
    if m2:
        return float(int(m2.group(1)) * 1.5)  # rough upper-bound proxy
    m3 = re.match(r'^(\d+)$', band.strip())
    if m3:
        return float(m3.group(1))
    return None


# ── 4. Bolt-on Potential (0–20) ───────────────────────────────────────────────

def score_bolt_on_potential(company: dict) -> dict:
    """
    Roll-up / bolt-on acquisition potential.

    Components:
      a. Number of appropriately sized regional competitors (8–10 pts)
      b. PE-backed competitor exit signals (2–4 pts) — creates motivated sellers
      c. Target revenue in platform-ready range (4–6 pts)
      d. No dominant player in local market (2 pts)
    """
    comp_analysis  = company.get("competitor_analysis") or {}
    local_count    = comp_analysis.get("competitor_count_local", 0)
    regional_count = comp_analysis.get("competitor_count_regional", 0)
    pe_comps       = comp_analysis.get("pe_backed_competitors") or []
    competitor_map = comp_analysis.get("competitor_map") or []

    rev = (company.get("rev_actual") or
           (company.get("revenue_estimate") or {}).get("revenue_mid") or 0)

    signals = []
    detail  = {}
    score   = 0

    # a. Bolt-on target availability (nearby small-medium competitors)
    high_fit = sum(1 for c in competitor_map if c.get("acquisition_fit") == "High")
    med_fit  = sum(1 for c in competitor_map if c.get("acquisition_fit") == "Medium")

    if high_fit >= 5:
        score += 10; signals.append(f"{high_fit} high-fit bolt-on targets identified locally — strong roll-up thesis")
    elif high_fit >= 3:
        score += 8;  signals.append(f"{high_fit} high-fit bolt-on targets in region")
    elif high_fit >= 1:
        score += 6;  signals.append(f"{high_fit} high-fit bolt-on target(s) identified")
    elif med_fit >= 3:
        score += 4;  signals.append(f"{med_fit} medium-fit bolt-on candidates")
    elif regional_count >= 3:
        score += 2

    # b. PE-backed competitors signal motivated sellers / exit cycle
    if len(pe_comps) >= 2:
        score += 4
        signals.append(f"{len(pe_comps)} PE-backed/group competitors — sector in active M&A cycle")
    elif len(pe_comps) == 1:
        score += 2
        signals.append(f"PE-backed competitor ({pe_comps[0]}) — sector M&A activity confirmed")

    # c. Target itself in platform range
    if PLATFORM_REV_MIN <= rev <= PLATFORM_REV_MAX:
        score += 6
        signals.append(f"Revenue £{rev/1e6:.1f}M — in PE platform sweet spot (£5–30M)")
    elif rev > PLATFORM_REV_MAX:
        score += 3  # large — potential acquirer itself
        signals.append(f"Revenue £{rev/1e6:.1f}M — above sweet spot; natural acquirer")
    elif BOLT_ON_REV_MIN <= rev < PLATFORM_REV_MIN:
        score += 4
        signals.append(f"Revenue £{rev/1e6:.1f}M — bolt-on size, attractive for existing platform")

    detail = {
        "high_fit_targets": high_fit,
        "medium_fit_targets": med_fit,
        "pe_backed_competitors": len(pe_comps),
        "regional_competitor_count": regional_count,
        "target_revenue": rev,
        "in_platform_range": PLATFORM_REV_MIN <= rev <= PLATFORM_REV_MAX,
    }

    return {"score": min(20, score), "signals": signals, "detail": detail,
            "data_tier": "Tier 1 CH + Tier 4 competitor analysis"}


# ── 5. Exit Attractiveness (0–20) ─────────────────────────────────────────────

def score_exit_attractiveness(company: dict) -> dict:
    """
    How attractive is this company to future buyers (strategic or PE exit).

    Components:
      a. Sector PE interest and multiple quality (fixed for this sector) (0–6)
      b. Company-specific margin quality (EBITDA margin proxy)          (0–6)
      c. Regulatory moat / certification (LOLER-registered, etc.)       (0–4)
      d. Customer concentration proxy (B2B + maintenance contracts)     (0–4)
    """
    rev      = (company.get("rev_actual") or
                (company.get("revenue_estimate") or {}).get("revenue_mid") or 0)
    pbt      = (company.get("profit_before_tax") or
                (company.get("financials") or {}).get("profit_before_tax"))
    net_assets = (company.get("net_assets") or
                  (company.get("financials") or {}).get("net_assets"))
    age      = company.get("company_age_years") or 0
    accreds  = company.get("accreditations") or {}
    has_iso  = accreds.get("has_iso_9001") or accreds.get("has_iso_45001")
    has_bsria= accreds.get("has_bsria") or False

    signals = []
    detail  = {}
    score   = 0

    # a. Sector PE multiple quality (fixed: lift maintenance scores 8.5x — attractive)
    if SECTOR["ebitda_multiple"] >= 9.0:
        score += 6; signals.append(f"Sector trades at {SECTOR['ebitda_multiple']:.1f}x EBITDA — premium exit multiple")
    elif SECTOR["ebitda_multiple"] >= 7.5:
        score += 5; signals.append(f"Sector trades at {SECTOR['ebitda_multiple']:.1f}x EBITDA — strong exit multiple")
    else:
        score += 3

    # b. EBITDA margin proxy
    if pbt and rev and rev > 0:
        margin = pbt / rev
        if margin >= 0.20:
            score += 6; signals.append(f"PBT margin {margin:.0%} — premium quality earnings")
        elif margin >= 0.14:
            score += 4; signals.append(f"PBT margin {margin:.0%} — above sector average")
        elif margin >= 0.08:
            score += 2
        else:
            score += 0; signals.append(f"Low PBT margin {margin:.0%} — margin improvement needed pre-exit")
    elif net_assets and rev and rev > 0:
        # Net assets as rough proxy for balance sheet quality
        na_ratio = net_assets / rev
        if na_ratio >= 0.30:
            score += 3; signals.append("Strong net asset base relative to revenue")
        elif na_ratio >= 0.15:
            score += 2

    # c. Regulatory/accreditation moat
    moat_score = 0
    if has_iso:
        moat_score += 2; signals.append("ISO-certified — quality system moat, premium buyer attraction")
    if has_bsria:
        moat_score += 2; signals.append("BSRIA/LEIA member — regulatory standing supports exit narrative")
    if age >= 20:
        moat_score += 1; signals.append(f"Age {age} yrs — established brand / long-term client relationships")
    score += min(4, moat_score)

    # d. Customer concentration / recurring revenue (B2B maintenance = clean exit story)
    # Using staff count as proxy — large stable staff = maintained client base
    if age >= 15 and rev >= 2_000_000:
        score += 4
        signals.append("Established maintenance revenue base — clean recurring revenue story for buyer")
    elif rev >= 1_000_000:
        score += 2

    detail = {
        "ebitda_multiple": SECTOR["ebitda_multiple"],
        "pbt_margin": round(pbt / rev, 3) if (pbt and rev and rev > 0) else None,
        "has_iso": bool(has_iso),
        "has_bsria": bool(has_bsria),
        "company_age": age,
        "revenue": rev,
    }

    return {"score": min(20, score), "signals": signals, "detail": detail,
            "data_tier": "Tier 1 CH + Tier 4 sector benchmarks"}


# ── Composite Score ───────────────────────────────────────────────────────────

def acquisition_attractiveness_score(company: dict) -> dict:
    """
    Compute composite Acquisition Attractiveness Score (0–100).

    Dimensions (each 0–20):
      1. Market Fragmentation    — roll-up thesis quality
      2. Recurring Revenue       — earnings quality / predictability
      3. Operational Improvement — PE value creation potential
      4. Bolt-on Potential       — platform / consolidation opportunity
      5. Exit Attractiveness     — buyer appetite, multiple quality

    Score bands:
      80+   Tier 1 — Prime platform target
      65–79 Tier 2 — Strong acquisition candidate
      50–64 Tier 3 — Quality bolt-on
      35–49 Tier 4 — Watch list
      < 35  Out of scope
    """
    d1 = score_fragmentation(company)
    d2 = score_recurring_revenue(company)
    d3 = score_operational_improvement(company)
    d4 = score_bolt_on_potential(company)
    d5 = score_exit_attractiveness(company)

    total = d1["score"] + d2["score"] + d3["score"] + d4["score"] + d5["score"]
    total = min(100, total)

    if   total >= 80: tier = "Tier 1 — Prime Platform Target"
    elif total >= 65: tier = "Tier 2 — Strong Acquisition Candidate"
    elif total >= 50: tier = "Tier 3 — Quality Bolt-on"
    elif total >= 35: tier = "Tier 4 — Watch List"
    else:             tier = "Out of Scope"

    all_signals = (d1["signals"] + d2["signals"] + d3["signals"] +
                   d4["signals"] + d5["signals"])

    return {
        "acquisition_score":      total,
        "acquisition_tier":       tier,
        "acquisition_signals":    all_signals,
        "dimensions": {
            "market_fragmentation":    d1,
            "recurring_revenue":       d2,
            "operational_improvement": d3,
            "bolt_on_potential":       d4,
            "exit_attractiveness":     d5,
        },
        "formula":    "Fragmentation(20) + Recurring(20) + OpsImprovement(20) + BoltOn(20) + Exit(20)",
        "data_tier":  "Tier 1 CH + Tier 4 derived",
    }


def acquisition_tier_label(score: int) -> str:
    if score >= 80: return "Tier 1"
    if score >= 65: return "Tier 2"
    if score >= 50: return "Tier 3"
    if score >= 35: return "Tier 4"
    return "OOS"


# ── Batch run ─────────────────────────────────────────────────────────────────

def run(companies: Optional[list[dict]] = None, enriched_path: Optional[str] = None) -> list[dict]:
    """
    Add acquisition_attractiveness to each company.
    Run AFTER competitor_map.run() and sell_signals.run() for full scoring.
    """
    if companies is None:
        if enriched_path is None:
            enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
        with open(enriched_path) as f:
            companies = json.load(f)

    print(f"\nAcquisition attractiveness scoring for {len(companies)} companies...")

    tier_counts = {"Tier 1": 0, "Tier 2": 0, "Tier 3": 0, "Tier 4": 0, "OOS": 0}

    for i, company in enumerate(companies):
        if i % 100 == 0:
            print(f"  [{i+1}/{len(companies)}] scoring...")
        result = acquisition_attractiveness_score(company)
        company["acquisition_attractiveness"] = result
        tier_label = acquisition_tier_label(result["acquisition_score"])
        tier_counts[tier_label] = tier_counts.get(tier_label, 0) + 1

    # Write back
    if enriched_path:
        with open(enriched_path, "w") as f:
            json.dump(companies, f, indent=2)
        print(f"  Acquisition scores written → {enriched_path}")

    print(f"\n  Tier 1 (Prime, 80+):    {tier_counts.get('Tier 1', 0)}")
    print(f"  Tier 2 (Strong, 65–79): {tier_counts.get('Tier 2', 0)}")
    print(f"  Tier 3 (Bolt-on, 50–64):{tier_counts.get('Tier 3', 0)}")
    print(f"  Tier 4 (Watch, 35–49):  {tier_counts.get('Tier 4', 0)}")
    print(f"  OOS (<35):              {tier_counts.get('OOS', 0)}")

    return companies


if __name__ == "__main__":
    run()
