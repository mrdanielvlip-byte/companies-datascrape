"""
ch_financials.py — Financial estimation engine

Pulls accounts metadata and balance sheet data from Companies House,
then applies revenue estimation models:
  1. Employee model   — Employees × Revenue per Employee
  2. Asset model      — Total Assets × Sector Asset Turnover
  3. Location model   — Locations × Revenue per Site
  4. PE Triangulation — Sector-aware multi-model via revenue_estimate.py
     (Staff Cost Model, Net Asset Scaling, Activity cross-check)

Produces low / base / high revenue estimates, EBITDA estimates,
key balance sheet ratios, and a data reliability tier for every figure.
"""

import requests
import json
import time
import os
import re
from datetime import datetime

from revenue_estimate import estimate_revenue

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
    for _ in range(retries):
        try:
            r = requests.get(f"{BASE}{path}", auth=AUTH, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(2)
        except requests.RequestException:
            time.sleep(1)
    return {}


# ── Data reliability tiers ────────────────────────────────────────────────────
# Tier 1 — Official regulatory/registry data (Companies House filings)
# Tier 2 — Structured industry datasets (UKAS, Contracts Finder)
# Tier 3 — Verified corporate website data
# Tier 4 — Derived estimates (models)

TIER_1 = "Tier 1 — Companies House filing"
TIER_3 = "Tier 3 — Website / directory"
TIER_4 = "Tier 4 — Derived estimate"


# ── Accounts data ─────────────────────────────────────────────────────────────

def get_filing_history(company_number: str) -> list[dict]:
    data = get(f"/company/{company_number}/filing-history?category=accounts&items_per_page=5")
    return data.get("items", [])


def get_accounts_history(company_number: str, years: int = 3) -> list[dict]:
    """
    Fetch the last N years of accounts filings and extract what data is available.

    For each filing returns:
      period_end       — accounts period end date (YYYY-MM-DD)
      accounts_type    — e.g. total-exemption-full, full, micro-entity
      net_assets       — from XBRL where available (null for most SMEs)
      total_assets     — from XBRL where available
      total_employees  — from XBRL where available (rare for UK SMEs)
      staff_costs      — from XBRL where available

    Most UK SMEs file Total Exemption accounts so structured financial data
    is unavailable — only the period end and accounts type will be populated.
    """
    filings = get(f"/company/{company_number}/filing-history?category=accounts&items_per_page={years + 2}")
    items = filings.get("items", [])
    history = []

    for filing in items[:years]:
        acc_type_raw = filing.get("description", "")
        # Normalise CH description string e.g. "accounts-with-accounts-type-total-exemption-full"
        acc_type = acc_type_raw.replace("accounts-with-accounts-type-", "").replace("-", " ").title()
        period_end = filing.get("action_date", "")

        entry = {
            "period_end":      period_end,
            "accounts_type":   acc_type,
            "net_assets":      None,
            "total_assets":    None,
            "total_employees": None,
            "staff_costs":     None,
        }

        # Try to get XBRL structured data from document metadata link
        # (Available for larger / full accounts; not for Total Exemption)
        doc_url = filing.get("links", {}).get("document_metadata", "")
        if doc_url:
            try:
                doc_meta = get(doc_url.replace("https://api.company-information.service.gov.uk", ""))
                xbrl = doc_meta.get("xbrl_data", {}) or {}
                if xbrl:
                    entry["net_assets"]      = xbrl.get("net_assets") or xbrl.get("NetAssets")
                    entry["total_assets"]    = xbrl.get("total_assets") or xbrl.get("TotalAssets")
                    entry["total_employees"] = xbrl.get("employees") or xbrl.get("NumberEmployees")
                    entry["staff_costs"]     = xbrl.get("staff_costs") or xbrl.get("StaffCosts")
            except Exception:
                pass

        history.append(entry)

    return history


def estimate_employees(company: dict) -> tuple[int | None, str]:
    """
    Best-effort employee count estimate.
    Returns (count, source) where source describes the data tier.

    Priority:
      1. bs.total_employees  — from filed accounts (Tier 1)
      2. staff_costs / avg sector salary  — derived (Tier 4)
      3. rev_base / sector revenue-per-head  — derived (Tier 4)
    """
    bs = company.get("bs", {})
    emp = bs.get("total_employees")
    if emp and emp > 0:
        return int(emp), "Tier 1 — filed accounts"

    # Staff costs proxy: assume ~£35,000 avg fully-loaded salary for service sector SME
    sc = bs.get("staff_costs")
    if sc and sc > 0:
        est = max(1, round(sc / 35_000))
        return est, "Tier 4 — staff costs ÷ £35k"

    # Revenue-per-head proxy: assume ~£80,000 revenue per employee for service sector
    rev = company.get("rev_base")
    if rev and rev > 0:
        est = max(1, round(rev / 80_000))
        return est, "Tier 4 — revenue ÷ £80k"

    return None, ""


def get_accounts_document_metadata(company_number: str) -> dict:
    """
    Pull the most recent accounts filing metadata.
    Returns period end date, accounts type, and any balance sheet values
    that appear in the structured XBRL data (where available).
    """
    filings = get_filing_history(company_number)
    if not filings:
        return {}

    latest = filings[0]
    meta = {
        "accounts_type":    latest.get("description", ""),
        "period_end":       latest.get("action_date", ""),
        "filing_date":      latest.get("date", ""),
        "data_source":      TIER_1,
        "accounts_link":    f"https://find-and-update.company-information.service.gov.uk/company/{company_number}/filing-history",
    }

    # Attempt to get document data (structured XBRL where available)
    doc_links = latest.get("links", {})
    doc_url = doc_links.get("document_metadata", "")
    if doc_url:
        doc_meta = get(doc_url.replace("https://api.company-information.service.gov.uk", ""))
        meta["document_metadata"] = doc_meta

    return meta


def get_balance_sheet(company_number: str) -> dict:
    """
    Extract balance sheet values from Companies House structured data.
    For most UK SMEs (Total Exemption accounts) only the balance sheet
    is publicly available — turnover is not disclosed.
    Returns values with data reliability tier tags.
    """
    # Try the company profile for any embedded financial data
    profile = get(f"/company/{company_number}")
    accounts_meta = profile.get("accounts", {})

    result = {
        "total_assets":       None,
        "net_assets":         None,
        "total_liabilities":  None,
        "cash":               None,
        "current_assets":     None,
        "current_liabilities":None,
        "fixed_assets":       None,
        "total_employees":    None,
        "accounts_type":      accounts_meta.get("last_accounts", {}).get("type", "unknown"),
        "period_end":         accounts_meta.get("last_accounts", {}).get("period_start_on", ""),
        "data_tier":          TIER_1,
        "notes":              [],
    }

    # Flag accounts type — tells us how much data is available
    acct_type = result["accounts_type"].lower()
    if "total-exemption" in acct_type or "micro" in acct_type:
        result["notes"].append("Total Exemption accounts — turnover not publicly disclosed")
    elif "full" in acct_type or "group" in acct_type:
        result["notes"].append("Full accounts filed — structured data may be available")

    return result


# ── Financial estimation models ───────────────────────────────────────────────

def employee_model(employees: int) -> dict | None:
    """
    Revenue = Employees × Sector Revenue per Employee
    Data tier: Tier 4 (derived estimate)
    """
    if not employees or employees <= 0:
        return None
    return {
        "low":        employees * cfg.REVENUE_PER_HEAD_LOW,
        "base":       employees * cfg.REVENUE_PER_HEAD_MID,
        "high":       employees * cfg.REVENUE_PER_HEAD_HIGH,
        "method":     "Employee model",
        "formula":    f"Employees ({employees}) × Revenue/Head (£{cfg.REVENUE_PER_HEAD_LOW:,}–£{cfg.REVENUE_PER_HEAD_HIGH:,})",
        "data_tier":  TIER_4,
        "confidence": "Medium",
    }


def asset_model(total_assets: float) -> dict | None:
    """
    Revenue = Total Assets × Sector Asset Turnover Ratio
    Data tier: Tier 1 (balance sheet from Companies House) → Tier 4 estimate
    """
    if not total_assets or total_assets <= 0:
        return None
    ratio = cfg.ASSET_TURNOVER_RATIO
    return {
        "low":        total_assets * (ratio * 0.7),
        "base":       total_assets * ratio,
        "high":       total_assets * (ratio * 1.4),
        "method":     "Asset model",
        "formula":    f"Total Assets (£{total_assets:,.0f}) × Asset Turnover ({ratio}×)",
        "data_tier":  f"Tier 1 (balance sheet) → {TIER_4}",
        "confidence": "Medium–High" if total_assets > 100_000 else "Low",
    }


def location_model(locations: int) -> dict | None:
    """
    Revenue = Locations × Revenue per Site Benchmark
    Data tier: Tier 3/4 (estimated site count)
    """
    if not locations or locations <= 0:
        return None
    rev_per_site = cfg.SECTOR_BENCHMARKS.get("revenue_per_site", 0)
    if not rev_per_site:
        return None
    return {
        "low":        locations * rev_per_site * 0.7,
        "base":       locations * rev_per_site,
        "high":       locations * rev_per_site * 1.4,
        "method":     "Location model",
        "formula":    f"Sites ({locations}) × Revenue/Site (£{rev_per_site:,})",
        "data_tier":  TIER_4,
        "confidence": "Low",
    }


def blend_estimates(models: list[dict]) -> dict:
    """
    Blend available models using configured weights.
    More models → higher blended confidence.
    """
    valid = [m for m in models if m is not None]
    if not valid:
        return {
            "revenue_low": None, "revenue_base": None, "revenue_high": None,
            "confidence": "None", "models_used": [],
        }

    # Simple average across valid models
    low  = sum(m["low"]  for m in valid) / len(valid)
    base = sum(m["base"] for m in valid) / len(valid)
    high = sum(m["high"] for m in valid) / len(valid)

    confidence_map = {1: "Low", 2: "Medium", 3: "High"}
    confidence = confidence_map.get(len(valid), "High")

    return {
        "revenue_low":   round(low),
        "revenue_base":  round(base),
        "revenue_high":  round(high),
        "confidence":    confidence,
        "models_used":   [m["method"] for m in valid],
        "data_tier":     TIER_4,
        "formula":       f"Average of: {', '.join(m['method'] for m in valid)}",
    }


def ebitda_estimate(revenue_base: float | None) -> dict:
    """
    EBITDA = Revenue × Sector EBITDA Margin
    Uses sector benchmark table from config.
    """
    if not revenue_base:
        return {}
    benchmarks = cfg.SECTOR_BENCHMARKS
    return {
        "ebitda_low":   round(revenue_base * benchmarks["ebitda_margin_low"]),
        "ebitda_base":  round(revenue_base * benchmarks["ebitda_margin_base"]),
        "ebitda_high":  round(revenue_base * benchmarks["ebitda_margin_high"]),
        "margin_used":  f"{benchmarks['ebitda_margin_base']*100:.0f}% (sector benchmark)",
        "formula":      f"Revenue × EBITDA Margin ({benchmarks['ebitda_margin_low']*100:.0f}%–{benchmarks['ebitda_margin_high']*100:.0f}%)",
        "data_tier":    TIER_4,
    }


def balance_sheet_ratios(bs: dict) -> dict:
    """
    Compute standard balance sheet ratios where data is available.
    Formula:
        Net Assets = Total Assets − Total Liabilities
        Cash Ratio = Cash / Current Liabilities
        Asset Turnover = Revenue / Total Assets
    """
    ratios = {}
    ta  = bs.get("total_assets")
    tl  = bs.get("total_liabilities")
    ca  = bs.get("current_assets")
    cl  = bs.get("current_liabilities")
    cash= bs.get("cash")

    if ta and tl:
        ratios["net_assets"]    = ta - tl
        ratios["net_assets_formula"] = f"Total Assets (£{ta:,.0f}) − Total Liabilities (£{tl:,.0f})"

    if cash and cl and cl > 0:
        ratios["cash_ratio"]    = round(cash / cl, 2)
        ratios["cash_ratio_formula"] = f"Cash (£{cash:,.0f}) / Current Liabilities (£{cl:,.0f})"

    return ratios


# ── Registered charges (debt signals) ────────────────────────────────────────

def get_charges(company_number: str) -> dict:
    """
    Pull registered charges from Companies House.
    Outstanding charges indicate secured debt — a key dealability signal.
    Data tier: Tier 1
    """
    data = get(f"/company/{company_number}/charges?items_per_page=25")
    items = data.get("items", [])

    outstanding = [c for c in items if c.get("status") == "outstanding"]
    satisfied   = [c for c in items if c.get("status") == "satisfied"]

    charge_details = []
    for c in outstanding[:5]:
        charge_details.append({
            "created":     c.get("created_on", ""),
            "type":        c.get("classification", {}).get("description", ""),
            "persons":     [p.get("name", "") for p in c.get("persons_entitled", [])],
            "status":      c.get("status", ""),
        })

    return {
        "total_charges":       len(items),
        "outstanding_charges": len(outstanding),
        "satisfied_charges":   len(satisfied),
        "charge_details":      charge_details,
        "has_debt":            len(outstanding) > 0,
        "data_tier":           TIER_1,
        "source":              f"Companies House charges register",
    }


# ── Dealability signals ───────────────────────────────────────────────────────

def get_dealability_signals(company_number: str, directors: list[dict]) -> dict:
    """
    Identify signals that suggest the company may be ready for a transaction.

    Signals checked:
    • New HoldCo formation (recent group restructuring)
    • PSC changes (share transfers or new investors)
    • Governance hires (CFO, MD, independent directors)
    • Recent filing activity (accounts, confirmation statements)
    • Debt Growth = (Current Debt − Previous Debt) / Previous Debt
    Data tier: Tier 1 (Companies House filing history)
    """
    filing_history = get(f"/company/{company_number}/filing-history?items_per_page=25")
    filings = filing_history.get("items", [])

    signals = []
    signal_score = 0  # 0–20

    # Check for recent HoldCo / group restructuring
    restructure_keywords = ["holding", "group", "parent", "reorganis", "transfer of shares"]
    for f in filings:
        desc = (f.get("description", "") + " " + f.get("description_values", {}).get("description", "")).lower()
        if any(kw in desc for kw in restructure_keywords):
            signals.append({
                "type":   "Corporate restructuring",
                "detail": f.get("description", ""),
                "date":   f.get("date", ""),
                "tier":   TIER_1,
            })
            signal_score += 5
            break

    # Check for governance hires (CFO, MD, COO in officer occupations)
    governance_roles = ["chief financial", "cfo", "managing director", "chief operating",
                        "finance director", "independent director", "non-executive"]
    for d in directors:
        occ = (d.get("occupation") or "").lower()
        if any(role in occ for role in governance_roles):
            signals.append({
                "type":   "Governance hire",
                "detail": f"{d['name']} — {d.get('occupation','')}",
                "date":   d.get("appointed", ""),
                "tier":   TIER_1,
            })
            signal_score += 5

    # PSC changes in filing history
    psc_filings = [f for f in filings
                   if "persons-with-significant-control" in f.get("links", {}).get("self", "")
                   or "psc" in f.get("description", "").lower()]
    if psc_filings:
        signals.append({
            "type":   "PSC / ownership change",
            "detail": f"{len(psc_filings)} PSC filing(s) on record",
            "date":   psc_filings[0].get("date", ""),
            "tier":   TIER_1,
        })
        signal_score += 4

    # Filing regularity (active company = good sign)
    if len(filings) >= 3:
        signal_score += 3

    # Recent confirmation statement (up-to-date register)
    conf_filings = [f for f in filings if "confirmation-statement" in f.get("type", "").lower()]
    if conf_filings:
        signal_score += 3

    return {
        "signals":       signals,
        "signal_count":  len(signals),
        "signal_score":  min(signal_score, 20),
        "data_tier":     TIER_1,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def enrich_financials(company: dict) -> dict:
    """
    Full financial enrichment for a single company.
    Returns a financials dict to be merged into the company record.
    """
    num       = company["company_number"]
    employees = company.get("total_employees")
    bs        = get_balance_sheet(num)
    charges   = get_charges(num)
    accounts_history = get_accounts_history(num, years=3)
    time.sleep(0.1)

    # ── Legacy single-sector models (kept for backward compat) ───────────────
    emp_est  = employee_model(employees)
    asset_est= asset_model(bs.get("total_assets"))
    loc_est  = location_model(company.get("location_count"))
    blended  = blend_estimates([m for m in [emp_est, asset_est, loc_est] if m])
    ebitda   = ebitda_estimate(blended.get("revenue_base"))
    ratios   = balance_sheet_ratios(bs)

    # ── PE triangulation model (sector-aware, multi-signal) ──────────────────
    pe_input = {
        "company_name":    company.get("company_name", ""),
        "sic1":            company.get("sic1") or (company.get("sic_codes") or [None])[0],
        "employees":       employees,
        "total_assets":    bs.get("total_assets"),
        "net_assets":      (bs.get("total_assets") or 0) - (bs.get("total_liabilities") or 0)
                           if bs.get("total_assets") else None,
        "staff_costs":     bs.get("staff_costs"),
        "director_salary": bs.get("director_emoluments"),   # total director remuneration
        "num_sites":       company.get("location_count") or 1,
    }
    pe_est = estimate_revenue(pe_input)
    pe_dict = pe_est.to_dict()

    # Use PE triangulation as the primary revenue_estimate if it has at least
    # 2 models available (more reliable than the legacy single-sector blended),
    # otherwise fall back to the legacy blended result.
    available_models = len(pe_dict.get("models_used", []))
    if available_models >= 2:
        revenue_estimate = {
            "revenue_low":    pe_dict["revenue_low"],
            "revenue_base":   pe_dict["revenue_base"],
            "revenue_high":   pe_dict["revenue_high"],
            "confidence":     pe_dict["confidence_label"],
            "models_used":    pe_dict["models_used"],
            "formula":        f"PE Triangulation ({', '.join(pe_dict['models_used'])})",
            "sector":         pe_dict["sector"],
            "warnings":       pe_dict.get("warnings", []),
        }
        ebitda_out = {
            "ebitda_low":     pe_dict["ebitda_low"],
            "ebitda_base":    pe_dict["ebitda_base"],
            "ebitda_high":    pe_dict["ebitda_high"],
            "formula":        f"Sector EBITDA margin applied to triangulated base",
        }
    else:
        revenue_estimate = blended
        ebitda_out       = ebitda

    return {
        "balance_sheet":         bs,
        "charges":               charges,
        "employee_model":        emp_est,
        "asset_model":           asset_est,
        "location_model":        loc_est,
        "revenue_estimate":      revenue_estimate,
        "ebitda_estimate":       ebitda_out,
        "balance_sheet_ratios":  ratios,
        "pe_triangulation":      pe_dict,   # full detail always stored
        "accounts_history":      accounts_history,   # last 3 years of filed accounts
    }


def run():
    global AUTH
    AUTH = (load_api_key(), "")

    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(enriched_path) as f:
        companies = json.load(f)

    print(f"\nFinancial enrichment for {len(companies)} companies...")

    for i, c in enumerate(companies):
        if i % 25 == 0:
            print(f"  [{i+1}/{len(companies)}] processing...")
        fin = enrich_financials(c)
        c["financials"] = fin
        # Surface key fields at top level for easy access in build_excel + downstream
        c["accounts_history"] = fin.get("accounts_history", [])
        emp_count, emp_source = estimate_employees(c)
        if emp_count is not None:
            c["estimated_employees"]        = emp_count
            c["estimated_employees_source"] = emp_source

        # ── Employee delta (3-year change) ────────────────────────────────────
        # Uses XBRL employee data from accounts history where available.
        # Format: "+5", "-3", or None if insufficient data.
        hist = c.get("accounts_history", [])
        emp_vals = [h.get("total_employees") for h in hist
                    if h.get("total_employees") is not None and h["total_employees"] > 0]
        if len(emp_vals) >= 2:
            # hist[0] = most recent, hist[-1] = oldest with data
            newest = emp_vals[0]
            oldest = emp_vals[-1]
            delta  = newest - oldest
            c["employee_delta"]       = delta
            c["employee_delta_label"] = f"+{delta}" if delta > 0 else str(delta)
            c["employee_latest"]      = newest
            c["employee_oldest"]      = oldest
            c["employee_delta_years"] = len(emp_vals)
        else:
            c["employee_delta"]       = None
            c["employee_delta_label"] = None

        time.sleep(0.1)

    fin_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(fin_path, "w") as f:
        json.dump(companies, f, indent=2)

    print(f"Done. Financials saved → {fin_path}")
    return companies


if __name__ == "__main__":
    run()
