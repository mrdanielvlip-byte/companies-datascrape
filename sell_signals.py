"""
sell_signals.py — Sell Intent Signal Engine

Identifies behavioural and structural signals that suggest a founder/owner
is approaching exit readiness. Produces a composite Sell Intent Score (0–100).

Signal dimensions:
  A. Age & Tenure         (0–40 pts) — Founder age, director tenure
  B. Business Structure   (0–25 pts) — Solo management, no governance hires
  C. Operational Stress   (0–20 pts) — Late filings, director departures
  D. Company Maturity     (0–15 pts) — Years in operation

Scoring bands:
  70–100  Strong sell signals — priority outreach
  50–69   Moderate signals — include in contact list
  30–49   Weak signals — monitor
  < 30    Low signal — background intelligence only

Data tiers:
  Tier 1 — Companies House filing history, officers register
  Tier 4 — Derived / inferred signals
"""

import requests
import json
import time
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import config as cfg

BASE  = "https://api.company-information.service.gov.uk"
AUTH  = None
TODAY = datetime.now()


def load_api_key() -> str:
    key_file = os.path.join(os.path.dirname(__file__), ".ch_api_key")
    if os.path.exists(key_file):
        with open(key_file) as f:
            for line in f:
                if "=" in line:
                    return line.strip().split("=", 1)[1].strip()
    return os.environ.get("COMPANIES_HOUSE_API_KEY", "")


def _get(path: str, retries: int = 3) -> dict:
    for _ in range(retries):
        try:
            r = requests.get(f"{BASE}{path}", auth=AUTH, timeout=12)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(2)
        except requests.RequestException:
            time.sleep(1)
    return {}


def _parse_date(date_str: str) -> datetime | None:
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(date_str[:len(fmt)+2], fmt)
        except (ValueError, IndexError):
            continue
    return None


# ── A. Age & Tenure Signals ───────────────────────────────────────────────────

def age_tenure_score(directors: list[dict]) -> dict:
    """
    Score founder age and director tenure.

    Max age score (0–25): older founders = stronger exit signal
    Tenure score   (0–15): very long tenure = succession risk + founder identity

    Returns component scores and signals list.
    """
    if not directors:
        return {"score": 0, "signals": [], "details": {}}

    ages          = [d["age"] for d in directors if d.get("age")]
    tenures       = [d.get("years_active", 0) for d in directors]
    max_age       = max(ages, default=0)
    max_tenure    = max(tenures, default=0)

    signals = []

    # Age score (0–25)
    if   max_age >= 70: age_sc = 25; signals.append(f"Oldest director {max_age} yrs — near/past typical retirement age")
    elif max_age >= 65: age_sc = 20; signals.append(f"Oldest director {max_age} yrs — retirement horizon")
    elif max_age >= 60: age_sc = 15; signals.append(f"Oldest director {max_age} yrs — approaching exit window")
    elif max_age >= 55: age_sc = 10; signals.append(f"Oldest director {max_age} yrs — medium-term exit plausible")
    elif max_age >= 50: age_sc = 5
    else:               age_sc = 0

    # Tenure score (0–15): long-serving single directors highly likely to be founders
    if   max_tenure >= 25: ten_sc = 15; signals.append(f"Director tenure {max_tenure:.0f} yrs — likely founder identity stake")
    elif max_tenure >= 20: ten_sc = 12; signals.append(f"Director tenure {max_tenure:.0f} yrs — strong owner-manager profile")
    elif max_tenure >= 15: ten_sc = 8;  signals.append(f"Director tenure {max_tenure:.0f} yrs — long-term owner")
    elif max_tenure >= 10: ten_sc = 4
    else:                  ten_sc = 0

    return {
        "score":      age_sc + ten_sc,
        "age_score":  age_sc,
        "tenure_score": ten_sc,
        "max_age":    max_age,
        "max_tenure": round(max_tenure, 1),
        "signals":    signals,
        "data_tier":  "Tier 1 — Companies House officers register",
    }


# ── B. Business Structure Signals ────────────────────────────────────────────

GOVERNANCE_KEYWORDS = {
    "chief financial", "cfo", "finance director", "fd",
    "chief operating", "coo", "managing director", "md",
    "independent director", "non-executive", "ned",
    "group finance", "commercial director",
}

def structure_score(directors: list[dict]) -> dict:
    """
    Score business structure signals.

    Solo/duo management with no governance layer = high succession risk.
    Absence of professionalised management = signal seller needs help.

    Max: 25 pts
    """
    n    = len(directors)
    occs = [(d.get("occupation") or "").lower() for d in directors]

    has_governance = any(
        any(kw in occ for kw in GOVERNANCE_KEYWORDS)
        for occ in occs
    )

    signals = []

    # Director count (0–15)
    if   n == 0: dir_sc = 15; signals.append("No active directors registered")
    elif n == 1: dir_sc = 15; signals.append("Single-director company — extreme key-person concentration")
    elif n == 2: dir_sc = 10; signals.append("Two-director company — owner-managed structure")
    elif n == 3: dir_sc = 5;  signals.append("Three directors — likely owner-managed")
    else:        dir_sc = 0

    # Governance absence (0–10)
    if not has_governance:
        gov_sc = 10
        signals.append("No FD/CFO/MD/NED governance roles — unprofessionalised management")
    else:
        gov_sc = 0

    return {
        "score":           dir_sc + gov_sc,
        "dir_count_score": dir_sc,
        "governance_score": gov_sc,
        "director_count":  n,
        "has_governance":  has_governance,
        "signals":         signals,
        "data_tier":       "Tier 1 — Companies House officers register",
    }


# ── C. Operational Stress Signals ─────────────────────────────────────────────

def get_all_officers(company_number: str) -> dict:
    """
    Pull all officers (active + resigned) with appointment/resignation dates.
    Used for director churn analysis.
    """
    data = _get(f"/company/{company_number}/officers?items_per_page=100&register_type=directors")
    return data


def _late_filing_penalty(filing_date: str, action_date: str, accounts_type: str) -> int:
    """
    Return days late for a filing, given the statutory deadline.

    UK rules (private companies):
        Confirmation statement: 14 days after anniversary of incorporation
        Accounts:
          - From incorporation: 21 months
          - Subsequent years: 9 months after accounting reference date
    """
    fd = _parse_date(filing_date)
    ad = _parse_date(action_date)
    if not fd or not ad:
        return 0

    # 9-month deadline from period end for private company accounts
    deadline = ad + relativedelta(months=9)
    if fd > deadline:
        return (fd - deadline).days
    return 0


def operational_stress_score(company_number: str, company: dict) -> dict:
    """
    Score operational stress signals from filing history and officer churn.

    Late filings: up to +5 pts each, max +10
    Director departures (last 3 yrs): +5 per departure, max +10
    Long gap since any new appointment: +5

    Max: 20 pts
    """
    signals = []
    score   = 0

    # ── 1. Late filings ──
    fh = _get(f"/company/{company_number}/filing-history?category=accounts&items_per_page=10")
    late_count = 0
    for f in fh.get("items", []):
        filing_date = f.get("date", "")
        action_date = f.get("action_date", "")
        days_late   = _late_filing_penalty(filing_date, action_date, f.get("description",""))
        if days_late > 30:  # grace period
            late_count += 1

    if late_count >= 2:
        score += 10
        signals.append(f"Accounts filed late on {late_count} occasions — disengagement signal")
    elif late_count == 1:
        score += 5
        signals.append("Accounts filed late once — possible compliance fatigue")

    # ── 2. Director churn ──
    officers_data = get_all_officers(company_number)
    cutoff        = TODAY - timedelta(days=3*365)
    resignations  = []

    for o in officers_data.get("items", []):
        resigned_on = o.get("resigned_on", "")
        if resigned_on:
            rd = _parse_date(resigned_on)
            if rd and rd >= cutoff:
                role = o.get("officer_role", "")
                if "director" in role.lower():
                    resignations.append({
                        "name":        o.get("name", ""),
                        "resigned_on": resigned_on,
                        "role":        role,
                    })

    churn_pts = min(len(resignations) * 5, 10)
    score    += churn_pts
    if resignations:
        signals.append(f"{len(resignations)} director resignation(s) in last 3 years — board instability / transition")

    # ── 3. No recent new appointment ──
    all_appt_dates = []
    for o in officers_data.get("items", []):
        appt = o.get("appointed_on", "")
        if appt:
            ad = _parse_date(appt)
            if ad:
                all_appt_dates.append(ad)

    if all_appt_dates:
        most_recent_appt = max(all_appt_dates)
        years_since_appt = (TODAY - most_recent_appt).days / 365.25
        if years_since_appt >= 5:
            score += 5
            signals.append(f"No new director appointed in {years_since_appt:.0f} yrs — stagnant board")
    else:
        score += 5
        signals.append("No appointment date data — board history unclear")

    return {
        "score":          min(score, 20),
        "late_filings":   late_count,
        "resignations_3yr": len(resignations),
        "resignation_list": resignations[:5],
        "signals":        signals,
        "data_tier":      "Tier 1 — Companies House filing history + officers register",
    }


# ── D. Company Maturity Signals ───────────────────────────────────────────────

def maturity_score(company_age: int) -> dict:
    """
    Older, established companies are more likely exit-ready.

    Max: 15 pts
    """
    signals = []

    if   company_age >= 25: sc = 15; signals.append(f"Company age {company_age} yrs — mature business, founder likely considering succession")
    elif company_age >= 20: sc = 12; signals.append(f"Company age {company_age} yrs — well-established")
    elif company_age >= 15: sc = 8;  signals.append(f"Company age {company_age} yrs — established business")
    elif company_age >= 10: sc = 4
    else:                   sc = 0

    return {
        "score":       sc,
        "company_age": company_age,
        "signals":     signals,
        "data_tier":   "Tier 1 — Companies House incorporation date",
    }


# ── Composite Sell Intent Score ───────────────────────────────────────────────

def sell_intent_score(company: dict, company_number: str) -> dict:
    """
    Composite Sell Intent Score (0–100).

    Dimensions:
      A. Age & Tenure       max 40
      B. Business Structure max 25
      C. Operational Stress max 20
      D. Company Maturity   max 15
                            -----
                            100

    Bands:
      70+   Strong exit signals — prioritise outreach
      50–69 Moderate signals — include in pipeline
      30–49 Weak signals — monitor
      <30   Low signal — background only
    """
    directors   = company.get("directors", [])
    company_age = company.get("company_age_years", 0)

    a = age_tenure_score(directors)
    b = structure_score(directors)
    c = operational_stress_score(company_number, company)
    d = maturity_score(company_age)

    total = a["score"] + b["score"] + c["score"] + d["score"]
    total = min(total, 100)

    # Aggregate all signals
    all_signals = a["signals"] + b["signals"] + c["signals"] + d["signals"]

    if   total >= 70: band = "Strong"
    elif total >= 50: band = "Moderate"
    elif total >= 30: band = "Weak"
    else:             band = "Low"

    return {
        "sell_intent_score": total,
        "sell_intent_band":  band,
        "sell_signals":      all_signals,
        "signal_count":      len(all_signals),
        "components": {
            "age_tenure":        a,
            "business_structure": b,
            "operational_stress": c,
            "company_maturity":   d,
        },
        "formula":    "Age/Tenure(40) + Structure(25) + Stress(20) + Maturity(15)",
        "data_tier":  "Tier 1 — Companies House",
    }


def sell_intent_grade(score: int) -> str:
    if score >= 70: return "Strong"
    if score >= 50: return "Moderate"
    if score >= 30: return "Weak"
    return "Low"


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    global AUTH
    AUTH = (load_api_key(), "")

    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(enriched_path) as f:
        companies = json.load(f)

    print(f"\nSell signal analysis for {len(companies)} companies...")

    strong = moderate = weak = low = 0

    for i, c in enumerate(companies):
        if i % 20 == 0:
            print(f"  [{i+1}/{len(companies)}] processing...")
        num = c["company_number"]
        result = sell_intent_score(c, num)
        c["sell_intent"] = result
        time.sleep(0.1)

        band = result["sell_intent_band"]
        if band == "Strong":   strong   += 1
        elif band == "Moderate": moderate += 1
        elif band == "Weak":   weak     += 1
        else:                  low      += 1

    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(out_path, "w") as f:
        json.dump(companies, f, indent=2)

    print(f"\nSell intent analysis complete → {out_path}")
    print(f"  Strong (70+): {strong}  |  Moderate (50–69): {moderate}  |  Weak (30–49): {weak}  |  Low (<30): {low}")
    return companies


if __name__ == "__main__":
    run()
