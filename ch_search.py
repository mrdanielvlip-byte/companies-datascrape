"""
ch_search.py — Companies House API sweep: SIC codes + name search
Produces raw_companies.json and filtered_companies.json in output/
"""

import requests
import json
import time
import os

import config as cfg


# ── Auth ──────────────────────────────────────────────────────────────────────

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
    raise RuntimeError(
        "No Companies House API key found. "
        "Add it to .ch_api_key (COMPANIES_HOUSE_API_KEY=...) or set the env var."
    )

BASE = "https://api.company-information.service.gov.uk"
AUTH = None  # set in main()


# ── Helpers ───────────────────────────────────────────────────────────────────

def get(path, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(f"{BASE}{path}", auth=AUTH, timeout=15)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(3)
        except requests.RequestException:
            time.sleep(1)
    return {}


def is_genuine(name: str) -> bool:
    n = name.lower()
    if any(ex in n for ex in cfg.EXCLUDE_TERMS):
        return False
    if any(ex in n for ex in cfg.EXCLUDE_SUBSECTORS):
        return False
    return any(kw in n for kw in cfg.INCLUDE_STEMS)


# ── SIC sweep ─────────────────────────────────────────────────────────────────

def fetch_sic(sic: str, max_results=5000) -> list[dict]:
    """Pull all active companies registered under a given SIC code."""
    companies, start = [], 0
    per_page = 100
    while start < max_results:
        data = get(
            f"/advanced-search/companies"
            f"?sic_codes={sic}&items_per_page={per_page}"
            f"&start_index={start}&company_status=active"
        )
        items = data.get("items", [])
        if not items:
            break
        companies.extend(items)
        if start + per_page >= data.get("hits", 0):
            break
        start += per_page
        time.sleep(0.1)
    return companies


def sweep_sic_codes() -> dict:
    all_companies = {}
    for sic in cfg.SIC_CODES:
        print(f"  SIC {sic}...", end=" ", flush=True)
        results = fetch_sic(sic)
        matched = 0
        for c in results:
            num  = c.get("company_number", "")
            name = c.get("company_name", "")
            if is_genuine(name) and num not in all_companies:
                all_companies[num] = _normalise(c, source=f"sic_{sic}")
                matched += 1
        print(f"{len(results):,} fetched → {matched} new calibration matches")
    return all_companies


# ── Name search ───────────────────────────────────────────────────────────────

def name_search(query: str, max_results=500) -> list[dict]:
    companies, start = [], 0
    per_page = 100
    while start < max_results:
        data = get(
            f"/search/companies"
            f"?q={requests.utils.quote(query)}"
            f"&items_per_page={per_page}&start_index={start}"
        )
        items = data.get("items", [])
        if not items:
            break
        for c in items:
            name = c.get("title") or c.get("company_name", "")
            if c.get("company_status") == "active" and name:
                companies.append({**c, "company_name": name})
        total = data.get("total_results", 0)
        start += per_page
        if start >= min(total, max_results):
            break
        time.sleep(0.1)
    return companies


def sweep_name_queries(existing: dict) -> dict:
    all_companies = dict(existing)
    for query in cfg.NAME_QUERIES:
        print(f"  Name search '{query}'...", end=" ", flush=True)
        results = name_search(query)
        new = 0
        for c in results:
            num  = c.get("company_number", "")
            name = c.get("company_name", "") or c.get("title", "")
            if num and num not in all_companies and is_genuine(name):
                all_companies[num] = _normalise(c, source="name_search")
                new += 1
        print(f"{len(results)} active → {new} new")
    return all_companies


def _normalise(c: dict, source: str) -> dict:
    name = c.get("company_name") or c.get("title", "")
    return {
        "company_number":            c.get("company_number", ""),
        "company_name":              name,
        "company_status":            c.get("company_status", "active"),
        "date_of_creation":          c.get("date_of_creation", ""),
        "registered_office_address": c.get("registered_office_address", {}),
        "sic_codes":                 c.get("sic_codes", []),
        "relevance_score":           90 if source == "name_search" else 95,
        "source":                    source,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    global AUTH
    AUTH = (load_api_key(), "")

    os.makedirs(cfg.OUTPUT_DIR, exist_ok=True)

    print(f"\n{'='*60}")
    print(f" Companies House Sweep — {cfg.SECTOR_LABEL}")
    print(f"{'='*60}\n")

    print("Phase 1: SIC code sweep")
    companies = sweep_sic_codes()

    print("\nPhase 2: Name search")
    companies = sweep_name_queries(companies)

    raw_list = list(companies.values())
    raw_path = os.path.join(cfg.OUTPUT_DIR, cfg.RAW_JSON)
    with open(raw_path, "w") as f:
        json.dump(raw_list, f, indent=2)

    print(f"\nTotal unique companies: {len(raw_list)}")
    print(f"Saved → {raw_path}")
    return raw_list


if __name__ == "__main__":
    run()
