"""
contracts_finder.py — Government Contracts Intelligence

Searches UK government procurement databases for contracts awarded to
target companies. Revenue visibility from government contracts is a
key PE due diligence signal (recurring, creditworthy counterparty).

APIs used (both free, no auth required):
  1. Contracts Finder — contracts < £5m (mainly public sector)
     https://www.contractsfinder.service.gov.uk/Published/Notices/PublicSearch/Search
  2. Find a Tender (FTS) — contracts ≥ £138k (OJEU / post-Brexit)
     https://find-tender.service.gov.uk/api/1.0/ocds/notices/paged

Output per company:
  • contracts_found         — number of contracts found
  • total_contract_value    — cumulative £ value
  • latest_contract_date    — most recent award date
  • buyers                  — list of awarding authorities
  • contract_list           — up to 10 contracts with detail
  • revenue_quality         — signal: "Government-backed recurring revenue"
  • data_tier               — Tier 1 (public register)
"""

import requests
import json
import time
import os
from datetime import datetime

import config as cfg


CF_SEARCH  = "https://www.contractsfinder.service.gov.uk/Published/Notices/PublicSearch/Search"
FTS_SEARCH = "https://find-tender.service.gov.uk/api/1.0/ocds/notices/paged"

HEADERS = {
    "Accept":       "application/json",
    "Content-Type": "application/json",
    "User-Agent":   "PE-Pipeline/1.0 (+research)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def _cf_search(company_name: str, max_results: int = 10) -> list[dict]:
    """
    Search Contracts Finder by supplier name.
    Returns a normalised list of contract dicts.
    """
    # Strip legal suffix for better matching
    clean_name = (company_name
                  .replace(" LIMITED", "").replace(" LTD", "")
                  .replace(" LLP", "").replace("  ", " ").strip())

    payload = {
        "searchCriteria": {
            "keyword": clean_name,
            "noticeType": ["awarded_contract", "contract_award_notice"],
            "maximumResponseCount": max_results,
        }
    }

    try:
        r = SESSION.post(CF_SEARCH, json=payload, timeout=15)
        if r.status_code != 200:
            return []
        data = r.json()
        notices = data.get("results", data.get("noticeList", []))
    except Exception:
        return []

    contracts = []
    for n in notices:
        # Supplier match check — Contracts Finder results can be loose
        suppliers = n.get("suppliers", [n.get("supplier", {})])
        supplier_names = " ".join(
            (s.get("name", "") + " " + s.get("organisationName", "")).lower()
            for s in (suppliers if isinstance(suppliers, list) else [suppliers])
        )
        if not _name_match(clean_name, supplier_names):
            continue

        try:
            value = float(n.get("value", {}).get("amount", 0) or 0)
        except (TypeError, ValueError):
            value = 0

        contracts.append({
            "source":        "Contracts Finder",
            "title":         n.get("title", ""),
            "buyer":         n.get("organisation", {}).get("name", n.get("buyer", "")),
            "awarded_date":  n.get("awardedDate", n.get("publishDate", "")),
            "value_gbp":     value,
            "description":   (n.get("description", "") or "")[:200],
            "data_tier":     "Tier 1 — Contracts Finder (public register)",
        })

    return contracts


def _fts_search(company_name: str, max_results: int = 10) -> list[dict]:
    """
    Search Find a Tender (post-Brexit OJEU equivalent).
    Returns normalised contract dicts.
    """
    clean_name = (company_name
                  .replace(" LIMITED", "").replace(" LTD", "")
                  .replace(" LLP", "").strip())

    params = {
        "q":           clean_name,
        "status":      "planning,active,complete",
        "size":        max_results,
    }

    try:
        r = SESSION.get(FTS_SEARCH, params=params, timeout=15)
        if r.status_code != 200:
            return []
        data = r.json()
        releases = data.get("releases", [])
    except Exception:
        return []

    contracts = []
    for rel in releases:
        # Check supplier name in awards
        awards = rel.get("awards", [])
        for award in awards:
            suppliers = award.get("suppliers", [])
            sup_names = " ".join(s.get("name", "").lower() for s in suppliers)
            if not _name_match(clean_name, sup_names):
                continue

            try:
                value = float(award.get("value", {}).get("amount", 0) or 0)
            except (TypeError, ValueError):
                value = 0

            tender  = rel.get("tender", {})
            parties = rel.get("parties", [])
            buyer_name = next(
                (p.get("name", "") for p in parties if "buyer" in p.get("roles", [])),
                ""
            )

            contracts.append({
                "source":        "Find a Tender",
                "title":         tender.get("title", rel.get("id", "")),
                "buyer":         buyer_name,
                "awarded_date":  award.get("date", ""),
                "value_gbp":     value,
                "description":   (tender.get("description", "") or "")[:200],
                "data_tier":     "Tier 1 — Find a Tender (public register)",
            })

    return contracts


def _name_match(query: str, target: str) -> bool:
    """Fuzzy supplier name match — check significant words overlap."""
    query_words = set(w.lower() for w in query.split() if len(w) > 3)
    if not query_words:
        return False
    matches = sum(1 for w in query_words if w in target.lower())
    return matches / len(query_words) >= 0.5


def enrich_contracts(company: dict) -> dict:
    """
    Full contract enrichment for one company.
    Searches both Contracts Finder and Find a Tender.
    """
    name = company.get("company_name", "")

    cf_results  = _cf_search(name)
    time.sleep(0.5)
    fts_results = _fts_search(name)
    time.sleep(0.5)

    all_contracts = cf_results + fts_results

    # Deduplicate by title + buyer similarity
    seen    = set()
    unique  = []
    for c in all_contracts:
        key = (c["title"][:30].lower(), c["buyer"][:20].lower())
        if key not in seen:
            seen.add(key)
            unique.append(c)

    # Aggregate stats
    total_value = sum(c["value_gbp"] for c in unique if c["value_gbp"])
    buyers      = list({c["buyer"] for c in unique if c["buyer"]})
    dates       = [c["awarded_date"] for c in unique if c.get("awarded_date")]
    latest_date = max(dates) if dates else ""

    # Revenue quality signal
    if len(unique) >= 3:
        revenue_quality = "Strong — multiple government contracts (recurring revenue signal)"
    elif len(unique) >= 1:
        revenue_quality = "Present — government contract(s) identified"
    else:
        revenue_quality = "None detected"

    return {
        "contracts_found":      len(unique),
        "total_contract_value": round(total_value),
        "latest_contract_date": latest_date,
        "buyers":               buyers[:10],
        "buyer_count":          len(buyers),
        "contract_list":        unique[:10],
        "revenue_quality":      revenue_quality,
        "data_tier":            "Tier 1 — Contracts Finder + Find a Tender (public registers)",
    }


def run():
    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(enriched_path) as f:
        companies = json.load(f)

    top_n     = getattr(cfg, "CONTRACTS_TOP_N", None)   # None = process all companies
    to_enrich = companies[:top_n] if top_n else companies
    skipped   = len(companies) - len(to_enrich)

    print(f"\nContracts Finder enrichment for {len(to_enrich)} companies"
          + (f" (top {top_n})" if top_n else " (all)")
          + (f" — {skipped} skipped" if skipped else "") + "...")

    def _enrich_one_contract(c):
        from concurrent_pipeline import rate_limited_sleep
        result = enrich_contracts(c)
        c["government_contracts"] = result
        rate_limited_sleep()
        return c

    with_contracts = 0
    if len(to_enrich) > 1:
        from concurrent_pipeline import process_batch
        to_enrich = process_batch(
            items=to_enrich,
            func=_enrich_one_contract,
            max_workers=min(8, len(to_enrich)),
            description="Gov. contracts (Contracts Finder + FTS)",
        )
        to_enrich = [c for c in to_enrich if c is not None]
        companies[:top_n] = to_enrich
    else:
        for i, c in enumerate(to_enrich):
            if i % 10 == 0:
                print(f"  [{i+1}/{len(to_enrich)}] processing {c['company_name'][:40]}...")
            _enrich_one_contract(c)

    for c in to_enrich:
        if c.get("government_contracts", {}).get("contracts_found", 0) > 0:
            with_contracts += 1

    # Fill empty for skipped companies
    for c in companies[top_n:]:
        c["government_contracts"] = {
            "contracts_found": 0, "revenue_quality": "Not searched", "data_tier": "N/A"
        }

    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(out_path, "w") as f:
        json.dump(companies, f, indent=2)

    print(f"\nContracts enrichment complete → {out_path}")
    print(f"  {with_contracts} / {len(to_enrich)} companies have government contracts")
    return companies


if __name__ == "__main__":
    run()
