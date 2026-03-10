#!/usr/bin/env python3
"""
estimate.py — Quick sector estimate: SIC code matching + company count

Runs in ~60–90 seconds. Outputs output/estimate.json with:
  - matched SIC codes + descriptions + company counts
  - total estimated company universe
  - accuracy score (curated map vs fuzzy fallback)
  - match source
  - 10 sample company names

Usage:
    python3 estimate.py "waste management"
"""

import sys
import json
import os
import time
import requests

# ── Load CH API key ────────────────────────────────────────────────────────────
def _load_api_key():
    for path in [".ch_api_key", os.path.expanduser("~/.ch_api_key")]:
        if os.path.exists(path):
            for line in open(path):
                if "=" in line:
                    return line.strip().split("=", 1)[1]
    return os.environ.get("COMPANIES_HOUSE_API_KEY", "")

API_KEY = _load_api_key()
BASE    = "https://api.company-information.service.gov.uk"


# ── CH API helpers ─────────────────────────────────────────────────────────────
def _count_by_sic(sic_code: str) -> tuple[int, list[str]]:
    """Return (total_active_companies, [sample_names]) for one SIC code."""
    try:
        r = requests.get(
            f"{BASE}/advanced-search/companies"
            f"?sic_codes={sic_code}&items_per_page=5&company_status=active",
            auth=(API_KEY, ""), timeout=12,
        )
        if r.status_code == 200:
            data    = r.json()
            total   = data.get("hits", 0)
            samples = [c.get("company_name", "") for c in data.get("items", []) if c.get("company_name")]
            return total, samples
    except Exception:
        pass
    return 0, []


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    sector = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "lift maintenance"
    print(f"\n⚡ Quick estimate for: '{sector}'")

    # Step 1: SIC code discovery
    from sic_discovery import discover
    cfg = discover(sector, api_key=API_KEY, validate=False)

    sic_matches    = cfg._sic_matches          # list of {code, description, score, source}
    market_score   = cfg.MARKET_ATTRACTIVENESS_SCORE
    bench_category = cfg._benchmark_category
    match_source   = sic_matches[0].get("source", "fuzzy") if sic_matches else "fuzzy"

    # Derive an accuracy label
    if match_source == "curated":
        accuracy_label = "High — matched curated sector map"
        accuracy_pct   = market_score
    else:
        best_score = max((m.get("score", 0) for m in sic_matches), default=0)
        accuracy_pct   = min(int(best_score * 100), 79)
        accuracy_label = "Medium — matched via fuzzy keyword scoring"

    # Step 2: Count companies per SIC code (quick CH API queries)
    print(f"\n  Counting companies per SIC code …")
    sic_breakdown  = []
    total_companies = 0
    all_samples    = []

    for m in sic_matches[:8]:   # cap at 8 SIC codes for speed
        code  = m["code"]
        desc  = m.get("description", "")
        count, samples = _count_by_sic(code)
        sic_breakdown.append({
            "code":        code,
            "description": desc,
            "count":       count,
            "score":       round(m.get("score", 1.0), 3),
            "source":      m.get("source", "fuzzy"),
        })
        total_companies += count
        all_samples.extend(samples)
        print(f"    {code}  {desc[:50]:<50}  →  {count:,} companies")
        time.sleep(0.15)

    # Deduplicate samples
    seen, unique_samples = set(), []
    for s in all_samples:
        key = s.lower()
        if key not in seen:
            seen.add(key)
            unique_samples.append(s)
        if len(unique_samples) >= 10:
            break

    result = {
        "sector":          sector,
        "sic_codes":       [m["code"] for m in sic_breakdown],
        "sic_breakdown":   sic_breakdown,
        "total_companies": total_companies,
        "accuracy_pct":    accuracy_pct,
        "accuracy_label":  accuracy_label,
        "match_source":    match_source,
        "bench_category":  bench_category,
        "sample_companies": unique_samples,
    }

    os.makedirs("output", exist_ok=True)
    out_path = "output/estimate.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n  ✅ Total companies:  {total_companies:,}")
    print(f"  ✅ Accuracy:         {accuracy_pct}% ({accuracy_label})")
    print(f"  ✅ Written to:       {out_path}")


if __name__ == "__main__":
    main()
