#!/usr/bin/env python3
"""
estimate.py — Quick sector estimate: SIC code matching + company count

Runs in ~10–30 seconds. Outputs output/estimate.json with:
  - matched SIC codes + descriptions + company counts
  - total estimated company universe
  - accuracy score (curated map vs fuzzy fallback)
  - match source
  - 10 sample company names

Strategy:
  1. Query local SQLite DB (data/companies_house.db) — instant, no rate limits
  2. Fall back to Companies House API if DB unavailable

Usage:
    python3 estimate.py "waste management"
"""

import sys
import json
import os
import sqlite3
import time
import requests
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent / "data"
DB_PATH  = DATA_DIR / "companies_house.db"

# ── Load CH API key (fallback only) ───────────────────────────────────────────
def _load_api_key():
    for path in [".ch_api_key", os.path.expanduser("~/.ch_api_key")]:
        if os.path.exists(path):
            for line in open(path):
                if "=" in line:
                    return line.strip().split("=", 1)[1]
    return os.environ.get("COMPANIES_HOUSE_API_KEY", "")

API_KEY = _load_api_key()
BASE    = "https://api.company-information.service.gov.uk"


# ── Local DB helpers (fast, no rate limits) ────────────────────────────────────
def _db_ready() -> bool:
    if not DB_PATH.exists():
        return False
    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        n = con.execute("SELECT COUNT(*) FROM companies LIMIT 1").fetchone()[0]
        con.close()
        return n > 0
    except Exception:
        return False


def _count_by_sic_local(sic_code: str) -> tuple[int, list[str]]:
    """Query local SQLite DB for active company count + sample names."""
    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """
            SELECT company_name FROM companies
            WHERE company_status = 'Active'
              AND (sic1=? OR sic2=? OR sic3=? OR sic4=?)
            LIMIT 10
            """,
            (sic_code, sic_code, sic_code, sic_code),
        ).fetchall()
        count = con.execute(
            """
            SELECT COUNT(*) FROM companies
            WHERE company_status = 'Active'
              AND (sic1=? OR sic2=? OR sic3=? OR sic4=?)
            """,
            (sic_code, sic_code, sic_code, sic_code),
        ).fetchone()[0]
        con.close()
        samples = [r["company_name"] for r in rows if r["company_name"]]
        return count, samples
    except Exception as exc:
        print(f"    ⚠️  Local DB error for SIC {sic_code}: {exc}")
        return -1, []


# ── CH API fallback (handles rate-limiting with retries) ──────────────────────
def _count_by_sic_api(sic_code: str, retries: int = 4) -> tuple[int, list[str]]:
    """CH API fallback with exponential back-off on 429 rate limits."""
    url  = (
        f"{BASE}/advanced-search/companies"
        f"?sic_codes={sic_code}&items_per_page=5&company_status=active"
    )
    wait = 8
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, auth=(API_KEY, ""), timeout=15)
            if r.status_code == 200:
                data    = r.json()
                total   = data.get("hits", 0)
                samples = [
                    c.get("company_name", "")
                    for c in data.get("items", [])
                    if c.get("company_name")
                ]
                return total, samples
            elif r.status_code == 429:
                if attempt < retries:
                    print(f"    ⚠️  Rate-limited on SIC {sic_code} — waiting {wait}s …")
                    time.sleep(wait)
                    wait *= 2
                else:
                    print(f"    ❌  SIC {sic_code}: still rate-limited after {retries} retries.")
                    return -1, []
            else:
                print(f"    ❌  SIC {sic_code}: HTTP {r.status_code}")
                return -1, []
        except Exception as exc:
            print(f"    ❌  SIC {sic_code}: request error — {exc}")
            return -1, []
    return -1, []


def _count_by_sic(sic_code: str, use_local: bool) -> tuple[int, list[str]]:
    """Use local DB if available, otherwise fall back to CH API."""
    if use_local:
        return _count_by_sic_local(sic_code)
    return _count_by_sic_api(sic_code)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    sector = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "lift maintenance"
    print(f"\n⚡ Quick estimate for: '{sector}'")

    use_local = _db_ready()
    if use_local:
        print(f"  📂 Using local Companies House DB ({DB_PATH})")
    else:
        print(f"  🌐 Local DB not found — querying Companies House API")

    # Step 1: SIC code discovery
    from sic_discovery import discover
    cfg = discover(sector, api_key=API_KEY, validate=False)

    sic_matches    = cfg._sic_matches
    market_score   = cfg.MARKET_ATTRACTIVENESS_SCORE
    bench_category = cfg._benchmark_category
    match_source   = sic_matches[0].get("source", "fuzzy") if sic_matches else "fuzzy"

    if match_source == "curated":
        accuracy_label = "High — matched curated sector map"
        accuracy_pct   = market_score
    else:
        best_score   = max((m.get("score", 0) for m in sic_matches), default=0)
        accuracy_pct = min(int(best_score * 100), 79)
        accuracy_label = "Medium — matched via fuzzy keyword scoring"

    # Step 2: Count companies per SIC code
    print(f"\n  Counting companies per SIC code …")
    sic_breakdown   = []
    total_companies = 0
    all_samples     = []
    api_errors      = 0

    for m in sic_matches[:8]:
        code  = m["code"]
        desc  = m.get("description", "")
        count, samples = _count_by_sic(code, use_local)

        if count == -1:
            api_errors += 1
            count = 0
            label = "⚠️ lookup error"
        else:
            label = f"{count:,} companies"

        sic_breakdown.append({
            "code":        code,
            "description": desc,
            "count":       count,
            "score":       round(m.get("score", 1.0), 3),
            "source":      m.get("source", "fuzzy"),
        })
        total_companies += count
        all_samples.extend(samples)
        print(f"    {code}  {desc[:50]:<50}  →  {label}")

        if not use_local:
            time.sleep(0.5)   # rate-limit protection for API mode

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
        "sector":           sector,
        "sic_codes":        [m["code"] for m in sic_breakdown],
        "sic_breakdown":    sic_breakdown,
        "total_companies":  total_companies,
        "accuracy_pct":     accuracy_pct,
        "accuracy_label":   accuracy_label,
        "match_source":     match_source,
        "bench_category":   bench_category,
        "sample_companies": unique_samples,
        "api_errors":       api_errors,
        "count_source":     "local_db" if use_local else "ch_api",
    }

    os.makedirs("output", exist_ok=True)
    out_path = "output/estimate.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n  ✅ Total companies:  {total_companies:,}")
    print(f"  ✅ Accuracy:         {accuracy_pct}% ({accuracy_label})")
    print(f"  ✅ Count source:     {'Local DB' if use_local else 'CH API'}")
    print(f"  ✅ Written to:       {out_path}")


if __name__ == "__main__":
    main()
