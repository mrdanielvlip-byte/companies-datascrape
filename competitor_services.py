"""
competitor_services.py — Web-enrichment of competitor service descriptions

Enriches each competitor in the competitor_map with a plain-English description
of what they actually offer — richer than dry SIC code labels.

Sources (in priority order):
  1. Target is in the enriched dataset → use stored digital_health.website_description
  2. Competitor has a website URL in their CH data → fetch and extract meta description
  3. Guess likely homepage URLs from company name (acmepestcontrol.co.uk, etc.)
     and try each until one returns a useful description
  4. Fallback: SIC code descriptions (already populated by build_excel.py)

Results are cached in data/sector_cache.db (persists across pipeline runs).

Design goals:
  - No external API keys required
  - No Google / Bing scraping (unreliable, often blocked)
  - Graceful degradation — every failure path returns '' and doesn't crash the pipeline
  - Configurable delay to be polite to websites
"""

import re
import sqlite3
import time
from pathlib import Path
from typing import Optional

import requests

DATA_DIR = Path(__file__).parent / "data"
CACHE_DB = DATA_DIR / "sector_cache.db"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PE-Research-Bot/1.0; +https://github.com)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-GB,en;q=0.9",
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ── Cache helpers ─────────────────────────────────────────────────────────────

def _ensure_cache_table():
    try:
        con = sqlite3.connect(str(CACHE_DB))
        con.execute("""
            CREATE TABLE IF NOT EXISTS competitor_services_cache (
                company_number TEXT PRIMARY KEY,
                services_description TEXT,
                website_url TEXT,
                fetched_at TEXT DEFAULT (datetime('now'))
            )
        """)
        con.commit()
        con.close()
    except Exception:
        pass


def _cache_get(company_number: str) -> Optional[str]:
    try:
        con = sqlite3.connect(str(CACHE_DB))
        row = con.execute(
            "SELECT services_description FROM competitor_services_cache WHERE company_number=?",
            (company_number,)
        ).fetchone()
        con.close()
        return row[0] if row else None
    except Exception:
        return None


def _cache_set(company_number: str, description: str, website_url: str = ""):
    try:
        con = sqlite3.connect(str(CACHE_DB))
        con.execute(
            """INSERT OR REPLACE INTO competitor_services_cache
               (company_number, services_description, website_url)
               VALUES (?,?,?)""",
            (company_number, description, website_url)
        )
        con.commit()
        con.close()
    except Exception:
        pass


# ── Meta description extraction ───────────────────────────────────────────────

def _extract_description(html: str) -> str:
    """Extract og:description → meta description → <h1> from HTML."""
    if not html:
        return ""
    for pattern in [
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']{15,300})["\']',
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{15,300})["\']',
        r'<meta[^>]+content=["\']([^"\']{15,300})["\'][^>]+name=["\']description["\']',
        r'<meta[^>]+content=["\']([^"\']{15,300})["\'][^>]+property=["\']og:description["\']',
    ]:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            return re.sub(r'\s+', ' ', m.group(1)).strip()
    # Fallback: first meaningful <h1>
    m = re.search(r'<h1[^>]*>([^<]{10,100})</h1>', html, re.IGNORECASE)
    if m:
        text = re.sub(r'\s+', ' ', m.group(1)).strip()
        if len(text) >= 10:
            return text
    return ""


# ── Website fetch ─────────────────────────────────────────────────────────────

def _fetch_description_from_url(url: str, timeout: int = 7) -> str:
    """Fetch URL and return description string, or '' on failure."""
    if not url:
        return ""
    if not url.startswith("http"):
        url = "https://" + url
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return _extract_description(r.text[:40_000])
    except Exception:
        pass
    return ""


# ── URL candidates from company name ─────────────────────────────────────────

def _candidate_urls(company_name: str) -> list[str]:
    """
    Generate up to 6 likely homepage URLs from a UK company name.
    E.g. 'ACME PEST CONTROL LIMITED' → acmepestcontrol.co.uk, acme-pest-control.co.uk …
    """
    name = company_name.lower()
    # Strip common legal suffixes
    for suffix in (
        " limited", " ltd", " plc", " llp", " lp", " & co", "& co.",
        " co.", " inc", " group", " holdings", " services",
    ):
        name = name.replace(suffix, "")
    # Keep only alphanumeric and spaces
    name = re.sub(r"[^a-z0-9 ]", "", name).strip()
    words = name.split()
    if not words:
        return []

    slug_joined = "".join(words)           # acmepestcontrol
    slug_hyphen = "-".join(words)          # acme-pest-control

    candidates = []
    for slug in (slug_joined, slug_hyphen):
        for tld in (".co.uk", ".com", ".uk"):
            candidates.append(f"https://{slug}{tld}")

    return candidates[:6]


# ── Main enrichment function ──────────────────────────────────────────────────

def enrich_competitor_services(
    competitor: dict,
    enriched_index: dict[str, dict],
    delay: float = 0.4,
) -> str:
    """
    Return a plain-English description of what this competitor offers.

    Args:
        competitor:     Entry from competitor_map list (has company_number, company_name,
                        postcode, sic_codes).
        enriched_index: company_number → full company dict from the enriched dataset.
                        If competitor is in here, we use their stored digital_health data.
        delay:          Seconds to sleep between HTTP fetches.

    Returns:
        Description string, or "" if nothing useful found.
    """
    comp_num  = competitor.get("company_number", "")
    comp_name = competitor.get("company_name", "")

    # 1. Cache hit
    if comp_num:
        cached = _cache_get(comp_num)
        if cached is not None:
            return cached

    # 2. Competitor is in our enriched dataset → use stored website_description
    enriched = enriched_index.get(comp_num, {})
    dh = enriched.get("digital_health", {})
    stored_desc = dh.get("website_description", "")
    if stored_desc and len(stored_desc) >= 20:
        _cache_set(comp_num, stored_desc, dh.get("domain", ""))
        return stored_desc

    # 3. Try any known website URL from enriched data
    known_website = (
        enriched.get("contacts", {}).get("website")
        or enriched.get("website")
        or dh.get("domain")
        or ""
    )
    if known_website:
        time.sleep(delay)
        desc = _fetch_description_from_url(known_website)
        if desc and len(desc) >= 20:
            _cache_set(comp_num, desc, known_website)
            return desc

    # 4. Guess likely URLs from company name
    if comp_name:
        for url in _candidate_urls(comp_name):
            time.sleep(delay * 0.5)   # shorter delay for URL guessing
            desc = _fetch_description_from_url(url)
            if desc and len(desc) >= 20:
                _cache_set(comp_num, desc, url)
                return desc

    # 5. Nothing found
    _cache_set(comp_num, "", "")
    return ""


def enrich_all_competitor_services(
    companies: list[dict],
    verbose: bool = True,
    max_per_run: int = 200,   # cap web fetches per pipeline run
) -> list[dict]:
    """
    Enrich service descriptions for all competitor entries across all companies.
    Adds 'services_description' field to each competitor_map entry.

    Args:
        companies:   Full enriched companies list (targets + competitors all from same set).
        verbose:     Print progress.
        max_per_run: Max new web fetch attempts per run (cached hits don't count).

    Returns updated companies list.
    """
    _ensure_cache_table()
    enriched_index = {c.get("company_number", ""): c for c in companies}

    total_entries = sum(
        len((c.get("competitor_analysis") or {}).get("competitor_map", []))
        for c in companies
    )
    enriched_count = 0
    fetch_attempts = 0

    if verbose:
        print(f"\nEnriching competitor service descriptions "
              f"({total_entries} entries, max {max_per_run} new fetches)...")

    for i, company in enumerate(companies):
        comp_map = (company.get("competitor_analysis") or {}).get("competitor_map", [])

        for comp in comp_map:
            comp_num = comp.get("company_number", "")

            # If already cached, use it without counting as a new fetch
            cached = _cache_get(comp_num) if comp_num else None
            if cached is not None:
                comp["services_description"] = cached
                if cached:
                    enriched_count += 1
                continue

            if fetch_attempts >= max_per_run:
                comp["services_description"] = ""
                continue

            fetch_attempts += 1
            desc = enrich_competitor_services(
                competitor     = comp,
                enriched_index = enriched_index,
                delay          = 0.3,
            )
            comp["services_description"] = desc
            if desc:
                enriched_count += 1

        if verbose and i % 10 == 0:
            print(f"  [{i+1}/{len(companies)}] {company.get('company_name','')[:40]}  "
                  f"(fetches: {fetch_attempts}, found: {enriched_count})")

    if verbose:
        print(f"  Service descriptions populated: {enriched_count} / {total_entries}")

    return companies
