"""
competitor_services.py — Web-enrichment of competitor service descriptions

Enriches each competitor in the competitor_map with a plain-English description
of what they actually offer — richer than dry SIC code labels.

Website discovery sources (in priority order):
  1. Competitor is already in the enriched dataset
     → use their stored digital_health.website_description (free, no HTTP)
  2. Company registration number search
     UK limited companies are legally required to print their reg number on
     their website, so searching "{number} {name}" via Bing usually returns
     their official site as the top result.
  3. Bing search with company name + postcode prefix
  4. Direct URL guessing from company name
     e.g. 'ACME PEST CONTROL LTD' → acmepestcontrol.co.uk, etc.
  5. Fallback: SIC code description (already populated by build_excel.py)

Results are cached in data/sector_cache.db (persists across runs).
All HTTP calls are wrapped in try/except; any failure falls through silently.
"""

import re
import sqlite3
import time
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus, unquote

import requests

DATA_DIR = Path(__file__).parent / "data"
CACHE_DB = DATA_DIR / "sector_cache.db"

_HEADERS_BROWSER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-GB,en;q=0.9",
}
_HEADERS_BOT = {
    "User-Agent": "Mozilla/5.0 (compatible; PE-Research-Bot/1.0)",
    "Accept": "text/html,application/xhtml+xml",
}

SESSION = requests.Session()
SESSION.headers.update(_HEADERS_BROWSER)


# ── Cache helpers ─────────────────────────────────────────────────────────────

def _ensure_cache_table():
    try:
        con = sqlite3.connect(str(CACHE_DB))
        con.execute("""
            CREATE TABLE IF NOT EXISTS competitor_services_cache (
                company_number  TEXT PRIMARY KEY,
                services_description TEXT,
                website_url     TEXT,
                fetched_at      TEXT DEFAULT (datetime('now'))
            )
        """)
        con.commit()
        con.close()
    except Exception:
        pass


def _cache_get(company_number: str) -> Optional[tuple[str, str]]:
    """Return (description, website_url) from cache, or None if not cached."""
    try:
        con = sqlite3.connect(str(CACHE_DB))
        row = con.execute(
            "SELECT services_description, website_url FROM competitor_services_cache WHERE company_number=?",
            (company_number,),
        ).fetchone()
        con.close()
        return (row[0] or "", row[1] or "") if row else None
    except Exception:
        return None


def _cache_set(company_number: str, description: str, website_url: str = ""):
    try:
        con = sqlite3.connect(str(CACHE_DB))
        con.execute(
            """INSERT OR REPLACE INTO competitor_services_cache
               (company_number, services_description, website_url)
               VALUES (?,?,?)""",
            (company_number, description, website_url),
        )
        con.commit()
        con.close()
    except Exception:
        pass


# ── Meta description extraction ───────────────────────────────────────────────

def _extract_description(html: str) -> str:
    """Extract og:description / meta description / <h1> from page HTML."""
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
            return re.sub(r"\s+", " ", m.group(1)).strip()
    # Fallback: first meaningful <h1>
    m = re.search(r"<h1[^>]*>([^<]{10,100})</h1>", html, re.IGNORECASE)
    if m:
        text = re.sub(r"\s+", " ", m.group(1)).strip()
        if len(text) >= 10:
            return text
    return ""


# ── Fetch website and extract description ─────────────────────────────────────

def _fetch_description_from_url(url: str, timeout: int = 8) -> str:
    """GET a URL and return its description string, or '' on failure."""
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


# ── Bing search → first organic result URL ───────────────────────────────────

def _bing_first_url(query: str, timeout: int = 8) -> Optional[str]:
    """
    Search Bing for query and return the first organic result URL.
    Uses a browser User-Agent. Returns None on failure or if nothing found.
    """
    try:
        url = f"https://www.bing.com/search?q={quote_plus(query)}&setlang=en-GB&cc=GB"
        r = SESSION.get(url, timeout=timeout)
        if r.status_code != 200:
            return None
        html = r.text

        # Bing organic results are anchored in <li class="b_algo"> blocks.
        # The first <h2><a href="..."> in each block is the result URL.
        block_pattern = re.compile(
            r'<li[^>]+class=["\'][^"\']*b_algo[^"\']*["\'][^>]*>(.*?)</li>',
            re.DOTALL | re.IGNORECASE,
        )
        link_pattern = re.compile(r'<h2[^>]*>.*?<a[^>]+href=["\']([^"\']+)["\']', re.DOTALL)

        for block in block_pattern.finditer(html):
            m = link_pattern.search(block.group(1))
            if m:
                href = m.group(1)
                # Skip Bing-internal and irrelevant domains
                if any(skip in href for skip in (
                    "bing.com", "microsoft.com", "facebook.com",
                    "linkedin.com", "companies-house", "gov.uk",
                    "companieshouse.gov.uk", "opencorporates.com",
                )):
                    continue
                if href.startswith("http"):
                    return href

        # Fallback: grab any external http URL from the response body
        all_hrefs = re.findall(r'href="(https?://[^"]+)"', html)
        blocked = {"bing.com", "microsoft.com", "w3.org", "schemas.org", "gov.uk"}
        for href in all_hrefs:
            domain = re.sub(r"https?://(www\.)?", "", href).split("/")[0]
            if not any(b in domain for b in blocked):
                return href

    except Exception:
        pass
    return None


# ── URL candidates generated from company name ───────────────────────────────

def _candidate_urls(company_name: str) -> list[str]:
    """
    Generate up to 6 likely homepage URLs from a company name.
    'ACME PEST CONTROL LIMITED' → acmepestcontrol.co.uk, acme-pest-control.co.uk …
    """
    name = company_name.lower()
    for suffix in (
        " limited", " ltd", " plc", " llp", " lp",
        " & co", "& co.", " co.", " inc",
        " group", " holdings", " services", " solutions",
    ):
        name = name.replace(suffix, "")
    name = re.sub(r"[^a-z0-9 ]", "", name).strip()
    words = name.split()
    if not words:
        return []

    slug_joined = "".join(words)      # acmepestcontrol
    slug_hyphen = "-".join(words)     # acme-pest-control

    candidates = []
    for slug in (slug_joined, slug_hyphen):
        for tld in (".co.uk", ".com", ".uk"):
            candidates.append(f"https://{slug}{tld}")
    return candidates[:6]


# ── Main enrichment function ──────────────────────────────────────────────────

def enrich_competitor_services(
    competitor: dict,
    enriched_index: dict[str, dict],
    delay: float = 0.5,
) -> tuple[str, str]:
    """
    Return (description, website_url) for what this competitor offers.

    Lookup order:
      1. Cache
      2. Stored digital_health.website_description from enriched dataset
      3. Known website URL from enriched data
      4. Bing search: "{company_number} {company_name}" (reg number is unique —
         companies are legally required to print it on their website)
      5. Bing search: "{company_name} {postcode_prefix} services"
      6. URL guessing from company name
    """
    comp_num  = competitor.get("company_number", "")
    comp_name = competitor.get("company_name", "")
    postcode  = competitor.get("postcode", "")
    pc_prefix = postcode.split()[0] if postcode else ""

    # ── 1. Cache hit ──────────────────────────────────────────────────────────
    if comp_num:
        cached = _cache_get(comp_num)
        if cached is not None:
            return cached  # (desc, url) tuple

    # ── 2. Already enriched — use stored description ─────────────────────────
    enriched = enriched_index.get(comp_num, {})
    dh = enriched.get("digital_health", {})
    stored_desc = dh.get("website_description", "")
    domain = dh.get("domain", "")
    stored_url = (f"https://{domain}" if domain and not domain.startswith("http") else domain)
    if stored_desc and len(stored_desc) >= 20:
        _cache_set(comp_num, stored_desc, stored_url)
        return (stored_desc, stored_url)

    # ── 3. Known website URL ──────────────────────────────────────────────────
    known_website = (
        enriched.get("contacts", {}).get("website")
        or enriched.get("website")
        or stored_url
        or ""
    )
    if known_website:
        time.sleep(delay)
        desc = _fetch_description_from_url(known_website)
        if desc and len(desc) >= 20:
            _cache_set(comp_num, desc, known_website)
            return (desc, known_website)

    # ── 4. Bing: registration number + company name ───────────────────────────
    # UK companies must print their reg number on their website (Companies Act 2006).
    # Searching for the number finds their official site reliably.
    if comp_num and comp_name:
        query = f'"{comp_num}" "{comp_name}"'
        time.sleep(delay)
        found_url = _bing_first_url(query)
        if found_url:
            time.sleep(delay * 0.5)
            desc = _fetch_description_from_url(found_url)
            if desc and len(desc) >= 20:
                _cache_set(comp_num, desc, found_url)
                return (desc, found_url)
            # URL found but description empty — still save the URL
            if found_url:
                _cache_set(comp_num, "", found_url)
                return ("", found_url)

    # ── 5. Bing: name + postcode prefix + "services" ─────────────────────────
    if comp_name:
        query2 = f'"{comp_name}" {pc_prefix} services site:.co.uk OR site:.com'
        time.sleep(delay)
        found_url2 = _bing_first_url(query2)
        if found_url2:
            time.sleep(delay * 0.5)
            desc = _fetch_description_from_url(found_url2)
            if desc and len(desc) >= 20:
                _cache_set(comp_num, desc, found_url2)
                return (desc, found_url2)
            if found_url2:
                _cache_set(comp_num, "", found_url2)
                return ("", found_url2)

    # ── 6. URL guessing from company name ─────────────────────────────────────
    for candidate_url in _candidate_urls(comp_name):
        time.sleep(delay * 0.3)
        desc = _fetch_description_from_url(candidate_url)
        if desc and len(desc) >= 20:
            _cache_set(comp_num, desc, candidate_url)
            return (desc, candidate_url)

    # ── 7. Nothing found ──────────────────────────────────────────────────────
    _cache_set(comp_num, "", "")
    return ("", "")


# ── Batch run ─────────────────────────────────────────────────────────────────

def enrich_all_competitor_services(
    companies: list[dict],
    verbose: bool = True,
    max_per_run: int = 300,
) -> list[dict]:
    """
    Enrich services_description for every competitor entry across all companies.
    Modifies competitor_map entries in-place.

    max_per_run caps the number of NEW web fetch sequences (cached hits are free).
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
        print(
            f"\nEnriching competitor service descriptions "
            f"({total_entries} competitor entries, cap={max_per_run} new fetches)..."
        )

    for i, company in enumerate(companies):
        comp_map = (company.get("competitor_analysis") or {}).get("competitor_map", [])

        for comp in comp_map:
            comp_num = comp.get("company_number", "")

            # Free cache hit — doesn't count toward cap
            if comp_num:
                cached = _cache_get(comp_num)
                if cached is not None:
                    desc, url = cached
                    comp["services_description"] = desc
                    comp["website_url"] = url
                    if desc:
                        enriched_count += 1
                    continue

            if fetch_attempts >= max_per_run:
                comp["services_description"] = ""
                comp["website_url"] = ""
                continue

            fetch_attempts += 1
            desc, url = enrich_competitor_services(
                competitor     = comp,
                enriched_index = enriched_index,
                delay          = 0.4,
            )
            comp["services_description"] = desc
            comp["website_url"] = url
            if desc:
                enriched_count += 1

        if verbose and i % 10 == 0:
            print(
                f"  [{i+1}/{len(companies)}] {company.get('company_name','')[:40]}"
                f"  (fetches: {fetch_attempts}, found: {enriched_count})"
            )

    if verbose:
        print(f"  Service descriptions populated: {enriched_count} / {total_entries}")

    return companies
