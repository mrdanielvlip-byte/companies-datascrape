"""
trade_body_finder.py — Dynamic UK Trade Body & Industry Association Discovery

For any sector the pipeline hasn't seen before, this module:
  1. Searches DuckDuckGo for relevant UK trade/industry bodies
  2. Identifies likely member-list pages (table, directory, find-a-member)
  3. Scrapes member company names generically (no hardcoded structure needed)
  4. Cross-references each name with Companies House
  5. Returns standard pipeline company dicts ready to merge into filtered_companies.json

Also contains a small KNOWN_BODIES catalogue for the most common UK bodies —
these skip the search step and go straight to scraping (faster, more reliable).

Usage:
    from trade_body_finder import find_trade_bodies, discover_members_from_url

    # Step A — discover candidate body URLs for a sector
    bodies = find_trade_bodies("lift maintenance")
    # → [{"name": "LEIA", "url": "https://www.leia.co.uk/memberlist/",
    #      "source": "known", "member_count_est": 159}, ...]

    # Step B — scrape members from a URL + CH cross-reference
    companies = discover_members_from_url(
        url="https://www.leia.co.uk/memberlist/",
        ch_api_key="abc123",
        body_name="LEIA",
    )
    # → [{"company_name": "Saltire Lift Services Ltd",
    #      "company_number": "SC123456", "source": "LEIA", ...}, ...]
"""

import re
import time
import json
import requests
from urllib.parse import urljoin, urlparse

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False


# ── Known bodies catalogue (fast-path, skip web search) ─────────────────────
# Only add entries here once a body is confirmed scrapeable.
# Dynamic web search handles everything else automatically.
KNOWN_BODIES = {
    "LEIA": {
        "name":              "Lift & Escalator Industry Association",
        "url":               "https://www.leia.co.uk/memberlist/",
        "sector_keywords":   ["lift", "elevator", "escalator", "platform lift",
                              "stairlift", "dumbwaiter", "lift engineer",
                              "lift maintenance", "lift installation"],
        "sic_hints":         ["43290", "28221", "33120"],
        "scrape_type":       "html_table",
        "member_count_est":  159,
    },
    "GAS_SAFE": {
        "name":              "Gas Safe Register",
        "url":               "https://www.gassaferegister.co.uk/find-an-engineer/",
        "sector_keywords":   ["gas", "heating", "boiler", "gas engineer",
                              "gas installation", "central heating", "gas contractor"],
        "sic_hints":         ["43220"],
        "scrape_type":       "blocked",
        "note":              "WAF-protected — website keyword detection only",
    },
    "BESA": {
        "name":              "Building Engineering Services Association",
        "url":               "https://www.thebesa.com/find-a-member/",
        "sector_keywords":   ["hvac", "mechanical services", "ventilation",
                              "air conditioning", "building services", "m&e contractor",
                              "mechanical contractor"],
        "sic_hints":         ["43220", "43290"],
        "scrape_type":       "search_form",
        "note":              "Search form — requires keyword submission",
    },
    "ECA": {
        "name":              "Electrical Contractors Association",
        "url":               "https://www.eca.co.uk/find-a-member",
        "sector_keywords":   ["electrical contractor", "electrician",
                              "electrical installation", "electrical engineer",
                              "wiring", "electrical maintenance"],
        "sic_hints":         ["43210"],
        "scrape_type":       "search_form",
    },
}

# Signals that a URL is a member/directory page worth scraping
_MEMBER_URL_SIGNALS = [
    "member", "members", "memberlist", "membership", "member-list",
    "member-directory", "find-a-member", "find-a-contractor", "find-a-builder",
    "directory", "find-an-engineer", "find-a-company", "contractor-search",
    "registered", "accredited", "approved", "our-members",
]

# Signals that a page title/snippet is about a trade body member directory
_MEMBER_TEXT_SIGNALS = [
    "member directory", "find a member", "member list", "our members",
    "member search", "find a contractor", "registered members",
    "accredited members", "approved contractors", "find an engineer",
    "member companies", "list of members", "company directory",
]

# Domains to skip (they're not trade body directories)
_SKIP_DOMAINS = {
    "linkedin.com", "facebook.com", "twitter.com", "instagram.com",
    "youtube.com", "wikipedia.org", "gov.uk", "companieshouse.gov.uk",
    "amazon.co.uk", "amazon.com", "trustpilot.com", "checkatrade.com",
    "rated.people.com", "mybuilder.com", "bark.com", "yell.com",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
}


# ── Web search (DuckDuckGo HTML) ─────────────────────────────────────────────

def _ddg_search(query: str, max_results: int = 10) -> list[dict]:
    """
    Search DuckDuckGo (HTML interface, no API key needed).
    Returns [{title, url, snippet}, ...].
    """
    if not HAS_BS4:
        return []

    try:
        r = requests.post(
            "https://html.duckduckgo.com/html/",
            data={"q": query, "kl": "uk-en"},
            headers=_HEADERS,
            timeout=12,
        )
        if r.status_code != 200:
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        results = []

        for result in soup.select(".result")[:max_results * 2]:
            title_el = result.select_one(".result__title")
            url_el   = result.select_one(".result__url")
            snip_el  = result.select_one(".result__snippet")

            if not title_el:
                continue

            title   = title_el.get_text(strip=True)
            raw_url = ""
            if url_el:
                raw_url = url_el.get_text(strip=True)
            # DDG wraps URLs — get href from anchor
            a = title_el.find("a")
            if a and a.get("href"):
                href = a["href"]
                # DDG redirect URLs: extract uddg param
                if "uddg=" in href:
                    from urllib.parse import unquote
                    m = re.search(r"uddg=([^&]+)", href)
                    if m:
                        href = unquote(m.group(1))
                raw_url = href

            snippet = snip_el.get_text(strip=True) if snip_el else ""

            if raw_url:
                results.append({"title": title, "url": raw_url, "snippet": snippet})

            if len(results) >= max_results:
                break

        return results

    except Exception as e:
        print(f"  ⚠️  DDG search failed: {e}")
        return []


def _score_result(title: str, url: str, snippet: str) -> int:
    """
    Score a search result for relevance as a trade body member directory.
    Higher = more likely to be a useful member list.
    """
    score = 0
    title_l   = title.lower()
    url_l     = url.lower()
    snippet_l = snippet.lower()

    # Strong URL signals
    for sig in _MEMBER_URL_SIGNALS:
        if sig in url_l:
            score += 3
            break

    # Text signals in title
    for sig in _MEMBER_TEXT_SIGNALS:
        if sig in title_l:
            score += 4
            break

    # Text signals in snippet
    for sig in _MEMBER_TEXT_SIGNALS:
        if sig in snippet_l:
            score += 2
            break

    # UK trade body domain signals
    if ".org.uk" in url_l or ".co.uk" in url_l:
        score += 2
    if "association" in url_l or "association" in title_l:
        score += 2
    if "federation" in url_l or "federation" in title_l:
        score += 2
    if "institute" in url_l or "institute" in title_l:
        score += 1
    if "guild" in url_l or "guild" in title_l:
        score += 1

    # Skip commercial directories
    domain = urlparse(url).netloc.lower().replace("www.", "")
    if domain in _SKIP_DOMAINS:
        score = -99

    return score


def _is_uk_trade_body_url(url: str, title: str, snippet: str) -> bool:
    domain = urlparse(url).netloc.lower().replace("www.", "")
    if domain in _SKIP_DOMAINS:
        return False
    return _score_result(title, url, snippet) >= 4


# ── Main discovery function ───────────────────────────────────────────────────

def find_trade_bodies(sector_text: str, max_bodies: int = 3) -> list[dict]:
    """
    Find relevant UK trade/industry bodies for a sector.

    1. Checks KNOWN_BODIES catalogue first (instant, reliable)
    2. Falls back to DuckDuckGo web search for unknown sectors

    Returns list of dicts:
      {name, url, source ("known"|"web"), scrape_type, member_count_est,
       sector_match (bool), discoverable (bool)}
    """
    results = []
    found_urls = set()
    text_lower = sector_text.lower()

    # ── Step 1: Check known catalogue ────────────────────────────────────────
    for key, body in KNOWN_BODIES.items():
        matched = any(kw.lower() in text_lower
                      for kw in body.get("sector_keywords", []))
        if matched:
            results.append({
                "key":              key,
                "name":             body["name"],
                "url":              body["url"],
                "source":           "known",
                "scrape_type":      body.get("scrape_type", "html_table"),
                "member_count_est": body.get("member_count_est"),
                "discoverable":     body.get("scrape_type") not in ("blocked",),
                "sic_hints":        body.get("sic_hints", []),
                "note":             body.get("note", ""),
            })
            found_urls.add(body["url"])

    # ── Step 2: Web search for unknown/additional bodies ──────────────────────
    if not HAS_BS4:
        print("  ℹ️  BeautifulSoup not installed — skipping web search for trade bodies.")
        return results

    queries = [
        f'UK "{sector_text}" trade association member list',
        f'UK "{sector_text}" industry association members directory',
        f'"{sector_text}" registered members UK trade body',
    ]

    web_candidates = []
    for q in queries[:2]:  # Limit to 2 queries to stay fast
        hits = _ddg_search(q, max_results=8)
        for h in hits:
            if h["url"] not in found_urls:
                score = _score_result(h["title"], h["url"], h["snippet"])
                if score >= 4:
                    web_candidates.append({**h, "_score": score})
                    found_urls.add(h["url"])
        time.sleep(0.5)

    # Sort by score, deduplicate by domain
    web_candidates.sort(key=lambda x: -x["_score"])
    seen_domains = set()
    for c in web_candidates:
        domain = urlparse(c["url"]).netloc.lower().replace("www.", "")
        if domain in seen_domains:
            continue
        seen_domains.add(domain)
        results.append({
            "key":              f"WEB_{domain.replace('.', '_').upper()[:20]}",
            "name":             c["title"],
            "url":              c["url"],
            "source":           "web",
            "scrape_type":      "html_generic",
            "member_count_est": None,
            "discoverable":     True,
            "sic_hints":        [],
            "note":             c["snippet"][:120] if c["snippet"] else "",
        })
        if len(results) >= max_bodies + len([r for r in results if r["source"] == "known"]):
            break

    return results


# ── Generic member list scraper ───────────────────────────────────────────────

def _scrape_member_names(url: str) -> list[str]:
    """
    Scrape a trade body member list page and return a list of company name strings.
    Uses multiple strategies in order of reliability:
      1. HTML <table> — most structured
      2. <ul>/<ol> with member-like class names
      3. <div>/<li> cards (member grids)
      4. Links to individual member pages
      5. Any text that looks like a company name (contains Ltd/Limited etc.)
    """
    if not HAS_BS4:
        return []

    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"  ⚠️  Fetch failed for {url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    # Remove nav, footer, header noise
    for tag in soup(["nav", "footer", "header", "script", "style", "noscript"]):
        tag.decompose()

    names = []

    # ── Strategy 1: Tables ────────────────────────────────────────────────────
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue
        # Determine name column: first column that looks like company names
        for row in rows[1:]:  # Skip header
            cells = row.find_all(["td", "th"])
            if cells:
                candidate = cells[0].get_text(strip=True)
                if _looks_like_company(candidate):
                    names.append(candidate)

    if len(names) >= 5:
        return _dedupe(names)

    # ── Strategy 2: Lists with member class hints ─────────────────────────────
    for ul in soup.find_all(["ul", "ol"]):
        cls = " ".join(ul.get("class", [])).lower()
        if any(sig in cls for sig in ["member", "company", "directory", "list"]):
            for li in ul.find_all("li"):
                text = li.get_text(strip=True)
                if _looks_like_company(text):
                    names.append(text)

    if len(names) >= 5:
        return _dedupe(names)

    # ── Strategy 3: Div/article cards ────────────────────────────────────────
    for el in soup.find_all(["div", "article", "section", "li"]):
        cls = " ".join(el.get("class", [])).lower()
        if any(sig in cls for sig in ["member", "company", "contractor",
                                       "firm", "card", "entry", "item",
                                       "directory", "listing"]):
            # Get the heading or first strong/bold text inside
            heading = el.find(["h1", "h2", "h3", "h4", "h5", "strong", "b"])
            if heading:
                text = heading.get_text(strip=True)
            else:
                text = el.get_text(strip=True)[:80]
            if _looks_like_company(text):
                names.append(text)

    if len(names) >= 5:
        return _dedupe(names)

    # ── Strategy 4: Links to member profile pages ─────────────────────────────
    base_domain = urlparse(url).netloc
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(url, href)
        if urlparse(full_url).netloc != base_domain:
            continue
        href_l = href.lower()
        if any(sig in href_l for sig in ["member", "company", "profile",
                                          "contractor", "firm"]):
            text = a.get_text(strip=True)
            if _looks_like_company(text):
                names.append(text)

    if len(names) >= 5:
        return _dedupe(names)

    # ── Strategy 5: Brute force — any company-like text in the page ──────────
    for el in soup.find_all(["p", "li", "td", "div", "span"]):
        if el.find(["p", "div", "ul", "ol", "table"]):
            continue  # Skip container elements
        text = el.get_text(strip=True)
        if _looks_like_company(text) and len(text) < 100:
            names.append(text)

    return _dedupe(names)


def _looks_like_company(text: str) -> bool:
    """Heuristic: does this string look like a UK company name?"""
    if not text or len(text) < 4 or len(text) > 120:
        return False
    t = text.lower()
    # Must contain at least one of these company-type indicators
    company_indicators = [
        " ltd", " limited", " llp", " plc", " llc",
        " group", " services", " solutions", " systems",
        " engineering", " contractors", " associates",
        " co.", " & co", "lifts", "lift ", "elevat",
    ]
    if any(ind in t for ind in company_indicators):
        return True
    # Or: Title Case with 2+ words (likely a proper name)
    words = text.split()
    if (len(words) >= 2 and
            sum(1 for w in words if w and w[0].isupper()) >= len(words) * 0.6):
        return True
    return False


def _dedupe(names: list[str]) -> list[str]:
    """Deduplicate names, preserving order."""
    seen, out = set(), []
    for n in names:
        key = re.sub(r"\s+", " ", n.lower().strip())
        if key and key not in seen:
            seen.add(key)
            out.append(n.strip())
    return out


# ── Companies House cross-reference ──────────────────────────────────────────

def _ch_lookup(company_name: str, ch_api_key: str) -> dict | None:
    """
    Search CH by name, return best active match dict or None.
    """
    try:
        r = requests.get(
            "https://api.company-information.service.gov.uk/search/companies",
            params={"q": company_name, "items_per_page": 5},
            auth=(ch_api_key, ""),
            timeout=10,
        )
        if r.status_code != 200:
            return None
        items = r.json().get("items", [])
        if not items:
            return None

        # Prefer active companies; prefer closer name matches
        def _name_sim(a: str, b: str) -> float:
            a_words = set(re.sub(r"\b(ltd|limited|llp|plc|group|the)\b", "",
                                  a.lower()).split())
            b_words = set(re.sub(r"\b(ltd|limited|llp|plc|group|the)\b", "",
                                  b.lower()).split())
            if not a_words or not b_words:
                return 0.0
            return len(a_words & b_words) / max(len(a_words), len(b_words))

        active = [i for i in items if i.get("company_status") == "active"]
        pool   = active if active else items

        best = max(pool, key=lambda i: _name_sim(company_name, i.get("title", "")))

        return {
            "company_number":            best.get("company_number", ""),
            "company_name":              best.get("title", company_name),
            "company_status":            best.get("company_status", ""),
            "date_of_creation":          best.get("date_of_creation", ""),
            "sic_codes":                 best.get("sic_codes", []),
            "registered_office_address": best.get("address", {}),
        }

    except Exception:
        return None


# ── Main discovery entry point ────────────────────────────────────────────────

def discover_members_from_url(
    url: str,
    ch_api_key: str,
    body_name: str = "",
    body_key:  str = "",
    max_members: int = 300,
) -> list[dict]:
    """
    Scrape member names from `url` and cross-reference each with Companies House.
    Returns a list of standard pipeline company dicts with `source` = body_name.

    Works on any HTML member list — no hardcoded structure assumptions.
    """
    print(f"\n  🔍 Scraping members: {body_name or url} ...")
    names = _scrape_member_names(url)

    if not names:
        print(f"  ⚠️  No member names found at {url}")
        return []

    print(f"  Found {len(names)} candidate names → cross-referencing with Companies House ...")

    companies   = []
    found_nums  = set()
    no_match    = 0

    for i, name in enumerate(names[:max_members]):
        if i > 0 and i % 25 == 0:
            print(f"    [{i}/{min(len(names), max_members)}] CH lookups in progress ...")

        result = _ch_lookup(name, ch_api_key)

        if result and result.get("company_number"):
            num = result["company_number"]
            if num not in found_nums:
                found_nums.add(num)
                result["source"]             = body_name or body_key or "trade_body"
                result["trade_body"]         = body_key or body_name
                result["trade_body_name"]    = body_name
                result["trade_body_raw_name"] = name
                companies.append(result)
        else:
            no_match += 1

        time.sleep(0.15)  # Stay within CH API rate limit

    print(f"  {body_name}: {len(names)} scraped → "
          f"{len(companies)} CH matches ({no_match} not found)")
    return companies


def discover_from_trade_body(body: dict, ch_api_key: str) -> list[dict]:
    """
    High-level: given a body dict from find_trade_bodies(), scrape and return companies.
    Handles blocked/not-discoverable bodies gracefully.
    """
    if not body.get("discoverable", True):
        note = body.get("note", "not available for automated discovery")
        print(f"  ⚠️  {body['name']} skipped — {note}")
        return []

    return discover_members_from_url(
        url=body["url"],
        ch_api_key=ch_api_key,
        body_name=body["name"],
        body_key=body.get("key", ""),
    )


# ── Streamlit helper ──────────────────────────────────────────────────────────

def suggest_for_streamlit(sector_text: str) -> list[dict]:
    """
    Fast version for the Streamlit Phase-1 form: checks KNOWN_BODIES only
    (no web search, instant). Returns the same dict format as find_trade_bodies().
    Used for real-time checkbox suggestions as the user types a sector.
    """
    text_lower = sector_text.lower()
    results = []
    for key, body in KNOWN_BODIES.items():
        if any(kw.lower() in text_lower for kw in body.get("sector_keywords", [])):
            results.append({
                "key":              key,
                "name":             body["name"],
                "url":              body["url"],
                "source":           "known",
                "scrape_type":      body.get("scrape_type", "html_table"),
                "member_count_est": body.get("member_count_est"),
                "discoverable":     body.get("scrape_type") not in ("blocked",),
                "sic_hints":        body.get("sic_hints", []),
                "note":             body.get("note", ""),
            })
    return results


# ── CLI test harness ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    sector = " ".join(sys.argv[1:]) or "lift maintenance"
    print(f"\nFinding trade bodies for: '{sector}'")

    bodies = find_trade_bodies(sector)
    if not bodies:
        print("  No trade bodies found.")
    else:
        for b in bodies:
            disc = "✅ scrapeable" if b["discoverable"] else "🚫 blocked"
            src  = f"[{b['source']}]"
            cnt  = f"~{b['member_count_est']} members" if b["member_count_est"] else ""
            print(f"  {disc} {b['name']} {src} {cnt}")
            print(f"     {b['url']}")
