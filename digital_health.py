"""
digital_health.py — Digital Presence & Health Assessment

Assesses a company's digital maturity and online presence signals.
A weak/stale digital presence combined with sell intent signals suggests
the owner is not investing for future growth — classic pre-exit behaviour.

Checks:
  1. Domain age              — whois lookup (older domain = established presence)
  2. Website status          — HTTP 200 check
  3. LinkedIn presence       — detect company LinkedIn page link on website
  4. Job posting signals     — detect active hiring (growth vs stagnation)
  5. Social signals          — Twitter/X, Facebook links detected on website
  6. Digital health score    (0–100)
  7. Sector relevance score  — confirms company website matches the searched sector

Sector relevance score interpretation:
  Confirmed (70–100)   — Website/name clearly mentions the sector
  Likely    (40–69)    — Partial keyword match on website or name
  Uncertain (1–39)     — Website found but few/no sector keywords
  Unverified (0)       — No website to check; use SIC code as proxy

Data tiers:
  Tier 3 — Verified corporate website data
  Tier 4 — Derived / inferred
"""

import requests
import re
import json
import time
import os
from datetime import datetime
from urllib.parse import urlparse

import config as cfg

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PE-Research-Bot/1.0)",
    "Accept":     "text/html,application/xhtml+xml",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ── Domain age via WHOIS ──────────────────────────────────────────────────────

def get_domain_age_years(domain: str) -> float | None:
    """
    Use python-whois to get domain creation date, then compute age in years.
    Falls back to None on any error.
    """
    if not domain:
        return None
    try:
        import whois  # python-whois package
        w = whois.whois(domain)
        created = w.creation_date
        if isinstance(created, list):
            created = created[0]
        if created and isinstance(created, datetime):
            age = (datetime.now() - created).days / 365.25
            return round(age, 1)
    except Exception:
        pass
    return None


# ── Website content fetch ────────────────────────────────────────────────────

def fetch_website_content(url: str, timeout: int = 10) -> str | None:
    """
    Fetch homepage HTML. Returns text or None on failure.
    """
    if not url:
        return None
    if not url.startswith("http"):
        url = "https://" + url
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r.text[:50_000]  # cap at 50KB
    except Exception:
        pass
    # Try www variant if bare domain
    try:
        parsed = urlparse(url)
        if not parsed.netloc.startswith("www."):
            alt = f"{parsed.scheme}://www.{parsed.netloc}{parsed.path}"
            r2  = SESSION.get(alt, timeout=timeout, allow_redirects=True)
            if r2.status_code == 200:
                return r2.text[:50_000]
    except Exception:
        pass
    return None


# ── Social / LinkedIn detection ───────────────────────────────────────────────

LINKEDIN_PATTERNS = [
    r"linkedin\.com/company/[a-z0-9\-]+",
    r"linkedin\.com/in/[a-z0-9\-]+",
]
TWITTER_PATTERNS = [
    r"twitter\.com/[a-zA-Z0-9_]+",
    r"x\.com/[a-zA-Z0-9_]+",
]
FACEBOOK_PATTERN = r"facebook\.com/[a-zA-Z0-9.\-]+"

JOB_PATTERNS = [
    r"(careers|jobs|vacancies|join.?us|we.?are.?hiring|current.?openings)",
    r"(job.?opportunities|work.?with.?us|open.?positions|recruitment)",
]

ACCREDITATION_PATTERNS = [
    r"\bISO\s*\d{4,5}\b",
    r"\bUKAS\b",
    r"\bCHAS\b",
    r"\bSafe\s*Contractor\b",
    r"\bConstruction\s*Line\b",
    r"\bISO\s*9001\b",
    r"\bISO\s*14001\b",
    r"\bISO\s*27001\b",
    r"\bCQC\b",
    r"\bOfsted\b",
    r"\bFCA\s*(authoris|regulat)",
    r"\bGas\s*Safe\b",
    r"\bNICEIC\b",
    r"\bELECSA\b",
    r"\bNAPIT\b",
]


def extract_meta_description(html: str) -> str:
    """
    Extract a plain-English description of the company from the page HTML.
    Priority: og:description → meta description → first non-trivial <h1> text.
    Returns empty string if nothing useful found.
    """
    if not html:
        return ""
    # og:description (often the richest)
    m = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']{20,300})["\']',
                  html, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Standard meta description
    m = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{20,300})["\']',
                  html, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Reverse attribute order variant
    m = re.search(r'<meta[^>]+content=["\']([^"\']{20,300})["\'][^>]+name=["\']description["\']',
                  html, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Fallback: first <h1> text
    m = re.search(r'<h1[^>]*>([^<]{15,120})</h1>', html, re.IGNORECASE)
    if m:
        return re.sub(r'\s+', ' ', m.group(1)).strip()
    return ""


def extract_social_signals(html: str) -> dict:
    """Parse HTML for social media links and job signals."""
    html_lower = html.lower()

    linkedin_matches = re.findall(LINKEDIN_PATTERNS[0], html, re.IGNORECASE)
    twitter_matches  = re.findall(TWITTER_PATTERNS[0],  html, re.IGNORECASE) + \
                       re.findall(TWITTER_PATTERNS[1],  html, re.IGNORECASE)
    facebook_matches = re.findall(FACEBOOK_PATTERN, html, re.IGNORECASE)

    # Jobs / hiring signals
    has_jobs = any(
        re.search(pat, html_lower)
        for pat in JOB_PATTERNS
    )

    # Accreditation mentions
    accreditations = []
    for pat in ACCREDITATION_PATTERNS:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            accreditations.append(m.group(0).strip())

    return {
        "linkedin_url":     f"https://{linkedin_matches[0]}" if linkedin_matches else None,
        "twitter_url":      f"https://{twitter_matches[0]}"  if twitter_matches  else None,
        "facebook_url":     f"https://{facebook_matches[0]}" if facebook_matches else None,
        "has_linkedin":     bool(linkedin_matches),
        "has_twitter":      bool(twitter_matches),
        "has_facebook":     bool(facebook_matches),
        "has_job_postings": has_jobs,
        "accreditations_on_site": list(set(accreditations))[:10],
    }


# ── Digital Health Score ──────────────────────────────────────────────────────

def digital_health_score(
    domain_age:    float | None,
    website_live:  bool,
    has_linkedin:  bool,
    has_jobs:      bool,
    has_twitter:   bool,
    has_facebook:  bool,
) -> dict:
    """
    Digital Health Score (0–100).

    Website live:       +30
    Domain age ≥10yr:   +20  |  5-9yr: +15  |  2-4yr: +8
    LinkedIn presence:  +20
    Active job postings:+15
    Social presence:    +15 (split: Twitter/X +8, Facebook +7)
    """
    score   = 0
    signals = []

    # Website live
    if website_live:
        score += 30
    else:
        signals.append("Website unreachable — poor digital presence")

    # Domain age
    if domain_age is not None:
        if   domain_age >= 10: score += 20; signals.append(f"Domain age {domain_age:.0f} yrs — established online presence")
        elif domain_age >= 5:  score += 15; signals.append(f"Domain age {domain_age:.1f} yrs — moderate presence")
        elif domain_age >= 2:  score += 8;  signals.append(f"Domain age {domain_age:.1f} yrs — relatively new")
        else:                  signals.append(f"Domain age {domain_age:.1f} yrs — very new")
    else:
        score += 5  # unknown age, slight credit
        signals.append("Domain age unknown — WHOIS lookup failed")

    # LinkedIn
    if has_linkedin:
        score += 20
        signals.append("LinkedIn company page linked — professional digital presence")
    else:
        signals.append("No LinkedIn presence detected — limited professional network")

    # Job postings
    if has_jobs:
        score += 15
        signals.append("Active job postings detected — company is hiring / growing")
    else:
        signals.append("No job postings detected — possible growth stagnation")

    # Social
    if has_twitter:
        score += 8
    if has_facebook:
        score += 7

    score = min(score, 100)

    if   score >= 80: band = "Mature"
    elif score >= 60: band = "Adequate"
    elif score >= 40: band = "Below Average"
    else:             band = "Poor"

    return {
        "digital_health_score": score,
        "digital_health_band":  band,
        "digital_signals":      signals,
    }


# ── Sector Relevance Score ────────────────────────────────────────────────────

def sector_relevance_score(
    company_name: str,
    html:         str | None,
    name_queries: list[str],
    include_stems: list[str],
    exclude_terms: list[str],
) -> dict:
    """
    Score how well this company's name + website content matches the sector.

    Scoring (max 100):
      +40  Company name contains a sector keyword / stem
      +20  Website content has ≥3 distinct sector keyword hits
      +15  Website content has 1–2 sector keyword hits
      +15  Company name contains a secondary stem match
      -25  Company name or content contains an exclusion term

    Returns:
      sector_relevance_score   int 0–100
      sector_relevance_label   "Confirmed" | "Likely" | "Uncertain" | "Unverified"
      sector_match_signals     list[str]  — what was found / not found
    """
    name_lower    = (company_name or "").lower()
    content_lower = re.sub(r"<[^>]+>", " ", html or "").lower()  # strip HTML tags
    signals: list[str] = []
    score = 0

    # ── 1. Company name check ─────────────────────────────────────────────────
    name_hit = False
    for kw in name_queries:
        if kw.lower() in name_lower:
            signals.append(f"Company name contains '{kw}'")
            score += 40
            name_hit = True
            break

    if not name_hit:
        for stem in include_stems:
            if stem.lower() in name_lower:
                signals.append(f"Company name contains stem '{stem}'")
                score += 15
                break

    # ── 2. Website content check ──────────────────────────────────────────────
    if html:
        matched_kws: list[str] = []
        for kw in name_queries:
            if kw.lower() in content_lower and kw not in matched_kws:
                matched_kws.append(kw)
        for stem in include_stems:
            if stem.lower() in content_lower and stem not in matched_kws:
                matched_kws.append(stem)

        if len(matched_kws) >= 3:
            signals.append(f"Website mentions {len(matched_kws)} sector terms: {', '.join(matched_kws[:5])}")
            score += 30   # name(40) + website(30) = 70 → Confirmed
        elif len(matched_kws) >= 1:
            signals.append(f"Website mentions sector term(s): {', '.join(matched_kws)}")
            score += 15
        else:
            signals.append("Website found but no sector keywords detected")
    else:
        signals.append("No website — sector relevance based on SIC code only")

    # ── 3. Exclusion check ────────────────────────────────────────────────────
    for excl in exclude_terms:
        excl_l = excl.lower()
        hit_name    = excl_l in name_lower
        hit_content = excl_l in content_lower[:5000]  # check first 5KB of content
        if hit_name or hit_content:
            signals.append(f"Exclusion term '{excl}' found — possible false positive")
            score -= 25
            break  # one penalty per company

    score = max(0, min(100, score))

    if   score >= 70: label = "Confirmed"
    elif score >= 40: label = "Likely"
    elif html:        label = "Uncertain"
    else:             label = "Unverified"

    return {
        "sector_relevance_score": score,
        "sector_relevance_label": label,
        "sector_match_signals":   signals,
    }


# ── Main enrichment function ──────────────────────────────────────────────────

def enrich_digital(company: dict) -> dict:
    """
    Full digital health enrichment for one company.
    Uses website URL from contacts enrichment (if available).
    """
    website = (
        company.get("contacts", {}).get("website")
        or company.get("website")
        or ""
    )

    domain = ""
    if website:
        try:
            parsed = urlparse(website if "://" in website else "https://" + website)
            domain = parsed.netloc.replace("www.", "")
        except Exception:
            domain = ""

    # Domain age
    domain_age = get_domain_age_years(domain) if domain else None

    # Fetch website
    html         = fetch_website_content(website) if website else None
    website_live = html is not None

    social = extract_social_signals(html) if html else {
        "linkedin_url": None, "twitter_url": None, "facebook_url": None,
        "has_linkedin": False, "has_twitter": False, "has_facebook": False,
        "has_job_postings": False, "accreditations_on_site": [],
    }

    dhs = digital_health_score(
        domain_age   = domain_age,
        website_live = website_live,
        has_linkedin = social["has_linkedin"],
        has_jobs     = social["has_job_postings"],
        has_twitter  = social["has_twitter"],
        has_facebook = social["has_facebook"],
    )

    # ── Sector relevance check (reuses already-fetched HTML, zero extra cost) ──
    srs = sector_relevance_score(
        company_name  = company.get("company_name", ""),
        html          = html,
        name_queries  = getattr(cfg, "NAME_QUERIES",   []),
        include_stems = getattr(cfg, "INCLUDE_STEMS",  []),
        exclude_terms = getattr(cfg, "EXCLUDE_TERMS",  []),
    )

    # Extract plain-English description from homepage (zero extra HTTP cost)
    website_desc = extract_meta_description(html) if html else ""

    return {
        "domain":               domain,
        "domain_age_years":     domain_age,
        "website_live":         website_live,
        "website_description":  website_desc,
        **social,
        **dhs,
        **srs,
        "data_tier":            "Tier 3 — Website analysis / WHOIS",
    }


def run():
    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(enriched_path) as f:
        companies = json.load(f)

    top_n     = getattr(cfg, "DIGITAL_TOP_N", 75)
    to_enrich = companies[:top_n]
    skipped   = len(companies) - top_n

    print(f"\nDigital health assessment for top {len(to_enrich)} companies"
          f" ({skipped} skipped)...")

    def _enrich_one_digital(c):
        from concurrent_pipeline import rate_limited_sleep
        result = enrich_digital(c)
        c["digital_health"] = result
        rate_limited_sleep()
        return c

    live_count  = 0
    linked_count = 0

    if len(to_enrich) > 1:
        from concurrent_pipeline import process_batch
        to_enrich = process_batch(
            items=to_enrich,
            func=_enrich_one_digital,
            max_workers=min(8, len(to_enrich)),
            description="Digital health (domain, LinkedIn, jobs)",
        )
        to_enrich = [c for c in to_enrich if c is not None]
        companies[:top_n] = to_enrich
    else:
        for i, c in enumerate(to_enrich):
            if i % 10 == 0:
                print(f"  [{i+1}/{len(to_enrich)}] processing {c['company_name'][:40]}...")
            _enrich_one_digital(c)

    for c in to_enrich:
        dh = c.get("digital_health", {})
        if dh.get("website_live"):  live_count  += 1
        if dh.get("has_linkedin"): linked_count += 1

    for c in companies[top_n:]:
        c["digital_health"] = {"digital_health_score": None, "digital_health_band": "Not assessed", "data_tier": "N/A"}

    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(out_path, "w") as f:
        json.dump(companies, f, indent=2)

    print(f"\nDigital health complete → {out_path}")
    print(f"  Live websites: {live_count} / {len(to_enrich)}  |  LinkedIn detected: {linked_count} / {len(to_enrich)}")
    return companies


if __name__ == "__main__":
    run()
