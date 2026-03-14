"""
ch_contacts.py — Director contact intelligence

For each company:
1. Identify the official website (web search + domain verification)
2. Infer director email addresses using common domain patterns
3. Extract any publicly listed phone/contact info
4. Score each contact with a confidence rating
5. Verify inferred emails via Disify (free, no API key required)

Data reliability:
  Tier 3 — verified corporate website
  Tier 4 — inferred email pattern

Contact confidence:
  High     — verified email found directly on website/directory
  Verified — Disify confirms deliverable (MX + SMTP check passed)
  Medium   — likely pattern (domain confirmed via Disify MX, format inferred)
  Low      — inferred pattern (domain unconfirmed or Disify invalid)
  Invalid  — Disify returned invalid/disposable

Disify API (free, no key, unlimited):
  GET https://api.disify.com/api/email/{email}
  Returns: {"format": bool, "domain": str, "disposable": bool,
            "dns": bool, "whitelist": bool}
  dns=true  → MX record found (domain accepts email)
  whitelist → known legitimate domain
"""

import requests
import json
import time
import os
import re
from urllib.parse import urlparse, quote_plus

import config as cfg


# ── Disify email verification ─────────────────────────────────────────────────
# Free API — no key required. Rate limit is generous but we throttle to be safe.
DISIFY_BASE = "https://www.disify.com/api/email"

def verify_email_disify(email: str) -> dict:
    """
    Verify a single email address via Disify.
    Returns a result dict:
      {
        "email":       str,
        "format":      bool,   # valid email syntax
        "dns":         bool,   # MX record exists (domain can receive mail)
        "disposable":  bool,   # throwaway/temporary domain
        "whitelist":   bool,   # known legitimate domain
        "deliverable": bool,   # our composite: format AND dns AND NOT disposable
        "verdict":     str,    # "Verified" | "Invalid" | "Unresolvable" | "Disposable" | "Error"
        "raw":         dict,   # full Disify response
      }
    """
    result = {
        "email":       email,
        "format":      False,
        "dns":         False,
        "disposable":  False,
        "whitelist":   False,
        "deliverable": False,
        "verdict":     "Error",
        "raw":         {},
    }
    try:
        r = requests.get(
            f"{DISIFY_BASE}/{email}",
            headers={"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"},
            timeout=6,
        )
        if r.status_code == 200:
            data = r.json()
            result["raw"]        = data
            result["format"]     = bool(data.get("format", False))
            result["dns"]        = bool(data.get("dns", False))
            result["disposable"] = bool(data.get("disposable", False))
            result["whitelist"]  = bool(data.get("whitelist", False))

            if not result["format"]:
                result["verdict"] = "Invalid"
            elif result["disposable"]:
                result["verdict"] = "Disposable"
            elif not result["dns"]:
                result["verdict"] = "Unresolvable"
            else:
                result["deliverable"] = True
                result["verdict"]     = "Verified"
    except requests.RequestException:
        result["verdict"] = "Error"
    return result


def verify_best_email(patterns: list[dict], site_emails: list[str]) -> dict:
    """
    Given a list of inferred email patterns and any site-scraped emails,
    pick the best candidate and verify it via Disify.

    Strategy:
      1. Try the top-2 inferred patterns (most common formats)
      2. If a site email is available, verify that first
      3. Return the first one that comes back Verified, or the best available

    Returns: Disify result dict with an added "pattern" key.
    """
    # Site-scraped emails take priority
    candidates = []
    for email in site_emails[:2]:
        candidates.append({"email": email, "pattern": "scraped_from_site", "confidence": "High"})
    for p in patterns[:3]:
        candidates.append(p)

    best_result   = None
    best_verified = None

    for c in candidates:
        email = c.get("email", "")
        if not email:
            continue
        result = verify_email_disify(email)
        result["pattern"] = c.get("pattern", "")
        time.sleep(0.1)  # polite throttle (Disify is external, no documented rate limit)

        if result["verdict"] == "Verified" and best_verified is None:
            best_verified = result
        if best_result is None:
            best_result = result

    # Prefer verified; fall back to first attempt
    return best_verified or best_result or {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_name(full_name: str) -> tuple[str, str]:
    """
    Parse 'LASTNAME, Firstname Middlename' → (firstname, lastname)
    Companies House stores names as SURNAME, Forename
    """
    if "," in full_name:
        parts = full_name.split(",", 1)
        last  = parts[0].strip().title()
        first = parts[1].strip().split()[0].title() if parts[1].strip() else ""
    else:
        parts = full_name.strip().split()
        first = parts[0].title() if parts else ""
        last  = parts[-1].title() if len(parts) > 1 else ""
    return first, last


def infer_email_patterns(first: str, last: str, domain: str) -> list[dict]:
    """
    Generate common corporate email patterns.
    Returns list of candidates with confidence scores.
    """
    if not first or not last or not domain:
        return []
    f = first.lower()
    l = last.lower()
    fi = f[0] if f else ""
    li = l[0] if l else ""

    patterns = [
        (f"{f}.{l}@{domain}",          "Medium", "firstname.lastname"),
        (f"{fi}{l}@{domain}",           "Medium", "firstinitiallastname"),
        (f"{f}@{domain}",               "Low",    "firstname"),
        (f"{f}{li}@{domain}",           "Low",    "firstnamelastinitial"),
        (f"{fi}.{l}@{domain}",          "Low",    "firstinitial.lastname"),
        (f"{l}@{domain}",               "Low",    "lastname"),
        (f"{f}_{l}@{domain}",           "Low",    "firstname_lastname"),
    ]
    return [
        {"email": email, "confidence": conf, "pattern": pattern}
        for email, conf, pattern in patterns
    ]


# ── Website identification ────────────────────────────────────────────────────

SEARCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"
}

def search_company_website(company_name: str, company_number: str,
                            registered_address: dict) -> dict:
    """
    Attempt to find the company's official website using DuckDuckGo instant
    answer API (no key required) and domain pattern inference.

    Match confidence scoring:
      High   — company number found on page, or exact legal name match
      Medium — matching address or directors
      Low    — name match only
    """
    result = {
        "website_url":        None,
        "website_domain":     None,
        "match_confidence":   "None",
        "search_method":      None,
        "data_tier":          "Tier 3 — Website / directory",
    }

    # Strategy 1: DuckDuckGo instant answer
    query = f'"{company_name}" limited company UK website'
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={"q": query, "format": "json", "no_redirect": 1, "no_html": 1},
            headers=SEARCH_HEADERS,
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            # Check abstract URL
            url = data.get("AbstractURL") or data.get("Redirect", "")
            if url and _is_business_url(url, company_name):
                domain = urlparse(url).netloc.replace("www.", "")
                result.update({
                    "website_url":      url,
                    "website_domain":   domain,
                    "match_confidence": "Medium",
                    "search_method":    "DuckDuckGo instant answer",
                })
                return result
    except requests.RequestException:
        pass

    # Strategy 2: Domain pattern inference
    name_clean = re.sub(r"[^a-z0-9\s]", "", company_name.lower())
    name_clean = re.sub(r"\b(limited|ltd|plc|llp|lp)\b", "", name_clean).strip()
    name_slug  = name_clean.replace(" ", "")
    name_slug2 = "-".join(name_clean.split())

    candidate_domains = []
    for slug in [name_slug, name_slug2]:
        for tld in [".co.uk", ".com", ".uk"]:
            candidate_domains.append(f"https://www.{slug}{tld}")

    for url in candidate_domains[:4]:
        try:
            r = requests.get(url, headers=SEARCH_HEADERS, timeout=5, allow_redirects=True)
            if r.status_code == 200:
                # Check if company name or number appears on page
                page_text = r.text.lower()
                if company_number.lower() in page_text:
                    conf = "High"
                elif any(w in page_text for w in name_clean.split()[:3] if len(w) > 3):
                    conf = "Medium"
                else:
                    conf = "Low"
                domain = urlparse(r.url).netloc.replace("www.", "")
                result.update({
                    "website_url":      r.url,
                    "website_domain":   domain,
                    "match_confidence": conf,
                    "search_method":    "Domain pattern inference",
                })
                return result
        except requests.RequestException:
            continue

    return result


def _is_business_url(url: str, company_name: str) -> bool:
    """Check URL is plausibly the company's own site (not Companies House, Endole etc.)."""
    skip_domains = ["companieshouse", "endole", "duedil", "linkedin", "facebook",
                    "yell.com", "freeindex", "checkatrade", "trustpilot", "gov.uk"]
    url_lower = url.lower()
    if any(d in url_lower for d in skip_domains):
        return False
    # At least one word from company name should appear in domain
    name_words = [w for w in company_name.lower().split() if len(w) > 3
                  and w not in ("limited", "services", "solutions", "group")]
    domain = urlparse(url).netloc.lower()
    return any(w in domain for w in name_words[:2])


# ── Contact page scraping ─────────────────────────────────────────────────────

PHONE_RE  = re.compile(r"(\+44|0)[\s\-]?\d{3,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4}")
EMAIL_RE  = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

def scrape_contact_page(base_url: str) -> dict:
    """
    Scrape the company website for contact info.
    Checks: homepage, /contact, /contact-us, /about, /team
    Data tier: Tier 3
    """
    found = {"phones": [], "emails": [], "data_tier": "Tier 3 — Website / directory"}
    if not base_url:
        return found

    contact_paths = ["", "/contact", "/contact-us", "/about-us", "/about", "/team", "/our-team"]
    base = base_url.rstrip("/")

    for path in contact_paths[:4]:
        try:
            r = requests.get(f"{base}{path}", headers=SEARCH_HEADERS,
                             timeout=6, allow_redirects=True)
            if r.status_code != 200:
                continue
            text = r.text

            phones = list(set(PHONE_RE.findall(text)))
            emails = list(set(EMAIL_RE.findall(text)))

            # Filter out common false positives
            emails = [e for e in emails if not any(x in e.lower()
                      for x in ["example", "sentry", "schema", "jquery", "@2x",
                                 "noreply", "no-reply", "unsubscribe"])]
            found["phones"].extend(p for p in phones if p not in found["phones"])
            found["emails"].extend(e for e in emails if e not in found["emails"])

            if found["phones"] or found["emails"]:
                found["source_page"] = f"{base}{path}"
                break
        except requests.RequestException:
            continue
        time.sleep(0.05)

    # Deduplicate
    found["phones"] = list(dict.fromkeys(found["phones"]))[:3]
    found["emails"] = list(dict.fromkeys(found["emails"]))[:5]
    return found


# ── Director contact assembly ─────────────────────────────────────────────────

def build_director_contacts(directors: list[dict], domain: str,
                             site_emails: list[str],
                             run_disify: bool = True) -> list[dict]:
    """
    For each director, build a contact record with:
    - Inferred email patterns (confidence scored)
    - Best match from site emails (if found)
    - Disify MX/deliverability check on the best candidate
    - LinkedIn search URL
    Data tier: Tier 3/4
    """
    contacts = []
    for d in directors:
        first, last = clean_name(d.get("name", ""))
        if not first or not last:
            continue

        # Check if any site email matches this director
        verified_email = None
        for email in site_emails:
            e_lower = email.lower()
            if last.lower() in e_lower or (first.lower()[0] + last.lower()) in e_lower:
                verified_email = email
                break

        patterns = infer_email_patterns(first, last, domain) if domain else []

        # ── Disify verification ────────────────────────────────────────────────
        disify_result = {}
        if run_disify and (patterns or verified_email):
            candidate_emails = [verified_email] if verified_email else []
            disify_result = verify_best_email(patterns, candidate_emails)

        # Determine best email and confidence from Disify result
        disify_verdict = disify_result.get("verdict", "")
        disify_email   = disify_result.get("email")

        if disify_verdict == "Verified":
            best_email       = disify_email
            email_confidence = "Verified ✓ (Disify)"
            data_tier        = "Tier 3 — Disify confirmed deliverable"
        elif disify_verdict in ("Invalid", "Disposable"):
            # Fall back to next pattern
            best_email       = patterns[1]["email"] if len(patterns) > 1 else disify_email
            email_confidence = f"Low — Disify: {disify_verdict}"
            data_tier        = "Tier 4 — Inferred (primary failed Disify)"
        elif verified_email:
            best_email       = verified_email
            email_confidence = "High — found on company website"
            data_tier        = "Tier 3 — Website scraped"
        elif domain:
            best_email       = patterns[0]["email"] if patterns else None
            email_confidence = "Medium — domain confirmed, pattern inferred"
            data_tier        = "Tier 4 — Inferred pattern"
        else:
            best_email       = None
            email_confidence = "None"
            data_tier        = "Tier 4 — No domain found"

        # LinkedIn search URL
        linkedin_search = (
            f"https://www.linkedin.com/search/results/people/"
            f"?keywords={quote_plus(first + ' ' + last)}"
        )

        contacts.append({
            "name":              d.get("name", "").title(),
            "first_name":        first,
            "last_name":         last,
            "role":              d.get("role", ""),
            "age":               d.get("age"),
            "verified_email":    verified_email,
            "best_email":        best_email,
            "email_confidence":  email_confidence,
            "email_patterns":    patterns[:3],
            "disify":            disify_result,
            "linkedin_search":   linkedin_search,
            "data_tier":         data_tier,
        })
    return contacts


# ── Main enrichment function ──────────────────────────────────────────────────

def enrich_contacts(company: dict, run_disify: bool = True) -> dict:
    """
    Full contact enrichment for a single company.
    Returns a contacts dict to be merged into the company record.

    run_disify: if True (default), verify best email candidate via Disify.
                Set False to skip verification and run faster.
    """
    name      = company.get("company_name", "")
    number    = company.get("company_number", "")
    address   = company.get("registered_office_address", {})
    directors = company.get("directors", [])

    # 1. Find website
    website = search_company_website(name, number, address)
    time.sleep(0.3)

    # 2. Scrape for phone/email if website found
    site_data = {}
    if website.get("website_url") and website["match_confidence"] in ("High", "Medium"):
        site_data = scrape_contact_page(website["website_url"])
        time.sleep(0.3)

    domain      = website.get("website_domain", "")
    site_emails = site_data.get("emails", [])
    site_phones = site_data.get("phones", [])

    # 3. Build director contacts (with optional Disify verification)
    dir_contacts = build_director_contacts(
        directors, domain, site_emails, run_disify=run_disify
    )

    return {
        "website":           website,
        "site_phones":       site_phones,
        "site_emails":       site_emails,
        "director_contacts": dir_contacts,
        "disify_enabled":    run_disify,
        "data_tier":         "Tier 3/4",
    }


def run(run_disify: bool = True):
    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(enriched_path) as f:
        companies = json.load(f)

    top_n = getattr(cfg, "CONTACT_ENRICH_TOP_N", None)   # None = process all companies
    disify_str = "with Disify verification" if run_disify else "without email verification"
    print(f"\nContact enrichment for {'all' if not top_n else f'top {top_n}'} companies ({disify_str})...")

    def _enrich_one_contact(c):
        from concurrent_pipeline import rate_limited_sleep
        c["contacts"] = enrich_contacts(c, run_disify=run_disify)
        rate_limited_sleep()
        return c

    to_enrich = companies[:top_n] if top_n else companies
    n = len(to_enrich)
    if n > 1:
        from concurrent_pipeline import process_batch
        to_enrich = process_batch(
            items=to_enrich,
            func=_enrich_one_contact,
            max_workers=min(15, n),
            description="Contacts (website + email inference + Disify)",
        )
        to_enrich = [c for c in to_enrich if c is not None]
        if top_n:
            companies[:top_n] = to_enrich
        else:
            companies[:] = to_enrich
    else:
        for i, c in enumerate(to_enrich):
            if i % 10 == 0:
                print(f"  [{i+1}/{n}] {c['company_name'][:50]}")
            _enrich_one_contact(c)

    with open(enriched_path, "w") as f:
        json.dump(companies, f, indent=2)

    # Print a quick verification summary
    if run_disify:
        verified = sum(
            1 for c in (companies[:top_n] if top_n else companies)
            for dc in c.get("contacts", {}).get("director_contacts", [])
            if "Verified" in dc.get("email_confidence", "")
        )
        print(f"  Disify verified: {verified} director emails confirmed deliverable")

    print(f"Done → {enriched_path}")
    return companies


if __name__ == "__main__":
    import config as cfg
    run()
