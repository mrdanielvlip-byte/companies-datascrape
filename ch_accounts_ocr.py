#!/usr/bin/env python3
"""
ch_accounts_ocr.py — CH Document API + OCR accounts extractor

Pipeline Step 4b — runs after ch_financials.py (Step 4).
For each company where accounts are filed and accessible:
  1. Fetch latest filing from CH filing history API
  2. Download PDF via CH Document API
  3. OCR using Tesseract (image-based PDFs) or text extract (digital PDFs)
  4. Parse P&L and balance sheet figures with regex
  5. Merge actual figures back into the enriched companies JSON
     (replaces revenue estimates with actual values where available)

Priority tiers:
  A — full / medium / group / small / large  (P&L + balance sheet)
  B — total-exemption-full / unaudited-abridged  (balance sheet only)
  C — micro-entity / dormant / unknown → skip (no useful data)

Integrates with run.py:
  - Reads:  cfg.OUTPUT_DIR / cfg.ENRICHED_JSON
  - Writes: cfg.OUTPUT_DIR / cfg.ENRICHED_JSON  (updated in place)
  - Also writes: cfg.OUTPUT_DIR / "accounts_ocr.json" (raw extraction cache)

Standalone usage:
  python ch_accounts_ocr.py                     # uses default config.py
  python ch_accounts_ocr.py --resume            # skip already-done companies
"""

import json, re, time, os, sys, subprocess, tempfile, io
import requests
import fitz          # PyMuPDF

# ── API auth ───────────────────────────────────────────────────────────────────
def _load_api_key():
    key_file = os.path.join(os.path.dirname(__file__), ".ch_api_key")
    if os.path.exists(key_file):
        with open(key_file) as f:
            for line in f:
                if "=" in line:
                    return line.strip().split("=", 1)[1].strip()
    return os.environ.get("COMPANIES_HOUSE_API_KEY", "")

API_KEY = _load_api_key()

BASE     = "https://api.company-information.service.gov.uk"
DOC_BASE = "https://document-api.company-information.service.gov.uk"
AUTH     = (API_KEY, "")

# ── priority tiers ─────────────────────────────────────────────────────────────
TIER_A = {"full", "medium", "group", "small", "small-full", "large"}
TIER_B = {"total-exemption-full", "unaudited-abridged", "total-exemption-small"}
SKIP   = {"dormant", "micro-entity", "no-accounts-type-available", "audit-exemption-subsidiary",
          "null", "none", "unknown"}

# ── helpers ────────────────────────────────────────────────────────────────────
def ch_get(path, retries=3):
    url = BASE + path if path.startswith("/") else path
    for _ in range(retries):
        try:
            r = requests.get(url, auth=AUTH, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(3)
        except Exception:
            time.sleep(1)
    return {}

def doc_get_meta(url, retries=3):
    for _ in range(retries):
        try:
            r = requests.get(url, auth=AUTH, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(3)
        except Exception:
            time.sleep(1)
    return {}

def doc_get_pdf(url, retries=3):
    for _ in range(retries):
        try:
            r = requests.get(url, auth=AUTH, timeout=60,
                             headers={"Accept": "application/pdf"})
            if r.status_code == 200:
                return r.content
        except Exception:
            time.sleep(2)
    return None

# ── OCR a single PDF page ──────────────────────────────────────────────────────
def ocr_page(page, dpi=250):
    """Render PyMuPDF page, OCR with tesseract, return text."""
    mat = fitz.Matrix(dpi/72, dpi/72)
    pix = page.get_pixmap(matrix=mat)
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        pix.save(tmp.name)
        result = subprocess.run(
            ["tesseract", tmp.name, "stdout", "-l", "eng", "--psm", "6"],
            capture_output=True, text=True, timeout=60
        )
        os.unlink(tmp.name)
    return result.stdout

def get_page_text(page):
    """Try native text first; fall back to OCR."""
    text = page.get_text()
    if len(text.strip()) > 50:
        return text
    return ocr_page(page)

# ── Regex extractors ───────────────────────────────────────────────────────────
NUM_RE = re.compile(r'([\d,]+(?:\.\d+)?)')

def extract_num(s):
    """Parse a string like '28,918,212' or '3,394,555' → float or None."""
    m = NUM_RE.search(s.replace(" ", ""))
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except:
            return None
    return None

# Patterns for P&L items on a line
P_TURNOVER = re.compile(
    r'(?:turnover|revenue|total\s+revenue|sales|net\s+sales)'
    r'[\s\d]*\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

P_OPERATING_PROFIT = re.compile(
    r'(?:operating\s+profit|profit\s+from\s+operations|profit\s+on\s+ordinary\s+activities\s+before)'
    r'[\s\d:]*\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

P_PBT = re.compile(
    r'(?:profit\s+before\s+(?:tax|taxation)|pbt)'
    r'[\s\d:]*\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

P_STAFF = re.compile(
    r'(?:staff\s+costs|wages\s+and\s+salaries|employee\s+costs)'
    r'[\s\d:]*\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

# Average number of persons employed — standard line in UK accounts
# Matches: "Average number of persons employed 35" / "Average number of employees 12"
# Also: "Monthly average of employees during the year 28"
# Note: captures plain integers (no £ sign) — distinct from financial value patterns
P_GROSS_PROFIT = re.compile(
    r'(?:gross\s+profit|gross\s+margin|gross\s+surplus)'
    r'[\s\d:]*\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

P_EMPLOYEES = re.compile(
    r'(?:average\s+(?:monthly\s+)?number\s+of\s+(?:persons?\s+employed|employees?)'
    r'|monthly\s+average\s+of\s+employees?\s+(?:during|in)\s+the\s+(?:year|period)'
    r'|employees?\s*[:—–-]\s*(?:number|headcount)?)'
    r'[\s:—–\-]*(\d[\d,]*)',
    re.IGNORECASE | re.MULTILINE)

P_NET_ASSETS = re.compile(
    r'(?:net\s+assets|shareholders[\'\s]+(?:funds|equity)|total\s+equity)'
    r'[\s\d:]*\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

P_TOTAL_ASSETS = re.compile(
    r'(?:total\s+assets)\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

P_FIXED_ASSETS = re.compile(
    r'(?:total\s+fixed\s+assets|fixed\s+assets)\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

P_CURRENT_ASSETS = re.compile(
    r'(?:total\s+current\s+assets|current\s+assets)\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

# Trade debtors / receivables — HIGH ACCURACY revenue proxy via debtor book method
P_TRADE_DEBTORS = re.compile(
    r'(?:trade\s+debtors?|trade\s+and\s+other\s+receivables?|trade\s+receivables?'
    r'|accounts?\s+receivable|debtors?\s+(?:due\s+within|falling\s+due))'
    r'[\s\d:]*\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

# Total creditors / liabilities — for debt capacity model
P_CREDITORS = re.compile(
    r'(?:total\s+creditors?|creditors?\s+(?:due|falling\s+due)|total\s+liabilities?'
    r'|net\s+current\s+liabilities?)'
    r'[\s\d:]*\s+([\(\)£$\d,\.\-]+)',
    re.IGNORECASE | re.MULTILINE)

P_CURRENCY = re.compile(r'(?:£|\$|€|USD|GBP|EUR)', re.IGNORECASE)

def currency_from_text(text):
    """Detect primary currency in accounts."""
    usd = len(re.findall(r'\$|\bUSD\b|\bUS\s+dollar', text, re.IGNORECASE))
    gbp = len(re.findall(r'£|\bGBP\b|\bsterling\b', text, re.IGNORECASE))
    eur = len(re.findall(r'€|\bEUR\b|\beuro\b', text, re.IGNORECASE))
    counts = {"GBP": gbp, "USD": usd, "EUR": eur}
    return max(counts, key=counts.get) if max(counts.values()) > 0 else "GBP"

def is_financial_page(text):
    """Return True if this page likely contains financial statements."""
    keywords = [
        "turnover", "revenue", "profit", "loss account", "income statement",
        "balance sheet", "net assets", "total assets", "creditors",
        "fixed assets", "current assets", "profit and loss", "p&l",
    ]
    tl = text.lower()
    return sum(1 for k in keywords if k in tl) >= 2

def safe_val(match_obj, text_around=None):
    """Extract the best numeric value from a regex match."""
    if not match_obj:
        return None
    raw = match_obj.group(1).strip()
    # Handle bracketed negatives like (1,234) → negative
    if raw.startswith("(") and raw.endswith(")"):
        v = extract_num(raw)
        return -v if v else None
    return extract_num(raw)

def parse_financials(full_text, priority):
    """
    Parse key financial figures from full OCR text.
    Returns dict of extracted figures.
    """
    result = {
        "turnover": None, "operating_profit": None, "profit_before_tax": None,
        "staff_costs": None, "net_assets": None, "total_assets": None,
        "fixed_assets": None, "current_assets": None,
        "trade_debtors": None,    # for debtor book revenue model (Method 7)
        "total_liabilities": None, # for debt capacity model (Method 8)
        "gross_profit": None,      # gross profit / gross margin
        "employees": None,         # average number of persons employed
        "currency": currency_from_text(full_text),
        "source": "CH accounts PDF (OCR)",
        "data_tier": "Tier 1 — Companies House filing",
    }

    result["turnover"]          = safe_val(P_TURNOVER.search(full_text))
    result["operating_profit"]  = safe_val(P_OPERATING_PROFIT.search(full_text))
    result["profit_before_tax"] = safe_val(P_PBT.search(full_text))
    result["staff_costs"]       = safe_val(P_STAFF.search(full_text))
    result["net_assets"]        = safe_val(P_NET_ASSETS.search(full_text))
    result["total_assets"]      = safe_val(P_TOTAL_ASSETS.search(full_text))
    result["fixed_assets"]      = safe_val(P_FIXED_ASSETS.search(full_text))
    result["current_assets"]    = safe_val(P_CURRENT_ASSETS.search(full_text))
    result["trade_debtors"]     = safe_val(P_TRADE_DEBTORS.search(full_text))
    result["total_liabilities"] = safe_val(P_CREDITORS.search(full_text))
    result["gross_profit"]      = safe_val(P_GROSS_PROFIT.search(full_text))

    # Employee headcount — plain integer, not a £ value
    emp_match = P_EMPLOYEES.search(full_text)
    if emp_match:
        try:
            emp_val = int(emp_match.group(1).replace(",", ""))
            if 0 < emp_val < 500_000:   # sanity guard
                result["employees"] = emp_val
        except (ValueError, IndexError):
            pass

    # Sanity-check trade_debtors: must be positive and < total_assets (if known)
    if result["trade_debtors"] and result["trade_debtors"] < 0:
        result["trade_debtors"] = None
    if (result["trade_debtors"] and result["total_assets"]
            and result["trade_debtors"] > result["total_assets"] * 2):
        result["trade_debtors"] = None  # OCR mis-parse — discard

    # Quality: how many figures did we get?
    extracted = sum(1 for v in result.values()
                    if isinstance(v, (int, float)) and v is not None)
    result["figures_extracted"] = extracted
    return result

# ── main scrape function ───────────────────────────────────────────────────────
def scrape_company_accounts(company_number, company_name, acct_type, period_end):
    """
    Full pipeline: filing history → doc meta → PDF → OCR → parse.
    Returns dict with extracted financials.
    """
    # 1. filing history (latest accounts)
    history = ch_get(f"/company/{company_number}/filing-history?category=accounts&items_per_page=3")
    filings = history.get("items", [])
    if not filings:
        return {"error": "no filings found"}

    latest = filings[0]
    doc_meta_url = latest.get("links", {}).get("document_metadata", "")
    filing_date = latest.get("date", "")
    period = latest.get("action_date", period_end)

    if not doc_meta_url:
        return {"error": "no document_metadata link"}

    time.sleep(0.3)

    # 2. doc metadata → content URL
    meta = doc_get_meta(doc_meta_url)
    resources = meta.get("resources", {})
    content_url = meta.get("links", {}).get("document", "")
    pages = meta.get("pages", 0)

    if not content_url:
        return {"error": "no content URL in metadata"}

    if "application/pdf" not in resources:
        return {"error": f"no PDF resource — available: {list(resources.keys())}"}

    time.sleep(0.3)

    # 3. download PDF
    pdf_bytes = doc_get_pdf(content_url)
    if not pdf_bytes:
        return {"error": "PDF download failed"}

    # 4. open with PyMuPDF, find financial statement pages
    try:
        doc = fitz.open(stream=io.BytesIO(pdf_bytes), filetype="pdf")
    except Exception as e:
        return {"error": f"fitz open error: {e}"}

    n_pages = doc.page_count
    full_text = ""
    financial_pages_found = 0

    # Scan pages — skip cover pages (0-4), focus on middle
    scan_start = max(0, min(5, n_pages - 1))
    for pg_idx in range(scan_start, n_pages):
        text = get_page_text(doc[pg_idx])
        full_text += "\n" + text
        if is_financial_page(text):
            financial_pages_found += 1
        # Stop after finding and processing enough financial content
        if financial_pages_found >= 5 and pg_idx > 10:
            break

    # If we found nothing financial in the middle, try all pages
    if financial_pages_found == 0:
        for pg_idx in range(0, min(5, n_pages)):
            full_text += "\n" + get_page_text(doc[pg_idx])

    doc.close()

    # 5. parse
    parsed = parse_financials(full_text, acct_type)
    parsed.update({
        "accounts_type":    acct_type,
        "period_end":       period[:10] if period else "",
        "filing_date":      filing_date,
        "pdf_pages":        n_pages,
        "financial_pages_ocrd": financial_pages_found,
        "full_text":        full_text[:5000],   # first 5k chars for PE/ownership scanning
    })

    return parsed

# ── pipeline run function ──────────────────────────────────────────────────────
def run(resume: bool = True):
    """
    Pipeline-compatible entry point. Called from run.py as Step 4b.
    Reads/writes cfg.OUTPUT_DIR / cfg.ENRICHED_JSON.
    Merges real account figures into the enriched companies data.
    """
    global AUTH
    AUTH = (API_KEY, "")

    import config as cfg

    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    cache_path    = os.path.join(cfg.OUTPUT_DIR, "accounts_ocr.json")

    with open(enriched_path) as f:
        companies = json.load(f)

    # Load OCR cache (resume)
    done = {}
    if resume and os.path.exists(cache_path):
        try:
            done = {r["company_number"]: r
                    for r in json.load(open(cache_path))
                    if not r.get("error")}
            print(f"  OCR resume: {len(done)} companies already cached")
        except Exception:
            done = {}

    # Build queue (Tier A then B, skip already done and skip tiers)
    def _acct_type(c):
        return (c.get("bs") or c.get("financials", {}).get("balance_sheet") or {}).get("accounts_type", "").lower()

    tier_a = [c for c in companies if _acct_type(c) in TIER_A and c["company_number"] not in done]
    tier_b = [c for c in companies if _acct_type(c) in TIER_B and c["company_number"] not in done]
    tier_a_nums = {c["company_number"] for c in tier_a}
    queue = tier_a + tier_b

    print(f"  Tier A (P&L available):    {len(tier_a) + len([c for c in companies if c['company_number'] in done and _acct_type(c) in TIER_A])} total  ({len(tier_a)} remaining)")
    print(f"  Tier B (balance sheet):    {len(tier_b) + len([c for c in companies if c['company_number'] in done and _acct_type(c) in TIER_B])} total  ({len(tier_b)} remaining)")

    ocr_results = dict(done)
    errors = 0

    for i, company in enumerate(queue):
        cn     = company["company_number"]
        name   = company.get("company_name", "")
        acct   = _acct_type(company)
        period = (company.get("bs") or {}).get("period_end", "")
        tier   = "A" if cn in tier_a_nums else "B"

        print(f"  [{i+1}/{len(queue)}] [{tier}] {name[:45]:<45} ({acct})", end="  ", flush=True)

        try:
            result = scrape_company_accounts(cn, name, acct, period)
            result["company_number"] = cn
            result["company_name"]   = name
            turn = result.get("turnover")
            na   = result.get("net_assets")
            curr = result.get("currency", "GBP")
            if "error" in result:
                print(f"ERR: {result['error']}")
                errors += 1
            else:
                if turn:
                    print(f"✓  {curr} Turnover={turn:,.0f}")
                elif na:
                    print(f"✓  {curr} NetAssets={na:,.0f}")
                else:
                    print(f"  ({result.get('figures_extracted', 0)} figures)")
                ocr_results[cn] = result
        except Exception as e:
            result = {"company_number": cn, "company_name": name,
                      "error": str(e), "accounts_type": acct}
            print(f"EXC: {e}")
            errors += 1

        # Save checkpoint every 10
        if (i + 1) % 10 == 0:
            with open(cache_path, "w") as f:
                json.dump(list(ocr_results.values()), f)

    # Final cache save
    with open(cache_path, "w") as f:
        json.dump(list(ocr_results.values()), f, indent=2)

    # ── Merge real figures back into enriched companies ────────────────────────
    merged_count   = 0
    turnover_count = 0

    for company in companies:
        cn = company["company_number"]
        ocr = ocr_results.get(cn)
        if not ocr or ocr.get("error"):
            continue

        # Store raw OCR data
        company["accounts_ocr"] = ocr

        turn = ocr.get("turnover")
        na   = ocr.get("net_assets")
        ta   = ocr.get("total_assets")
        pbt  = ocr.get("profit_before_tax")
        op   = ocr.get("operating_profit")
        sc   = ocr.get("staff_costs")
        curr = ocr.get("currency", "GBP")

        # Convert USD/EUR → GBP if needed (approximate)
        fx = {"GBP": 1.0, "USD": 0.79, "EUR": 0.86}
        rate = fx.get(curr, 1.0)

        def to_gbp(v):
            return round(v * rate) if v else None

        # Update balance sheet with real figures
        if not company.get("bs"):
            company["bs"] = {}

        if ta:    company["bs"]["total_assets"]  = to_gbp(ta)
        if na:    company["bs"]["net_assets"]     = to_gbp(na)
        if sc:    company["bs"]["staff_costs"]    = to_gbp(sc)

        # Employee headcount from OCR (Tier 1 — actual from filed accounts)
        ocr_emp = ocr.get("employees")
        if ocr_emp and ocr_emp > 0:
            company["bs"]["total_employees"]      = ocr_emp
            company["estimated_employees"]        = ocr_emp
            company["estimated_employees_source"] = "Tier 1 — filed accounts (OCR)"

        # If we have actual turnover, replace the revenue estimate
        if turn and turn > 0:
            gbp_turn = to_gbp(turn)
            company["rev_actual"]    = gbp_turn
            company["rev_currency"]  = curr
            company["rev_source"]    = "Tier 1 — CH filed accounts (OCR)"
            # Also update the fields used by Excel/Word
            company["rev_low"]   = round(gbp_turn * 0.92)
            company["rev_base"]  = gbp_turn
            company["rev_high"]  = round(gbp_turn * 1.08)
            company["confidence"] = "Actual (Tier 1)"
            if pbt:
                company["ebitda_base"] = to_gbp(pbt)
            elif op:
                company["ebitda_base"] = to_gbp(op)
            turnover_count += 1

        elif na and not turn:
            # Balance sheet only: refine asset-based estimate
            company["bs"]["net_assets_actual"] = to_gbp(na)

        merged_count += 1

    # ── Re-run ownership/PE analysis with OCR text ───────────────────────────
    # The initial enrichment step didn't have OCR text; now we can check for
    # PE/group ownership mentions in the accounts PDF text.
    try:
        from ch_enrich import analyse_ownership
        pe_updated = 0
        for company in companies:
            cn = company["company_number"]
            ocr = ocr_results.get(cn)
            if not ocr or ocr.get("error"):
                continue
            # Get the raw OCR text stored in the result
            ocr_full_text = ocr.get("full_text", "")
            if not ocr_full_text:
                continue
            psc = company.get("psc", [])
            ownership = analyse_ownership(psc, ocr_text=ocr_full_text)
            # Update if OCR found new PE signals
            if ownership["pe_signals"] and len(ownership["pe_signals"]) > len(company.get("pe_signals", [])):
                company["ownership"]     = ownership
                company["pe_likelihood"] = ownership["pe_likelihood"]
                company["pe_signals"]    = ownership["pe_signals"]
                company["pe_backed"]     = ownership["pe_likelihood"] in ("High", "Medium")
                pe_updated += 1
        if pe_updated:
            print(f"  PE ownership updated from OCR text: {pe_updated} companies")
    except Exception as e:
        print(f"  (OCR PE re-analysis skipped: {e})")

    # Save updated enriched file
    with open(enriched_path, "w") as f:
        json.dump(companies, f, indent=2)

    print(f"\n  OCR complete: {len(ocr_results)} scraped, {errors} errors")
    print(f"  Merged into enriched: {merged_count} companies")
    print(f"  With actual turnover: {turnover_count} (revenue estimates replaced)")
    print(f"  Cache → {cache_path}")
    return companies


def main():
    """Standalone entry point — runs against default config."""
    import argparse
    p = argparse.ArgumentParser(description="CH Accounts OCR standalone")
    p.add_argument("--no-resume", action="store_true", help="Ignore existing cache")
    args = p.parse_args()
    run(resume=not args.no_resume)


if __name__ == "__main__":
    main()
