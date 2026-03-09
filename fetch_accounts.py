#!/usr/bin/env python3
"""
fetch_accounts.py — CH document API + OCR accounts extractor

For each lift maintenance company with accessible accounts:
  1. Fetch latest filing from CH filing history API
  2. Download PDF via CH Document API
  3. OCR using Tesseract (image-based PDFs) or text extract (digital PDFs)
  4. Parse P&L and balance sheet figures with regex
  5. Save to /tmp/lift_accounts.json

Priority tiers:
  A — full / medium / group / small  (P&L + balance sheet available)
  B — total-exemption-full / unaudited-abridged (balance sheet only)
  C — all others (micro, dormant → skip)
"""

import json, re, time, os, sys, subprocess, tempfile, io
import requests
import fitz          # PyMuPDF

# ── API auth ───────────────────────────────────────────────────────────────────
# Priority: COMPANIES_HOUSE_API_KEY env var (set by GitHub Actions secret)
# Fallback: .ch_api_key file in script directory
_script_dir = os.path.dirname(os.path.abspath(__file__))
API_KEY     = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
if not API_KEY:
    for _kf in [os.path.join(_script_dir, '.ch_api_key'),
                os.path.join(_script_dir, 'ch-pe-sourcing/.ch_api_key')]:
        if os.path.exists(_kf):
            with open(_kf) as f:
                for line in f:
                    if "=" in line:
                        API_KEY = line.strip().split("=", 1)[1].strip()
                        break
            if API_KEY:
                break

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
        "currency": currency_from_text(full_text),
        "source": "CH accounts PDF (OCR)",
        "data_tier": "Tier 1 — Companies House filing",
    }

    result["turnover"]         = safe_val(P_TURNOVER.search(full_text))
    result["operating_profit"] = safe_val(P_OPERATING_PROFIT.search(full_text))
    result["profit_before_tax"]= safe_val(P_PBT.search(full_text))
    result["staff_costs"]      = safe_val(P_STAFF.search(full_text))
    result["net_assets"]       = safe_val(P_NET_ASSETS.search(full_text))
    result["total_assets"]     = safe_val(P_TOTAL_ASSETS.search(full_text))
    result["fixed_assets"]     = safe_val(P_FIXED_ASSETS.search(full_text))
    result["current_assets"]   = safe_val(P_CURRENT_ASSETS.search(full_text))

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
    })

    return parsed

# ── batch run ──────────────────────────────────────────────────────────────────
def main():
    # Input: enriched JSON — env var override for CI, else default local path
    _script_dir     = os.path.dirname(os.path.abspath(__file__))
    enriched_default = os.path.join(_script_dir, "data/sectors/lift_maintenance_enriched.json")
    enriched_path    = os.environ.get("ENRICHED_JSON", enriched_default)
    # Also check legacy /tmp path (local Cowork sessions)
    if not os.path.exists(enriched_path):
        if os.path.exists("/tmp/lift_enriched_patched.json"):
            enriched_path = "/tmp/lift_enriched_patched.json"
    data = json.load(open(enriched_path))

    # Checkpoint path — env var override for CI
    checkpoint_path = os.environ.get("OCR_JSON", "/tmp/lift_accounts.json")
    done = {}
    if os.path.exists(checkpoint_path):
        try:
            done = {r["company_number"]: r for r in json.load(open(checkpoint_path))}
            print(f"Resuming: {len(done)} already done")
        except:
            done = {}

    # Tier A first (full P&L), then Tier B (balance sheet)
    tier_a = [r for r in data
              if r.get("bs",{}).get("accounts_type","").lower() in TIER_A
              and r["company_number"] not in done]
    tier_b = [r for r in data
              if r.get("bs",{}).get("accounts_type","").lower() in TIER_B
              and r["company_number"] not in done]
    skip_set = SKIP

    # Deduplicate (tier A takes priority)
    tier_a_nums = {r["company_number"] for r in tier_a}

    print(f"Tier A (full P&L):     {len(tier_a)} companies")
    print(f"Tier B (balance sheet):{len(tier_b)} companies")
    print(f"Already done:          {len(done)}")
    print()

    results = list(done.values())
    total_queue = tier_a + tier_b
    errors = 0

    for i, company in enumerate(total_queue):
        cn   = company["company_number"]
        name = company["company_name"]
        acct = company.get("bs",{}).get("accounts_type","unknown")
        period = company.get("bs",{}).get("period_end","")
        tier = "A" if cn in tier_a_nums else "B"

        print(f"[{i+1}/{len(total_queue)}] [{tier}] {name[:45]:<45} ({acct})", end="  ", flush=True)

        try:
            result = scrape_company_accounts(cn, name, acct, period)
            result["company_number"] = cn
            result["company_name"]   = name
            fig = result.get("figures_extracted", 0)
            turn = result.get("turnover")
            na   = result.get("net_assets")
            curr = result.get("currency", "GBP")
            if "error" in result:
                print(f"ERR: {result['error']}")
                errors += 1
            else:
                print(f"✓  {curr} Turnover={turn:,.0f}" if turn else
                      f"✓  {curr} NetAssets={na:,.0f}" if na else
                      f"  ({fig} figures)")
        except Exception as e:
            result = {"company_number": cn, "company_name": name,
                      "error": str(e), "accounts_type": acct}
            print(f"EXC: {e}")
            errors += 1

        results.append(result)
        done[cn] = result

        # Save checkpoint every 10 companies
        if (i + 1) % 10 == 0:
            with open(checkpoint_path, "w") as f:
                json.dump(results, f)
            print(f"  — checkpoint saved ({len(results)} records)")

    # Final save
    with open(checkpoint_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nComplete: {len(results)} companies scraped, {errors} errors")
    print(f"Saved → {checkpoint_path}")

    # Summary stats
    with_turnover  = sum(1 for r in results if r.get("turnover"))
    with_net_assets= sum(1 for r in results if r.get("net_assets"))
    print(f"With turnover:    {with_turnover}")
    print(f"With net_assets:  {with_net_assets}")

if __name__ == "__main__":
    main()
