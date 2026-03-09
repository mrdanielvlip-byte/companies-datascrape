"""
scrape_audit_register.py
Scrapes auditregister.org.uk (Register of Statutory Auditors) and stores
results in the local sector_cache table, cross-referenced with Companies House.

Usage:
    python scrape_audit_register.py            # full scrape + CH match
    python scrape_audit_register.py --stats    # show cached stats only
"""

import sqlite3, re, time, sys, json, argparse
import urllib.request
from datetime import datetime
from pathlib import Path

# Shared cache helpers — all register scrapers use this so results always land in DB
from reg_sources import (
    save_register_to_cache,
    load_register_from_cache,
    cache_stats,
    _db_connect,
    _ensure_cache_table,
)

DB_PATH      = Path("data/companies_house.db")
BASE_URL     = "https://www.auditregister.org.uk"
REGISTER_KEY = "AUDIT_REGISTER"   # used as the sector key: reg_audit_register
PAGE_SIZE    = 100
DELAY        = 0.3   # polite delay between requests (seconds)


# ─── HTTP helper ─────────────────────────────────────────────────────────────

def fetch(url: str, retries: int = 3) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"}
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as r:
                return r.read().decode("utf-8", errors="replace")
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
    return ""


# ─── Parse list page ─────────────────────────────────────────────────────────

def parse_list_page(html: str) -> list[dict]:
    """Extract firm name, RSB, and detail URL from a search results page."""
    firms = []
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
    for row in rows[1:]:  # skip header row
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) < 2:
            continue
        rsb  = re.sub(r'<[^>]+>', '', cells[0]).strip()
        name_cell = cells[1]
        # Extract detail URL
        link = re.search(r'href="(/firm/firmdetails/(\d+))"', name_cell)
        reg_no = link.group(2) if link else ""
        detail_path = link.group(1) if link else ""
        name   = re.sub(r'<[^>]+>', '', name_cell).strip()
        name   = re.sub(r'\s+', ' ', name)
        if name and rsb and rsb != "RSB":  # skip header
            firms.append({
                "rsb":         rsb,
                "firm_name":   name,
                "reg_no":      reg_no,
                "detail_path": detail_path,
            })
    return firms


# ─── Parse detail page ────────────────────────────────────────────────────────

def parse_detail_page(html: str) -> dict:
    """Extract address, postcode, website, legal form from a firm detail page."""
    text = re.sub(r'<[^>]+>', '\n', html)
    lines = [l.strip() for l in text.split('\n') if l.strip()]

    data = {}

    # Reg number
    for i, ln in enumerate(lines):
        if ln == "Reg No:" and i + 1 < len(lines):
            data["reg_no_confirm"] = lines[i + 1]
        if ln == "Legal Form:" and i + 1 < len(lines):
            data["legal_form"] = lines[i + 1]
        if ln == "Website:" and i + 1 < len(lines):
            ws = lines[i + 1]
            if ws.startswith("http"):
                data["website"] = ws
        if ln == "Main Office:" and i + 1 < len(lines):
            # Grab address lines until we hit a known label
            addr_lines = []
            j = i + 1
            stop_labels = {"Registered By:", "Legal Form:", "Website:", "Primary Contact:",
                           "Network Details:", "Linked Individuals", "Linked Offices"}
            while j < len(lines) and lines[j] not in stop_labels:
                addr_lines.append(lines[j])
                j += 1
            data["address_raw"] = ", ".join(addr_lines)
            # Extract postcode (UK postcode pattern)
            pc = re.search(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b',
                           data["address_raw"], re.I)
            if pc:
                data["postcode"] = pc.group(1).upper().replace(" ", "")

    return data


# ─── CH cross-reference ───────────────────────────────────────────────────────

def ch_lookup_by_name(con, firm_name: str) -> dict | None:
    """
    Fuzzy-match a firm name against the local CH companies table.
    Tries: exact upper match, then LIKE prefix match.
    Returns best match dict or None.
    """
    name_up = firm_name.upper()

    # 1. Exact match (case insensitive)
    row = con.execute(
        "SELECT company_number, company_name, company_status, sic1, postcode, "
        "incorporation_date, company_age_years FROM companies "
        "WHERE company_name_upper=? LIMIT 1",
        (name_up,)
    ).fetchone()
    if row:
        return dict(row)

    # 2. LIKE prefix (first 30 chars)
    prefix = name_up[:30]
    row = con.execute(
        "SELECT company_number, company_name, company_status, sic1, postcode, "
        "incorporation_date, company_age_years FROM companies "
        "WHERE company_name_upper LIKE ? AND company_status='Active' LIMIT 1",
        (prefix + "%",)
    ).fetchone()
    if row:
        return dict(row)

    # 3. FTS5 search as last resort
    try:
        clean = re.sub(r'[^a-zA-Z0-9 ]', ' ', firm_name)
        words = clean.split()[:4]
        if words:
            query = " ".join(f'"{w}"' for w in words if len(w) > 2)
            if query:
                row = con.execute(
                    "SELECT c.company_number, c.company_name, c.company_status, "
                    "c.sic1, c.postcode, c.incorporation_date, c.company_age_years "
                    "FROM companies c "
                    "JOIN companies_fts fts ON c.rowid = fts.rowid "
                    "WHERE companies_fts MATCH ? LIMIT 1",
                    (query,)
                ).fetchone()
                if row:
                    return dict(row)
    except Exception:
        pass

    return None


# ─── Main scrape ──────────────────────────────────────────────────────────────

def scrape_and_store(fetch_details: bool = False):
    if not DB_PATH.exists():
        print("❌ Local CH DB not found. Run build_local_db.py first.")
        sys.exit(1)

    con = _db_connect()
    _ensure_cache_table(con)
    sector = f"reg_{REGISTER_KEY.lower()}"

    # Clear existing cache for this register
    existing = con.execute(
        "SELECT COUNT(*) FROM sector_cache WHERE sector=?", (sector,)
    ).fetchone()[0]
    if existing:
        print(f"  Clearing {existing:,} existing '{sector}' records ...")
        con.execute("DELETE FROM sector_cache WHERE sector=?", (sector,))
        con.commit()

    # ── 1. Scrape all list pages ──────────────────────────────────────────────
    all_firms = []
    page = 0
    print(f"\n📋 Scraping {BASE_URL}/firm (page size {PAGE_SIZE}) ...")
    while True:
        url  = f"{BASE_URL}/firm?pageNumber={page}&pageSize={PAGE_SIZE}&orderBy=ASC"
        html = fetch(url)
        firms = parse_list_page(html)
        if not firms:
            break
        all_firms.extend(firms)
        print(f"  Page {page:>2}: {len(firms):>3} firms  (total so far: {len(all_firms):,})",
              end="\r")
        time.sleep(DELAY)
        page += 1

    print(f"\n✅ {len(all_firms):,} firms scraped from {page} pages")

    # ── 2. Optionally fetch detail pages ─────────────────────────────────────
    if fetch_details:
        print(f"\n🔍 Fetching detail pages ({len(all_firms):,} requests) ...")
        for i, firm in enumerate(all_firms):
            if firm.get("detail_path"):
                try:
                    dhtml = fetch(BASE_URL + firm["detail_path"])
                    detail = parse_detail_page(dhtml)
                    firm.update(detail)
                except Exception:
                    pass
            if (i + 1) % 50 == 0:
                print(f"  {i+1:>4}/{len(all_firms)} details fetched ...", end="\r")
            time.sleep(DELAY)
        print(f"\n✅ Detail pages fetched")

    # ── 3. Cross-reference with CH DB + build pipeline dicts ─────────────────
    print(f"\n🔗 Cross-referencing {len(all_firms):,} firms with CH database ...")
    matched   = 0
    companies = []

    for i, firm in enumerate(all_firms):
        ch = ch_lookup_by_name(con, firm["firm_name"])
        matched += bool(ch)

        # Build a pipeline-compatible dict; register details go into registrations
        c = {
            "company_number":   ch["company_number"]    if ch else None,
            "company_name":     firm["firm_name"],
            "company_status":   ch["company_status"]    if ch else "Unknown",
            "company_type":     None,
            "sic1":             ch["sic1"]              if ch else None,
            "postcode":         firm.get("postcode") or (ch["postcode"] if ch else None),
            "incorporation_date": ch["incorporation_date"] if ch else None,
            "company_age_years":  ch["company_age_years"]  if ch else None,
            "registrations": {
                REGISTER_KEY: {
                    "company_name":  firm["firm_name"],
                    "reg_no":        firm.get("reg_no"),
                    "rsb":           firm.get("rsb"),
                    "address_raw":   firm.get("address_raw"),
                    "website":       firm.get("website"),
                    "legal_form":    firm.get("legal_form"),
                }
            },
        }
        companies.append(c)

        if (i + 1) % 200 == 0:
            print(f"  {i+1:>4}/{len(all_firms)} processed, {matched} CH matches ...",
                  end="\r")

    print(f"\n  ✅ CH matched: {matched:,} / {len(all_firms):,} "
          f"({100*matched//len(all_firms) if all_firms else 0}%)")

    # ── 4. Save via shared helper (handles table creation + columns) ──────────
    save_register_to_cache(
        register_key = REGISTER_KEY,
        companies    = companies,
        source_url   = f"{BASE_URL}/firm",
    )

    # ── 5. Summary ────────────────────────────────────────────────────────────
    stats = cache_stats(REGISTER_KEY)
    s = stats.get(f"reg_{REGISTER_KEY.lower()}", {})
    total   = s.get("total", 0)
    ch_hits = s.get("matched", 0)

    # RSB breakdown
    con2 = _db_connect()
    by_rsb = con2.execute("""
        SELECT register_rsb, COUNT(*) FROM sector_cache
        WHERE sector=? GROUP BY register_rsb ORDER BY COUNT(*) DESC
    """, (f"reg_{REGISTER_KEY.lower()}",)).fetchall()
    con2.close()

    print(f"\n  ┌─ Cached: 'reg_{REGISTER_KEY.lower()}' ──────────────────────")
    print(f"  │  Total firms:       {total:>8,}")
    print(f"  │  CH matched:        {ch_hits:>8,}  ({100*ch_hits//total if total else 0}%)")
    print(f"  │  By Recognised Supervisory Body:")
    for rsb, cnt in by_rsb:
        print(f"  │    {(rsb or '?'):<35} {cnt:>6,}")
    print(f"  └────────────────────────────────────────────────────")
    con.close()
    return total


def print_stats():
    stats = cache_stats(REGISTER_KEY)
    sector = f"reg_{REGISTER_KEY.lower()}"
    s = stats.get(sector, {})
    total = s.get("total", 0)
    if not total:
        print("No audit register data cached yet. Run: python scrape_audit_register.py")
        return
    ch_hits = s.get("matched", 0)
    print(f"Cached '{sector}': {total:,} firms, {ch_hits:,} CH matched")
    con = _db_connect()
    sample = con.execute("""
        SELECT register_name, register_rsb, postcode, company_number
        FROM sector_cache WHERE sector=? LIMIT 10
    """, (sector,)).fetchall()
    con.close()
    for r in sample:
        print(f"  {(r['register_rsb'] or '?'):<8} {(r['register_name'] or '?'):<45} "
              f"PC:{r['postcode'] or '?':>8}  CH:{r['company_number'] or '—'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape UK Audit Register")
    parser.add_argument("--stats",   action="store_true", help="Show cached stats")
    parser.add_argument("--details", action="store_true",
                        help="Also fetch detail pages (3,700 extra requests, slower)")
    args = parser.parse_args()

    if args.stats:
        print_stats()
    else:
        scrape_and_store(fetch_details=args.details)
