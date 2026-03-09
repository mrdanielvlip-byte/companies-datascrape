"""
local_search.py — Fast local Companies House search using SQLite

Drop-in replacement for ch_search.py Step 1 discovery.
Queries the local SQLite database built by build_local_db.py
instead of hitting the Companies House API.

Search methods:
  search_by_sic(sic_codes)         — find all active companies by SIC code(s)
  search_by_name(query)            — FTS5 full-text name search
  search_by_keyword(keyword)       — keyword search across company names
  search_by_postcode(prefix)       — find companies in a postcode area
  search_sic_in_region(sics, pc)   — SIC + postcode filter combined
  list_sic_codes(query)            — search the SIC code reference table
  run(cfg)                         — full Step 1 equivalent, writes filtered_companies.json

Performance vs API:
  SIC search (1 code):    ~0.05s  (vs 30-120s via API)
  Name search:            ~0.02s  (vs 5-15s via API)
  Full SIC sweep (10 codes): ~1s  (vs 10+ minutes via API)
"""

import json
import os
import re
import sqlite3
from pathlib import Path
from datetime import datetime

import config as cfg

DATA_DIR = Path(__file__).parent / "data"
DB_PATH  = DATA_DIR / "companies_house.db"


# ── DB connection ─────────────────────────────────────────────────────────────

def get_connection() -> sqlite3.Connection:
    """Return a read-only connection to the local DB."""
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Local database not found at {DB_PATH}\n"
            f"Run: python build_local_db.py"
        )
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA cache_size=-128000")
    return con


def db_ready() -> bool:
    """Return True if the local database exists and has data."""
    if not DB_PATH.exists():
        return False
    try:
        con = get_connection()
        count = con.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        con.close()
        return count > 0
    except Exception:
        return False


def db_info() -> dict:
    """Return metadata about the local DB."""
    if not DB_PATH.exists():
        return {"available": False}
    try:
        con = get_connection()
        meta  = dict(con.execute("SELECT key, value FROM db_meta").fetchall())
        count = con.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        con.close()
        return {
            "available":  True,
            "build_date": meta.get("build_date", "unknown"),
            "total":      count,
            "active":     int(meta.get("active_count", 0)),
            "path":       str(DB_PATH),
            "size_mb":    round(DB_PATH.stat().st_size / 1024 / 1024),
        }
    except Exception as e:
        return {"available": False, "error": str(e)}


# ── Row normalisation ─────────────────────────────────────────────────────────

def _normalise_row(row: sqlite3.Row, source: str = "local_db") -> dict:
    """Convert a SQLite row to the standard pipeline company dict format."""
    r = dict(row)

    # Build SIC codes list (non-empty codes only)
    sics = [s for s in [r.get("sic1",""), r.get("sic2",""),
                         r.get("sic3",""), r.get("sic4","")] if s]

    addr = {
        "address_line_1": r.get("address_line1", ""),
        "locality":       r.get("address_town", ""),
        "region":         r.get("address_county", ""),
        "country":        r.get("address_country", ""),
        "postal_code":    r.get("postcode", ""),
    }

    return {
        "company_number":            r["company_number"],
        "company_name":              r["company_name"],
        "company_status":            r.get("company_status", "Active"),
        "company_type":              r.get("company_type", ""),
        "date_of_creation":          r.get("incorporation_date", ""),
        "registered_office_address": addr,
        "sic_codes":                 sics,
        "company_age_years":         r.get("company_age_years"),
        "mortgages_outstanding":     r.get("mortgages_outstanding", 0),
        "accounts_category":         r.get("accounts_category", ""),
        "relevance_score":           90,
        "source":                    source,
    }


# ── Search functions ──────────────────────────────────────────────────────────

def search_by_sic(
    sic_codes:   list[str],
    status:      str = "Active",
    limit:       int = 50_000,
    min_age_yrs: float | None = None,
    max_age_yrs: float | None = None,
    postcode_prefix: str | None = None,
) -> list[dict]:
    """
    Return all companies registered under one or more SIC codes.

    Args:
        sic_codes:       List of 5-digit SIC codes, e.g. ['38110', '38120']
        status:          'Active' | 'Dissolved' | None (all)
        limit:           Max results (per SIC code)
        min_age_yrs:     Only include companies older than N years
        max_age_yrs:     Only include companies younger than N years
        postcode_prefix: Filter to postcode area, e.g. 'LS' or 'M1'

    Returns list of normalised company dicts.
    """
    con     = get_connection()
    results = {}

    for sic in sic_codes:
        # Match sic1..sic4 columns
        clauses = ["(sic1=? OR sic2=? OR sic3=? OR sic4=?)"]
        params  = [sic, sic, sic, sic]

        if status:
            clauses.append("company_status=?")
            params.append(status)
        if min_age_yrs is not None:
            clauses.append("company_age_years >= ?")
            params.append(min_age_yrs)
        if max_age_yrs is not None:
            clauses.append("company_age_years <= ?")
            params.append(max_age_yrs)
        if postcode_prefix:
            clauses.append("postcode LIKE ?")
            params.append(f"{postcode_prefix.upper()}%")

        sql = f"SELECT * FROM companies WHERE {' AND '.join(clauses)} LIMIT ?"
        params.append(limit)

        rows = con.execute(sql, params).fetchall()
        for row in rows:
            num = row["company_number"]
            if num not in results:
                results[num] = _normalise_row(row, source=f"local_sic_{sic}")

    con.close()
    return list(results.values())


def search_by_name(query: str, status: str = "Active", limit: int = 500) -> list[dict]:
    """
    FTS5 full-text search on company names.
    Supports phrases ("drainage services"), prefix (drainage*), and boolean (drainage NOT waste).

    Args:
        query:  FTS5 query string
        status: 'Active' | 'Dissolved' | None (all)
        limit:  Max results

    Returns list of normalised company dicts.
    """
    con = get_connection()

    # FTS5 search — join back to main table for full data
    sql = """
        SELECT c.*
        FROM companies_fts f
        JOIN companies c ON c.company_number = f.company_number
        WHERE f.company_name MATCH ?
    """
    params = [query]

    if status:
        sql += " AND c.company_status = ?"
        params.append(status)

    sql += f" LIMIT {limit}"

    try:
        rows = con.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        # Fall back to LIKE if FTS query syntax invalid
        sql2 = """
            SELECT * FROM companies
            WHERE company_name_upper LIKE ?
        """
        p2 = [f"%{query.upper()}%"]
        if status:
            sql2 += " AND company_status = ?"
            p2.append(status)
        sql2 += f" LIMIT {limit}"
        rows = con.execute(sql2, p2).fetchall()

    con.close()
    return [_normalise_row(row, "local_name_search") for row in rows]


def search_by_keyword(keyword: str, status: str = "Active", limit: int = 2000) -> list[dict]:
    """
    LIKE-based keyword search on company names (slower than FTS but more flexible).
    Strips common suffixes before searching for better recall.

    Args:
        keyword: Word or phrase to search for in company names
        status:  'Active' | 'Dissolved' | None
        limit:   Max results
    """
    clean = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC|AND|THE|OF)\b", "",
                   keyword.upper()).strip()

    con    = get_connection()
    sql    = "SELECT * FROM companies WHERE company_name_upper LIKE ?"
    params = [f"%{clean}%"]

    if status:
        sql += " AND company_status=?"
        params.append(status)

    sql += f" ORDER BY company_age_years DESC NULLS LAST LIMIT {limit}"
    rows = con.execute(sql, params).fetchall()
    con.close()

    return [_normalise_row(row, "local_keyword") for row in rows]


def search_by_postcode(
    prefix:    str,
    sic_codes: list[str] | None = None,
    status:    str = "Active",
    limit:     int = 10_000,
) -> list[dict]:
    """
    Find companies in a postcode area (e.g., 'LS' = Leeds, 'M' = Manchester).
    Optionally filter by SIC code(s).
    """
    con     = get_connection()
    clauses = ["postcode LIKE ?"]
    params  = [f"{prefix.upper()}%"]

    if status:
        clauses.append("company_status=?")
        params.append(status)

    if sic_codes:
        sic_or = " OR ".join(["sic1=? OR sic2=? OR sic3=? OR sic4=?"] * len(sic_codes))
        clauses.append(f"({sic_or})")
        for sic in sic_codes:
            params.extend([sic, sic, sic, sic])

    sql  = f"SELECT * FROM companies WHERE {' AND '.join(clauses)} LIMIT {limit}"
    rows = con.execute(sql, params).fetchall()
    con.close()

    return [_normalise_row(row, "local_postcode") for row in rows]


def list_sic_codes(query: str = "", min_count: int = 0, limit: int = 100) -> list[dict]:
    """
    Search the SIC code reference table. Useful for discovering relevant SIC codes.

    Args:
        query:     Text to search in SIC code descriptions
        min_count: Only return SIC codes with at least this many companies
        limit:     Max results

    Returns list of {'sic_code': '38110', 'description': 'Collection...', 'count': 12345}
    """
    con = get_connection()

    sql    = "SELECT sic_code, description, count FROM sic_codes WHERE 1=1"
    params: list = []

    if query:
        sql    += " AND (description LIKE ? OR sic_code LIKE ?)"
        params += [f"%{query}%", f"%{query}%"]

    if min_count:
        sql    += " AND count >= ?"
        params.append(min_count)

    sql += f" ORDER BY count DESC LIMIT {limit}"
    rows = con.execute(sql, params).fetchall()
    con.close()

    return [dict(row) for row in rows]


def get_company(company_number: str) -> dict | None:
    """Look up a single company by company number."""
    con = get_connection()
    row = con.execute(
        "SELECT * FROM companies WHERE company_number=?", (company_number,)
    ).fetchone()
    con.close()
    return _normalise_row(row, "local_lookup") if row else None


# ── Filtering (mirrors ch_search.py logic) ────────────────────────────────────

def _is_genuine(name: str) -> bool:
    """Apply the same quality filter as ch_search.py."""
    n = name.lower()

    # Exclude holding/shell/PE-backed patterns
    exclude = getattr(cfg, "EXCLUDE_TERMS", [
        "holdings", "holding company", "group plc", "quoted", "listed",
        "investment trust", "venture capital",
    ])
    if any(ex in n for ex in exclude):
        return False

    subsectors = getattr(cfg, "EXCLUDE_SUBSECTORS", [])
    if any(ex in n for ex in subsectors):
        return False

    # INCLUDE_STEMS: if configured, require at least one stem to be present
    stems = getattr(cfg, "INCLUDE_STEMS", [])
    if stems:
        return any(s in n for s in stems)

    return True


# ── Step 1 replacement ────────────────────────────────────────────────────────

def run(override_cfg=None) -> dict:
    """
    Full Step 1 replacement using local DB.
    Sweeps configured SIC codes + name queries, applies filters,
    writes raw_companies.json and filtered_companies.json.

    Returns dict of {company_number: company_dict} (same as ch_search.run()).
    """
    c = override_cfg or cfg

    if not db_ready():
        raise FileNotFoundError(
            "Local database not ready. Run: python build_local_db.py"
        )

    info = db_info()
    print(f"\nLocal DB search ({info['active']:,} active companies, "
          f"built {info['build_date'][:10]})")

    os.makedirs(c.OUTPUT_DIR, exist_ok=True)

    sic_codes   = getattr(c, "SIC_CODES",   [])
    name_queries= getattr(c, "NAME_QUERIES", [])

    all_companies: dict[str, dict] = {}

    # ── SIC code sweep ────────────────────────────────────────────────────────
    if sic_codes:
        print(f"\nSweeping {len(sic_codes)} SIC codes against local DB ...")
        for sic in sic_codes:
            t0   = __import__("time").time()
            rows = search_by_sic([sic], status="Active")
            new  = 0
            for c_dict in rows:
                num  = c_dict["company_number"]
                name = c_dict["company_name"]
                if num and num not in all_companies and _is_genuine(name):
                    all_companies[num] = c_dict
                    new += 1
            elapsed = __import__("time").time() - t0
            print(f"  SIC {sic}  →  {len(rows):,} total  |  {new:,} new  ({elapsed:.2f}s)")

    # ── Name queries ─────────────────────────────────────────────────────────
    if name_queries:
        print(f"\nRunning {len(name_queries)} name queries ...")
        for query in name_queries:
            t0   = __import__("time").time()
            rows = search_by_name(query, status="Active")
            new  = 0
            for c_dict in rows:
                num  = c_dict["company_number"]
                name = c_dict["company_name"]
                if num and num not in all_companies and _is_genuine(name):
                    all_companies[num] = c_dict
                    new += 1
            elapsed = __import__("time").time() - t0
            print(f"  '{query}'  →  {len(rows)} found  |  {new} new  ({elapsed:.2f}s)")

    # ── Write raw JSON ────────────────────────────────────────────────────────
    companies_list = list(all_companies.values())
    raw_path = os.path.join(c.OUTPUT_DIR, c.RAW_JSON)
    with open(raw_path, "w") as f:
        json.dump(companies_list, f, indent=2)
    print(f"\nRaw:      {len(companies_list):,} companies → {raw_path}")

    # ── Apply filters → filtered JSON ─────────────────────────────────────────
    filtered = [c_d for c_d in companies_list if _is_genuine(c_d.get("company_name",""))]
    filt_path = os.path.join(c.OUTPUT_DIR, c.FILTERED_JSON)
    with open(filt_path, "w") as f:
        json.dump(filtered, f, indent=2)
    print(f"Filtered: {len(filtered):,} companies → {filt_path}")

    return all_companies


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Query the local Companies House SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check DB status
  python local_search.py --info

  # Search by SIC codes
  python local_search.py --sic 38110 38120 38210

  # Name keyword search
  python local_search.py --name "drainage"
  python local_search.py --name "fire safety"

  # SIC + postcode region
  python local_search.py --sic 43220 --postcode LS

  # Discover relevant SIC codes
  python local_search.py --list-sic "waste"
  python local_search.py --list-sic "fire"
  python local_search.py --list-sic "care"
        """,
    )
    parser.add_argument("--info",       action="store_true",
                        help="Show database info and exit")
    parser.add_argument("--sic",        nargs="+", metavar="CODE",
                        help="SIC codes to search, e.g. --sic 38110 38120")
    parser.add_argument("--name",       metavar="QUERY",
                        help="Company name keyword search")
    parser.add_argument("--postcode",   metavar="PREFIX",
                        help="Filter to postcode area, e.g. LS M1 SW")
    parser.add_argument("--list-sic",   metavar="QUERY",
                        help="Search SIC code descriptions")
    parser.add_argument("--limit",      type=int, default=20,
                        help="Max results to display (default 20)")
    parser.add_argument("--dissolved",  action="store_true",
                        help="Include dissolved companies")
    args = parser.parse_args()

    if args.info or (not args.sic and not args.name and
                     not args.list_sic and not args.postcode):
        info = db_info()
        if not info.get("available"):
            print("  ❌ Database not built. Run: python build_local_db.py")
        else:
            print(f"\n  ✅ Local DB ready")
            print(f"     Path:        {info['path']}")
            print(f"     Size:        {info['size_mb']} MB")
            print(f"     Built:       {info['build_date'][:19]}")
            print(f"     Total cos:   {info['total']:,}")
            print(f"     Active cos:  {info['active']:,}")
        import sys; sys.exit(0)

    status = None if args.dissolved else "Active"

    if args.list_sic:
        codes = list_sic_codes(args.list_sic, limit=args.limit)
        print(f"\n  SIC codes matching '{args.list_sic}':")
        print(f"  {'Code':<8} {'Count':>8}  Description")
        print(f"  {'─'*8} {'─'*8}  {'─'*50}")
        for row in codes:
            print(f"  {row['sic_code']:<8} {row['count']:>8,}  {row['description']}")
        import sys; sys.exit(0)

    results = []

    if args.sic:
        results = search_by_sic(args.sic, status=status,
                                postcode_prefix=args.postcode)
    elif args.name:
        results = search_by_name(args.name, status=status, limit=500)
        if args.postcode:
            pc = args.postcode.upper()
            results = [r for r in results
                       if r.get("registered_office_address",{}).get("postal_code","").upper().startswith(pc)]
    elif args.postcode:
        sics = args.sic or []
        results = search_by_postcode(args.postcode, sic_codes=sics, status=status)

    print(f"\n  Found {len(results):,} companies  (showing first {args.limit})\n")
    print(f"  {'Number':<12} {'Name':<50} {'SIC':<8} {'Postcode':<10} {'Age (yrs)'}")
    print(f"  {'─'*12} {'─'*50} {'─'*8} {'─'*10} {'─'*9}")
    for c_d in results[:args.limit]:
        addr  = c_d.get("registered_office_address", {})
        pc    = addr.get("postal_code", "")
        sics  = c_d.get("sic_codes", [])
        age   = c_d.get("company_age_years")
        age_s = f"{age:.1f}" if age else "—"
        print(f"  {c_d['company_number']:<12} {c_d['company_name'][:50]:<50} "
              f"{(sics[0] if sics else ''):8} {pc:<10} {age_s}")
