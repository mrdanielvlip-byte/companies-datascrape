"""
db_lookup.py — DB-first company data lookups with API fallback.

The local SQLite DB (built from bulk Companies House CSV) already holds:
  company_number, company_name, company_status, company_type,
  incorporation_date, address, sic1–sic4, accounts_category,
  accounts_last_date, accounts_next_due, mortgages_outstanding

Any enrichment module that only needs these fields should query the DB
first and skip the API call entirely.  For data NOT in the DB (officers,
PSC, charges, filing history, XBRL), the API is still required.

Usage:
    from db_lookup import get_company_profile, get_holding_co_sics, search_company_by_name
"""

import os
import sqlite3
from pathlib import Path

# ── DB connection ────────────────────────────────────────────────────────────

_DB_DIR   = Path(os.path.dirname(__file__)) / "data"
_DB_PATH  = _DB_DIR / "companies_house.db"
_DB_AVAILABLE = None  # lazy init


def db_available() -> bool:
    """Return True if the local SQLite DB exists and is usable."""
    global _DB_AVAILABLE
    if _DB_AVAILABLE is None:
        _DB_AVAILABLE = _DB_PATH.exists()
    return _DB_AVAILABLE


def _get_connection() -> sqlite3.Connection:
    """Return a read-only connection to the local DB."""
    con = sqlite3.connect(f"file:{_DB_PATH}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


# ── Public lookup functions ──────────────────────────────────────────────────

def get_company_profile(company_number: str) -> dict | None:
    """
    Look up a company's profile data from the local DB.

    Returns a dict with the fields that downstream modules typically
    fetch via GET /company/{number}:
      - sic_codes (list)
      - accounts.last_accounts.type
      - accounts.last_accounts.period_start_on
      - company_status
      - company_type
      - registered_office_address
      - mortgages (outstanding count)

    Returns None if the DB is unavailable or the company isn't found.
    """
    if not db_available():
        return None

    try:
        con = _get_connection()
        row = con.execute(
            "SELECT * FROM companies WHERE company_number = ?",
            (company_number,)
        ).fetchone()
        con.close()
    except Exception:
        return None

    if not row:
        return None

    r = dict(row)
    sics = [s for s in [r.get("sic1", ""), r.get("sic2", ""),
                         r.get("sic3", ""), r.get("sic4", "")] if s]

    return {
        "company_number":  r["company_number"],
        "company_name":    r.get("company_name", ""),
        "company_status":  r.get("company_status", ""),
        "company_type":    r.get("company_type", ""),
        "sic_codes":       sics,
        "date_of_creation": r.get("incorporation_date", ""),
        "registered_office_address": {
            "address_line_1": r.get("address_line1", ""),
            "locality":       r.get("address_town", ""),
            "region":         r.get("address_county", ""),
            "country":        r.get("address_country", ""),
            "postal_code":    r.get("postcode", ""),
        },
        "accounts": {
            "last_accounts": {
                "type":            r.get("accounts_category", "unknown"),
                "period_start_on": r.get("accounts_last_date", ""),
            },
            "next_due": r.get("accounts_next_due", ""),
        },
        "mortgages": {
            "outstanding": r.get("mortgages_outstanding", 0),
        },
    }


def get_holding_co_sics(company_number: str) -> list[str] | None:
    """
    Look up SIC codes for a holding company from the local DB.
    Returns list of SIC codes, or None if DB unavailable / not found.
    """
    if not db_available():
        return None
    try:
        con = _get_connection()
        row = con.execute(
            "SELECT sic1, sic2, sic3, sic4 FROM companies WHERE company_number = ?",
            (company_number,)
        ).fetchone()
        con.close()
    except Exception:
        return None

    if not row:
        return None
    return [s for s in [row["sic1"], row["sic2"], row["sic3"], row["sic4"]] if s]


def search_company_by_name(name: str, limit: int = 5) -> list[dict]:
    """
    Search the local DB for a company by name.

    Used by ch_enrich.analyse_ownership() to find holding companies
    instead of calling the CH /search/companies API.

    Returns list of dicts with company_number, company_name, sic_codes.
    """
    if not db_available():
        return []

    try:
        con = _get_connection()

        # Try FTS5 first for fast matching
        clean = name.upper().replace(" LIMITED", "").replace(" LTD", "").strip()
        # Escape FTS special characters
        fts_safe = clean.replace('"', '""')

        rows = con.execute(
            """
            SELECT c.company_number, c.company_name,
                   c.sic1, c.sic2, c.sic3, c.sic4,
                   c.company_status
            FROM companies_fts fts
            JOIN companies c ON c.company_number = fts.company_number
            WHERE companies_fts MATCH ?
            LIMIT ?
            """,
            (f'"{fts_safe}"', limit)
        ).fetchall()

        # If FTS returns nothing, fall back to LIKE
        if not rows:
            rows = con.execute(
                """
                SELECT company_number, company_name,
                       sic1, sic2, sic3, sic4,
                       company_status
                FROM companies
                WHERE company_name_upper LIKE ?
                LIMIT ?
                """,
                (f"%{clean}%", limit)
            ).fetchall()

        con.close()

        results = []
        for r in rows:
            r = dict(r)
            sics = [s for s in [r.get("sic1",""), r.get("sic2",""),
                                 r.get("sic3",""), r.get("sic4","")] if s]
            results.append({
                "company_number": r["company_number"],
                "company_name":   r["company_name"],
                "title":          r["company_name"],  # compat with API format
                "sic_codes":      sics,
                "company_status": r.get("company_status", ""),
            })

        return results

    except Exception:
        return []
