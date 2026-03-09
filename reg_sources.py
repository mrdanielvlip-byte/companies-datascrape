"""
reg_sources.py — UK Regulatory Register Discovery & Verification

Two operational modes:

  DISCOVERY MODE  (invoked by run.py --reg-source)
  ─────────────────────────────────────────────────
  Query a regulatory register directly to obtain a list of registered
  companies, then look each up on Companies House to get company numbers
  and SIC codes. The resulting list replaces the usual SIC-sweep step and
  feeds into the standard 11-step enrichment pipeline.

    python run.py --reg-source EA_WASTE    --reg-query "drainage"
    python run.py --reg-source EA_CARRIERS --reg-query ""        # all carriers
    python run.py --reg-source CQC         --reg-query "domiciliary care"
    python run.py --reg-source FCA         --reg-query "mortgage broker"

  VERIFICATION MODE  (called automatically by accreditations.py, step 9)
  ───────────────────────────────────────────────────────────────────────
  For a company already in the pipeline, verify whether it appears on
  one or more registers.  Returns structured registration details.

────────────────────────────────────────────────────────────────────────
Register catalogue
────────────────────────────────────────────────────────────────────────
  Key              Name                           Access          Status
  ─────────────    ─────────────────────────────  ──────────────  ──────
  EA_WASTE         EA Waste Operations Permits    HTML GET        ✅
  EA_CARRIERS      EA Waste Carriers & Brokers    HTML GET        ✅
  EA_ABSTRACTION   EA Water Abstraction Licences  HTML GET        ✅
  EA_DISCHARGES    EA Discharge Consents          HTML GET        ✅
  CQC              Care Quality Commission        REST API        ✅ (key)
  FCA              FCA Authorised Firms           REST API        ✅ (key)
  ICO              ICO Data Controllers           HTML GET        ✅ (verify only)
  GAS_SAFE         Gas Safe Register              HTML GET        ⚠️  WAF blocked
  NICEIC           NICEIC Approved Contractors    HTML GET        ⚠️  ViewState
  OFSTED           Ofsted Registered Providers    HTML GET        ✅ (verify only)
  SIA              SIA Approved Contractors       HTML GET        ✅
  TrustMark        TrustMark Registered Firms     HTML GET        ✅

API keys (add to .ch_api_key file):
  CQC_API_KEY=your_key_here
  FCA_SUBSCRIPTION_KEY=your_key_here

Free key registration:
  CQC  → https://api.cqc.org.uk/register  (instant)
  FCA  → https://register.fca.org.uk/Developer/s/  (requires approval)
"""

import os
import re
import json
import time
import sqlite3
import requests
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus

# ─────────────────────────────────────────────────────────────────────────────
#  DB cache — every register scrape is persisted for instant future access
# ─────────────────────────────────────────────────────────────────────────────

_DB_PATH = Path(__file__).parent / "data" / "companies_house.db"
_CACHE_TTL_DAYS = 30   # re-scrape if cached data is older than this


def _db_connect() -> sqlite3.Connection | None:
    """Return a read-write connection to the local CH DB, or None if unavailable."""
    if not _DB_PATH.exists():
        return None
    con = sqlite3.connect(str(_DB_PATH))
    con.row_factory = sqlite3.Row
    return con


def _ensure_cache_table(con: sqlite3.Connection):
    """Create sector_cache + register columns if they don't already exist."""
    con.executescript("""
    CREATE TABLE IF NOT EXISTS sector_cache (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        sector               TEXT NOT NULL,
        company_number       TEXT,
        company_name         TEXT,
        company_status       TEXT,
        company_type         TEXT,
        sic1                 TEXT,
        sic2                 TEXT,
        sic3                 TEXT,
        sic4                 TEXT,
        postcode             TEXT,
        address_town         TEXT,
        address_county       TEXT,
        incorporation_date   TEXT,
        company_age_years    REAL,
        mortgages_outstanding INTEGER,
        uri                  TEXT,
        cached_at            TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_sc_sector ON sector_cache(sector);
    CREATE INDEX IF NOT EXISTS idx_sc_number ON sector_cache(company_number);
    """)

    # ── Migration: drop NOT NULL on company_number if it was added in an older version ──
    col_info = con.execute("PRAGMA table_info(sector_cache)").fetchall()
    cn_col = next((r for r in col_info if r[1] == "company_number"), None)
    if cn_col and cn_col[3] == 1:   # notnull == 1 → must migrate
        print("  ⚙️  Migrating sector_cache: removing NOT NULL from company_number ...",
              flush=True)
        con.executescript("""
        ALTER TABLE sector_cache RENAME TO sector_cache_old;
        CREATE TABLE sector_cache (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            sector               TEXT NOT NULL,
            company_number       TEXT,
            company_name         TEXT,
            company_status       TEXT,
            company_type         TEXT,
            sic1                 TEXT,
            sic2                 TEXT,
            sic3                 TEXT,
            sic4                 TEXT,
            postcode             TEXT,
            address_town         TEXT,
            address_county       TEXT,
            incorporation_date   TEXT,
            company_age_years    REAL,
            mortgages_outstanding INTEGER,
            uri                  TEXT,
            cached_at            TEXT NOT NULL,
            register_name        TEXT,
            register_source      TEXT,
            register_reg_no      TEXT,
            register_rsb         TEXT,
            register_address     TEXT,
            register_website     TEXT,
            register_legal_form  TEXT,
            ch_matched           INTEGER DEFAULT 0,
            register_raw         TEXT
        );
        INSERT INTO sector_cache SELECT * FROM sector_cache_old;
        DROP TABLE sector_cache_old;
        CREATE INDEX IF NOT EXISTS idx_sc_sector ON sector_cache(sector);
        CREATE INDEX IF NOT EXISTS idx_sc_number ON sector_cache(company_number);
        """)
        print("  ✅ Migration complete", flush=True)
    # Add register-specific columns that may not exist in older DBs
    existing_cols = {r[1] for r in con.execute("PRAGMA table_info(sector_cache)")}
    extras = [
        ("register_name",    "TEXT"),
        ("register_source",  "TEXT"),
        ("register_reg_no",  "TEXT"),
        ("register_rsb",     "TEXT"),
        ("register_address", "TEXT"),
        ("register_website", "TEXT"),
        ("register_legal_form", "TEXT"),
        ("register_raw",     "TEXT"),   # full JSON blob of raw register entry
        ("ch_matched",       "INTEGER DEFAULT 0"),
    ]
    for col, defn in extras:
        if col not in existing_cols:
            con.execute(f"ALTER TABLE sector_cache ADD COLUMN {col} {defn}")
    try:
        con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_sc_sector_number "
            "ON sector_cache(sector, company_number) WHERE company_number IS NOT NULL"
        )
    except Exception:
        pass
    con.commit()


def save_register_to_cache(
    register_key: str,
    companies: list[dict],
    source_url: str = "",
) -> int:
    """
    Persist a list of discovered register companies into sector_cache.
    Called automatically after every discover() run.

    Args:
        register_key: e.g. "EA_WASTE", "CQC", "AUDIT_REGISTER"
        companies:    list of company dicts (output of discover() or custom scrapers)
        source_url:   URL scraped (for provenance)

    Returns:
        Number of rows inserted.
    """
    con = _db_connect()
    if con is None:
        return 0
    _ensure_cache_table(con)

    now    = datetime.utcnow().isoformat()
    sector = f"reg_{register_key.lower()}"
    rows   = []

    for c in companies:
        # Support both pipeline-format dicts and raw register dicts
        reg_raw = c.get("registrations", {}).get(register_key, {})
        rows.append((
            sector,
            c.get("company_number") or c.get("CompanyNumber"),
            c.get("company_name")   or c.get("company_name_raw") or c.get("name"),
            c.get("company_status", "Active"),
            c.get("company_type"),
            c.get("sic_codes", [None])[0] if isinstance(c.get("sic_codes"), list)
                else c.get("sic1"),
            None, None, None,          # sic2/3/4
            c.get("registered_office_address", {}).get("postal_code") or c.get("postcode"),
            c.get("registered_office_address", {}).get("locality")    or c.get("address_town"),
            c.get("registered_office_address", {}).get("region")      or c.get("address_county"),
            c.get("date_of_creation") or c.get("incorporation_date"),
            c.get("company_age_years"),
            None,                      # mortgages_outstanding
            c.get("links", {}).get("self") or c.get("uri"),
            now,
            # register columns
            reg_raw.get("company_name") or c.get("company_name"),
            source_url or register_key,
            reg_raw.get("registration_number") or c.get("reg_no"),
            reg_raw.get("rsb") or reg_raw.get("register_key"),
            reg_raw.get("address_raw") or reg_raw.get("permit_location"),
            reg_raw.get("website"),
            reg_raw.get("legal_form") or c.get("company_type"),
            json.dumps(reg_raw) if reg_raw else None,
            1 if c.get("company_number") else 0,
        ))

    if not rows:
        con.close()
        return 0

    con.executemany("""
        INSERT OR REPLACE INTO sector_cache (
            sector, company_number, company_name, company_status, company_type,
            sic1, sic2, sic3, sic4, postcode, address_town, address_county,
            incorporation_date, company_age_years, mortgages_outstanding, uri,
            cached_at, register_name, register_source, register_reg_no,
            register_rsb, register_address, register_website,
            register_legal_form, register_raw, ch_matched
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    con.commit()
    inserted = con.execute(
        "SELECT COUNT(*) FROM sector_cache WHERE sector=?", (sector,)
    ).fetchone()[0]
    con.close()
    print(f"  💾 Cached {inserted:,} '{register_key}' results → sector_cache (sector='{sector}')")
    return inserted


def load_register_from_cache(
    register_key: str,
    max_age_days: int = _CACHE_TTL_DAYS,
) -> list[dict] | None:
    """
    Load previously cached register results from the DB.

    Returns:
        List of company dicts if cache is fresh, None if cache is missing/stale.
    """
    con = _db_connect()
    if con is None:
        return None
    try:
        _ensure_cache_table(con)
    except Exception:
        con.close()
        return None

    sector     = f"reg_{register_key.lower()}"
    cutoff     = (datetime.utcnow() - timedelta(days=max_age_days)).isoformat()
    count      = con.execute(
        "SELECT COUNT(*) FROM sector_cache WHERE sector=? AND cached_at > ?",
        (sector, cutoff)
    ).fetchone()[0]

    if count == 0:
        con.close()
        return None

    rows = con.execute(
        "SELECT * FROM sector_cache WHERE sector=? AND cached_at > ? ORDER BY id",
        (sector, cutoff)
    ).fetchall()
    con.close()

    results = []
    for r in rows:
        d = dict(r)
        # Re-inflate register_raw JSON
        if d.get("register_raw"):
            try:
                d["register_raw"] = json.loads(d["register_raw"])
            except Exception:
                pass
        results.append(d)

    print(f"  📦 Loaded {len(results):,} '{register_key}' results from cache "
          f"(sector='{sector}', ≤{max_age_days}d old)")
    return results


def cache_stats(register_key: str | None = None) -> dict:
    """
    Return summary stats for the sector_cache table.
    If register_key is given, scoped to that register only.
    """
    con = _db_connect()
    if con is None:
        return {}
    try:
        _ensure_cache_table(con)
    except Exception:
        con.close()
        return {}

    if register_key:
        sector = f"reg_{register_key.lower()}"
        rows = con.execute("""
            SELECT sector, COUNT(*) as total,
                   SUM(ch_matched) as matched,
                   MAX(cached_at) as last_updated
            FROM sector_cache WHERE sector=? GROUP BY sector
        """, (sector,)).fetchall()
    else:
        rows = con.execute("""
            SELECT sector, COUNT(*) as total,
                   SUM(ch_matched) as matched,
                   MAX(cached_at) as last_updated
            FROM sector_cache GROUP BY sector ORDER BY total DESC
        """).fetchall()

    con.close()
    return {r["sector"]: dict(r) for r in rows}


# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

EA_BASE = "https://environment.data.gov.uk/public-register"
CQC_BASE = "https://api.cqc.org.uk/public/v1"
FCA_BASE = "https://register.fca.org.uk/services/V0.1"
ICO_BASE = "https://ico.org.uk/ESDWebPages/Search"
OFSTED_BASE = "https://reports.ofsted.gov.uk/search"
SIA_BASE = "https://services.sia.homeoffice.gov.uk/rolh"
TRUSTMARK_BASE = "https://www.trustmark.org.uk/find-a-tradesperson"

# Register catalogue — metadata for each supported register
REGISTER_CATALOGUE = {

    # ── Environment Agency ───────────────────────────────────────────────────
    "EA_WASTE": {
        "name":        "EA Waste Operations Permits",
        "authority":   "Environment Agency",
        "description": "Organisations holding Environmental Permits for waste operations "
                       "(landfill, transfer, treatment, MRF, etc.)",
        "sectors":     ["waste management", "recycling", "skip hire", "remediation"],
        "sic_hints":   ["38110", "38120", "38210", "38220", "39000"],
        "type":        "ea_html",
        "base_url":    f"{EA_BASE}/waste-operations/registration",
        "query_param": "name-search",
        "discovery":   True,
        "api_key":     None,
    },
    "EA_CARRIERS": {
        "name":        "EA Waste Carriers & Brokers",
        "authority":   "Environment Agency",
        "description": "Carriers, brokers and dealers of controlled waste (Tier 2 = upper tier = full registration)",
        "sectors":     ["waste transport", "skip hire", "hazardous waste"],
        "sic_hints":   ["38110", "49410", "38220"],
        "type":        "ea_html",
        "base_url":    f"{EA_BASE}/waste-carriers-brokers/registration",
        "query_param": "name-search",
        "discovery":   True,
        "api_key":     None,
    },
    "EA_ABSTRACTION": {
        "name":        "EA Water Abstraction Licences",
        "authority":   "Environment Agency",
        "description": "Organisations licensed to abstract water from rivers, lakes or groundwater",
        "sectors":     ["water utilities", "agriculture", "industrial processing"],
        "sic_hints":   ["36000", "01110", "01500"],
        "type":        "ea_html",
        "base_url":    f"{EA_BASE}/water-abstraction/registration",
        "query_param": "name-search",
        "discovery":   True,
        "api_key":     None,
    },
    "EA_DISCHARGES": {
        "name":        "EA Discharge Consents",
        "authority":   "Environment Agency",
        "description": "Consents to discharge to watercourses or groundwater",
        "sectors":     ["water", "industrial", "sewage treatment"],
        "sic_hints":   ["37000", "36000", "20110"],
        "type":        "ea_html",
        "base_url":    f"{EA_BASE}/discharge-consents/registration",
        "query_param": "name-search",
        "discovery":   True,
        "api_key":     None,
    },

    # ── CQC ──────────────────────────────────────────────────────────────────
    "CQC": {
        "name":        "Care Quality Commission Providers",
        "authority":   "CQC",
        "description": "Regulated health and social care providers in England "
                       "(care homes, homecare, hospitals, dentists, GPs)",
        "sectors":     ["health", "social care", "domiciliary care", "dentistry", "GP"],
        "sic_hints":   ["86100", "86210", "86220", "86230", "86900",
                        "87100", "87200", "87300", "87900", "88100", "88990"],
        "type":        "cqc_api",
        "base_url":    CQC_BASE,
        "discovery":   True,
        "api_key":     "CQC_API_KEY",
    },

    # ── FCA ──────────────────────────────────────────────────────────────────
    "FCA": {
        "name":        "FCA Authorised Firms",
        "authority":   "Financial Conduct Authority",
        "description": "FCA-authorised and regulated financial services firms "
                       "(mortgage brokers, IFAs, insurers, credit firms, etc.)",
        "sectors":     ["financial services", "mortgage brokers", "IFA",
                        "insurance", "credit"],
        "sic_hints":   ["64110", "64191", "64192", "64910", "64991",
                        "65110", "65120", "65201", "65202", "66110",
                        "66120", "66190", "66210", "66220", "66290"],
        "type":        "fca_api",
        "base_url":    FCA_BASE,
        "discovery":   True,
        "api_key":     "FCA_SUBSCRIPTION_KEY",
    },

    # ── ICO ──────────────────────────────────────────────────────────────────
    "ICO": {
        "name":        "ICO Data Protection Register",
        "authority":   "Information Commissioner's Office",
        "description": "Organisations registered as data controllers under UK GDPR / DPA 2018",
        "sectors":     ["all"],
        "sic_hints":   [],
        "type":        "ico_html",
        "base_url":    ICO_BASE,
        "discovery":   False,   # Verify only — no bulk search supported
        "api_key":     None,
    },

    # ── Ofsted ───────────────────────────────────────────────────────────────
    "OFSTED": {
        "name":        "Ofsted Registered Providers",
        "authority":   "Ofsted",
        "description": "Schools, nurseries, childminders, further education and "
                       "children's social care providers registered with Ofsted",
        "sectors":     ["education", "nurseries", "social care for children"],
        "sic_hints":   ["85100", "85200", "85310", "85320", "85410",
                        "85421", "85422", "88910"],
        "type":        "ofsted_html",
        "base_url":    OFSTED_BASE,
        "discovery":   False,   # Verify only
        "api_key":     None,
    },

    # ── SIA ───────────────────────────────────────────────────────────────────
    "SIA": {
        "name":        "SIA Approved Contractors",
        "authority":   "Security Industry Authority",
        "description": "Security companies approved under the SIA Approved Contractor Scheme (ACS)",
        "sectors":     ["security", "guarding", "door supervision", "CCTV"],
        "sic_hints":   ["80100"],
        "type":        "sia_html",
        "base_url":    SIA_BASE,
        "discovery":   False,   # Verify only
        "api_key":     None,
    },

    # ── Gas Safe (blocked) ───────────────────────────────────────────────────
    "GAS_SAFE": {
        "name":        "Gas Safe Register",
        "authority":   "Gas Safe Register",
        "description": "Businesses and engineers legally registered to work with gas appliances",
        "sectors":     ["gas", "heating", "plumbing"],
        "sic_hints":   ["43220"],
        "type":        "blocked",
        "note":        "Protected by Incapsula WAF — programmatic access not available",
        "discovery":   False,
        "api_key":     None,
    },

    # ── NICEIC (blocked) ─────────────────────────────────────────────────────
    "NICEIC": {
        "name":        "NICEIC Approved Contractors",
        "authority":   "NICEIC",
        "description": "Electrical contractors assessed to NICEIC standards",
        "sectors":     ["electrical", "building services"],
        "sic_hints":   ["43210"],
        "type":        "blocked",
        "note":        "ASP.NET ViewState + reCAPTCHA — not accessible",
        "discovery":   False,
        "api_key":     None,
    },
}


HEADERS = {
    "User-Agent": "PE-Research-Pipeline/1.0 (regulatory register enrichment)",
    "Accept":     "text/html,application/xhtml+xml,application/json",
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ─────────────────────────────────────────────────────────────────────────────
#  Config loading
# ─────────────────────────────────────────────────────────────────────────────

def load_reg_key(key_name: str) -> str | None:
    """Load a named API key from the .ch_api_key file."""
    key_file = os.path.join(os.path.dirname(__file__), ".ch_api_key")
    if os.path.exists(key_file):
        with open(key_file) as f:
            for line in f:
                line = line.strip()
                if "=" in line and line.startswith(key_name):
                    return line.split("=", 1)[1].strip()
    return os.environ.get(key_name)


# ─────────────────────────────────────────────────────────────────────────────
#  EA HTML Register scraping
# ─────────────────────────────────────────────────────────────────────────────

def _ea_fetch_page(base_url: str, query: str, start: int = 0) -> str | None:
    """Fetch one page of EA register results (GOV.UK HTML table)."""
    params = {"name-search": query}
    if start:
        params["start"] = start
    try:
        r = SESSION.get(base_url, params=params, timeout=20)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None


def _ea_parse_table(html: str) -> list[dict]:
    """
    Parse the govuk-table in an EA public register HTML page.
    Returns a list of row dicts keyed by lowercased column headers.
    """
    # Extract headers
    headers = re.findall(
        r'<th[^>]*class="[^"]*govuk-table__header[^"]*"[^>]*>(.*?)</th>',
        html, re.DOTALL | re.IGNORECASE,
    )
    headers = [re.sub(r"<[^>]+>", "", h).strip().lower().replace(" ", "_")
               for h in headers]

    # Extract data rows
    row_htmls = re.findall(
        r'<tr[^>]*class="[^"]*govuk-table__row[^"]*"[^>]*>(.*?)</tr>',
        html, re.DOTALL,
    )

    rows = []
    for rh in row_htmls:
        cells_raw = re.findall(r'<td[^>]*>(.*?)</td>', rh, re.DOTALL)
        cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells_raw]
        if not cells:
            continue
        if headers and len(cells) == len(headers):
            row = dict(zip(headers, cells))
        else:
            # Fallback — positional
            row = {f"col_{i}": v for i, v in enumerate(cells)}
        rows.append(row)

    return rows


def _ea_total_results(html: str) -> int:
    """Try to extract the total result count from the page."""
    m = re.search(
        r"(\d[\d,]*)\s+result",
        html, re.IGNORECASE,
    )
    if m:
        return int(m.group(1).replace(",", ""))
    return 0


def _ea_normalise(row: dict, register_key: str) -> dict:
    """Convert a raw EA row to a standard registration dict."""
    # Try to extract common fields by common column names
    name_keys = ["operator_name", "permit_holder", "holder_name",
                 "organisation_name", "name", "col_0"]
    addr_keys = ["address", "site_address", "col_1"]
    ref_keys  = ["permit_number", "permit_reference", "reference",
                 "registration_number", "licence_number", "col_3"]
    type_keys = ["activity", "permit_type", "waste_type", "description", "col_2"]

    name = next((row.get(k, "") for k in name_keys if row.get(k)), "") or list(row.values())[0] if row else ""
    addr = next((row.get(k, "") for k in addr_keys if row.get(k)), "")
    ref  = next((row.get(k, "") for k in ref_keys  if row.get(k)), "")
    typ  = next((row.get(k, "") for k in type_keys  if row.get(k)), "")

    return {
        "company_name":      name.upper().strip(),
        "address":           addr.strip(),
        "permit_reference":  ref.strip(),
        "permit_type":       typ.strip(),
        "register":          register_key,
        "source":            REGISTER_CATALOGUE[register_key]["name"],
        "raw_row":           row,
    }


def discover_ea(register_key: str, keyword: str = "", max_pages: int = 20) -> list[dict]:
    """
    Search an Environment Agency HTML register for companies matching keyword.
    Pass keyword="" to attempt to retrieve all entries (may be paginated).

    Returns list of normalised registration dicts.
    """
    cfg_entry = REGISTER_CATALOGUE.get(register_key)
    if not cfg_entry or cfg_entry["type"] != "ea_html":
        raise ValueError(f"Register {register_key!r} is not an EA HTML register")

    base_url = cfg_entry["base_url"]
    results  = []
    start    = 0
    page_size = 25  # GOV.UK default page size for these registers

    print(f"  EA Register [{cfg_entry['name']}] — querying '{keyword}' ...")

    for page in range(max_pages):
        html = _ea_fetch_page(base_url, keyword, start=start if page > 0 else 0)
        if not html:
            break

        rows = _ea_parse_table(html)
        if not rows:
            break

        normalised = [_ea_normalise(r, register_key) for r in rows]
        results.extend(normalised)

        # Check for pagination
        total = _ea_total_results(html)
        fetched = start + len(rows)
        if total and fetched >= total:
            break
        if len(rows) < page_size:
            break  # Last page
        start += page_size
        time.sleep(0.5)

    # Deduplicate by company name
    seen  = set()
    dedup = []
    for r in results:
        key = r["company_name"].lower()
        if key not in seen and key:
            seen.add(key)
            dedup.append(r)

    print(f"    → {len(dedup)} unique registrants found")
    return dedup


def verify_ea(company_name: str, register_key: str) -> dict:
    """
    Check whether a specific company appears on an EA register.
    Uses company name search (strips LTD/LIMITED for better matching).
    Returns a match dict or {'found': False}.
    """
    clean = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC)\b", "", company_name,
                   flags=re.IGNORECASE).strip().strip(".,")
    if len(clean) < 3:
        clean = company_name

    cfg_entry = REGISTER_CATALOGUE.get(register_key, {})
    base_url  = cfg_entry.get("base_url", "")
    if not base_url:
        return {"found": False, "register": register_key}

    html = _ea_fetch_page(base_url, clean)
    if not html:
        return {"found": False, "register": register_key}

    rows = _ea_parse_table(html)
    if not rows:
        return {"found": False, "register": register_key}

    # Name match check
    for row in rows:
        r = _ea_normalise(row, register_key)
        if _name_match(clean, r["company_name"]):
            return {
                "found":           True,
                "register":        register_key,
                "register_name":   cfg_entry["name"],
                "company_name":    r["company_name"],
                "address":         r["address"],
                "permit_reference":r["permit_reference"],
                "permit_type":     r["permit_type"],
                "data_tier":       "Tier 1 — EA Public Register",
            }

    return {"found": False, "register": register_key}


# ─────────────────────────────────────────────────────────────────────────────
#  CQC REST API
# ─────────────────────────────────────────────────────────────────────────────

def _cqc_headers(api_key: str) -> dict:
    """CQC API accepts either Ocp-Apim-Subscription-Key or Bearer token."""
    return {
        "Accept":                    "application/json",
        "Ocp-Apim-Subscription-Key": api_key,
        "User-Agent":                "PE-Research-Pipeline/1.0",
    }


def discover_cqc(keyword: str, api_key: str, max_pages: int = 50) -> list[dict]:
    """
    Discover CQC-registered providers matching a keyword.
    Requires a free CQC API key from https://api.cqc.org.uk/register

    Returns list of normalised registration dicts.
    """
    if not api_key:
        print("  CQC: no API key — skipping (set CQC_API_KEY in .ch_api_key)")
        return []

    results    = []
    page       = 1
    page_size  = 100

    print(f"  CQC Register — querying '{keyword}' ...")

    while page <= max_pages:
        try:
            r = requests.get(
                f"{CQC_BASE}/providers/search",
                params={
                    "providerName": keyword,
                    "pageSize":     page_size,
                    "page":         page,
                },
                headers=_cqc_headers(api_key),
                timeout=20,
            )
            if r.status_code == 403:
                print(f"  CQC: 403 Forbidden — check your API key")
                break
            if r.status_code != 200:
                print(f"  CQC: HTTP {r.status_code}")
                break

            data      = r.json()
            providers = data.get("providers", [])
            if not providers:
                break

            for p in providers:
                results.append({
                    "company_name":      (p.get("name", "") or "").upper().strip(),
                    "address":           _cqc_format_address(p),
                    "cqc_provider_id":   p.get("providerId", ""),
                    "cqc_status":        p.get("registrationStatus", ""),
                    "cqc_type":          p.get("type", ""),
                    "cqc_rating":        (p.get("currentRatings") or {}).get("overall", {}).get("rating", ""),
                    "permit_reference":  p.get("providerId", ""),
                    "permit_type":       p.get("type", ""),
                    "register":          "CQC",
                    "source":            "CQC Public Register",
                })

            total     = data.get("total", 0)
            fetched   = (page - 1) * page_size + len(providers)
            if fetched >= total or len(providers) < page_size:
                break

            page += 1
            time.sleep(0.3)

        except Exception as e:
            print(f"  CQC error: {e}")
            break

    # Deduplicate by provider ID
    seen, dedup = set(), []
    for r in results:
        k = r["cqc_provider_id"]
        if k not in seen:
            seen.add(k)
            dedup.append(r)

    print(f"    → {len(dedup)} CQC providers found")
    return dedup


def verify_cqc(company_name: str, api_key: str) -> dict:
    """Verify if a specific company is CQC-registered."""
    if not api_key:
        return {"found": False, "register": "CQC", "note": "No API key"}

    clean = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC)\b", "", company_name,
                   flags=re.IGNORECASE).strip()
    try:
        r = requests.get(
            f"{CQC_BASE}/providers/search",
            params={"providerName": clean, "pageSize": 10},
            headers=_cqc_headers(api_key),
            timeout=15,
        )
        if r.status_code != 200:
            return {"found": False, "register": "CQC"}

        providers = r.json().get("providers", [])
        for p in providers:
            if _name_match(clean, p.get("name", "")):
                return {
                    "found":           True,
                    "register":        "CQC",
                    "register_name":   "CQC Registered Provider",
                    "company_name":    p.get("name", ""),
                    "address":         _cqc_format_address(p),
                    "cqc_provider_id": p.get("providerId", ""),
                    "cqc_status":      p.get("registrationStatus", ""),
                    "cqc_type":        p.get("type", ""),
                    "cqc_rating":      (p.get("currentRatings") or {}).get("overall", {}).get("rating", ""),
                    "data_tier":       "Tier 1 — CQC Public Register",
                }
    except Exception:
        pass

    return {"found": False, "register": "CQC"}


def _cqc_format_address(p: dict) -> str:
    addr = p.get("postalAddressLine1", "")
    town = p.get("postalAddressTownCity", "")
    pc   = p.get("postalCode", "")
    return ", ".join(filter(None, [addr, town, pc]))


# ─────────────────────────────────────────────────────────────────────────────
#  FCA REST API
# ─────────────────────────────────────────────────────────────────────────────

def discover_fca(keyword: str, api_key: str, max_results: int = 500) -> list[dict]:
    """
    Discover FCA-authorised firms matching a keyword.
    Requires a free FCA API subscription key from https://register.fca.org.uk/Developer/s/

    Returns list of normalised registration dicts.
    """
    if not api_key:
        print("  FCA: no API key — skipping (set FCA_SUBSCRIPTION_KEY in .ch_api_key)")
        return []

    headers = {
        "Accept":                    "application/json",
        "Ocp-Apim-Subscription-Key": api_key,
        "User-Agent":                "PE-Research-Pipeline/1.0",
    }

    results = []
    print(f"  FCA Register — querying '{keyword}' ...")

    try:
        r = requests.get(
            f"{FCA_BASE}/Firms",
            params={"q": keyword},
            headers=headers,
            timeout=20,
        )
        if r.status_code == 403:
            print(f"  FCA: 403 — check your Subscription Key")
            return []
        if r.status_code != 200:
            print(f"  FCA: HTTP {r.status_code}")
            return []

        data  = r.json()
        firms = data.get("Data", [])

        for f in firms[:max_results]:
            results.append({
                "company_name":  (f.get("Name", "") or "").upper().strip(),
                "fca_ref":       f.get("FirmReference", f.get("Reference", "")),
                "fca_status":    f.get("Status", ""),
                "fca_type":      f.get("Type", ""),
                "address":       f.get("Address", ""),
                "register":      "FCA",
                "source":        "FCA Authorised Firms Register",
            })

    except Exception as e:
        print(f"  FCA error: {e}")

    print(f"    → {len(results)} FCA firms found")
    return results


def verify_fca(company_name: str, api_key: str) -> dict:
    """Verify if a specific company is FCA-authorised."""
    if not api_key:
        return {"found": False, "register": "FCA", "note": "No API key"}

    clean = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC)\b", "", company_name,
                   flags=re.IGNORECASE).strip()

    headers = {
        "Accept":                    "application/json",
        "Ocp-Apim-Subscription-Key": api_key,
    }

    try:
        r = requests.get(
            f"{FCA_BASE}/Firms",
            params={"q": clean},
            headers=headers,
            timeout=15,
        )
        if r.status_code != 200:
            return {"found": False, "register": "FCA"}

        firms = r.json().get("Data", [])
        for f in firms:
            if _name_match(clean, f.get("Name", "")):
                return {
                    "found":         True,
                    "register":      "FCA",
                    "register_name": "FCA Authorised Firm",
                    "company_name":  f.get("Name", ""),
                    "fca_ref":       f.get("FirmReference", ""),
                    "fca_status":    f.get("Status", ""),
                    "fca_type":      f.get("Type", ""),
                    "data_tier":     "Tier 1 — FCA Public Register",
                }
    except Exception:
        pass

    return {"found": False, "register": "FCA"}


# ─────────────────────────────────────────────────────────────────────────────
#  ICO HTML check
# ─────────────────────────────────────────────────────────────────────────────

def verify_ico(company_name: str) -> dict:
    """
    Check the ICO data protection register for a specific company name.
    Returns registration number if found.
    """
    clean = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC)\b", "", company_name,
                   flags=re.IGNORECASE).strip()
    params = {
        "SearchType":   "Organisation",
        "SearchText":   clean,
        "SubmitButton": "Search",
    }
    try:
        r = SESSION.get(ICO_BASE, params=params, timeout=12,
                        headers={"Accept": "text/html"})
        if r.status_code == 200:
            rn_match   = re.search(r"\bZ\d{6,8}\b", r.text)
            name_frag  = clean.lower()[:10]
            name_match = name_frag in r.text.lower() if len(name_frag) >= 4 else False
            if rn_match and name_match:
                return {
                    "found":         True,
                    "register":      "ICO",
                    "register_name": "ICO Data Controller",
                    "ico_reg_number": rn_match.group(0),
                    "data_tier":     "Tier 1 — ICO Public Register",
                }
    except Exception:
        pass
    return {"found": False, "register": "ICO"}


# ─────────────────────────────────────────────────────────────────────────────
#  Ofsted HTML check
# ─────────────────────────────────────────────────────────────────────────────

def verify_ofsted(company_name: str) -> dict:
    """
    Check Ofsted reports search for a provider by name.
    Returns registration details if found.
    """
    clean = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC)\b", "", company_name,
                   flags=re.IGNORECASE).strip()
    params = {
        "q":      clean,
        "level_4": "1,2,3,4",
        "type":   "provider",
    }
    try:
        r = SESSION.get(OFSTED_BASE, params=params, timeout=12)
        if r.status_code == 200:
            # Detect presence of results in Ofsted search HTML
            if clean.lower()[:8] in r.text.lower() and "urn" in r.text.lower():
                urn_match = re.search(r"\bURN[:\s]*(\d{6,7})\b", r.text, re.IGNORECASE)
                return {
                    "found":         True,
                    "register":      "OFSTED",
                    "register_name": "Ofsted Registered Provider",
                    "ofsted_urn":    urn_match.group(1) if urn_match else "",
                    "data_tier":     "Tier 1 — Ofsted Public Register",
                }
    except Exception:
        pass
    return {"found": False, "register": "OFSTED"}


# ─────────────────────────────────────────────────────────────────────────────
#  SIA Approved Contractors
# ─────────────────────────────────────────────────────────────────────────────

def verify_sia(company_name: str) -> dict:
    """Check if a security company holds SIA Approved Contractor Scheme (ACS) status."""
    clean = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC)\b", "", company_name,
                   flags=re.IGNORECASE).strip()
    try:
        r = SESSION.get(
            SIA_BASE,
            params={"keywords": clean, "type": "acs"},
            timeout=12,
        )
        if r.status_code == 200 and clean.lower()[:8] in r.text.lower():
            return {
                "found":         True,
                "register":      "SIA",
                "register_name": "SIA Approved Contractor",
                "data_tier":     "Tier 1 — SIA ACS Register",
            }
    except Exception:
        pass
    return {"found": False, "register": "SIA"}


# ─────────────────────────────────────────────────────────────────────────────
#  Companies House reverse-lookup
# ─────────────────────────────────────────────────────────────────────────────

def _ch_lookup(company_name: str, ch_api_key: str, max_results: int = 5) -> dict | None:
    """
    Look up a company name on Companies House and return the best matching
    active company dict (company_number, company_name, sic_codes, address, etc.)
    Returns None if no match found.
    """
    if not ch_api_key:
        return None

    clean = company_name.strip()
    url   = "https://api.company-information.service.gov.uk/search/companies"
    try:
        r = requests.get(
            url,
            params={"q": clean, "items_per_page": max_results},
            auth=(ch_api_key, ""),
            timeout=15,
        )
        if r.status_code != 200:
            return None

        items = r.json().get("items", [])
        for item in items:
            name   = (item.get("title") or item.get("company_name", "")).upper()
            status = item.get("company_status", "")
            if status == "active" and _name_match(clean, name):
                addr = item.get("registered_office_address", {})
                return {
                    "company_number":            item.get("company_number", ""),
                    "company_name":              name,
                    "company_status":            "active",
                    "date_of_creation":          item.get("date_of_creation", ""),
                    "registered_office_address": addr,
                    "sic_codes":                 item.get("sic_codes", []),
                    "relevance_score":           85,
                    "source":                    "reg_source_discovery",
                }
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  Master discovery function
# ─────────────────────────────────────────────────────────────────────────────

def discover(
    register_key: str,
    keyword:      str,
    ch_api_key:   str,
    reg_api_key:  str | None = None,
    max_pages:    int = 30,
) -> list[dict]:
    """
    High-level discovery: query a regulatory register, then cross-reference
    each result with Companies House to get company numbers and SIC codes.

    Returns a list of normalised company dicts (same format as ch_search.py output)
    with an added 'registrations' field containing the raw register entry.

    Args:
        register_key: Key from REGISTER_CATALOGUE (e.g. "EA_WASTE", "CQC")
        keyword:      Search term (sector description, empty string = all)
        ch_api_key:   Companies House API key
        reg_api_key:  Register-specific API key (CQC, FCA); auto-loaded if None
        max_pages:    Maximum pagination pages to fetch

    Returns:
        List of company dicts ready for the enrichment pipeline.
    """
    cfg_entry = REGISTER_CATALOGUE.get(register_key)
    if not cfg_entry:
        raise ValueError(f"Unknown register: {register_key!r}. "
                         f"Valid keys: {list(REGISTER_CATALOGUE)}")

    if cfg_entry["type"] == "blocked":
        print(f"  {register_key}: {cfg_entry.get('note', 'Not accessible')}")
        return []

    if not cfg_entry.get("discovery"):
        print(f"  {register_key}: discovery mode not supported — use verify() instead")
        return []

    # ── Load API key if needed ────────────────────────────────────────────────
    if reg_api_key is None and cfg_entry.get("api_key"):
        reg_api_key = load_reg_key(cfg_entry["api_key"])

    # ── Fetch from register ──────────────────────────────────────────────────
    if cfg_entry["type"] == "ea_html":
        reg_results = discover_ea(register_key, keyword, max_pages=max_pages)
    elif cfg_entry["type"] == "cqc_api":
        reg_results = discover_cqc(keyword, reg_api_key or "", max_pages=max_pages)
    elif cfg_entry["type"] == "fca_api":
        reg_results = discover_fca(keyword, reg_api_key or "")
    else:
        print(f"  {register_key}: unsupported type {cfg_entry['type']!r}")
        return []

    if not reg_results:
        print(f"  No results from {register_key}")
        return []

    # ── CH cross-reference ────────────────────────────────────────────────────
    print(f"  Cross-referencing {len(reg_results)} companies against Companies House ...")

    companies   = []
    ch_found    = 0
    ch_not_found = 0

    for i, reg in enumerate(reg_results):
        if i % 20 == 0 and i > 0:
            print(f"    [{i}/{len(reg_results)}] CH lookups: {ch_found} found / "
                  f"{ch_not_found} not matched ...")

        name = reg["company_name"]
        if not name:
            continue

        ch = _ch_lookup(name, ch_api_key)
        if ch:
            ch["registrations"] = {register_key: reg}
            ch["reg_source"]    = register_key
            companies.append(ch)
            ch_found += 1
        else:
            # Include unmatched with placeholder company number
            # (will be filtered downstream if company_number is empty)
            ch_not_found += 1

        time.sleep(0.12)  # Respect CH API rate limit (600/min)

    print(f"\n  Discovery complete: {ch_found} Companies House matches from "
          f"{len(reg_results)} register entries ({ch_not_found} not matched)")

    # ── Auto-save to local DB cache ───────────────────────────────────────────
    try:
        save_register_to_cache(
            register_key = register_key,
            companies    = companies,
            source_url   = cfg_entry.get("base_url", register_key),
        )
    except Exception as e:
        print(f"  ⚠️  Cache save failed: {e} (results still returned)")

    return companies


# ─────────────────────────────────────────────────────────────────────────────
#  Master verification function
# ─────────────────────────────────────────────────────────────────────────────

def verify_all(
    company: dict,
    cqc_api_key: str | None = None,
    fca_api_key: str | None = None,
) -> dict:
    """
    Run all applicable register checks for a single company.
    Called by accreditations.py to enrich the 'registrations' field.

    Automatically selects which registers to check based on SIC codes:
      - EA_WASTE     → SIC 38xxx
      - EA_CARRIERS  → SIC 38xxx, 49410
      - CQC          → SIC 86xxx, 87xxx, 88xxx
      - FCA          → SIC 64xxx, 65xxx, 66xxx
      - ICO          → all companies (quick HTML check)
      - OFSTED       → SIC 85xxx, 88910
      - SIA          → SIC 80100

    Returns a dict of {register_key: result_dict}
    """
    name     = company.get("company_name", "")
    sic_list = [str(s) for s in company.get("sic_codes", [])]

    # Auto-load keys
    if cqc_api_key is None:
        cqc_api_key = load_reg_key("CQC_API_KEY")
    if fca_api_key is None:
        fca_api_key = load_reg_key("FCA_SUBSCRIPTION_KEY")

    regs = {}

    # EA Waste Operations — SIC 38xxx
    if any(s.startswith("38") for s in sic_list):
        regs["EA_WASTE"]    = verify_ea(name, "EA_WASTE")
        regs["EA_CARRIERS"] = verify_ea(name, "EA_CARRIERS")
        time.sleep(0.3)

    # CQC — health & social care
    if any(s.startswith(p) for s in sic_list
           for p in ("86", "87", "88")):
        regs["CQC"] = verify_cqc(name, cqc_api_key or "")
        time.sleep(0.3)

    # FCA — financial services
    if any(s.startswith(p) for s in sic_list
           for p in ("64", "65", "66")):
        regs["FCA"] = verify_fca(name, fca_api_key or "")
        time.sleep(0.3)

    # ICO — all companies (broad signal, quick)
    regs["ICO"] = verify_ico(name)
    time.sleep(0.3)

    # Ofsted — education & children's social care
    if any(s.startswith(p) for s in sic_list
           for p in ("85", "88910")):
        regs["OFSTED"] = verify_ofsted(name)
        time.sleep(0.3)

    # SIA — security sector
    if any(s.startswith("80") for s in sic_list):
        regs["SIA"] = verify_sia(name)
        time.sleep(0.3)

    return regs


# ─────────────────────────────────────────────────────────────────────────────
#  Score registrations
# ─────────────────────────────────────────────────────────────────────────────

REGISTER_WEIGHTS = {
    "EA_WASTE":    5,
    "EA_CARRIERS": 3,
    "CQC":         6,
    "FCA":         5,
    "ICO":         2,
    "OFSTED":      5,
    "SIA":         4,
}

def score_registrations(regs: dict) -> dict:
    """
    Produce a 0–25 regulatory score and a list of confirmed registrations.
    Higher score = stronger regulated market position.
    """
    score      = 0
    confirmed  = []

    for key, result in regs.items():
        if isinstance(result, dict) and result.get("found"):
            w = REGISTER_WEIGHTS.get(key, 2)
            score += w
            label = result.get("register_name") or result.get("register", key)
            ref   = (result.get("permit_reference") or
                     result.get("cqc_provider_id") or
                     result.get("fca_ref") or
                     result.get("ico_reg_number") or "")
            confirmed.append(f"{label}{': ' + ref if ref else ''}")

    score = min(score, 25)
    if   score >= 15: band = "Highly Regulated"
    elif score >= 9:  band = "Well Regulated"
    elif score >= 4:  band = "Some Registration"
    else:             band = "Minimal / None"

    return {
        "regulatory_score":  score,
        "regulatory_band":   band,
        "confirmed_regs":    confirmed,
        "reg_count":         len(confirmed),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Utility
# ─────────────────────────────────────────────────────────────────────────────

def _name_match(query: str, target: str) -> bool:
    """Fuzzy name match — require ≥50% significant word overlap."""
    q = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC|AND|THE|OF|&)\b", "", query.upper()).strip()
    t = re.sub(r"\b(LIMITED|LTD|LLP|PLC|CIC|AND|THE|OF|&)\b", "", target.upper()).strip()
    words = [w for w in q.split() if len(w) > 3]
    if not words:
        return False
    hits = sum(1 for w in words if w in t)
    return hits / len(words) >= 0.5


def list_registers() -> None:
    """Print a formatted table of all supported registers."""
    print(f"\n{'='*70}")
    print(f"  {'Register Key':<18} {'Name':<35} {'Discovery'}")
    print(f"{'='*70}")
    for key, cfg_e in REGISTER_CATALOGUE.items():
        disc = "✅ Yes" if cfg_e.get("discovery") else "⚠️  Verify only"
        if cfg_e["type"] == "blocked":
            disc = "❌ Blocked"
        name = cfg_e["name"][:34]
        print(f"  {key:<18} {name:<35} {disc}")
    print(f"{'='*70}\n")


# ─────────────────────────────────────────────────────────────────────────────
#  Standalone runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(
        description="Regulatory Register Discovery Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python reg_sources.py --list                                # Show all registers
  python reg_sources.py --register EA_WASTE --query ""        # All EA waste permit holders
  python reg_sources.py --register EA_CARRIERS --query "drainage"
  python reg_sources.py --register CQC --query "domiciliary care"
  python reg_sources.py --verify "Biffa Waste Services Ltd" --registers EA_WASTE CQC ICO
        """,
    )
    parser.add_argument("--list",     action="store_true",
                        help="List all available registers")
    parser.add_argument("--register", metavar="KEY",
                        help="Register to search (e.g. EA_WASTE)")
    parser.add_argument("--query",    metavar="TERM", default="",
                        help="Search keyword (empty = all)")
    parser.add_argument("--verify",   metavar="COMPANY_NAME",
                        help="Verify a specific company name across registers")
    parser.add_argument("--registers", nargs="+", metavar="KEY",
                        help="Registers to check for --verify (default: all applicable)")
    parser.add_argument("--no-ch",    action="store_true",
                        help="Skip Companies House lookup (register results only)")
    args = parser.parse_args()

    if args.list:
        list_registers()
        sys.exit(0)

    if args.verify:
        print(f"\nVerifying '{args.verify}' against registers ...\n")
        results = verify_all(
            {"company_name": args.verify, "sic_codes": []},
        )
        for reg_key, result in results.items():
            status = "✅ FOUND" if result.get("found") else "❌ Not found"
            print(f"  {reg_key:<16} {status}")
            if result.get("found"):
                for k, v in result.items():
                    if k not in ("found", "register", "data_tier"):
                        print(f"    {k}: {v}")
        scoring = score_registrations(results)
        print(f"\n  Regulatory Score: {scoring['regulatory_score']}/25 "
              f"({scoring['regulatory_band']})")
        print(f"  Confirmed: {', '.join(scoring['confirmed_regs']) or 'None'}")
        sys.exit(0)

    if args.register:
        from run import load_api_key
        ch_key = load_api_key()
        if args.no_ch:
            # Just show register results
            cfg_e = REGISTER_CATALOGUE.get(args.register, {})
            if cfg_e.get("type") == "ea_html":
                results = discover_ea(args.register, args.query)
                for r in results[:20]:
                    print(f"  {r['company_name']:<45} {r.get('permit_reference',''):<15} {r.get('permit_type','')[:40]}")
        else:
            results = discover(args.register, args.query, ch_api_key=ch_key)
            print(f"\n  {len(results)} companies matched on Companies House")
            for c in results[:20]:
                print(f"  {c['company_number']:<12} {c['company_name'][:50]}")
        sys.exit(0)

    parser.print_help()
