"""
smart_search.py — Interactive sector search with guided filtering

The smart search flow:
  1. User provides a sector description (e.g. "plumbers", "fire safety")
  2. Auto-discovers relevant SIC codes via fuzzy matching against the local DB
  3. Also identifies relevant regulatory registers (Gas Safe, NICEIC, CQC, etc.)
  4. Queries local DB to get initial count
  5. Asks user for filtering criteria (employees, age, region, etc.)
  6. Applies filters → shows refined count estimate
  7. User confirms → writes filtered_companies.json → rest of pipeline runs

Usage (interactive):
  python smart_search.py --sector "plumbers"
  python smart_search.py --sector "fire safety contractors"
  python smart_search.py --sector "domiciliary care"

Usage (non-interactive, all criteria supplied):
  python smart_search.py --sector "plumbers" --min-age 5 --region LS --max-results 500

  python run.py --smart --sector "plumbers"
"""

import json
import os
import re
import sqlite3
from pathlib import Path

import config as cfg

DATA_DIR = Path(__file__).parent / "data"
DB_PATH  = DATA_DIR / "companies_house.db"
SIC_PATH = DATA_DIR / "sic_codes.json"


# ─────────────────────────────────────────────────────────────────────────────
#  Sector → SIC code mapping
# ─────────────────────────────────────────────────────────────────────────────

# Curated seed mappings for common PE-relevant sectors.
# The local DB is also queried for fuzzy matches (see discover_sic_codes()).
SECTOR_SEEDS = {
    # ── Trades & Building Services ────────────────────────────────────────────
    "plumb": ["43220"],
    "heating": ["43220", "35300"],
    "boiler": ["43220"],
    "hvac": ["43220", "43210", "28250"],
    "electrical": ["43210"],
    "electrician": ["43210"],
    "gas": ["43220", "35210"],
    "gas safe": ["43220"],
    "fire": ["43290", "80200", "71200"],
    "fire safety": ["43290", "80200"],
    "sprinkler": ["43290"],
    "roofing": ["43910"],
    "painting": ["43340"],
    "flooring": ["43330"],
    "glazing": ["43291"],
    "fensa": ["43291"],
    "scaffolding": ["43990"],
    "construction": ["41100", "41202", "42110", "43110", "43120", "43130",
                     "43210", "43220", "43290", "43310", "43320", "43330",
                     "43340", "43390", "43910", "43990"],
    "builder": ["41202", "41100"],
    "demolition": ["43110"],
    "groundwork": ["43120"],

    # ── Facilities Management ─────────────────────────────────────────────────
    "facilities": ["81100", "81210", "81220", "81290"],
    "cleaning": ["81210", "81220", "81290"],
    "pest control": ["81291"],
    "security": ["80100", "80200"],
    "guarding": ["80100"],
    "landscaping": ["81300"],
    "grounds maintenance": ["81300"],

    # ── Waste & Environment ───────────────────────────────────────────────────
    "waste": ["38110", "38120", "38210", "38220", "39000"],
    "recycling": ["38210", "38220"],
    "skip hire": ["38110", "49410"],
    "drainage": ["43220", "37000", "38110"],
    "sewage": ["37000"],
    "environmental": ["39000", "71122"],
    "remediation": ["39000"],
    "hazardous": ["38220"],

    # ── Care & Health ─────────────────────────────────────────────────────────
    "care": ["87100", "87200", "87300", "87900", "88100", "88990"],
    "care home": ["87100", "87200", "87300"],
    "domiciliary": ["88100"],
    "home care": ["88100"],
    "nursing": ["87100"],
    "residential": ["87100", "87200"],
    "children": ["87200", "88910"],
    "nursery": ["85100", "88910"],
    "childcare": ["85100", "88910"],
    "learning disability": ["87200"],
    "mental health": ["87200", "86900"],
    "dental": ["86230"],
    "optician": ["86220"],
    "pharmacy": ["47730"],
    "gp": ["86210"],
    "ambulance": ["86901"],
    "healthcare": ["86100", "86210", "86220", "86230", "86900"],
    "hospital": ["86100"],
    "veterinary": ["75000"],

    # ── Education ─────────────────────────────────────────────────────────────
    "education": ["85100", "85200", "85310", "85320", "85410", "85421", "85590"],
    "training": ["85590"],
    "tutoring": ["85590"],
    "driving school": ["85530"],

    # ── Transport & Logistics ─────────────────────────────────────────────────
    "transport": ["49100", "49200", "49310", "49320", "49390", "49410", "49420"],
    "haulage": ["49410"],
    "courier": ["53200", "49420"],
    "taxi": ["49320"],
    "bus": ["49310"],
    "logistics": ["49410", "52100", "52290"],
    "warehouse": ["52100"],
    "freight": ["49410", "52290"],
    "removal": ["49420"],

    # ── Financial Services ────────────────────────────────────────────────────
    "mortgage": ["64191", "66190"],
    "ifa": ["66190"],
    "financial adviser": ["66190"],
    "insurance broker": ["66220"],
    "accountant": ["69201"],
    "bookkeeping": ["69202"],
    "financial planning": ["66190"],

    # ── Professional Services ─────────────────────────────────────────────────
    "recruitment": ["78100", "78200", "78300"],
    "staffing": ["78200"],
    "it support": ["62020"],
    "software": ["62012", "62020"],
    "cyber": ["62090"],
    "print": ["18120"],
    "signage": ["18120", "74100"],
    "marketing": ["73110", "73120"],
    "pr": ["70210"],
    "legal": ["69100"],
    "solicitor": ["69100"],
    "engineering": ["71112", "71122", "71200"],
    "testing": ["71200"],
    "inspection": ["71200"],

    # ── Food & Hospitality ────────────────────────────────────────────────────
    "catering": ["56210"],
    "restaurant": ["56101"],
    "takeaway": ["56102"],
    "food": ["10110", "10200", "10710", "56101", "56102"],
    "bakery": ["10710"],
    "hotel": ["55100"],
    "pub": ["56302"],

    # ── Retail & Consumer ─────────────────────────────────────────────────────
    "garden centre": ["47760"],
    "funeral": ["96030"],
    "laundry": ["96010"],

    # ── Manufacturing ─────────────────────────────────────────────────────────
    "manufacturing": ["25110", "25120", "25910", "25990", "28110"],
    "fabrication": ["25110", "25120"],
    "engineering services": ["33110", "33120", "33190"],

    # ── Energy ───────────────────────────────────────────────────────────────
    "solar": ["35110", "43220"],
    "renewable": ["35110", "35120", "35140"],
    "insulation": ["43290"],
    "heat pump": ["43220"],
}

# Regulatory registers relevant by sector keyword
SECTOR_REGISTERS = {
    "plumb":      ["GAS_SAFE"],
    "gas":        ["GAS_SAFE"],
    "boiler":     ["GAS_SAFE"],
    "heating":    ["GAS_SAFE"],
    "electrical": ["NICEIC"],
    "electrician":["NICEIC"],
    "fire":       ["BAFE"],
    "care":       ["CQC"],
    "nursing":    ["CQC"],
    "domiciliary":["CQC"],
    "health":     ["CQC"],
    "hospital":   ["CQC"],
    "dental":     ["CQC"],
    "nursery":    ["OFSTED"],
    "childcare":  ["OFSTED"],
    "education":  ["OFSTED"],
    "school":     ["OFSTED"],
    "waste":      ["EA_WASTE", "EA_CARRIERS"],
    "recycling":  ["EA_WASTE"],
    "drainage":   ["EA_WASTE", "EA_CARRIERS"],
    "environmental": ["EA_WASTE"],
    "security":   ["SIA"],
    "guarding":   ["SIA"],
    "mortgage":   ["FCA"],
    "ifa":        ["FCA"],
    "financial":  ["FCA"],
    "insurance":  ["FCA"],
    "insulation": ["TrustMark"],
    "solar":      ["TrustMark"],
    "roofing":    ["TrustMark"],
    "glazing":    ["FENSA"],
    "window":     ["FENSA"],
}


def discover_sic_codes(sector: str, limit: int = 20) -> list[dict]:
    """
    Find relevant SIC codes for a sector description.
    Combines:
      1. Curated SECTOR_SEEDS lookup (fast, reliable)
      2. Fuzzy match against the sic_codes table in the local DB (broader)

    Returns list of {'sic_code': '43220', 'description': '...', 'count': N}
    sorted by count (company volume) descending.
    """
    s = sector.lower()

    # ── Step 1: curated seeds ────────────────────────────────────────────────
    seed_codes: set[str] = set()
    for keyword, codes in SECTOR_SEEDS.items():
        if keyword in s or s in keyword:
            seed_codes.update(codes)

    # ── Step 2: fuzzy DB match ────────────────────────────────────────────────
    db_codes: list[dict] = []
    if DB_PATH.exists():
        try:
            con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
            con.row_factory = sqlite3.Row

            # Split sector into keywords and search descriptions
            words = [w for w in re.split(r"\W+", s) if len(w) > 3]
            seen_codes: set[str] = set(seed_codes)

            for word in words:
                rows = con.execute(
                    "SELECT sic_code, description, count FROM sic_codes "
                    "WHERE description LIKE ? ORDER BY count DESC LIMIT 10",
                    (f"%{word}%",),
                ).fetchall()
                for row in rows:
                    if row["sic_code"] not in seen_codes:
                        seen_codes.add(row["sic_code"])
                        db_codes.append(dict(row))

            # Get counts for seed codes
            for code in seed_codes:
                row = con.execute(
                    "SELECT sic_code, description, count FROM sic_codes WHERE sic_code=?",
                    (code,),
                ).fetchone()
                if row:
                    db_codes.append(dict(row))

            con.close()
        except Exception:
            pass

    # ── Merge and sort by count ───────────────────────────────────────────────
    by_code: dict[str, dict] = {}
    for item in db_codes:
        code = item["sic_code"]
        if code not in by_code or item["count"] > by_code[code]["count"]:
            by_code[code] = item

    return sorted(by_code.values(), key=lambda x: x["count"], reverse=True)[:limit]


def discover_registers(sector: str) -> list[str]:
    """Return regulatory register keys relevant for this sector."""
    s = sector.lower()
    registers = set()
    for keyword, reg_keys in SECTOR_REGISTERS.items():
        if keyword in s or s in keyword:
            registers.update(reg_keys)
    return sorted(registers)


def count_companies(
    sic_codes:        list[str],
    status:           str = "Active",
    min_age_yrs:      float | None = None,
    max_age_yrs:      float | None = None,
    postcode_prefix:  str | None = None,
    postcode_prefixes: list[str] | None = None,
    max_mortgages:    int | None = None,
    company_types:    list[str] | None = None,
) -> dict:
    """
    Count matching companies in the local DB without loading all rows.
    Returns {'total': N, 'by_sic': {code: count}, 'by_region': {...}}
    """
    if not DB_PATH.exists():
        return {"total": 0, "error": "DB not built"}

    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row

    # Build WHERE clause
    clauses = []
    params: list = []

    if sic_codes:
        placeholders = ",".join("?" * len(sic_codes))
        clauses.append(
            f"(sic1 IN ({placeholders}) OR sic2 IN ({placeholders}) "
            f"OR sic3 IN ({placeholders}) OR sic4 IN ({placeholders}))"
        )
        params.extend(sic_codes * 4)

    if status:
        clauses.append("company_status=?")
        params.append(status)

    if min_age_yrs is not None:
        clauses.append("company_age_years >= ?")
        params.append(min_age_yrs)

    if max_age_yrs is not None:
        clauses.append("company_age_years <= ?")
        params.append(max_age_yrs)

    # Support single prefix or list of prefixes (e.g. London = E,EC,N,NW,...)
    all_prefixes = list(postcode_prefixes) if postcode_prefixes else (
        [postcode_prefix] if postcode_prefix else []
    )
    if all_prefixes:
        phs = " OR ".join(["postcode LIKE ?"] * len(all_prefixes))
        clauses.append(f"({phs})")
        params.extend(f"{p.upper()}%" for p in all_prefixes)

    if max_mortgages is not None:
        clauses.append("mortgages_outstanding <= ?")
        params.append(max_mortgages)

    if company_types:
        type_phs = ",".join("?" * len(company_types))
        clauses.append(f"company_type IN ({type_phs})")
        params.extend(company_types)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    total = con.execute(f"SELECT COUNT(*) FROM companies {where}", params).fetchone()[0]

    # Breakdown by SIC (top 10) — build fresh clauses per code
    by_sic = {}
    # Collect non-SIC clauses and their params separately
    non_sic_clauses = []
    non_sic_params: list = []
    if status:
        non_sic_clauses.append("company_status=?")
        non_sic_params.append(status)
    if min_age_yrs is not None:
        non_sic_clauses.append("company_age_years >= ?")
        non_sic_params.append(min_age_yrs)
    if max_age_yrs is not None:
        non_sic_clauses.append("company_age_years <= ?")
        non_sic_params.append(max_age_yrs)
    if all_prefixes:
        phs2 = " OR ".join(["postcode LIKE ?"] * len(all_prefixes))
        non_sic_clauses.append(f"({phs2})")
        non_sic_params.extend(f"{p.upper()}%" for p in all_prefixes)
    if max_mortgages is not None:
        non_sic_clauses.append("mortgages_outstanding <= ?")
        non_sic_params.append(max_mortgages)
    if company_types:
        type_phs = ",".join("?" * len(company_types))
        non_sic_clauses.append(f"company_type IN ({type_phs})")
        non_sic_params.extend(company_types)

    for code in sic_codes[:10]:
        sic_clause = "(sic1=? OR sic2=? OR sic3=? OR sic4=?)"
        all_parts  = [sic_clause] + non_sic_clauses
        all_params = [code, code, code, code] + non_sic_params
        sic_w = f"WHERE {' AND '.join(all_parts)}"
        cnt   = con.execute(f"SELECT COUNT(*) FROM companies {sic_w}", all_params).fetchone()[0]
        if cnt > 0:
            by_sic[code] = cnt

    con.close()
    return {"total": total, "by_sic": by_sic}



def fetch_companies(
    sic_codes:         list[str],
    status:            str = "Active",
    min_age_yrs:       float | None = None,
    max_age_yrs:       float | None = None,
    postcode_prefix:   str | None = None,
    postcode_prefixes: list[str] | None = None,
    max_mortgages:     int | None = None,
    company_types:     list[str] | None = None,
    limit:             int = 2000,
) -> list[dict]:
    """
    Fetch matching companies and return as normalised pipeline dicts.
    This is what gets written to filtered_companies.json.
    """
    from local_search import search_by_sic, _normalise_row

    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row

    clauses = []
    params: list = []

    if sic_codes:
        phs = ",".join("?" * len(sic_codes))
        clauses.append(
            f"(sic1 IN ({phs}) OR sic2 IN ({phs}) "
            f"OR sic3 IN ({phs}) OR sic4 IN ({phs}))"
        )
        params.extend(sic_codes * 4)

    if status:
        clauses.append("company_status=?")
        params.append(status)

    if min_age_yrs is not None:
        clauses.append("company_age_years >= ?")
        params.append(min_age_yrs)

    if max_age_yrs is not None:
        clauses.append("company_age_years <= ?")
        params.append(max_age_yrs)

    all_fetch_prefixes = list(postcode_prefixes) if postcode_prefixes else (
        [postcode_prefix] if postcode_prefix else []
    )
    if all_fetch_prefixes:
        phs = " OR ".join(["postcode LIKE ?"] * len(all_fetch_prefixes))
        clauses.append(f"({phs})")
        params.extend(f"{p.upper()}%" for p in all_fetch_prefixes)

    if max_mortgages is not None:
        clauses.append("mortgages_outstanding <= ?")
        params.append(max_mortgages)

    if company_types:
        type_phs = ",".join("?" * len(company_types))
        clauses.append(f"company_type IN ({type_phs})")
        params.extend(company_types)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql   = (f"SELECT * FROM companies {where} "
             f"ORDER BY company_age_years DESC NULLS LAST LIMIT {limit}")

    rows = con.execute(sql, params).fetchall()
    con.close()

    return [_normalise_row(row, "smart_search") for row in rows]


# ─────────────────────────────────────────────────────────────────────────────
#  Interactive smart search
# ─────────────────────────────────────────────────────────────────────────────

COMPANY_TYPE_MAP = {
    "limited":    ["Private Limited Company"],
    "ltd":        ["Private Limited Company"],
    "llp":        ["Limited Liability Partnership"],
    "plc":        ["Public Limited Company"],
    "all":        None,
}

REGION_HINTS = {
    "london":       "E,EC,N,NW,SE,SW,W,WC",
    "manchester":   "M",
    "birmingham":   "B",
    "leeds":        "LS",
    "sheffield":    "S",
    "bristol":      "BS",
    "liverpool":    "L",
    "nottingham":   "NG",
    "newcastle":    "NE",
    "edinburgh":    "EH",
    "glasgow":      "G",
    "cardiff":      "CF",
    "midlands":     "B,CV,DE,LE,NG,ST,WS,WV",
    "north west":   "BB,BL,CA,CH,CW,FY,L,LA,M,OL,PR,SK,WA,WN",
    "yorkshire":    "BD,DN,HD,HG,HU,LS,S,WF,YO",
    "south east":   "BN,BR,CT,GU,HP,KT,ME,MK,OX,RH,RG,SL,SO,TN",
    "east anglia":  "CB,CO,IP,NR,PE",
    "north east":   "DH,DL,NE,SR,TS",
    "scotland":     "AB,DD,DG,EH,FK,G,HS,IV,KA,KW,KY,ML,PA,PH,TD,ZE",
    "wales":        "CF,LD,LL,NP,SA,SY",
}


def run_interactive(sector: str, non_interactive: bool = False, **kwargs):
    """
    Interactive guided search flow.

    Args:
        sector:          Free-text sector description
        non_interactive: If True, use kwargs directly (no prompts)
        **kwargs:        min_age, max_age, postcode, max_mortgages, limit
    """
    print(f"\n{'='*60}")
    print(f"  SMART SECTOR SEARCH: '{sector}'")
    print(f"{'='*60}")

    # ── 1. Discover SIC codes ─────────────────────────────────────────────────
    print(f"\n📊 Discovering relevant SIC codes ...")
    sic_matches = discover_sic_codes(sector)

    if not sic_matches:
        print(f"  ⚠️  No SIC codes found for '{sector}'")
        print(f"  Try: python local_search.py --list-sic '{sector}'")
        return None

    print(f"\n  Matched {len(sic_matches)} SIC codes:")
    sic_table = []
    for i, s in enumerate(sic_matches[:15], 1):
        flag = "  ★" if i <= 5 else ""
        print(f"    {i:>2}. {s['sic_code']}  {s['count']:>8,} cos  "
              f"{s['description'][:50]}{flag}")
        sic_table.append(s)

    # Use top 10 by default (user can narrow)
    selected_sic_codes = [s["sic_code"] for s in sic_matches[:10]]

    # ── 2. Identify relevant registers ────────────────────────────────────────
    registers = discover_registers(sector)
    if registers:
        from reg_sources import REGISTER_CATALOGUE
        print(f"\n  🏛  Relevant regulatory registers:")
        for reg_key in registers:
            reg_info = REGISTER_CATALOGUE.get(reg_key, {})
            disc     = "✅ discoverable" if reg_info.get("discovery") else "⚠️  verify only"
            blocked  = reg_info.get("type") == "blocked"
            status_s = "❌ blocked" if blocked else disc
            print(f"    • {reg_key:<18} {reg_info.get('name','')[:40]}  ({status_s})")

    # ── 3. Initial count ──────────────────────────────────────────────────────
    print(f"\n  🔍 Initial count (active companies, no filters) ...")
    initial = count_companies(selected_sic_codes, status="Active")
    print(f"\n  {'─'*50}")
    print(f"  ✅ {initial['total']:,} active companies found across {len(selected_sic_codes)} SIC codes")
    if initial.get("by_sic"):
        for code, cnt in sorted(initial["by_sic"].items(), key=lambda x: -x[1])[:5]:
            desc = next((s["description"] for s in sic_matches if s["sic_code"] == code), "")
            print(f"     {code}  {cnt:>8,}  {desc[:45]}")
    print(f"  {'─'*50}")

    # ── 4. Gather filter criteria ─────────────────────────────────────────────
    if non_interactive:
        min_age_yrs   = kwargs.get("min_age")
        max_age_yrs   = kwargs.get("max_age")
        max_mortgages = kwargs.get("max_mortgages")
        limit         = kwargs.get("limit", 2000)
        # Resolve region name → postcode prefix(es)
        raw_region = kwargs.get("postcode") or ""
        display_region = None
        postcode_prefixes_list: list[str] | None = None
        postcode_prefix = None
        if raw_region:
            region_lower = raw_region.lower()
            if region_lower in REGION_HINTS:
                prefixes = [p.strip() for p in REGION_HINTS[region_lower].split(",")]
                postcode_prefixes_list = prefixes
                display_region = f"{raw_region} ({', '.join(prefixes)})"
                print(f"    → Region '{raw_region}' → {len(prefixes)} postcode prefixes: "
                      f"{REGION_HINTS[region_lower]}")
            else:
                postcode_prefix = raw_region.upper()
                display_region = raw_region.upper()
    else:
        print(f"\n  Let's narrow it down. Press Enter to skip any filter.\n")
        postcode_prefixes_list = None
        display_region = None

        # Min company age
        val = input("  Minimum company age (years)? [e.g. 5, 10, 15] > ").strip()
        min_age_yrs = float(val) if val else None

        # Max company age (usually skip)
        val = input("  Maximum company age (years)? [Enter to skip] > ").strip()
        max_age_yrs = float(val) if val else None

        # Region / postcode
        val = input("  Region or postcode prefix? [e.g. 'LS', 'Manchester', 'North West', Enter for national] > ").strip()
        if val:
            region_lower = val.lower()
            if region_lower in REGION_HINTS:
                prefixes = [p.strip() for p in REGION_HINTS[region_lower].split(",")]
                postcode_prefixes_list = prefixes
                postcode_prefix = None
                display_region = f"{val} ({', '.join(prefixes)})"
                print(f"    → {len(prefixes)} postcode prefixes: {REGION_HINTS[region_lower]}")
            else:
                postcode_prefix = val.upper()
                display_region = val.upper()
        else:
            postcode_prefix = None

        # Charges/debt
        val = input("  Max outstanding mortgages/charges? [0 = clean only, Enter to skip] > ").strip()
        max_mortgages = int(val) if val else None

        # Max results
        val = input("  Max companies to process in pipeline? [default 500] > ").strip()
        limit = int(val) if val else 500

    # ── 5. Refined count ─────────────────────────────────────────────────────
    print(f"\n  🔢 Applying filters ...")
    refined = count_companies(
        sic_codes          = selected_sic_codes,
        status             = "Active",
        min_age_yrs        = min_age_yrs,
        max_age_yrs        = max_age_yrs,
        postcode_prefix    = postcode_prefix,
        postcode_prefixes  = postcode_prefixes_list,
        max_mortgages      = max_mortgages,
        company_types      = ["Private Limited Company",
                              "Limited Liability Partnership"],
    )

    print(f"\n  {'─'*50}")
    print(f"  Filter summary:")
    if min_age_yrs:                    print(f"    • Min age:         {min_age_yrs} years")
    if max_age_yrs:                    print(f"    • Max age:         {max_age_yrs} years")
    if display_region:                 print(f"    • Region:          {display_region}")
    elif postcode_prefix:              print(f"    • Postcode prefix: {postcode_prefix}")
    if max_mortgages is not None:      print(f"    • Max charges:     {max_mortgages}")
    print(f"    • Types:           Private Ltd + LLP")
    print(f"\n  → {refined['total']:,} companies match your criteria")
    print(f"  → Processing cap:  {limit:,}")
    actual_to_process = min(refined["total"], limit)
    print(f"  → Will process:    {actual_to_process:,} companies")
    print(f"  {'─'*50}")

    if non_interactive:
        confirmed = True
    else:
        print()
        val = input(f"  Proceed with {actual_to_process:,} companies? [Y/n] > ").strip().lower()
        confirmed = val in ("", "y", "yes")

    if not confirmed:
        print("  Cancelled.")
        return None

    # ── 6. Fetch and write ────────────────────────────────────────────────────
    print(f"\n  Fetching {actual_to_process:,} companies from local DB ...")
    companies = fetch_companies(
        sic_codes       = selected_sic_codes,
        status             = "Active",
        min_age_yrs        = min_age_yrs,
        max_age_yrs        = max_age_yrs,
        postcode_prefix    = postcode_prefix,
        postcode_prefixes  = postcode_prefixes_list,
        max_mortgages      = max_mortgages,
        company_types      = ["Private Limited Company",
                              "Limited Liability Partnership"],
        limit              = limit,
    )

    os.makedirs(cfg.OUTPUT_DIR, exist_ok=True)

    # Write raw JSON
    raw_path = os.path.join(cfg.OUTPUT_DIR, cfg.RAW_JSON)
    with open(raw_path, "w") as f:
        json.dump(companies, f, indent=2)

    # Also write filtered (same at this stage — already filtered)
    filt_path = os.path.join(cfg.OUTPUT_DIR, cfg.FILTERED_JSON)
    with open(filt_path, "w") as f:
        json.dump(companies, f, indent=2)

    print(f"\n  ✅ {len(companies):,} companies written → {filt_path}")
    print(f"\n  SIC codes used: {', '.join(selected_sic_codes)}")
    if registers:
        print(f"  Registers to check in Step 9: {', '.join(registers)}")
    print(f"\n  Next: python run.py --enrich-only")
    print(f"        (or run full pipeline from Step 3 onwards)\n")

    return {
        "companies":      companies,
        "sic_codes":      selected_sic_codes,
        "registers":      registers,
        "count":          len(companies),
        "filtered_path":  filt_path,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Smart sector search with guided filtering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python smart_search.py --sector "plumbers"
  python smart_search.py --sector "fire safety"
  python smart_search.py --sector "domiciliary care"

  # Non-interactive (all criteria supplied):
  python smart_search.py --sector "waste management" --min-age 5 --region LS --limit 300
  python smart_search.py --sector "electrical contractors" --min-age 3 --limit 500 --yes

  # Then continue pipeline from Step 3:
  python run.py --enrich-only
        """,
    )
    parser.add_argument("--sector",      required=True,
                        help="Sector description, e.g. 'plumbers' or 'fire safety'")
    parser.add_argument("--min-age",     type=float, metavar="YEARS",
                        help="Minimum company age in years")
    parser.add_argument("--max-age",     type=float, metavar="YEARS",
                        help="Maximum company age in years")
    parser.add_argument("--region",      metavar="POSTCODE/REGION",
                        help="Region or postcode prefix, e.g. 'LS', 'Manchester'")
    parser.add_argument("--max-charges", type=int, metavar="N",
                        help="Max outstanding charges (0 = clean only)")
    parser.add_argument("--limit",       type=int, default=500,
                        help="Max companies to process (default 500)")
    parser.add_argument("--yes", "-y",   action="store_true",
                        help="Skip confirmation prompt")
    args = parser.parse_args()

    non_interactive = (args.min_age is not None or args.region or
                       args.max_charges is not None or args.yes)

    run_interactive(
        sector          = args.sector,
        non_interactive = non_interactive,
        min_age         = args.min_age,
        max_age         = args.max_age,
        postcode        = args.region,
        max_mortgages   = args.max_charges,
        limit           = args.limit,
    )
