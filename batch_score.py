"""
batch_score.py — Score ALL UK companies using local DB only (zero API calls).

Joins the companies table (from BasicCompanyData bulk CSV) with the psc table
(from PSC bulk snapshot) to produce a PE suitability score for every company.

Filters:
  - Active status only
  - Ltd / PLC / LTD only (excludes LLP, overseas, charity, etc.)
  - Minimum age (default 10 years)
  - Excludes dormant

Scoring signals (all from DB — no API needed):
  - Company age (older = more established)
  - Accounts category (FULL/TOTAL_EXEMPTION = larger, MICRO/DORMANT = smaller)
  - Mortgages outstanding (indicates asset base / leverage)
  - PSC: oldest individual owner's age (55+ = succession opportunity)
  - PSC: number of individual vs corporate PSCs (corporate = already in a group)
  - PSC: ownership concentration (75-100% single owner = cleaner deal)
  - PSC: has corporate PSC (may already be PE-owned or part of a group)

Output:
  CSV file with all scored companies, sorted by score descending.
  One row per company with: company data + PSC summary + PE score.

Usage:
  python batch_score.py                                    # Score all, default filters
  python batch_score.py --min-age 10 --output all_uk.csv   # Custom age + output
  python batch_score.py --stats                            # Show scoring distribution
  python batch_score.py --sic 43210,80200                  # Filter to specific SICs
"""

import os
import sys
import csv
import json
import sqlite3
import argparse
from datetime import date
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DB_PATH  = DATA_DIR / "companies_house.db"
OUTPUT_DIR = Path(__file__).parent / "output"

CURRENT_YEAR = date.today().year


# ── Scoring weights ──────────────────────────────────────────────────────────

def score_company(row: dict) -> dict:
    """
    Score a single company row (joined company + PSC aggregates).
    Returns the row with added score fields.
    """
    score = 0
    signals = []

    # ── Company age (max 20 pts) ─────────────────────────────────────────
    age = row.get("company_age_years") or 0
    if age >= 25:
        score += 20; signals.append("25yr+")
    elif age >= 15:
        score += 15; signals.append("15yr+")
    elif age >= 10:
        score += 10; signals.append("10yr+")

    # ── Accounts category (max 25 pts) ───────────────────────────────────
    acct = (row.get("accounts_category") or "").upper()
    if acct in ("FULL", "GROUP", "MEDIUM"):
        score += 25; signals.append(f"acct:{acct}")
    elif acct in ("SMALL",):
        score += 15; signals.append("acct:SMALL")
    elif acct in ("TOTAL_EXEMPTION", "TOTAL EXEMPTION FULL", "TOTAL EXEMPTION SMALL"):
        score += 10; signals.append("acct:EXEMPT")
    elif acct in ("MICRO",):
        score += 5; signals.append("acct:MICRO")
    elif acct in ("DORMANT", "NO_ACCOUNTS_FILED", "UNAUDITED_ABRIDGED"):
        score += 0; signals.append(f"acct:{acct}")
    else:
        score += 5  # unknown

    # ── Mortgages (max 10 pts) ───────────────────────────────────────────
    mortgages = row.get("mortgages_outstanding") or 0
    if mortgages >= 1:
        score += 10; signals.append(f"mortg:{mortgages}")
    # Companies with mortgages tend to have real assets

    # ── PSC: Oldest owner age (max 20 pts) ───────────────────────────────
    oldest_dob_year = row.get("oldest_psc_dob_year")
    owner_age = None
    if oldest_dob_year and oldest_dob_year > 1900:
        owner_age = CURRENT_YEAR - oldest_dob_year
        if owner_age >= 65:
            score += 20; signals.append(f"owner:{owner_age}yr")
        elif owner_age >= 55:
            score += 15; signals.append(f"owner:{owner_age}yr")
        elif owner_age >= 45:
            score += 8; signals.append(f"owner:{owner_age}yr")
        else:
            score += 2

    # ── PSC: Ownership concentration (max 10 pts) ────────────────────────
    individual_psc_count = row.get("individual_psc_count") or 0
    if individual_psc_count == 1:
        score += 10; signals.append("sole_owner")
    elif individual_psc_count == 2:
        score += 7; signals.append("2_owners")
    elif individual_psc_count <= 4:
        score += 4; signals.append(f"{individual_psc_count}_owners")

    # ── PSC: Corporate PSC flag (penalty) ────────────────────────────────
    corporate_psc_count = row.get("corporate_psc_count") or 0
    has_corporate_psc = corporate_psc_count > 0
    if has_corporate_psc:
        score -= 10; signals.append(f"corp_psc:{corporate_psc_count}")
        # May already be part of a group / PE-owned

    # ── PSC: No PSC at all (mild penalty) ────────────────────────────────
    total_psc = individual_psc_count + corporate_psc_count
    if total_psc == 0:
        score -= 5; signals.append("no_psc")

    # ── Clamp score 0-100 ────────────────────────────────────────────────
    score = max(0, min(100, score))

    row["pe_score"] = score
    row["pe_signals"] = "; ".join(signals)
    row["owner_age"] = owner_age
    row["has_corporate_psc"] = has_corporate_psc
    row["individual_psc_count"] = individual_psc_count
    row["corporate_psc_count"] = corporate_psc_count

    return row


# ── SQL query: join companies + PSC aggregates ───────────────────────────────

def build_query(min_age: int = 10, sic_filter: list[str] | None = None) -> tuple[str, list]:
    """
    Build the main query joining companies with PSC aggregates.
    Returns (sql, params).
    """
    sql = f"""
    SELECT
        c.company_number,
        c.company_name,
        c.company_status,
        c.company_type,
        c.incorporation_date,
        c.company_age_years,
        c.sic1, c.sic2, c.sic3, c.sic4,
        c.accounts_category,
        c.accounts_last_date,
        c.mortgages_outstanding,
        c.postcode,
        c.address_line1,
        c.address_town,
        c.address_county,
        c.address_country,

        -- PSC aggregates (individuals)
        psc_agg.individual_psc_count,
        psc_agg.oldest_psc_dob_year,
        psc_agg.youngest_psc_dob_year,
        psc_agg.psc_names,
        psc_agg.psc_nationalities,
        psc_agg.ownership_detail,

        -- PSC aggregates (corporates)
        psc_corp.corporate_psc_count,
        psc_corp.corporate_psc_names

    FROM companies c

    LEFT JOIN (
        SELECT
            company_number,
            COUNT(*) as individual_psc_count,
            MIN(dob_year) as oldest_psc_dob_year,
            MAX(dob_year) as youngest_psc_dob_year,
            GROUP_CONCAT(name, ' | ') as psc_names,
            GROUP_CONCAT(DISTINCT nationality) as psc_nationalities,
            GROUP_CONCAT(
                COALESCE(name, '') || ' (' || COALESCE(natures_of_control, '') || ')',
                ' | '
            ) as ownership_detail
        FROM psc
        WHERE kind LIKE '%individual%'
          AND (ceased_on IS NULL OR ceased_on = '')
        GROUP BY company_number
    ) psc_agg ON psc_agg.company_number = c.company_number

    LEFT JOIN (
        SELECT
            company_number,
            COUNT(*) as corporate_psc_count,
            GROUP_CONCAT(name, ' | ') as corporate_psc_names
        FROM psc
        WHERE kind LIKE '%corporate%'
          AND (ceased_on IS NULL OR ceased_on = '')
        GROUP BY company_number
    ) psc_corp ON psc_corp.company_number = c.company_number

    WHERE c.company_status = 'Active'
      AND c.company_type IN (
          'Private Limited Company',
          'PRI/LTD BY GUAR/NSC (Private, limited by guarantee, no share capital)',
          'Private Unlimited',
          'Old Public Company'
      )
      AND c.company_age_years >= ?
      AND (c.accounts_category IS NULL OR UPPER(c.accounts_category) NOT IN ('DORMANT'))
    """
    params = [min_age]

    # Optional SIC filter
    if sic_filter:
        placeholders = ",".join("?" for _ in sic_filter)
        sql += f"""
      AND (c.sic1 IN ({placeholders})
        OR c.sic2 IN ({placeholders})
        OR c.sic3 IN ({placeholders})
        OR c.sic4 IN ({placeholders}))
        """
        params.extend(sic_filter * 4)

    sql += "\n    ORDER BY c.company_age_years DESC"

    return sql, params


# ── Export ────────────────────────────────────────────────────────────────────

CSV_COLUMNS = [
    "pe_score", "pe_signals",
    "company_number", "company_name", "company_status", "company_type",
    "incorporation_date", "company_age_years",
    "sic1", "sic2", "sic3", "sic4",
    "accounts_category", "accounts_last_date",
    "mortgages_outstanding",
    "postcode", "address_line1", "address_town", "address_county", "address_country",
    "owner_age", "individual_psc_count", "corporate_psc_count",
    "has_corporate_psc",
    "psc_names", "psc_nationalities", "ownership_detail",
    "corporate_psc_names",
]


def run(
    min_age: int = 10,
    sic_filter: list[str] | None = None,
    output_path: str | None = None,
    limit: int = 0,
):
    """Main scoring run."""
    import time

    if not DB_PATH.exists():
        print(f"❌ Database not found at {DB_PATH}")
        print("   Run build_local_db.py first, then build_psc_db.py")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = output_path or str(OUTPUT_DIR / "all_uk_scored.csv")

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row

    # Check PSC table exists
    tables = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    if "psc" not in tables:
        print("⚠️  PSC table not found — scoring will work but without PSC signals.")
        print("   Run build_psc_db.py to add PSC data.\n")

    sql, params = build_query(min_age=min_age, sic_filter=sic_filter)
    if limit > 0:
        sql += f" LIMIT {limit}"

    print(f"🔍 Querying all companies (min age: {min_age} years) ...")
    t0 = time.time()
    rows = con.execute(sql, params).fetchall()
    query_time = time.time() - t0
    print(f"   Found {len(rows):,} companies in {query_time:.1f}s")

    # Score each company
    print(f"📊 Scoring {len(rows):,} companies ...")
    t0 = time.time()
    scored = []
    for row in rows:
        r = dict(row)
        scored.append(score_company(r))
    score_time = time.time() - t0
    print(f"   Scored in {score_time:.1f}s")

    # Sort by score descending
    scored.sort(key=lambda x: x["pe_score"], reverse=True)

    # Write CSV
    print(f"💾 Writing {len(scored):,} rows to {out_file} ...")
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(scored)

    file_size = os.path.getsize(out_file) / (1024 * 1024)
    print(f"   ✅ {file_size:.0f} MB written")

    # Score distribution
    bands = {"90-100": 0, "70-89": 0, "50-69": 0, "30-49": 0, "0-29": 0}
    for r in scored:
        s = r["pe_score"]
        if s >= 90:   bands["90-100"] += 1
        elif s >= 70: bands["70-89"] += 1
        elif s >= 50: bands["50-69"] += 1
        elif s >= 30: bands["30-49"] += 1
        else:         bands["0-29"] += 1

    print(f"\n📈 Score distribution:")
    for band, count in bands.items():
        pct = (count / len(scored) * 100) if scored else 0
        bar = "█" * int(pct / 2)
        print(f"   {band:>7s}: {count:>10,}  ({pct:5.1f}%)  {bar}")

    con.close()
    return out_file


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score all UK companies from local DB")
    parser.add_argument("--min-age", type=int, default=10, help="Min company age in years (default: 10)")
    parser.add_argument("--sic", default="", help="Comma-separated SIC codes to filter to")
    parser.add_argument("--output", default="", help="Output CSV path")
    parser.add_argument("--limit", type=int, default=0, help="Limit rows (0 = all)")
    parser.add_argument("--stats", action="store_true", help="Show DB stats and exit")

    args = parser.parse_args()

    if args.stats:
        con = sqlite3.connect(str(DB_PATH))
        total = con.execute("SELECT COUNT(*) FROM companies WHERE company_status='Active'").fetchone()[0]
        aged = con.execute(
            "SELECT COUNT(*) FROM companies WHERE company_status='Active' AND company_age_years >= 10"
        ).fetchone()[0]
        print(f"Active companies: {total:,}")
        print(f"Active, 10+ years: {aged:,}")
        try:
            psc_total = con.execute("SELECT COUNT(*) FROM psc").fetchone()[0]
            print(f"PSC records: {psc_total:,}")
        except sqlite3.OperationalError:
            print("PSC table: not yet built")
        con.close()
        sys.exit(0)

    sic_filter = [s.strip() for s in args.sic.split(",") if s.strip()] if args.sic else None
    run(
        min_age=args.min_age,
        sic_filter=sic_filter,
        output_path=args.output or None,
        limit=args.limit,
    )
