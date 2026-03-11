#!/usr/bin/env python3
"""
Query the UK Companies House DuckDB store.

Usage:
    python scripts/query.py setup              # First time: load zips into DuckDB
    python scripts/query.py stats              # Show row counts
    python scripts/query.py sql "SELECT ..."   # Run any SQL query
    python scripts/query.py search "plumbing"  # Search companies by name
    python scripts/query.py sic "43210"        # Find companies by SIC code
    python scripts/query.py postcode "SW1"     # Find companies by postcode prefix
    python scripts/query.py company "12345678" # Look up a specific company
    python scripts/query.py aging              # Find companies with likely aging founders
    python scripts/query.py single-director    # Companies with only 1 director (from PSC)
    python scripts/query.py export "SELECT ..." output.csv  # Export query to CSV
"""
import sys
import os
import csv

# Add parent to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.duckdb_store import DuckStore


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1].lower()
    store = DuckStore()

    try:
        if cmd == "setup":
            print("Loading company bulk data from zips...")
            count = store.ingest_companies()
            print(f"Companies loaded: {count:,}")
            print("\nLoading PSC bulk data...")
            psc_count = store.ingest_psc()
            print(f"PSCs loaded: {psc_count:,}")
            print("\nSetup complete! Run 'python scripts/query.py stats' to verify.")

        elif cmd == "stats":
            stats = store.stats()
            print("=== UK Companies House DuckDB Store ===")
            for table, count in stats.items():
                print(f"  {table}: {count:,} rows")
            # Show database file size
            db_size = os.path.getsize(store.db_path) / (1024 * 1024)
            print(f"\n  Database file: {store.db_path} ({db_size:.1f} MB)")

        elif cmd == "sql":
            if len(sys.argv) < 3:
                print("Usage: python scripts/query.py sql \"SELECT ...\"")
                return
            sql = sys.argv[2]
            results = store.query(sql)
            if not results:
                print("No results.")
                return
            # Print as table
            keys = results[0].keys()
            print(" | ".join(str(k) for k in keys))
            print("-" * 80)
            for row in results[:100]:
                print(" | ".join(str(row.get(k, "")) for k in keys))
            if len(results) > 100:
                print(f"\n... showing 100 of {len(results)} rows")

        elif cmd == "search":
            if len(sys.argv) < 3:
                print("Usage: python scripts/query.py search \"company name\"")
                return
            term = sys.argv[2]
            results = store.query(f"""
                SELECT company_number, company_name, company_status, postal_code,
                       sic_code_1, incorporation_date
                FROM companies
                WHERE company_name ILIKE '%{term}%'
                  AND company_status = 'Active'
                ORDER BY company_name
                LIMIT 50
            """)
            print(f"Found {len(results)} active companies matching '{term}':\n")
            for r in results:
                print(f"  {r['company_number']} | {r['company_name']} | {r['postal_code']} | {r['sic_code_1']}")

        elif cmd == "sic":
            if len(sys.argv) < 3:
                print("Usage: python scripts/query.py sic \"43210\"")
                return
            sic = sys.argv[2]
            results = store.query(f"""
                SELECT company_number, company_name, company_status, postal_code,
                       sic_code_1, incorporation_date
                FROM companies
                WHERE (sic_code_1 LIKE '{sic}%' OR sic_code_2 LIKE '{sic}%'
                       OR sic_code_3 LIKE '{sic}%' OR sic_code_4 LIKE '{sic}%')
                  AND company_status = 'Active'
                ORDER BY company_name
                LIMIT 100
            """)
            print(f"Found {len(results)} active companies with SIC starting '{sic}':\n")
            for r in results:
                print(f"  {r['company_number']} | {r['company_name']} | {r['postal_code']}")

        elif cmd == "postcode":
            if len(sys.argv) < 3:
                print("Usage: python scripts/query.py postcode \"SW1\"")
                return
            prefix = sys.argv[2]
            results = store.query(f"""
                SELECT company_number, company_name, postal_code, sic_code_1,
                       incorporation_date
                FROM companies
                WHERE postal_code LIKE '{prefix}%'
                  AND company_status = 'Active'
                ORDER BY company_name
                LIMIT 100
            """)
            print(f"Found {len(results)} active companies in '{prefix}' area:\n")
            for r in results:
                print(f"  {r['company_number']} | {r['company_name']} | {r['postal_code']} | {r['sic_code_1']}")

        elif cmd == "company":
            if len(sys.argv) < 3:
                print("Usage: python scripts/query.py company \"12345678\"")
                return
            number = sys.argv[2].zfill(8)
            results = store.query(f"""
                SELECT * FROM companies WHERE company_number = '{number}'
            """)
            if not results:
                print(f"Company {number} not found.")
                return
            r = results[0]
            print(f"=== {r['company_name']} ({r['company_number']}) ===")
            for k, v in r.items():
                if v is not None and str(v).strip():
                    print(f"  {k}: {v}")

            # Also check PSC
            pscs = store.query(f"""
                SELECT psc_name, psc_kind, birth_year, control_natures, ceased_on
                FROM pscs
                WHERE company_number = '{number}'
                ORDER BY ceased_on NULLS FIRST
            """)
            if pscs:
                print(f"\n  PSCs ({len(pscs)}):")
                for p in pscs:
                    status = "CEASED" if p.get("ceased_on") else "ACTIVE"
                    print(f"    [{status}] {p['psc_name']} ({p['psc_kind']}) born ~{p.get('birth_year', '?')}")

        elif cmd == "aging":
            current_year = 2026
            threshold_year = current_year - 55
            results = store.query(f"""
                SELECT p.company_number, c.company_name, c.postal_code,
                       c.sic_code_1, p.psc_name, p.birth_year,
                       ({current_year} - p.birth_year) as approx_age
                FROM pscs p
                JOIN companies c ON c.company_number = p.company_number
                WHERE p.birth_year IS NOT NULL
                  AND p.birth_year <= {threshold_year}
                  AND p.ceased_on IS NULL
                  AND c.company_status = 'Active'
                ORDER BY p.birth_year ASC
                LIMIT 100
            """)
            print(f"Active companies with PSCs likely aged 55+:\n")
            for r in results:
                print(f"  {r['company_number']} | {r['company_name']} | {r['psc_name']} (~{r['approx_age']}y) | {r['postal_code']}")

        elif cmd == "single-director":
            results = store.query("""
                SELECT p.company_number, c.company_name, c.postal_code,
                       c.sic_code_1, c.incorporation_date,
                       COUNT(*) as psc_count,
                       MIN(p.psc_name) as sole_psc
                FROM pscs p
                JOIN companies c ON c.company_number = p.company_number
                WHERE p.ceased_on IS NULL
                  AND p.psc_kind IN ('individual', 'individual-bo',
                      'individual-person-with-significant-control')
                  AND c.company_status = 'Active'
                GROUP BY p.company_number, c.company_name, c.postal_code,
                         c.sic_code_1, c.incorporation_date
                HAVING COUNT(*) = 1
                LIMIT 100
            """)
            print(f"Active companies with exactly 1 individual PSC:\n")
            for r in results:
                print(f"  {r['company_number']} | {r['company_name']} | {r['sole_psc']} | {r['postal_code']}")

        elif cmd == "export":
            if len(sys.argv) < 4:
                print("Usage: python scripts/query.py export \"SELECT ...\" output.csv")
                return
            sql = sys.argv[2]
            outfile = sys.argv[3]
            results = store.query(sql)
            if not results:
                print("No results to export.")
                return
            with open(outfile, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)
            print(f"Exported {len(results)} rows to {outfile}")

        else:
            print(f"Unknown command: {cmd}")
            print(__doc__)

    finally:
        store.close()


if __name__ == "__main__":
    main()
