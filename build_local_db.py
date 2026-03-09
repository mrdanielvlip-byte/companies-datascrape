"""
build_local_db.py — Download Companies House bulk data and build local SQLite database

Downloads the monthly BasicCompanyData snapshot from Companies House (~491MB zip,
~1.8GB CSV) and loads it into a fast local SQLite database with FTS5 full-text search
on company names and proper indexes for SIC code and status lookups.

The resulting database (~1.5GB) enables near-instant searches without hitting the
Companies House API, dramatically speeding up Step 1 discovery.

Usage:
  python build_local_db.py                  # Download latest + build DB
  python build_local_db.py --build-only     # Skip download, rebuild from existing zip
  python build_local_db.py --stats          # Show DB statistics
  python build_local_db.py --update         # Download latest snapshot + rebuild

Output:
  data/companies_house.db      — SQLite database (~1.5GB)
  data/BasicCompanyData.zip    — Cached zip (delete to force re-download)
  data/sic_codes.json          — SIC code reference extracted from bulk data

Database schema:
  companies           — all active+dissolved companies (4.9M+ rows)
  companies_fts       — FTS5 virtual table on company_name for fast text search
  sic_codes           — unique SIC codes + descriptions extracted from data
  db_meta             — build date, row count, source URL
"""

import os
import sys
import csv
import json
import sqlite3
import zipfile
import time
import re
import urllib.request
import urllib.error
from datetime import datetime, date
from pathlib import Path


# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR  = Path(__file__).parent / "data"
DB_PATH   = DATA_DIR / "companies_house.db"
ZIP_PATH  = DATA_DIR / "BasicCompanyData.zip"
SIC_PATH  = DATA_DIR / "sic_codes.json"

# CH bulk data URL — monthly snapshot, released 1st of each month
# Format: https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-YYYY-MM-01.zip
CH_BULK_BASE = "https://download.companieshouse.gov.uk"


# ── Helpers ───────────────────────────────────────────────────────────────────

def find_latest_url() -> tuple[str, str]:
    """
    Find the most recent available bulk data URL by trying the last 3 months.
    Returns (url, date_string).
    """
    from dateutil.relativedelta import relativedelta
    today = date.today()

    for months_back in range(0, 4):
        check_date = today - relativedelta(months=months_back)
        # Always use 1st of the month
        date_str = check_date.strftime("%Y-%m-01")
        url      = f"{CH_BULK_BASE}/BasicCompanyDataAsOneFile-{date_str}.zip"
        try:
            req = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(req, timeout=10) as r:
                if r.status == 200:
                    size = r.headers.get("Content-Length", "unknown")
                    print(f"  Found: {url}")
                    print(f"  Size:  {int(size)/1024/1024:.0f} MB" if size != "unknown" else "")
                    return url, date_str
        except Exception:
            continue

    raise RuntimeError("Could not find a recent CH bulk data file. Check your internet connection.")


def download_with_progress(url: str, dest: Path) -> None:
    """Download a file with a progress bar."""
    print(f"\nDownloading Companies House bulk data...")
    print(f"  URL:  {url}")
    print(f"  Dest: {dest}")

    def progress(count, block_size, total_size):
        if total_size > 0:
            pct  = min(count * block_size / total_size * 100, 100)
            done = int(pct / 2)
            bar  = "█" * done + "░" * (50 - done)
            mb   = min(count * block_size, total_size) / 1024 / 1024
            total_mb = total_size / 1024 / 1024
            print(f"\r  [{bar}] {pct:.1f}% ({mb:.0f}/{total_mb:.0f} MB)", end="", flush=True)

    urllib.request.urlretrieve(url, dest, reporthook=progress)
    print(f"\n  ✅ Download complete: {dest.stat().st_size / 1024 / 1024:.0f} MB")


def _clean(val: str) -> str:
    """Strip whitespace from a CSV value."""
    return (val or "").strip()


def _parse_date(val: str) -> str | None:
    """Parse DD/MM/YYYY date to ISO format, return None if invalid."""
    val = val.strip()
    if not val:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ── Database schema ───────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    company_number       TEXT PRIMARY KEY,
    company_name         TEXT NOT NULL,
    company_name_upper   TEXT NOT NULL,     -- normalised for fast LIKE searches
    company_status       TEXT,              -- Active, Dissolved, Liquidation, etc.
    company_type         TEXT,              -- Private Limited, PLC, LLP, etc.
    country_of_origin    TEXT,
    incorporation_date   TEXT,              -- ISO: YYYY-MM-DD
    dissolution_date     TEXT,
    postcode             TEXT,
    address_line1        TEXT,
    address_town         TEXT,
    address_county       TEXT,
    address_country      TEXT,
    sic1                 TEXT,
    sic2                 TEXT,
    sic3                 TEXT,
    sic4                 TEXT,
    accounts_category    TEXT,
    accounts_last_date   TEXT,
    accounts_next_due    TEXT,
    mortgages_outstanding INTEGER DEFAULT 0,
    uri                  TEXT,
    company_age_years    REAL
);

CREATE TABLE IF NOT EXISTS companies_fts (
    company_number TEXT,
    company_name   TEXT
);

CREATE TABLE IF NOT EXISTS sic_codes (
    sic_code    TEXT PRIMARY KEY,
    description TEXT,
    count       INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS db_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_status         ON companies(company_status)",
    "CREATE INDEX IF NOT EXISTS idx_sic1           ON companies(sic1)",
    "CREATE INDEX IF NOT EXISTS idx_sic2           ON companies(sic2)",
    "CREATE INDEX IF NOT EXISTS idx_sic3           ON companies(sic3)",
    "CREATE INDEX IF NOT EXISTS idx_sic4           ON companies(sic4)",
    "CREATE INDEX IF NOT EXISTS idx_type           ON companies(company_type)",
    "CREATE INDEX IF NOT EXISTS idx_postcode       ON companies(postcode)",
    "CREATE INDEX IF NOT EXISTS idx_inc_date       ON companies(incorporation_date)",
    "CREATE INDEX IF NOT EXISTS idx_name_upper     ON companies(company_name_upper)",
]

FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS companies_fts USING fts5(
    company_number UNINDEXED,
    company_name,
    content='companies',
    content_rowid='rowid'
);
"""


# ── CSV column mapping ────────────────────────────────────────────────────────

# The CH BasicCompanyData CSV columns (as per CH documentation)
COL_MAP = {
    "CompanyName":                  "company_name",
    "CompanyNumber":                "company_number",
    "RegAddress.AddressLine1":      "address_line1",
    "RegAddress.PostTown":          "address_town",
    "RegAddress.County":            "address_county",
    "RegAddress.Country":           "address_country",
    "RegAddress.PostCode":          "postcode",
    "CompanyCategory":              "company_type",
    "CompanyStatus":                "company_status",
    "CountryOfOrigin":              "country_of_origin",
    "DissolutionDate":              "dissolution_date",
    "IncorporationDate":            "incorporation_date",
    "Accounts.AccountCategory":     "accounts_category",
    "Accounts.LastMadeUpDate":      "accounts_last_date",
    "Accounts.NextDueDate":         "accounts_next_due",
    "Mortgages.NumMortOutstanding": "mortgages_outstanding",
    "SICCode.SicText_1":            "sic1",
    "SICCode.SicText_2":            "sic2",
    "SICCode.SicText_3":            "sic3",
    "SICCode.SicText_4":            "sic4",
    "URI":                          "uri",
}


def _extract_sic_code(sic_text: str) -> tuple[str, str]:
    """
    Extract (code, description) from CH SIC text like:
    '38110 - Collection of non-hazardous waste'
    Returns ('38110', 'Collection of non-hazardous waste') or ('', sic_text)
    """
    if not sic_text:
        return "", ""
    m = re.match(r"(\d{5})\s*[-–]\s*(.+)", sic_text.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    # Some entries are just the code
    m2 = re.match(r"^(\d{5})$", sic_text.strip())
    if m2:
        return m2.group(1), ""
    return "", sic_text.strip()


def _company_age(inc_date: str) -> float | None:
    """Calculate company age in years from incorporation date."""
    if not inc_date:
        return None
    try:
        inc  = datetime.strptime(inc_date, "%Y-%m-%d")
        days = (datetime.now() - inc).days
        return round(days / 365.25, 1)
    except Exception:
        return None


# ── Main build function ───────────────────────────────────────────────────────

def build_database(zip_path: Path, db_path: Path, active_only: bool = False) -> dict:
    """
    Unzip the CH bulk CSV and load into SQLite.
    If active_only=True, only loads Active companies (faster, smaller DB).
    Returns stats dict.
    """
    print(f"\nBuilding SQLite database...")
    print(f"  Source: {zip_path} ({zip_path.stat().st_size / 1024 / 1024:.0f} MB)")
    print(f"  Output: {db_path}")
    if active_only:
        print(f"  Mode:   Active companies only")

    # Connect and create schema
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA cache_size=-256000")  # 256MB cache
    con.executescript(SCHEMA)

    # Drop FTS if rebuilding
    con.execute("DROP TABLE IF EXISTS companies_fts")
    con.executescript(FTS_SCHEMA)
    con.commit()

    # Track SIC codes
    sic_counts: dict[str, dict] = {}

    stats = {
        "total_rows":     0,
        "loaded":         0,
        "skipped":        0,
        "active":         0,
        "dissolved":      0,
        "other_status":   0,
        "errors":         0,
    }

    t0 = time.time()

    with zipfile.ZipFile(str(zip_path)) as zf:
        # Find the CSV file inside the zip
        csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_files:
            raise RuntimeError("No CSV file found in zip")
        csv_name = csv_files[0]
        print(f"  CSV:    {csv_name}")

        with zf.open(csv_name) as raw:
            # Wrap in text mode
            import io
            reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8", errors="replace"))
            # CH CSV has leading/trailing spaces in some column names — strip them
            reader.fieldnames = [name.strip() for name in (reader.fieldnames or [])]

            batch     = []
            fts_batch = []
            BATCH_SIZE = 5000

            for i, row in enumerate(reader):
                stats["total_rows"] += 1

                # Map columns
                def g(col):
                    return _clean(row.get(col, ""))

                status = g("CompanyStatus")

                if active_only and status.lower() not in ("active", ""):
                    stats["skipped"] += 1
                    continue

                # Count by status
                sl = status.lower()
                if "active" in sl:   stats["active"] += 1
                elif "dissolved" in sl: stats["dissolved"] += 1
                else:                stats["other_status"] += 1

                # Parse fields
                name    = g("CompanyName")
                number  = g("CompanyNumber")
                if not name or not number:
                    stats["errors"] += 1
                    continue

                inc_date  = _parse_date(g("IncorporationDate"))
                diss_date = _parse_date(g("DissolutionDate"))

                sic1_raw = g("SICCode.SicText_1")
                sic2_raw = g("SICCode.SicText_2")
                sic3_raw = g("SICCode.SicText_3")
                sic4_raw = g("SICCode.SicText_4")

                sic1_code, sic1_desc = _extract_sic_code(sic1_raw)
                sic2_code, _         = _extract_sic_code(sic2_raw)
                sic3_code, _         = _extract_sic_code(sic3_raw)
                sic4_code, _         = _extract_sic_code(sic4_raw)

                # Track SIC codes
                if sic1_code:
                    if sic1_code not in sic_counts:
                        sic_counts[sic1_code] = {"description": sic1_desc, "count": 0}
                    sic_counts[sic1_code]["count"] += 1

                try:
                    mort = int(g("Mortgages.NumMortOutstanding") or 0)
                except ValueError:
                    mort = 0

                age = _company_age(inc_date)

                record = (
                    number,
                    name,
                    name.upper(),
                    status,
                    g("CompanyCategory"),
                    g("CountryOfOrigin"),
                    inc_date,
                    diss_date,
                    g("RegAddress.PostCode"),
                    g("RegAddress.AddressLine1"),
                    g("RegAddress.PostTown"),
                    g("RegAddress.County"),
                    g("RegAddress.Country"),
                    sic1_code,
                    sic2_code,
                    sic3_code,
                    sic4_code,
                    g("Accounts.AccountCategory"),
                    _parse_date(g("Accounts.LastMadeUpDate")),
                    _parse_date(g("Accounts.NextDueDate")),
                    mort,
                    g("URI"),
                    age,
                )

                batch.append(record)
                fts_batch.append((number, name))
                stats["loaded"] += 1

                if len(batch) >= BATCH_SIZE:
                    con.executemany(
                        "INSERT OR REPLACE INTO companies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        batch,
                    )
                    con.executemany(
                        "INSERT INTO companies_fts(company_number, company_name) VALUES (?,?)",
                        fts_batch,
                    )
                    con.commit()
                    batch     = []
                    fts_batch = []

                    elapsed = time.time() - t0
                    rate    = stats["loaded"] / elapsed
                    eta     = (5_000_000 - stats["loaded"]) / rate / 60
                    print(f"\r  [{stats['loaded']:>7,}] {rate:.0f} rows/s  |  "
                          f"Active: {stats['active']:,}  |  ETA: {eta:.0f} min  ",
                          end="", flush=True)

            # Final batch
            if batch:
                con.executemany(
                    "INSERT OR REPLACE INTO companies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    batch,
                )
                con.executemany(
                    "INSERT INTO companies_fts(company_number, company_name) VALUES (?,?)",
                    fts_batch,
                )
                con.commit()

    print(f"\n\n  Loaded {stats['loaded']:,} companies in {time.time()-t0:.0f}s")

    # Build indexes
    print("  Building indexes...")
    t1 = time.time()
    for idx_sql in INDEXES:
        con.execute(idx_sql)
    con.commit()
    print(f"  Indexes built in {time.time()-t1:.0f}s")

    # Populate SIC codes table
    print(f"  Writing {len(sic_counts):,} SIC codes...")
    con.executemany(
        "INSERT OR REPLACE INTO sic_codes VALUES (?,?,?)",
        [(code, d["description"], d["count"]) for code, d in sic_counts.items()],
    )

    # Save DB metadata
    con.execute("DELETE FROM db_meta")
    con.executemany("INSERT INTO db_meta VALUES (?,?)", [
        ("build_date",    datetime.now().isoformat()),
        ("total_loaded",  str(stats["loaded"])),
        ("active_count",  str(stats["active"])),
        ("source_file",   zip_path.name),
    ])
    con.commit()
    con.close()

    # Save SIC codes to JSON
    sic_for_json = {code: d for code, d in sic_counts.items()}
    with open(str(SIC_PATH), "w") as f:
        json.dump(sic_for_json, f, indent=2, sort_keys=True)
    print(f"  SIC codes saved → {SIC_PATH}  ({len(sic_counts):,} codes)")

    total_time = time.time() - t0
    print(f"\n  ✅ Database ready: {db_path}")
    print(f"     Size:    {db_path.stat().st_size / 1024 / 1024:.0f} MB")
    print(f"     Rows:    {stats['loaded']:,} companies")
    print(f"     Active:  {stats['active']:,}")
    print(f"     Time:    {total_time/60:.1f} minutes")

    return stats


def print_stats(db_path: Path = DB_PATH) -> None:
    """Print statistics about the local database."""
    if not db_path.exists():
        print(f"  Database not found: {db_path}")
        print("  Run: python build_local_db.py")
        return

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    meta = dict(con.execute("SELECT key, value FROM db_meta").fetchall())
    total   = con.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    active  = con.execute("SELECT COUNT(*) FROM companies WHERE company_status='Active'").fetchone()[0]
    sic_cnt = con.execute("SELECT COUNT(*) FROM sic_codes").fetchone()[0]

    print(f"\n{'='*55}")
    print(f"  Companies House Local Database")
    print(f"{'='*55}")
    print(f"  Location:     {db_path}")
    print(f"  Size:         {db_path.stat().st_size / 1024 / 1024:.0f} MB")
    print(f"  Built:        {meta.get('build_date','unknown')[:19]}")
    print(f"  Total rows:   {total:,}")
    print(f"  Active cos:   {active:,}")
    print(f"  SIC codes:    {sic_cnt:,}")
    print(f"{'='*55}")

    # Top 10 SIC codes
    top_sics = con.execute(
        "SELECT sic_code, description, count FROM sic_codes "
        "ORDER BY count DESC LIMIT 10"
    ).fetchall()
    print(f"\n  Top 10 SIC codes by company count:")
    for row in top_sics:
        print(f"    {row['sic_code']}  {row['count']:>8,}  {(row['description'] or '')[:50]}")

    con.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Download Companies House bulk data and build local SQLite DB",
    )
    parser.add_argument("--build-only",   action="store_true",
                        help="Skip download, rebuild DB from existing zip")
    parser.add_argument("--active-only",  action="store_true",
                        help="Only load Active companies (faster, smaller DB)")
    parser.add_argument("--stats",        action="store_true",
                        help="Show database statistics and exit")
    parser.add_argument("--update",       action="store_true",
                        help="Check for newer snapshot, download if available, rebuild")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.stats:
        print_stats()
        return

    # Determine download URL
    if not args.build_only:
        try:
            url, date_str = find_latest_url()
        except RuntimeError as e:
            print(f"Error: {e}")
            sys.exit(1)

        # Check if we already have it
        if ZIP_PATH.exists() and not args.update:
            print(f"  Zip already exists: {ZIP_PATH} ({ZIP_PATH.stat().st_size/1024/1024:.0f} MB)")
            print(f"  Skipping download (use --update to force)")
        else:
            download_with_progress(url, ZIP_PATH)
    else:
        if not ZIP_PATH.exists():
            print(f"Error: No zip found at {ZIP_PATH}. Run without --build-only first.")
            sys.exit(1)

    if not ZIP_PATH.exists():
        print(f"Error: Download failed or zip not found at {ZIP_PATH}")
        sys.exit(1)

    # Build (or rebuild) the database
    stats = build_database(ZIP_PATH, DB_PATH, active_only=args.active_only)
    print_stats()


if __name__ == "__main__":
    main()
