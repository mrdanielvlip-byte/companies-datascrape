"""
build_psc_db.py — Download Companies House PSC bulk snapshot and load into SQLite.

Downloads the daily PSC snapshot (31 ZIP parts, ~2GB total) from Companies House
and loads individual PSC records into the existing companies_house.db.

Adds a `psc` table with: company_number, name, date_of_birth (month/year),
nationality, natures_of_control, notified_on, ceased_on.

Usage:
  python build_psc_db.py                  # Download + build
  python build_psc_db.py --build-only     # Build from existing downloaded parts
  python build_psc_db.py --stats          # Show PSC table statistics
  python build_psc_db.py --download-only  # Download parts without building

Output:
  Adds `psc` table to data/companies_house.db
  Downloads parts to datasets/uk_psc/
"""

import os
import sys
import json
import sqlite3
import zipfile
import time
import io
import urllib.request
import urllib.error
from datetime import date, datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR     = Path(__file__).parent / "data"
DB_PATH      = DATA_DIR / "companies_house.db"
PSC_DIR      = Path(__file__).parent / "datasets" / "uk_psc"
NUM_PARTS    = 31

CH_DOWNLOAD_BASE = "https://download.companieshouse.gov.uk"


# ── Schema ────────────────────────────────────────────────────────────────────

PSC_SCHEMA = """
CREATE TABLE IF NOT EXISTS psc (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    company_number      TEXT NOT NULL,
    kind                TEXT,           -- individual, corporate-entity, statement
    name                TEXT,
    forename            TEXT,
    surname             TEXT,
    title               TEXT,
    dob_month           INTEGER,
    dob_year            INTEGER,
    nationality         TEXT,
    country_of_residence TEXT,
    natures_of_control  TEXT,           -- JSON array as string
    notified_on         TEXT,
    ceased_on           TEXT,
    address_postcode    TEXT,
    address_country     TEXT
);
"""

PSC_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_psc_company ON psc(company_number)",
    "CREATE INDEX IF NOT EXISTS idx_psc_kind    ON psc(kind)",
    "CREATE INDEX IF NOT EXISTS idx_psc_surname ON psc(surname)",
    "CREATE INDEX IF NOT EXISTS idx_psc_ceased  ON psc(ceased_on)",
    "CREATE INDEX IF NOT EXISTS idx_psc_dob     ON psc(dob_year, dob_month)",
]


# ── Download ──────────────────────────────────────────────────────────────────

def find_latest_snapshot_date() -> str:
    """Return today's date in YYYY-MM-DD format for the snapshot URL."""
    return date.today().strftime("%Y-%m-%d")


def download_parts(snapshot_date: str, force: bool = False) -> list[Path]:
    """Download all 31 PSC snapshot parts."""
    PSC_DIR.mkdir(parents=True, exist_ok=True)
    parts = []

    for i in range(1, NUM_PARTS + 1):
        filename = f"psc-snapshot-{snapshot_date}_{i}of{NUM_PARTS}.zip"
        url = f"{CH_DOWNLOAD_BASE}/{filename}"
        dest = PSC_DIR / filename

        if dest.exists() and not force:
            size_mb = dest.stat().st_size / (1024 * 1024)
            print(f"  [{i:2d}/{NUM_PARTS}] {filename} — already exists ({size_mb:.0f} MB)")
            parts.append(dest)
            continue

        print(f"  [{i:2d}/{NUM_PARTS}] Downloading {filename} ...", end="", flush=True)
        t0 = time.time()
        try:
            urllib.request.urlretrieve(url, dest)
            size_mb = dest.stat().st_size / (1024 * 1024)
            elapsed = time.time() - t0
            print(f"  {size_mb:.0f} MB in {elapsed:.0f}s")
            parts.append(dest)
        except urllib.error.HTTPError as e:
            print(f"  FAILED ({e.code})")
            # Try yesterday's date
            if e.code == 404:
                yesterday = date.today().replace(day=date.today().day - 1).strftime("%Y-%m-%d")
                url2 = f"{CH_DOWNLOAD_BASE}/psc-snapshot-{yesterday}_{i}of{NUM_PARTS}.zip"
                try:
                    urllib.request.urlretrieve(url2, dest)
                    size_mb = dest.stat().st_size / (1024 * 1024)
                    print(f"    → Fell back to {yesterday}: {size_mb:.0f} MB")
                    parts.append(dest)
                except Exception:
                    print(f"    → Fallback also failed")

    return parts


# ── Build ─────────────────────────────────────────────────────────────────────

def parse_psc_record(line: str) -> dict | None:
    """Parse one JSON line from the PSC snapshot. Returns dict or None if not useful."""
    try:
        rec = json.loads(line)
    except json.JSONDecodeError:
        return None

    company_number = rec.get("company_number", "")
    data = rec.get("data", {})
    kind = data.get("kind", "")

    # We want individuals and corporate entities, skip bare statements
    if "individual" not in kind and "corporate" not in kind:
        return None

    name_elements = data.get("name_elements", {})
    dob = data.get("date_of_birth", {})
    addr = data.get("address", {})
    natures = data.get("natures_of_control", [])

    return {
        "company_number":      company_number,
        "kind":                kind,
        "name":                data.get("name", ""),
        "forename":            name_elements.get("forename", ""),
        "surname":             name_elements.get("surname", ""),
        "title":               name_elements.get("title", ""),
        "dob_month":           dob.get("month"),
        "dob_year":            dob.get("year"),
        "nationality":         data.get("nationality", ""),
        "country_of_residence": data.get("country_of_residence", ""),
        "natures_of_control":  json.dumps(natures) if natures else "",
        "notified_on":         data.get("notified_on", ""),
        "ceased_on":           data.get("ceased_on", ""),
        "address_postcode":    addr.get("postal_code", ""),
        "address_country":     addr.get("country", ""),
    }


def build_psc_table(parts: list[Path], db_path: Path = DB_PATH):
    """Load PSC records from ZIP parts into SQLite."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA cache_size=-512000")  # 512MB cache

    # Drop and recreate for clean rebuild
    con.execute("DROP TABLE IF EXISTS psc")
    con.executescript(PSC_SCHEMA)

    total_loaded = 0
    total_skipped = 0

    INSERT_SQL = """
        INSERT INTO psc (
            company_number, kind, name, forename, surname, title,
            dob_month, dob_year, nationality, country_of_residence,
            natures_of_control, notified_on, ceased_on,
            address_postcode, address_country
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for part_path in parts:
        t0 = time.time()
        part_count = 0
        part_name = part_path.name

        print(f"  Loading {part_name} ...", end="", flush=True)

        try:
            with zipfile.ZipFile(part_path, "r") as zf:
                names = zf.namelist()
                if not names:
                    print(" empty ZIP")
                    continue

                with zf.open(names[0]) as f:
                    batch = []
                    for raw_line in f:
                        line = raw_line.decode("utf-8", errors="replace").strip()
                        if not line:
                            continue

                        rec = parse_psc_record(line)
                        if rec is None:
                            total_skipped += 1
                            continue

                        batch.append((
                            rec["company_number"], rec["kind"], rec["name"],
                            rec["forename"], rec["surname"], rec["title"],
                            rec["dob_month"], rec["dob_year"],
                            rec["nationality"], rec["country_of_residence"],
                            rec["natures_of_control"],
                            rec["notified_on"], rec["ceased_on"],
                            rec["address_postcode"], rec["address_country"],
                        ))

                        if len(batch) >= 50_000:
                            con.executemany(INSERT_SQL, batch)
                            con.commit()
                            part_count += len(batch)
                            batch = []

                    # Flush remaining
                    if batch:
                        con.executemany(INSERT_SQL, batch)
                        con.commit()
                        part_count += len(batch)

        except Exception as e:
            print(f" ERROR: {e}")
            continue

        elapsed = time.time() - t0
        total_loaded += part_count
        rate = part_count / elapsed if elapsed > 0 else 0
        print(f"  {part_count:,} records in {elapsed:.0f}s ({rate:,.0f}/s) | total: {total_loaded:,}")

    # Build indexes
    print(f"\n  Building indexes ...")
    for idx_sql in PSC_INDEXES:
        con.execute(idx_sql)
    con.commit()

    # Update metadata
    con.execute(
        "INSERT OR REPLACE INTO db_meta (key, value) VALUES (?, ?)",
        ("psc_loaded", str(total_loaded)),
    )
    con.execute(
        "INSERT OR REPLACE INTO db_meta (key, value) VALUES (?, ?)",
        ("psc_build_date", datetime.now().isoformat()),
    )
    con.commit()
    con.close()

    print(f"\n  ✅ PSC table built: {total_loaded:,} records loaded, {total_skipped:,} statements skipped")


def show_stats(db_path: Path = DB_PATH):
    """Print PSC table statistics."""
    if not db_path.exists():
        print("DB not found")
        return

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    try:
        total = con.execute("SELECT COUNT(*) FROM psc").fetchone()[0]
        individuals = con.execute("SELECT COUNT(*) FROM psc WHERE kind LIKE '%individual%'").fetchone()[0]
        corporates = con.execute("SELECT COUNT(*) FROM psc WHERE kind LIKE '%corporate%'").fetchone()[0]
        with_dob = con.execute("SELECT COUNT(*) FROM psc WHERE dob_year IS NOT NULL").fetchone()[0]
        active = con.execute("SELECT COUNT(*) FROM psc WHERE ceased_on IS NULL OR ceased_on = ''").fetchone()[0]
        unique_cos = con.execute("SELECT COUNT(DISTINCT company_number) FROM psc").fetchone()[0]
    except sqlite3.OperationalError:
        print("PSC table not found in DB")
        con.close()
        return

    print(f"\n📊 PSC Table Statistics:")
    print(f"  Total records:         {total:>12,}")
    print(f"  Individuals:           {individuals:>12,}")
    print(f"  Corporate entities:    {corporates:>12,}")
    print(f"  With date of birth:    {with_dob:>12,}")
    print(f"  Active (not ceased):   {active:>12,}")
    print(f"  Unique companies:      {unique_cos:>12,}")

    # Age distribution of active individual PSCs
    current_year = date.today().year
    try:
        age_dist = con.execute(f"""
            SELECT
                CASE
                    WHEN ({current_year} - dob_year) < 30 THEN 'Under 30'
                    WHEN ({current_year} - dob_year) BETWEEN 30 AND 44 THEN '30-44'
                    WHEN ({current_year} - dob_year) BETWEEN 45 AND 54 THEN '45-54'
                    WHEN ({current_year} - dob_year) BETWEEN 55 AND 64 THEN '55-64'
                    WHEN ({current_year} - dob_year) >= 65 THEN '65+'
                    ELSE 'Unknown'
                END as age_band,
                COUNT(*) as cnt
            FROM psc
            WHERE kind LIKE '%individual%'
              AND dob_year IS NOT NULL
              AND (ceased_on IS NULL OR ceased_on = '')
            GROUP BY age_band
            ORDER BY cnt DESC
        """).fetchall()

        print(f"\n  Active individual PSC age distribution:")
        for row in age_dist:
            print(f"    {row['age_band']:>12s}: {row['cnt']:>10,}")
    except Exception:
        pass

    con.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]

    if "--stats" in args:
        show_stats()
        sys.exit(0)

    if "--download-only" in args:
        snapshot_date = find_latest_snapshot_date()
        print(f"📥 Downloading PSC snapshot ({snapshot_date}) — {NUM_PARTS} parts ...")
        parts = download_parts(snapshot_date)
        print(f"✅ Downloaded {len(parts)} parts to {PSC_DIR}")
        sys.exit(0)

    if "--build-only" in args:
        parts = sorted(PSC_DIR.glob("psc-snapshot-*.zip"))
        if not parts:
            print("No PSC parts found in", PSC_DIR)
            sys.exit(1)
        print(f"🔨 Building PSC table from {len(parts)} local parts ...")
        build_psc_table(parts)
        show_stats()
        sys.exit(0)

    # Default: download + build
    snapshot_date = find_latest_snapshot_date()
    print(f"📥 Downloading PSC snapshot ({snapshot_date}) — {NUM_PARTS} parts ...")
    parts = download_parts(snapshot_date)
    if not parts:
        print("❌ No parts downloaded")
        sys.exit(1)

    print(f"\n🔨 Building PSC table from {len(parts)} parts ...")
    build_psc_table(parts)
    show_stats()
