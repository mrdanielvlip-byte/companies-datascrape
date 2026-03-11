"""
DuckDB-based local data store.

Zero-infrastructure alternative to PostgreSQL.
Reads bulk zips directly, builds a persistent .duckdb file,
and provides fast SQL queries over 5.67M+ companies and PSC data.

Usage:
    from app.duckdb_store import DuckStore

    store = DuckStore()          # opens/creates uk_ch_intel.duckdb
    store.ingest_companies()     # loads all 7 zip parts
    store.ingest_psc()           # loads PSC bulk data

    # Query
    results = store.query("SELECT * FROM companies WHERE postal_code LIKE 'SW1%' LIMIT 10")
    store.close()
"""
import os
import glob
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime

import duckdb
from loguru import logger


# ── Defaults ───────────────────────────────────────────────────────────

DEFAULT_DB_PATH = "uk_ch_intel.duckdb"
DEFAULT_COMPANIES_DIR = "datasets/uk_companies_full"
DEFAULT_PSC_DIR = "datasets/psc_bulk"


class DuckStore:
    """
    Persistent DuckDB store for Companies House data.

    The .duckdb file is a single portable file that can be committed
    to GitHub or shared. No server process needed.
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.conn = duckdb.connect(self.db_path)
        logger.info(f"Connected to DuckDB at {self.db_path}")
        self._ensure_schema()

    def _ensure_schema(self):
        """Create tables if they don't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                company_number VARCHAR PRIMARY KEY,
                company_name VARCHAR,
                company_status VARCHAR,
                company_type VARCHAR,
                jurisdiction VARCHAR,
                incorporation_date DATE,
                dissolution_date DATE,
                registered_address VARCHAR,
                postal_code VARCHAR,
                sic_code_1 VARCHAR,
                sic_code_2 VARCHAR,
                sic_code_3 VARCHAR,
                sic_code_4 VARCHAR,
                accounts_next_due DATE,
                accounts_last_made_up_to DATE,
                confirmation_statement_next_due DATE,
                confirmation_statement_last_made_up_to DATE,
                uri VARCHAR,
                source_file VARCHAR,
                loaded_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS psc_seq START 1")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS ingest_seq START 1")

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pscs (
                psc_row_id INTEGER PRIMARY KEY DEFAULT(nextval('psc_seq')),
                company_number VARCHAR,
                psc_name VARCHAR,
                psc_kind VARCHAR,
                birth_month INTEGER,
                birth_year INTEGER,
                notified_on DATE,
                ceased_on DATE,
                control_natures VARCHAR,
                source_file VARCHAR,
                loaded_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ingest_log (
                run_id INTEGER PRIMARY KEY DEFAULT(nextval('ingest_seq')),
                ingest_type VARCHAR,
                file_name VARCHAR,
                rows_loaded INTEGER,
                started_at TIMESTAMP,
                completed_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

        logger.info("Schema verified")

    # ── Company ingestion ──────────────────────────────────────────────

    def ingest_companies(self, data_dir: str = None):
        """
        Load all BasicCompanyData zip parts into the companies table.

        Extracts each zip to a temp directory, loads the CSV with DuckDB,
        then cleans up. The .duckdb file persists the data permanently.
        """
        data_dir = data_dir or DEFAULT_COMPANIES_DIR
        zip_files = sorted(glob.glob(os.path.join(data_dir, "BasicCompanyData*.zip")))

        if not zip_files:
            logger.error(f"No company zip files found in {data_dir}")
            return 0

        # Check if already loaded
        existing = self.conn.execute("SELECT count(*) FROM companies").fetchone()[0]
        if existing > 0:
            logger.info(f"Companies table already has {existing:,} rows. Skipping ingestion.")
            logger.info("Run store.reset_companies() first to reload.")
            return existing

        total = 0
        for zip_path in zip_files:
            started = datetime.utcnow()
            fname = os.path.basename(zip_path)
            logger.info(f"Loading {fname}...")

            try:
                # Extract CSV from zip to temp directory
                with tempfile.TemporaryDirectory() as tmpdir:
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        csv_name = zf.namelist()[0]
                        zf.extract(csv_name, tmpdir)
                    csv_path = os.path.join(tmpdir, csv_name)

                    self.conn.execute(f"""
                        INSERT INTO companies
                        SELECT
                            LPAD(TRIM(COALESCE("CompanyNumber", '')), 8, '0')
                                AS company_number,
                            TRIM("CompanyName") AS company_name,
                            TRIM("CompanyStatus") AS company_status,
                            TRIM("CompanyCategory") AS company_type,
                            COALESCE(NULLIF(TRIM("CountryOfOrigin"), ''), 'United Kingdom')
                                AS jurisdiction,
                            TRY_CAST("IncorporationDate" AS DATE) AS incorporation_date,
                            TRY_CAST("DissolutionDate" AS DATE) AS dissolution_date,
                            CONCAT_WS(', ',
                                NULLIF(TRIM("RegAddress.AddressLine1"), ''),
                                NULLIF(TRIM("RegAddress.AddressLine2"), ''),
                                NULLIF(TRIM("RegAddress.PostTown"), ''),
                                NULLIF(TRIM("RegAddress.County"), ''),
                                NULLIF(TRIM("RegAddress.Country"), '')
                            ) AS registered_address,
                            NULLIF(TRIM("RegAddress.PostCode"), '') AS postal_code,
                            NULLIF(TRIM("SICCode.SicText_1"), '') AS sic_code_1,
                            NULLIF(TRIM("SICCode.SicText_2"), '') AS sic_code_2,
                            NULLIF(TRIM("SICCode.SicText_3"), '') AS sic_code_3,
                            NULLIF(TRIM("SICCode.SicText_4"), '') AS sic_code_4,
                            TRY_CAST("Accounts.NextDueDate" AS DATE) AS accounts_next_due,
                            TRY_CAST("Accounts.LastMadeUpDate" AS DATE)
                                AS accounts_last_made_up_to,
                            TRY_CAST("Returns.NextDueDate" AS DATE)
                                AS confirmation_statement_next_due,
                            TRY_CAST("Returns.LastMadeUpDate" AS DATE)
                                AS confirmation_statement_last_made_up_to,
                            NULLIF(TRIM("URI"), '') AS uri,
                            '{fname}' AS source_file,
                            current_timestamp AS loaded_at
                        FROM read_csv('{csv_path}',
                            delim=',',
                            header=true,
                            quote='"',
                            all_varchar=true,
                            ignore_errors=true,
                            null_padding=true
                        )
                        WHERE TRIM(COALESCE("CompanyNumber", '')) != ''
                    """)

                count = self.conn.execute(
                    f"SELECT count(*) FROM companies WHERE source_file = '{fname}'"
                ).fetchone()[0]
                total += count

                # Log ingestion
                self.conn.execute("""
                    INSERT INTO ingest_log (ingest_type, file_name, rows_loaded, started_at)
                    VALUES (?, ?, ?, ?)
                """, ["companies", fname, count, started])

                logger.info(f"  Loaded {count:,} companies from {fname}")

            except Exception as e:
                logger.error(f"  Failed to load {fname}: {e}")
                continue

        logger.info(f"Company ingestion complete: {total:,} total rows")
        return total

    # ── PSC ingestion ──────────────────────────────────────────────────

    def ingest_psc(self, data_dir: str = None):
        """
        Load PSC bulk data into the pscs table.

        PSC data comes as zip files containing JSONL (.txt) files.
        Each zip is extracted to a temp directory, loaded via DuckDB's
        read_json, then cleaned up.

        Supports:
        - psc-snapshot-*_Nof31.zip (multi-part, from download_psc.py)
        - *.jsonl / *.json (raw JSONL files)
        """
        data_dir = data_dir or DEFAULT_PSC_DIR

        # Find all PSC data files
        zip_files = sorted(glob.glob(os.path.join(data_dir, "psc-snapshot-*.zip")))
        jsonl_files = sorted(glob.glob(os.path.join(data_dir, "*.jsonl")))
        txt_files = sorted(glob.glob(os.path.join(data_dir, "*.txt")))
        all_raw = jsonl_files + txt_files

        if not zip_files and not all_raw:
            logger.warning(f"No PSC data files found in {data_dir}")
            logger.info("Run: python scripts/download_psc.py")
            return 0

        existing = self.conn.execute("SELECT count(*) FROM pscs").fetchone()[0]
        if existing > 0:
            logger.info(f"PSCs table already has {existing:,} rows. Skipping.")
            logger.info("Run store.reset_pscs() first to reload.")
            return existing

        total = 0

        # Process zip files (extract JSONL from inside)
        for zip_path in zip_files:
            fname = os.path.basename(zip_path)
            logger.info(f"Loading PSC data from {fname}...")

            try:
                with tempfile.TemporaryDirectory() as tmpdir:
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        txt_name = zf.namelist()[0]
                        zf.extract(txt_name, tmpdir)
                    jsonl_path = os.path.join(tmpdir, txt_name)

                    count = self._load_psc_jsonl(jsonl_path, fname)
                    total += count

            except Exception as e:
                logger.error(f"  Failed to load {fname}: {e}")

        # Process raw JSONL/TXT files
        for fpath in all_raw:
            fname = os.path.basename(fpath)
            logger.info(f"Loading PSC data from {fname}...")
            try:
                count = self._load_psc_jsonl(fpath, fname)
                total += count
            except Exception as e:
                logger.error(f"  Failed to load {fname}: {e}")

        logger.info(f"PSC ingestion complete: {total:,} total rows")
        return total

    def _load_psc_jsonl(self, jsonl_path: str, source_name: str) -> int:
        """Load a single JSONL file of PSC records into the pscs table."""
        self.conn.execute(f"""
            INSERT INTO pscs (company_number, psc_name, psc_kind,
                birth_month, birth_year, notified_on, ceased_on,
                control_natures, source_file)
            SELECT
                LPAD(TRIM(company_number), 8, '0'),
                COALESCE(
                    data->>'name',
                    TRIM(CONCAT_WS(' ',
                        data->'name_elements'->>'forename',
                        data->'name_elements'->>'surname'
                    )),
                    'UNKNOWN'
                ),
                COALESCE(data->>'kind', 'unknown'),
                TRY_CAST(data->'date_of_birth'->>'month' AS INTEGER),
                TRY_CAST(data->'date_of_birth'->>'year' AS INTEGER),
                TRY_CAST(data->>'notified_on' AS DATE),
                TRY_CAST(data->>'ceased_on' AS DATE),
                data->>'natures_of_control',
                '{source_name}'
            FROM read_json('{jsonl_path}',
                format='newline_delimited',
                ignore_errors=true,
                columns={{
                    company_number: 'VARCHAR',
                    data: 'JSON'
                }}
            )
            WHERE company_number IS NOT NULL
        """)

        count = self.conn.execute(
            f"SELECT count(*) FROM pscs WHERE source_file = '{source_name}'"
        ).fetchone()[0]
        logger.info(f"  Loaded {count:,} PSC records from {source_name}")
        return count

    # ── Query interface ────────────────────────────────────────────────

    def query(self, sql: str, params: list = None):
        """Run a SQL query and return results as a list of dicts."""
        if params:
            result = self.conn.execute(sql, params)
        else:
            result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def query_df(self, sql: str, params: list = None):
        """Run a SQL query and return a pandas DataFrame."""
        if params:
            return self.conn.execute(sql, params).fetchdf()
        return self.conn.execute(sql).fetchdf()

    def stats(self) -> dict:
        """Return row counts for all tables."""
        tables = ["companies", "pscs", "ingest_log"]
        result = {}
        for t in tables:
            try:
                count = self.conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                result[t] = count
            except Exception:
                result[t] = 0
        return result

    # ── Maintenance ────────────────────────────────────────────────────

    def reset_companies(self):
        """Drop and recreate the companies table for fresh ingestion."""
        self.conn.execute("DROP TABLE IF EXISTS companies")
        self._ensure_schema()
        logger.info("Companies table reset")

    def reset_pscs(self):
        """Drop and recreate the pscs table."""
        self.conn.execute("DROP TABLE IF EXISTS pscs")
        self._ensure_schema()
        logger.info("PSCs table reset")

    def vacuum(self):
        """Reclaim disk space after deletions."""
        self.conn.execute("CHECKPOINT")
        logger.info("Database checkpointed")

    def close(self):
        """Close the database connection."""
        self.conn.close()
        logger.info("DuckDB connection closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
