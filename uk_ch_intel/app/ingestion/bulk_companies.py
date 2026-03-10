import hashlib
import io
import csv
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd
import requests
from loguru import logger
from tqdm import tqdm

from app.db import get_session, get_raw_connection_context
from app.models import Company, IngestRun
from app.config import get_settings


def get_latest_bulk_url() -> str:
    """Detect latest Companies House bulk company data URL."""
    base_url = "https://download.companieshouse.gov.uk/"

    try:
        # Try to get current date
        today = datetime.now()
        # Companies House typically has a single current file
        # Try the most common naming pattern
        candidate_url = f"{base_url}BasicCompanyDataAsOneFile-{today.strftime('%Y%m%d')}.zip"

        # Make a HEAD request to check if it exists
        response = requests.head(candidate_url, timeout=10)
        if response.status_code == 200:
            logger.info(f"Found bulk data at {candidate_url}")
            return candidate_url

        # If not found, try without date (default file)
        fallback_url = f"{base_url}BasicCompanyDataAsOneFile.zip"
        response = requests.head(fallback_url, timeout=10)
        if response.status_code == 200:
            logger.info(f"Found bulk data at {fallback_url}")
            return fallback_url

        raise ValueError("Could not find latest bulk data URL")
    except Exception as e:
        logger.error(f"Error detecting bulk URL: {e}")
        raise


def calculate_sha256(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def download_bulk_companies(force: bool = False) -> Path:
    """Download the latest bulk companies house data."""
    settings = get_settings()
    data_dir = settings.bulk_data_dir / "companies_house"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Check if we already have this file
    local_zip = data_dir / "BasicCompanyDataAsOneFile.zip"
    if local_zip.exists() and not force:
        logger.info(f"Using existing bulk data at {local_zip}")
        return local_zip

    url = get_latest_bulk_url()
    logger.info(f"Downloading bulk companies data from {url}")

    try:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        with open(local_zip, "wb") as f:
            with tqdm(total=total_size, unit="B", unit_scale=True) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

        logger.info(f"Downloaded to {local_zip}")
        checksum = calculate_sha256(local_zip)
        logger.info(f"SHA256: {checksum}")

        return local_zip
    except Exception as e:
        logger.error(f"Failed to download bulk data: {e}")
        if local_zip.exists():
            local_zip.unlink()
        raise


def extract_bulk_companies(zip_path: Path) -> Path:
    """Extract bulk companies CSV from zip file."""
    extract_dir = zip_path.parent / "extracted"
    extract_dir.mkdir(exist_ok=True)

    with ZipFile(zip_path, "r") as z:
        logger.info(f"Extracting {zip_path}")
        z.extractall(extract_dir)

    # Find the CSV file
    csv_files = list(extract_dir.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV file found in {zip_path}")

    return csv_files[0]


def ingest_bulk_companies(csv_path: Optional[Path] = None, download: bool = True) -> IngestRun:
    """Ingest bulk companies data into database."""

    if csv_path is None:
        if download:
            zip_path = download_bulk_companies()
            csv_path = extract_bulk_companies(zip_path)
        else:
            raise ValueError("Must provide csv_path or set download=True")

    logger.info(f"Starting bulk ingestion from {csv_path}")

    start_time = datetime.utcnow()
    rows_processed = 0
    rows_inserted = 0
    rows_updated = 0
    file_hash = calculate_sha256(csv_path)

    try:
        with get_session() as session:
            # Read CSV in chunks
            chunk_size = 100000
            chunks_read = 0

            for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype={"SICCode.SicText_1": str}):
                chunks_read += 1
                logger.info(f"Processing chunk {chunks_read} ({chunk_size} rows)")

                for idx, row in chunk.iterrows():
                    try:
                        # Parse SIC codes (comma separated in raw data)
                        sic_codes = None
                        if pd.notna(row.get("SICCode.SicText_1")):
                            sic_text = str(row.get("SICCode.SicText_1", "")).strip()
                            if sic_text:
                                sic_codes = [s.strip() for s in sic_text.split(",") if s.strip()]

                        company_data = {
                            "company_number": str(row.get("CompanyNumber", "")).strip(),
                            "company_name": str(row.get("CompanyName", "")).strip(),
                            "company_status": str(row.get("CompanyStatus", "")).strip(),
                            "company_type": str(row.get("CompanyType", "")).strip(),
                            "jurisdiction": str(row.get("CompanyJurisdiction", "GB")).strip(),
                            "incorporation_date": pd.to_datetime(row.get("IncorporationDate"), errors="coerce"),
                            "dissolution_date": pd.to_datetime(row.get("DissolutionDate"), errors="coerce"),
                            "registered_address": str(row.get("RegAddress.PostalCode", "")).strip(),
                            "postal_code": str(row.get("RegAddress.PostalCode", "")).strip(),
                            "sic_codes": sic_codes,
                            "accounts_next_due": pd.to_datetime(row.get("Accounts.NextDueDate"), errors="coerce"),
                            "accounts_last_made_up_to": pd.to_datetime(
                                row.get("Accounts.LastMadeUpDate"), errors="coerce"
                            ),
                            "confirmation_statement_next_due": pd.to_datetime(
                                row.get("ConfStmt.NextDueDate"), errors="coerce"
                            ),
                            "confirmation_statement_last_made_up_to": pd.to_datetime(
                                row.get("ConfStmt.LastMadeUpDate"), errors="coerce"
                            ),
                            "source": "bulk",
                            "source_file": csv_path.name,
                        }

                        # Skip if no company number
                        if not company_data["company_number"]:
                            continue

                        # Upsert
                        existing = session.query(Company).filter_by(company_number=company_data["company_number"]).first()
                        if existing:
                            for key, value in company_data.items():
                                if key not in ["source_file"]:
                                    setattr(existing, key, value)
                            rows_updated += 1
                        else:
                            company = Company(**company_data)
                            session.add(company)
                            rows_inserted += 1

                        rows_processed += 1

                        if rows_processed % 10000 == 0:
                            logger.info(f"Processed {rows_processed} rows (inserted: {rows_inserted}, updated: {rows_updated})")

                    except Exception as e:
                        logger.error(f"Error processing row {idx}: {e}")
                        continue

                # Commit after each chunk
                session.commit()

            # Record the ingest run
            ingest_run = IngestRun(
                ingest_type="bulk_companies",
                file_name=csv_path.name,
                file_hash=file_hash,
                rows_processed=rows_processed,
                rows_inserted=rows_inserted,
                rows_updated=rows_updated,
                status="success",
                completed_at=datetime.utcnow(),
            )
            session.add(ingest_run)
            session.commit()

            logger.info(
                f"Bulk ingestion complete: {rows_processed} processed, {rows_inserted} inserted, {rows_updated} updated"
            )
            return ingest_run

    except Exception as e:
        logger.error(f"Bulk ingestion failed: {e}")
        ingest_run = IngestRun(
            ingest_type="bulk_companies",
            file_name=csv_path.name,
            file_hash=file_hash,
            rows_processed=rows_processed,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            status="failed",
            error_message=str(e),
            completed_at=datetime.utcnow(),
        )
        with get_session() as session:
            session.add(ingest_run)
            session.commit()
        raise


if __name__ == "__main__":
    ingest_bulk_companies()
