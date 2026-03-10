import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.request import urlopen
from zipfile import ZipFile

import requests
from loguru import logger
from tqdm import tqdm

from app.db import get_session
from app.models import PSC, IngestRun
from app.config import get_settings


def get_latest_psc_url() -> str:
    """Get the latest PSC (People with Significant Control) bulk data URL."""
    # Companies House PSC data is available at a standard URL
    psc_url = "https://download.companieshouse.gov.uk/en_pscdata.html"

    # Try direct download link (common pattern)
    try:
        # The actual download URL for PSC data
        direct_url = "https://download.companieshouse.gov.uk/persons-with-significant-control.zip"

        response = requests.head(direct_url, timeout=10)
        if response.status_code == 200:
            logger.info(f"Found PSC data at {direct_url}")
            return direct_url

        raise ValueError("Could not find PSC bulk data URL")
    except Exception as e:
        logger.error(f"Error detecting PSC URL: {e}")
        raise


def calculate_sha256(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def download_bulk_psc(force: bool = False) -> Path:
    """Download the latest bulk PSC data."""
    settings = get_settings()
    data_dir = settings.bulk_data_dir / "companies_house"
    data_dir.mkdir(parents=True, exist_ok=True)

    local_zip = data_dir / "persons-with-significant-control.zip"
    if local_zip.exists() and not force:
        logger.info(f"Using existing PSC data at {local_zip}")
        return local_zip

    url = get_latest_psc_url()
    logger.info(f"Downloading bulk PSC data from {url}")

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
        logger.error(f"Failed to download PSC data: {e}")
        if local_zip.exists():
            local_zip.unlink()
        raise


def extract_bulk_psc(zip_path: Path) -> Path:
    """Extract bulk PSC JSONL from zip file."""
    extract_dir = zip_path.parent / "psc_extracted"
    extract_dir.mkdir(exist_ok=True)

    with ZipFile(zip_path, "r") as z:
        logger.info(f"Extracting {zip_path}")
        z.extractall(extract_dir)

    # Find the JSONL or JSON file
    jsonl_files = list(extract_dir.glob("*.jsonl"))
    if jsonl_files:
        return jsonl_files[0]

    json_files = list(extract_dir.glob("*.json"))
    if json_files:
        return json_files[0]

    raise ValueError(f"No JSONL or JSON file found in {zip_path}")


def ingest_bulk_psc(jsonl_path: Optional[Path] = None, download: bool = True) -> IngestRun:
    """Ingest bulk PSC data into database."""

    if jsonl_path is None:
        if download:
            zip_path = download_bulk_psc()
            jsonl_path = extract_bulk_psc(zip_path)
        else:
            raise ValueError("Must provide jsonl_path or set download=True")

    logger.info(f"Starting bulk PSC ingestion from {jsonl_path}")

    start_time = datetime.utcnow()
    rows_processed = 0
    rows_inserted = 0
    rows_updated = 0
    file_hash = calculate_sha256(jsonl_path)

    try:
        with get_session() as session:
            with open(jsonl_path, "r") as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        if not line.strip():
                            continue

                        data = json.loads(line)

                        # Parse control natures array
                        control_natures = None
                        if "control_natures" in data and data["control_natures"]:
                            control_natures = data["control_natures"]
                        elif "controlNatures" in data and data["controlNatures"]:
                            control_natures = data["controlNatures"]

                        psc_data = {
                            "company_number": str(data.get("company_number", data.get("companyNumber", ""))).strip(),
                            "psc_name": str(data.get("name", "")).strip(),
                            "psc_kind": str(data.get("kind", "")).strip(),
                            "birth_month": data.get("birth_month") or data.get("birthMonth"),
                            "birth_year": data.get("birth_year") or data.get("birthYear"),
                            "notified_on": _parse_date(data.get("notified_on") or data.get("notifiedOn")),
                            "ceased_on": _parse_date(data.get("ceased_on") or data.get("ceasedOn")),
                            "control_natures": control_natures,
                            "source": "bulk",
                            "source_file": jsonl_path.name,
                        }

                        # Skip if no company number
                        if not psc_data["company_number"]:
                            continue

                        # Check for existing PSC (by company + name + role)
                        existing = (
                            session.query(PSC)
                            .filter_by(company_number=psc_data["company_number"], psc_name=psc_data["psc_name"])
                            .first()
                        )
                        if existing:
                            for key, value in psc_data.items():
                                setattr(existing, key, value)
                            rows_updated += 1
                        else:
                            psc = PSC(**psc_data)
                            session.add(psc)
                            rows_inserted += 1

                        rows_processed += 1

                        if rows_processed % 10000 == 0:
                            session.commit()
                            logger.info(f"Processed {rows_processed} rows (inserted: {rows_inserted}, updated: {rows_updated})")

                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing JSON at line {line_num}: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing PSC at line {line_num}: {e}")
                        continue

            # Final commit
            session.commit()

            # Record the ingest run
            ingest_run = IngestRun(
                ingest_type="bulk_psc",
                file_name=jsonl_path.name,
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
                f"Bulk PSC ingestion complete: {rows_processed} processed, {rows_inserted} inserted, {rows_updated} updated"
            )
            return ingest_run

    except Exception as e:
        logger.error(f"Bulk PSC ingestion failed: {e}")
        ingest_run = IngestRun(
            ingest_type="bulk_psc",
            file_name=jsonl_path.name if jsonl_path else "unknown",
            file_hash=file_hash if "file_hash" in locals() else "",
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


def _parse_date(date_str):
    """Parse date string from Companies House format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(str(date_str), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    ingest_bulk_psc()
