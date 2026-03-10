from datetime import datetime
from typing import Optional

from loguru import logger

from app.db import get_session
from app.models import Filing, EnrichmentJob
from app.ingestion.api_officers import CompaniesHouseAPIClient


def fetch_filings_for_company(company_number: str) -> bool:
    """Fetch and store filings for a company."""
    from app.config import get_settings

    settings = get_settings()
    client = CompaniesHouseAPIClient(settings.ch_api_key, settings.api_rate_limit_per_minute)

    with get_session() as session:
        try:
            # Update job status
            job = (
                session.query(EnrichmentJob)
                .filter_by(company_number=company_number, job_type="filings_fetch", status="pending")
                .first()
            )
            if job:
                job.status = "in_progress"
                job.started_at = datetime.utcnow()
                session.commit()

            # Fetch filings
            filings = client.get_filing_history(company_number)

            # Store filings
            for filing in filings:
                filing_data = {
                    "company_number": company_number,
                    "filing_date": _parse_date(filing.get("date")),
                    "category": filing.get("category", ""),
                    "type": filing.get("type", ""),
                    "description": filing.get("description"),
                    "description_values": filing.get("description_values"),
                    "transaction_id": filing.get("transaction_id"),
                    "source": "api",
                    "fetched_at": datetime.utcnow(),
                }

                # Check if filing already exists
                existing = (
                    session.query(Filing)
                    .filter_by(company_number=company_number, transaction_id=filing_data["transaction_id"])
                    .first()
                )
                if not existing:
                    filing_obj = Filing(**filing_data)
                    session.add(filing_obj)

            session.commit()

            # Update job status
            if job:
                job.status = "completed"
                job.finished_at = datetime.utcnow()
                session.commit()

            logger.info(f"Stored {len(filings)} filings for company {company_number}")
            return True

        except Exception as e:
            logger.error(f"Error fetching filings for {company_number}: {e}")

            # Update job with error
            if job:
                job.status = "failed"
                job.last_error = str(e)
                job.attempt_count += 1
                job.finished_at = datetime.utcnow()
                session.commit()

            return False


def fetch_filings_batch(company_numbers: list[str]) -> dict:
    """Fetch filings for multiple companies."""
    results = {"succeeded": 0, "failed": 0, "skipped": 0}

    for company_number in company_numbers:
        try:
            if fetch_filings_for_company(company_number):
                results["succeeded"] += 1
            else:
                results["failed"] += 1
        except Exception as e:
            logger.error(f"Batch error for {company_number}: {e}")
            results["failed"] += 1

    logger.info(f"Batch filings fetch results: {results}")
    return results


def _parse_date(date_str):
    """Parse date string from Companies House format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(str(date_str), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    fetch_filings_for_company("00000191")
