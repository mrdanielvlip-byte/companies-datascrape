from datetime import datetime
from typing import Optional

from loguru import logger

from app.db import get_session
from app.models import Company, EnrichmentJob
from app.ingestion.api_officers import CompaniesHouseAPIClient


def fetch_company_profile(company_number: str) -> bool:
    """Fetch and upsert company profile from API."""
    from app.config import get_settings

    settings = get_settings()
    client = CompaniesHouseAPIClient(settings.ch_api_key, settings.api_rate_limit_per_minute)

    with get_session() as session:
        try:
            # Update job status
            job = (
                session.query(EnrichmentJob)
                .filter_by(company_number=company_number, job_type="company_profile_fetch", status="pending")
                .first()
            )
            if job:
                job.status = "in_progress"
                job.started_at = datetime.utcnow()
                session.commit()

            # Fetch profile
            profile = client.get_company_profile(company_number)

            # Parse company data
            company_data = {
                "company_number": profile.get("company_number", company_number),
                "company_name": profile.get("company_name", ""),
                "company_status": profile.get("company_status", ""),
                "company_type": profile.get("type", ""),
                "jurisdiction": profile.get("jurisdiction", "GB"),
                "incorporation_date": _parse_date(profile.get("date_of_creation")),
                "dissolution_date": _parse_date(profile.get("date_of_cessation")),
                "registered_address": _format_address(profile.get("registered_office_address")),
                "postal_code": profile.get("registered_office_address", {}).get("postal_code"),
                "sic_codes": profile.get("sic_codes"),
                "accounts_next_due": _parse_date(profile.get("accounts", {}).get("next_due_on")),
                "accounts_last_made_up_to": _parse_date(profile.get("accounts", {}).get("last_accounts", {}).get("made_up_to")),
                "confirmation_statement_next_due": _parse_date(
                    profile.get("confirmation_statement", {}).get("next_due_on")
                ),
                "confirmation_statement_last_made_up_to": _parse_date(
                    profile.get("confirmation_statement", {}).get("last_made_up_to")
                ),
                "source": "api",
                "source_file": None,
                "updated_at": datetime.utcnow(),
            }

            # Upsert company
            existing = session.query(Company).filter_by(company_number=company_data["company_number"]).first()
            if existing:
                for key, value in company_data.items():
                    if value is not None:
                        setattr(existing, key, value)
                logger.info(f"Updated company profile for {company_number}")
            else:
                company = Company(**company_data)
                session.add(company)
                logger.info(f"Created company profile for {company_number}")

            session.commit()

            # Update job status
            if job:
                job.status = "completed"
                job.finished_at = datetime.utcnow()
                session.commit()

            return True

        except Exception as e:
            logger.error(f"Error fetching profile for {company_number}: {e}")

            # Update job with error
            if job:
                job.status = "failed"
                job.last_error = str(e)
                job.attempt_count += 1
                job.finished_at = datetime.utcnow()
                session.commit()

            return False


def fetch_company_profiles_batch(company_numbers: list[str]) -> dict:
    """Fetch profiles for multiple companies."""
    results = {"succeeded": 0, "failed": 0, "skipped": 0}

    for company_number in company_numbers:
        try:
            if fetch_company_profile(company_number):
                results["succeeded"] += 1
            else:
                results["failed"] += 1
        except Exception as e:
            logger.error(f"Batch error for {company_number}: {e}")
            results["failed"] += 1

    logger.info(f"Batch profile fetch results: {results}")
    return results


def _parse_date(date_str):
    """Parse date string from Companies House format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(str(date_str), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _format_address(address: Optional[dict]) -> Optional[str]:
    """Format address dict to string."""
    if not address:
        return None

    parts = []
    for key in ["premises", "address_line_1", "address_line_2", "locality", "region", "postal_code"]:
        if key in address and address[key]:
            parts.append(str(address[key]))

    return ", ".join(parts) if parts else None


if __name__ == "__main__":
    fetch_company_profile("00000191")
