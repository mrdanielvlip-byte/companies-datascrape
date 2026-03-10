import re
from datetime import datetime
from decimal import Decimal
from typing import Tuple, Optional

from loguru import logger
from sqlalchemy import func

from app.db import get_session
from app.models import OfficerRaw, OfficerResolved, Appointment, Company


# Name normalization patterns
TITLE_PATTERNS = [
    r"\bMr\.?\b",
    r"\bMrs\.?\b",
    r"\bMs\.?\b",
    r"\bDr\.?\b",
    r"\bProf\.?\b",
    r"\bSir\b",
    r"\bLady\b",
    r"\bDame\b",
]


def normalize_name(name: str) -> str:
    """Normalize officer name by removing titles and standardizing."""
    if not name:
        return ""

    # Remove titles
    normalized = name.strip()
    for pattern in TITLE_PATTERNS:
        normalized = re.sub(pattern, "", normalized, flags=re.IGNORECASE)

    # Remove extra whitespace
    normalized = " ".join(normalized.split())

    # Lowercase for matching
    normalized = normalized.lower()

    return normalized


def calculate_resolution_confidence(
    exact_name_match: bool, birth_month_match: bool, birth_year_match: bool
) -> Decimal:
    """Calculate confidence score for officer resolution.

    Scoring:
    - exact name + birth month + birth year: 0.97
    - exact name + birth year only: 0.85
    - exact name only: 0.70
    """
    if exact_name_match and birth_month_match and birth_year_match:
        return Decimal("0.97")
    elif exact_name_match and birth_year_match:
        return Decimal("0.85")
    elif exact_name_match:
        return Decimal("0.70")
    else:
        return Decimal("0.00")


def resolve_officer(
    name: str, birth_month: Optional[int], birth_year: Optional[int], occupation: Optional[str] = None
) -> Tuple[OfficerResolved, Decimal]:
    """Find or create resolved officer record."""

    normalized = normalize_name(name)

    with get_session() as session:
        # Try exact match first
        exact_match = (
            session.query(OfficerResolved)
            .filter_by(normalized_name=normalized, birth_month=birth_month, birth_year=birth_year)
            .first()
        )

        if exact_match:
            confidence = calculate_resolution_confidence(True, birth_month is not None, birth_year is not None)
            return exact_match, confidence

        # Try name + year match
        if birth_year:
            year_match = (
                session.query(OfficerResolved).filter_by(normalized_name=normalized, birth_year=birth_year).first()
            )
            if year_match:
                confidence = calculate_resolution_confidence(True, False, True)
                return year_match, confidence

        # Try name only match
        name_match = session.query(OfficerResolved).filter_by(normalized_name=normalized).first()
        if name_match:
            confidence = calculate_resolution_confidence(True, False, False)
            return name_match, confidence

        # Create new officer record
        confidence = calculate_resolution_confidence(False, False, False)
        new_officer = OfficerResolved(
            normalized_name=normalized,
            display_name=name,
            birth_month=birth_month,
            birth_year=birth_year,
            occupation=occupation,
            resolution_confidence=Decimal("0.50"),  # Default for new records
        )

        session.add(new_officer)
        session.commit()

        return new_officer, confidence


def normalize_officers_for_company(company_number: str) -> dict:
    """Normalize and resolve officers for a company."""
    with get_session() as session:
        # Get all raw officer data for this company
        raw_officers = session.query(OfficerRaw).filter_by(company_number=company_number).all()

        if not raw_officers:
            logger.warning(f"No raw officer data found for {company_number}")
            return {"processed": 0, "created": 0, "updated": 0}

        processed = 0
        created = 0
        updated = 0

        for raw in raw_officers:
            try:
                payload = raw.source_officer_payload
                if not isinstance(payload, dict):
                    logger.warning(f"Invalid officer payload for {company_number}")
                    continue

                officer_name = payload.get("name", "")
                if not officer_name:
                    continue

                # Extract birth information
                birth_date = payload.get("date_of_birth", {})
                birth_month = None
                birth_year = None

                if isinstance(birth_date, dict):
                    birth_month = birth_date.get("month")
                    birth_year = birth_date.get("year")

                # Resolve officer
                officer, confidence = resolve_officer(
                    officer_name,
                    birth_month,
                    birth_year,
                    payload.get("occupation"),
                )

                # Parse appointment dates
                appointed_on = payload.get("appointed_on")
                if appointed_on:
                    appointed_on = datetime.strptime(appointed_on, "%Y-%m-%d").date()

                resigned_on = payload.get("resigned_on")
                if resigned_on:
                    resigned_on = datetime.strptime(resigned_on, "%Y-%m-%d").date()

                is_current = resigned_on is None

                # Create or update appointment
                existing_appointment = (
                    session.query(Appointment)
                    .filter_by(
                        company_number=company_number,
                        officer_id=officer.officer_id,
                        role=payload.get("officer_role", ""),
                        appointed_on=appointed_on,
                    )
                    .first()
                )

                if existing_appointment:
                    existing_appointment.resigned_on = resigned_on
                    existing_appointment.is_current = is_current
                    existing_appointment.resolution_confidence = confidence
                    updated += 1
                else:
                    appointment = Appointment(
                        company_number=company_number,
                        officer_id=officer.officer_id,
                        officer_name_on_filing=officer_name,
                        role=payload.get("officer_role", ""),
                        appointed_on=appointed_on,
                        resigned_on=resigned_on,
                        is_current=is_current,
                        source="api",
                    )
                    session.add(appointment)
                    created += 1

                processed += 1

            except Exception as e:
                logger.error(f"Error normalizing officer for {company_number}: {e}")
                continue

        session.commit()
        logger.info(f"Normalized officers for {company_number}: {processed} processed, {created} created, {updated} updated")

        return {"processed": processed, "created": created, "updated": updated}


def normalize_all_officers(batch_size: int = 1000) -> dict:
    """Normalize all raw officers in batches."""
    with get_session() as session:
        # Get unique companies with raw officers
        companies = session.query(func.distinct(OfficerRaw.company_number)).all()
        company_numbers = [c[0] for c in companies]

        logger.info(f"Normalizing officers for {len(company_numbers)} companies")

        total_results = {"processed": 0, "created": 0, "updated": 0}

        for i, company_number in enumerate(company_numbers):
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{len(company_numbers)}")

            result = normalize_officers_for_company(company_number)
            total_results["processed"] += result["processed"]
            total_results["created"] += result["created"]
            total_results["updated"] += result["updated"]

        logger.info(f"Normalization complete: {total_results}")
        return total_results


if __name__ == "__main__":
    # Example usage
    results = normalize_all_officers()
    print(f"Results: {results}")
