from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

from loguru import logger
from sqlalchemy import func

from app.db import get_session
from app.models import (
    Company,
    Appointment,
    OfficerResolved,
    CompanySignal,
    PSC,
)


def calculate_estimated_age(birth_year: Optional[int]) -> Optional[int]:
    """Calculate estimated age from birth year."""
    if not birth_year:
        return None
    return datetime.now().year - birth_year


def check_aging_founder(company_number: str) -> Optional[dict]:
    """Check if company has founder-aged director (55+)."""
    with get_session() as session:
        # Get active directors with birth year
        active_directors = (
            session.query(OfficerResolved, Appointment)
            .join(Appointment, Appointment.officer_id == OfficerResolved.officer_id)
            .filter(
                Appointment.company_number == company_number,
                Appointment.is_current == True,
                OfficerResolved.birth_year.isnot(None),
            )
            .all()
        )

        for officer, appointment in active_directors:
            age = calculate_estimated_age(officer.birth_year)
            if age and age >= 55:
                return {
                    "signal_type": "aging_founder",
                    "signal_value": f"{officer.display_name} (age {age})",
                    "signal_score": Decimal("85.00"),
                    "explanation": f"Active director {officer.display_name} estimated age {age}",
                }

        return None


def check_long_tenure(company_number: str, years_threshold: int = 15) -> Optional[dict]:
    """Check if company has director with long tenure."""
    with get_session() as session:
        cutoff_date = datetime.now().date() - timedelta(days=years_threshold * 365)

        long_tenure = (
            session.query(OfficerResolved, Appointment)
            .join(Appointment, Appointment.officer_id == OfficerResolved.officer_id)
            .filter(
                Appointment.company_number == company_number,
                Appointment.is_current == True,
                Appointment.appointed_on <= cutoff_date,
            )
            .first()
        )

        if long_tenure:
            officer, appointment = long_tenure
            tenure_years = (datetime.now().date() - appointment.appointed_on).days / 365
            return {
                "signal_type": "long_tenure",
                "signal_value": f"{officer.display_name} ({tenure_years:.0f} years)",
                "signal_score": Decimal("75.00"),
                "explanation": f"Director {officer.display_name} appointed {tenure_years:.0f} years ago",
            }

        return None


def check_single_director(company_number: str) -> Optional[dict]:
    """Check if company has only one active director."""
    with get_session() as session:
        director_count = (
            session.query(func.count(Appointment.appointment_id))
            .filter(
                Appointment.company_number == company_number,
                Appointment.role.ilike("%director%"),
                Appointment.is_current == True,
            )
            .scalar()
        )

        if director_count == 1:
            return {
                "signal_type": "single_director",
                "signal_value": "1",
                "signal_score": Decimal("70.00"),
                "explanation": "Company has only one active director",
            }

        return None


def check_founder_era(company_number: str, years_ago: int = 15) -> Optional[dict]:
    """Check if company was incorporated in founder era."""
    with get_session() as session:
        company = session.query(Company).filter_by(company_number=company_number).first()

        if company and company.incorporation_date:
            cutoff_date = datetime.now().date() - timedelta(days=years_ago * 365)
            if company.incorporation_date <= cutoff_date:
                years_old = (datetime.now().date() - company.incorporation_date).days / 365
                return {
                    "signal_type": "founder_era",
                    "signal_value": f"{years_old:.0f} years",
                    "signal_score": Decimal("60.00"),
                    "explanation": f"Company incorporated {years_old:.0f} years ago",
                }

        return None


def check_concentrated_leadership(company_number: str, threshold: int = 2) -> Optional[dict]:
    """Check if company has concentrated leadership (<=2 active officers)."""
    with get_session() as session:
        officer_count = (
            session.query(func.count(func.distinct(Appointment.officer_id)))
            .filter(
                Appointment.company_number == company_number,
                Appointment.is_current == True,
            )
            .scalar()
        )

        if officer_count and officer_count <= threshold:
            return {
                "signal_type": "concentrated_leadership",
                "signal_value": str(officer_count),
                "signal_score": Decimal("65.00"),
                "explanation": f"Company has only {officer_count} active officer(s)",
            }

        return None


def check_dormant_succession(company_number: str, years_threshold: int = 5) -> Optional[dict]:
    """Check if company has no recent director appointments."""
    with get_session() as session:
        cutoff_date = datetime.now().date() - timedelta(days=years_threshold * 365)

        recent_appointments = (
            session.query(Appointment)
            .filter(
                Appointment.company_number == company_number,
                Appointment.appointed_on >= cutoff_date,
            )
            .count()
        )

        if recent_appointments == 0:
            return {
                "signal_type": "dormant_succession",
                "signal_value": "no recent appointments",
                "signal_score": Decimal("72.00"),
                "explanation": f"No director appointments in past {years_threshold} years",
            }

        return None


def check_complexity_fatigue(company_number: str, threshold: int = 5) -> Optional[dict]:
    """Check if company's officers are linked to many other companies."""
    with get_session() as session:
        # Get active officers for this company
        active_officers = (
            session.query(Appointment.officer_id)
            .filter(
                Appointment.company_number == company_number,
                Appointment.is_current == True,
            )
            .all()
        )

        officer_ids = [o[0] for o in active_officers]

        if not officer_ids:
            return None

        # Count how many companies each officer is linked to
        for officer_id in officer_ids:
            company_count = (
                session.query(func.count(func.distinct(Appointment.company_number)))
                .filter(
                    Appointment.officer_id == officer_id,
                    Appointment.is_current == True,
                )
                .scalar()
            )

            if company_count and company_count >= threshold:
                officer = session.query(OfficerResolved).filter_by(officer_id=officer_id).first()
                return {
                    "signal_type": "complexity_fatigue",
                    "signal_value": f"{officer.display_name} ({company_count} companies)",
                    "signal_score": Decimal("68.00"),
                    "explanation": f"Officer linked to {company_count} active companies",
                }

        return None


def compute_seller_score(signals: list[dict]) -> Decimal:
    """Compute composite seller score from signals."""
    if not signals:
        return Decimal("0.00")

    # Weights for each signal type
    weights = {
        "aging_founder": Decimal("25.00"),
        "long_tenure": Decimal("20.00"),
        "single_director": Decimal("18.00"),
        "founder_era": Decimal("15.00"),
        "concentrated_leadership": Decimal("12.00"),
        "dormant_succession": Decimal("18.00"),
        "complexity_fatigue": Decimal("10.00"),
    }

    total_weight = Decimal("0.00")
    weighted_sum = Decimal("0.00")

    for signal in signals:
        signal_type = signal.get("signal_type")
        weight = weights.get(signal_type, Decimal("10.00"))
        score = signal.get("signal_score", Decimal("0.00"))

        weighted_sum += (score / Decimal("100.00")) * weight
        total_weight += weight

    if total_weight == 0:
        return Decimal("0.00")

    return (weighted_sum / total_weight) * Decimal("100.00")


def compute_signals_for_company(company_number: str) -> dict:
    """Compute all signals for a company."""
    signals = []

    # Run all signal checks
    aging_founder = check_aging_founder(company_number)
    if aging_founder:
        signals.append(aging_founder)

    long_tenure = check_long_tenure(company_number)
    if long_tenure:
        signals.append(long_tenure)

    single_director = check_single_director(company_number)
    if single_director:
        signals.append(single_director)

    founder_era = check_founder_era(company_number)
    if founder_era:
        signals.append(founder_era)

    concentrated = check_concentrated_leadership(company_number)
    if concentrated:
        signals.append(concentrated)

    dormant = check_dormant_succession(company_number)
    if dormant:
        signals.append(dormant)

    complexity = check_complexity_fatigue(company_number)
    if complexity:
        signals.append(complexity)

    # Calculate seller score
    seller_score = compute_seller_score(signals)

    return {
        "company_number": company_number,
        "signals": signals,
        "seller_score": seller_score,
    }


def store_signals_for_company(company_number: str) -> int:
    """Compute and store all signals for a company."""
    result = compute_signals_for_company(company_number)
    signals = result["signals"]
    seller_score = result["seller_score"]

    with get_session() as session:
        # Clear existing signals for this company
        session.query(CompanySignal).filter_by(company_number=company_number).delete()

        # Store new signals
        for signal in signals:
            signal_obj = CompanySignal(
                company_number=company_number,
                signal_type=signal["signal_type"],
                signal_value=signal.get("signal_value"),
                signal_score=signal.get("signal_score", Decimal("0.00")),
                explanation=signal.get("explanation"),
                computed_at=datetime.utcnow(),
            )
            session.add(signal_obj)

        # Store seller score as a signal
        seller_signal = CompanySignal(
            company_number=company_number,
            signal_type="seller_score",
            signal_value=str(seller_score),
            signal_score=seller_score,
            explanation="Composite seller opportunity score",
            computed_at=datetime.utcnow(),
        )
        session.add(seller_signal)

        session.commit()

    logger.info(f"Stored {len(signals) + 1} signals for {company_number}, seller_score: {seller_score}")
    return len(signals) + 1


def compute_all_signals(batch_size: int = 100) -> dict:
    """Compute signals for all companies with enriched data."""
    with get_session() as session:
        # Get all companies with officers
        companies_with_officers = session.query(func.distinct(Company.company_number)).join(
            Appointment, Appointment.company_number == Company.company_number
        ).all()

        company_numbers = [c[0] for c in companies_with_officers]

        logger.info(f"Computing signals for {len(company_numbers)} companies")

        total_signals = 0
        for i, company_number in enumerate(company_numbers):
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{len(company_numbers)}")

            try:
                count = store_signals_for_company(company_number)
                total_signals += count
            except Exception as e:
                logger.error(f"Error computing signals for {company_number}: {e}")
                continue

        logger.info(f"Signal computation complete: {total_signals} signals stored")
        return {"companies_processed": len(company_numbers), "signals_stored": total_signals}


if __name__ == "__main__":
    result = compute_all_signals()
    print(f"Results: {result}")
