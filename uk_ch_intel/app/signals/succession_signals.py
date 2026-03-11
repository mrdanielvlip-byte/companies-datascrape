"""
Succession and management risk signals.

Computes signals related to leadership continuity, management bench
depth, and succession planning (or lack thereof).

Signals:
  - dormant_succession: No younger officers appointed in recent years
  - management_bench_depth: Number of active officers beyond founder/sole director
  - generation_gap: Age gap between oldest and youngest directors
  - no_recent_officer_changes: No officer filings in N years
  - concentrated_psc: PSC control concentrated in 1-2 individuals
  - filing_stress: Late or irregular filing behavior

Each signal produces:
  - signal_type: str identifier
  - signal_score: 0-100
  - signal_value: str (human-readable summary)
  - explanation: str (detailed formula/reasoning)
"""
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

from loguru import logger
from sqlalchemy import text, func

from ..db import get_session
from ..models.schema import (
    Company,
    Appointment,
    OfficerResolved,
    Filing,
    PSC,
    CompanySignal,
)


# ── Configuration ──────────────────────────────────────────────────────

# Thresholds
DORMANT_SUCCESSION_YEARS = 5  # No new officers in this many years
AGING_THRESHOLD_YEAR_OFFSET = 55  # Birth year <= current_year - 55
LONG_TENURE_YEARS = 15
FILING_STALENESS_DAYS = 400  # Days since last filing = concerning
MIN_BENCH_FOR_HEALTHY = 3  # At least 3 active officers for healthy bench


def compute_dormant_succession(company_number: str) -> Optional[dict]:
    """
    Detect companies with no new director appointments in recent years.

    High score = high succession risk (no fresh talent).
    """
    with get_session() as session:
        # Find most recent appointment date
        latest = (
            session.query(func.max(Appointment.appointed_on))
            .filter(
                Appointment.company_number == company_number,
                Appointment.is_current == True,
            )
            .scalar()
        )

        if not latest:
            return None

        years_since = (date.today() - latest).days / 365.25

        if years_since < DORMANT_SUCCESSION_YEARS:
            score = max(0, int(years_since / DORMANT_SUCCESSION_YEARS * 40))
        else:
            # Scale from 60-90 based on staleness
            score = min(90, int(60 + (years_since - DORMANT_SUCCESSION_YEARS) * 5))

        return {
            "company_number": company_number,
            "signal_type": "dormant_succession",
            "signal_score": Decimal(str(score)),
            "signal_value": f"{years_since:.1f} years since last appointment",
            "explanation": (
                f"Most recent director appointment was {latest}. "
                f"{years_since:.1f} years ago. "
                f"Threshold: {DORMANT_SUCCESSION_YEARS} years. "
                f"Score: {score}/100"
            ),
            "computed_at": datetime.utcnow(),
        }


def compute_management_bench(company_number: str) -> Optional[dict]:
    """
    Measure management bench depth.

    Score logic:
    - 1 director: score 80 (high risk)
    - 2 directors: score 55 (moderate risk)
    - 3+ directors: score 20 (healthy)
    """
    with get_session() as session:
        active_count = (
            session.query(func.count(Appointment.appointment_id))
            .filter(
                Appointment.company_number == company_number,
                Appointment.is_current == True,
            )
            .scalar()
        ) or 0

        if active_count == 0:
            return None

        if active_count == 1:
            score = 80
            label = "Single director — critical dependency"
        elif active_count == 2:
            score = 55
            label = "Two directors — limited bench"
        elif active_count < MIN_BENCH_FOR_HEALTHY:
            score = 35
            label = f"{active_count} directors — thin bench"
        else:
            score = max(5, 25 - (active_count - MIN_BENCH_FOR_HEALTHY) * 5)
            label = f"{active_count} directors — healthy bench"

        return {
            "company_number": company_number,
            "signal_type": "management_bench_depth",
            "signal_score": Decimal(str(score)),
            "signal_value": label,
            "explanation": (
                f"Active directors: {active_count}. "
                f"Healthy threshold: {MIN_BENCH_FOR_HEALTHY}+. "
                f"Score: {score}/100 (higher = riskier)"
            ),
            "computed_at": datetime.utcnow(),
        }


def compute_generation_gap(company_number: str) -> Optional[dict]:
    """
    Compute the age gap between oldest and youngest active directors.

    A large gap with few young directors suggests the young ones are
    recently appointed successors. No gap or no young directors is riskier.
    """
    current_year = date.today().year

    with get_session() as session:
        officers = (
            session.query(OfficerResolved.birth_year)
            .join(Appointment, Appointment.officer_id == OfficerResolved.officer_id)
            .filter(
                Appointment.company_number == company_number,
                Appointment.is_current == True,
                OfficerResolved.birth_year.isnot(None),
            )
            .all()
        )

        if not officers or len(officers) < 2:
            return None

        birth_years = [o[0] for o in officers if o[0]]
        if len(birth_years) < 2:
            return None

        oldest_age = current_year - min(birth_years)
        youngest_age = current_year - max(birth_years)
        gap = oldest_age - youngest_age

        # Scoring: larger gap is actually better for succession
        # No gap = same generation = higher risk
        if gap == 0:
            score = 70  # Same generation, no natural succession
        elif gap < 10:
            score = 55  # Small gap
        elif gap < 20:
            score = 30  # Healthy generational spread
        else:
            score = 15  # Clear generational diversity

        return {
            "company_number": company_number,
            "signal_type": "generation_gap",
            "signal_score": Decimal(str(score)),
            "signal_value": (
                f"Age range: ~{youngest_age}-{oldest_age} years "
                f"(gap: {gap} years)"
            ),
            "explanation": (
                f"Oldest director birth year: {min(birth_years)} (~{oldest_age}y). "
                f"Youngest: {max(birth_years)} (~{youngest_age}y). "
                f"Gap: {gap} years. "
                f"Larger gaps suggest generational succession planning. "
                f"Score: {score}/100 (higher = riskier)"
            ),
            "computed_at": datetime.utcnow(),
        }


def compute_filing_stress(company_number: str) -> Optional[dict]:
    """
    Detect irregular or stale filing behavior.

    Looks at:
    - Days since last filing
    - Whether accounts are overdue
    - Whether confirmation statement is overdue
    """
    with get_session() as session:
        company = (
            session.query(Company)
            .filter(Company.company_number == company_number)
            .first()
        )
        if not company:
            return None

        now = date.today()
        score = 0
        reasons = []

        # Check accounts overdue
        if company.accounts_next_due:
            if company.accounts_next_due < now:
                overdue_days = (now - company.accounts_next_due).days
                score += min(40, 20 + overdue_days // 30)
                reasons.append(f"Accounts overdue by {overdue_days} days")

        # Check confirmation statement overdue
        if company.confirmation_statement_next_due:
            if company.confirmation_statement_next_due < now:
                overdue_days = (now - company.confirmation_statement_next_due).days
                score += min(30, 15 + overdue_days // 30)
                reasons.append(f"Confirmation statement overdue by {overdue_days} days")

        # Check last filing date
        last_filing = (
            session.query(func.max(Filing.filing_date))
            .filter(Filing.company_number == company_number)
            .scalar()
        )

        if last_filing:
            days_since = (now - last_filing).days
            if days_since > FILING_STALENESS_DAYS:
                score += min(30, int((days_since - FILING_STALENESS_DAYS) / 30) * 5)
                reasons.append(f"No filings for {days_since} days")

        score = min(100, score)

        if not reasons:
            reasons.append("Filing behavior appears normal")

        return {
            "company_number": company_number,
            "signal_type": "filing_stress",
            "signal_score": Decimal(str(score)),
            "signal_value": "; ".join(reasons),
            "explanation": (
                f"Accounts next due: {company.accounts_next_due}. "
                f"Confirmation next due: {company.confirmation_statement_next_due}. "
                f"Last filing: {last_filing}. "
                f"Score: {score}/100"
            ),
            "computed_at": datetime.utcnow(),
        }


def compute_concentrated_psc(company_number: str) -> Optional[dict]:
    """
    Detect companies where PSC control is concentrated in 1-2 individuals.
    """
    with get_session() as session:
        active_pscs = (
            session.query(PSC)
            .filter(
                PSC.company_number == company_number,
                PSC.ceased_on.is_(None),
                PSC.psc_kind.in_(["individual", "individual-bo"]),
            )
            .all()
        )

        if not active_pscs:
            return None

        count = len(active_pscs)

        if count == 1:
            score = 75
            label = "Single individual PSC — full control concentration"
        elif count == 2:
            score = 45
            label = "Two individual PSCs"
        else:
            score = max(5, 30 - (count - 2) * 8)
            label = f"{count} individual PSCs — distributed control"

        return {
            "company_number": company_number,
            "signal_type": "concentrated_psc",
            "signal_score": Decimal(str(score)),
            "signal_value": label,
            "explanation": (
                f"Active individual PSCs: {count}. "
                f"Concentrated ownership increases acquisition likelihood. "
                f"Score: {score}/100"
            ),
            "computed_at": datetime.utcnow(),
        }


# ── Batch orchestration ────────────────────────────────────────────────


ALL_SUCCESSION_SIGNALS = [
    compute_dormant_succession,
    compute_management_bench,
    compute_generation_gap,
    compute_filing_stress,
    compute_concentrated_psc,
]


def compute_all_succession_signals(company_number: str) -> list[dict]:
    """
    Compute all succession signals for a company.

    Returns:
        List of signal dicts ready for insert into company_signals
    """
    signals = []
    for compute_fn in ALL_SUCCESSION_SIGNALS:
        try:
            result = compute_fn(company_number)
            if result:
                signals.append(result)
        except Exception as e:
            logger.error(
                f"Error computing {compute_fn.__name__} for {company_number}: {e}"
            )
    return signals


def store_succession_signals(company_number: str) -> int:
    """
    Compute and store all succession signals for a company.

    Replaces any existing succession signals for this company.

    Returns:
        Number of signals stored
    """
    signals = compute_all_succession_signals(company_number)
    if not signals:
        return 0

    signal_types = [s["signal_type"] for s in signals]

    with get_session() as session:
        # Delete existing signals of these types for this company
        session.query(CompanySignal).filter(
            CompanySignal.company_number == company_number,
            CompanySignal.signal_type.in_(signal_types),
        ).delete(synchronize_session="fetch")

        # Insert new signals
        for sig in signals:
            session.add(CompanySignal(**sig))

        session.commit()

    logger.info(
        f"Stored {len(signals)} succession signals for {company_number}"
    )
    return len(signals)
