from datetime import datetime
from decimal import Decimal
from typing import Set, Tuple

from loguru import logger
from sqlalchemy import func

from app.db import get_session
from app.models import (
    Company,
    Appointment,
    OfficerResolved,
    PSC,
    CompanyEdge,
)


def build_officer_to_company_edges() -> int:
    """Build edges between officers and companies via appointments."""
    with get_session() as session:
        # Clear existing officer->company edges
        session.query(CompanyEdge).filter_by(from_type="officer", to_type="company", edge_type="appoints").delete()

        # Get all unique officer->company relationships
        appointments = (
            session.query(
                Appointment.officer_id,
                Appointment.company_number,
                func.count(Appointment.appointment_id).label("count"),
                func.max(Appointment.appointed_on).label("latest_appointment"),
            )
            .filter(Appointment.officer_id.isnot(None), Appointment.is_current == True)
            .group_by(Appointment.officer_id, Appointment.company_number)
            .all()
        )

        edge_count = 0
        for officer_id, company_number, count, latest_appointment in appointments:
            edge = CompanyEdge(
                from_type="officer",
                from_id=str(officer_id),
                to_type="company",
                to_id=company_number,
                edge_type="appoints",
                weight=Decimal(min(count, 5)),  # Cap weight at 5
                metadata={
                    "appointment_count": count,
                    "latest_appointment": latest_appointment.isoformat() if latest_appointment else None,
                },
            )
            session.add(edge)
            edge_count += 1

        session.commit()
        logger.info(f"Created {edge_count} officer->company edges")
        return edge_count


def build_psc_to_company_edges() -> int:
    """Build edges between PSCs and companies via ownership."""
    with get_session() as session:
        # Clear existing psc->company edges
        session.query(CompanyEdge).filter_by(from_type="psc", to_type="company", edge_type="owns").delete()

        # Get all PSCs
        pscs = session.query(PSC).filter(PSC.ceased_on.is_(None)).all()

        edge_count = 0
        for psc in pscs:
            control_score = Decimal("1.00")

            # Adjust weight based on control nature
            if psc.control_natures:
                if any("voting" in str(nature).lower() for nature in psc.control_natures):
                    control_score = Decimal("2.00")
                elif any("ownership" in str(nature).lower() for nature in psc.control_natures):
                    control_score = Decimal("1.50")

            edge = CompanyEdge(
                from_type="psc",
                from_id=psc.psc_name,
                to_type="company",
                to_id=psc.company_number,
                edge_type="owns",
                weight=control_score,
                metadata={
                    "control_natures": psc.control_natures,
                    "notified_on": psc.notified_on.isoformat() if psc.notified_on else None,
                    "psc_kind": psc.psc_kind,
                },
            )
            session.add(edge)
            edge_count += 1

        session.commit()
        logger.info(f"Created {edge_count} psc->company edges")
        return edge_count


def build_officer_to_officer_edges() -> int:
    """Build edges between officers who serve together."""
    with get_session() as session:
        # Clear existing officer->officer edges
        session.query(CompanyEdge).filter_by(from_type="officer", to_type="officer", edge_type="co_serves").delete()

        # Get all active appointments grouped by company
        company_officers = (
            session.query(Appointment.company_number, Appointment.officer_id, Appointment.appointed_on)
            .filter(Appointment.is_current == True, Appointment.officer_id.isnot(None))
            .all()
        )

        # Build a map of company -> list of officers
        company_map = {}
        for company_number, officer_id, appointed_on in company_officers:
            if company_number not in company_map:
                company_map[company_number] = []
            company_map[company_number].append((officer_id, appointed_on))

        # Create edges between co-serving officers
        edge_count = 0
        created_edges = set()

        for company_number, officers in company_map.items():
            # For each pair of officers in the same company
            for i, (officer_id_1, appointed_1) in enumerate(officers):
                for officer_id_2, appointed_2 in officers[i + 1 :]:
                    # Create edge from lower ID to higher ID to avoid duplicates
                    from_id = min(officer_id_1, officer_id_2)
                    to_id = max(officer_id_1, officer_id_2)

                    edge_key = (from_id, to_id)
                    if edge_key in created_edges:
                        continue

                    created_edges.add(edge_key)

                    # Calculate overlap duration
                    earliest_start = max(appointed_1, appointed_2)
                    latest_appointed = min(appointed_1, appointed_2)

                    edge = CompanyEdge(
                        from_type="officer",
                        from_id=str(from_id),
                        to_type="officer",
                        to_id=str(to_id),
                        edge_type="co_serves",
                        weight=Decimal("1.00"),
                        metadata={
                            "company_number": company_number,
                            "overlap_start": earliest_start.isoformat() if earliest_start else None,
                            "latest_appointment": latest_appointed.isoformat() if latest_appointed else None,
                        },
                    )
                    session.add(edge)
                    edge_count += 1

        session.commit()
        logger.info(f"Created {edge_count} officer->officer edges")
        return edge_count


def build_company_to_company_edges() -> int:
    """Build edges between companies sharing officers."""
    with get_session() as session:
        # Clear existing company->company edges
        session.query(CompanyEdge).filter_by(from_type="company", to_type="company", edge_type="shared_officer").delete()

        # Get all officers with their companies
        officer_companies = (
            session.query(Appointment.officer_id, Appointment.company_number)
            .filter(Appointment.is_current == True, Appointment.officer_id.isnot(None))
            .group_by(Appointment.officer_id, Appointment.company_number)
            .all()
        )

        # Build a map of officer -> list of companies
        officer_map = {}
        for officer_id, company_number in officer_companies:
            if officer_id not in officer_map:
                officer_map[officer_id] = []
            officer_map[officer_id].append(company_number)

        # Create edges between companies with shared officers
        edge_count = 0
        created_edges = set()

        for officer_id, companies in officer_map.items():
            if len(companies) < 2:
                continue

            # For each pair of companies
            for i, company_1 in enumerate(companies):
                for company_2 in companies[i + 1 :]:
                    # Create edge from company_1 to company_2 (alphabetically)
                    from_id = min(company_1, company_2)
                    to_id = max(company_1, company_2)

                    edge_key = (from_id, to_id)
                    if edge_key in created_edges:
                        # Increase weight if already exists
                        existing = (
                            session.query(CompanyEdge)
                            .filter_by(
                                from_type="company",
                                from_id=from_id,
                                to_type="company",
                                to_id=to_id,
                                edge_type="shared_officer",
                            )
                            .first()
                        )
                        if existing:
                            existing.weight += Decimal("1.00")
                        continue

                    created_edges.add(edge_key)

                    edge = CompanyEdge(
                        from_type="company",
                        from_id=from_id,
                        to_type="company",
                        to_id=to_id,
                        edge_type="shared_officer",
                        weight=Decimal("1.00"),
                        metadata={"officer_id": officer_id},
                    )
                    session.add(edge)
                    edge_count += 1

        session.commit()
        logger.info(f"Created {edge_count} company->company edges")
        return edge_count


def build_all_edges() -> dict:
    """Build all company graph edges."""
    logger.info("Starting edge generation")

    results = {
        "officer_to_company": build_officer_to_company_edges(),
        "psc_to_company": build_psc_to_company_edges(),
        "officer_to_officer": build_officer_to_officer_edges(),
        "company_to_company": build_company_to_company_edges(),
    }

    total_edges = sum(results.values())
    logger.info(f"Edge generation complete: {total_edges} edges created")
    return results


if __name__ == "__main__":
    results = build_all_edges()
    print(f"Results: {results}")
