"""
Normalize raw filing history API responses into the filings table.

Handles:
- Parsing paginated filing history from CH API
- Extracting filing metadata (category, type, description)
- Deduplication on transaction_id
- Batch upsert with conflict resolution
"""
from datetime import datetime
from typing import Optional

from loguru import logger
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..db import get_session
from ..models.schema import Filing


# Filing categories that indicate significant events
SIGNIFICANT_CATEGORIES = {
    "accounts",
    "annual-return",
    "confirmation-statement",
    "capital",
    "change-of-name",
    "incorporation",
    "liquidation",
    "officers",
    "resolution",
    "mortgage",
    "persons-with-significant-control",
}


def normalize_filing_record(
    raw_item: dict, company_number: str, source: str = "api"
) -> Optional[dict]:
    """
    Normalize a single filing history item from the CH API.

    Args:
        raw_item: Single item from the filing history 'items' array
        company_number: The company this filing belongs to
        source: Provenance tag

    Returns:
        Dict matching the filings table schema, or None if invalid
    """
    transaction_id = raw_item.get("transaction_id")
    if not transaction_id:
        # Filings without transaction IDs can't be deduplicated
        return None

    # Parse filing date
    filing_date_str = raw_item.get("date")
    if not filing_date_str:
        return None

    try:
        filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        try:
            filing_date = datetime.fromisoformat(filing_date_str).date()
        except (ValueError, TypeError):
            logger.warning(f"Cannot parse filing date: {filing_date_str}")
            return None

    category = raw_item.get("category", "unknown").strip().lower()
    filing_type = raw_item.get("type", "unknown").strip()

    # Description may contain placeholders
    description = raw_item.get("description", "")
    description_values = raw_item.get("description_values")

    # Resolve description placeholders if possible
    if description and description_values and isinstance(description_values, dict):
        resolved_desc = description
        for key, val in description_values.items():
            resolved_desc = resolved_desc.replace(f"{{{key}}}", str(val))
    else:
        resolved_desc = description

    return {
        "company_number": company_number.strip().zfill(8),
        "filing_date": filing_date,
        "category": category,
        "type": filing_type,
        "description": resolved_desc or None,
        "description_values": description_values,
        "transaction_id": transaction_id,
        "source": source,
        "fetched_at": datetime.utcnow(),
    }


def normalize_filing_response(
    api_response: dict, company_number: str, source: str = "api"
) -> list[dict]:
    """
    Normalize the full filing history API response for a company.

    Args:
        api_response: Full JSON response from /company/{number}/filing-history
        company_number: The company number
        source: Provenance tag

    Returns:
        List of normalized filing dicts
    """
    items = api_response.get("items", [])
    if not items:
        return []

    records = []
    for item in items:
        normalized = normalize_filing_record(item, company_number, source)
        if normalized:
            records.append(normalized)

    logger.debug(
        f"Normalized {len(records)}/{len(items)} filing records for {company_number}"
    )
    return records


def upsert_filings_batch(records: list[dict], batch_size: int = 2000) -> int:
    """
    Upsert filing records using INSERT ... ON CONFLICT on transaction_id.

    Returns:
        Number of rows affected
    """
    if not records:
        return 0

    total = 0

    with get_session() as session:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            stmt = pg_insert(Filing).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["transaction_id"],
                set_={
                    "filing_date": stmt.excluded.filing_date,
                    "category": stmt.excluded.category,
                    "type": stmt.excluded.type,
                    "description": stmt.excluded.description,
                    "description_values": stmt.excluded.description_values,
                    "source": stmt.excluded.source,
                    "fetched_at": stmt.excluded.fetched_at,
                },
            )
            result = session.execute(stmt)
            total += result.rowcount
            logger.debug(f"Upserted filings batch {i // batch_size + 1}: {result.rowcount} rows")

        session.commit()

    logger.info(f"Filings upsert complete: {total} rows affected")
    return total


def classify_filing_significance(filing: dict) -> dict:
    """
    Add significance metadata to a filing for downstream signal computation.

    Returns the filing dict with added fields:
    - is_significant: bool
    - significance_reason: str or None
    """
    category = filing.get("category", "")
    filing_type = filing.get("type", "")

    is_significant = False
    reason = None

    if category in SIGNIFICANT_CATEGORIES:
        is_significant = True
        reason = f"Category: {category}"

    # Specific high-signal filing types
    if "liquidation" in category or "liquidation" in filing_type.lower():
        is_significant = True
        reason = "Liquidation filing"
    elif "strike-off" in filing_type.lower() or "dissolution" in filing_type.lower():
        is_significant = True
        reason = "Strike-off or dissolution"
    elif "change-of-name" in category:
        is_significant = True
        reason = "Company name change"
    elif "officers" in category:
        is_significant = True
        reason = "Officer change filing"
    elif "persons-with-significant-control" in category:
        is_significant = True
        reason = "PSC change"

    filing["is_significant"] = is_significant
    filing["significance_reason"] = reason
    return filing


def extract_filing_timeline(filings: list[dict]) -> dict:
    """
    Extract key timeline events from a company's filing history
    for use in signal computation.

    Returns dict with:
    - last_accounts_date: most recent accounts filing
    - last_officer_change: most recent officer filing
    - last_psc_change: most recent PSC filing
    - filing_gap_days: days since last filing
    - filing_frequency: average days between filings
    """
    if not filings:
        return {
            "last_accounts_date": None,
            "last_officer_change": None,
            "last_psc_change": None,
            "filing_gap_days": None,
            "filing_frequency": None,
        }

    sorted_filings = sorted(filings, key=lambda f: f.get("filing_date", ""), reverse=True)
    now = datetime.utcnow().date()

    last_accounts = None
    last_officer = None
    last_psc = None

    for f in sorted_filings:
        cat = f.get("category", "")
        fdate = f.get("filing_date")
        if not fdate:
            continue

        if not last_accounts and cat == "accounts":
            last_accounts = fdate
        if not last_officer and cat == "officers":
            last_officer = fdate
        if not last_psc and cat == "persons-with-significant-control":
            last_psc = fdate

    # Filing gap
    most_recent = sorted_filings[0].get("filing_date") if sorted_filings else None
    filing_gap = (now - most_recent).days if most_recent else None

    # Filing frequency
    dates = [f.get("filing_date") for f in sorted_filings if f.get("filing_date")]
    if len(dates) >= 2:
        total_span = (dates[0] - dates[-1]).days
        avg_gap = total_span / (len(dates) - 1) if len(dates) > 1 else None
    else:
        avg_gap = None

    return {
        "last_accounts_date": last_accounts,
        "last_officer_change": last_officer,
        "last_psc_change": last_psc,
        "filing_gap_days": filing_gap,
        "filing_frequency": avg_gap,
    }
