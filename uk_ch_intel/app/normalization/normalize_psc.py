"""
Normalize raw PSC (Persons with Significant Control) bulk JSONL data
into the pscs table.

Handles:
- Parsing nested JSON structures from CH bulk PSC snapshots
- Extracting and normalizing name fields (individual vs corporate PSCs)
- Mapping natures of control to a clean array
- Date parsing from ISO format
- Batch upsert with deduplication on (company_number, psc_name, notified_on)
"""
from datetime import datetime
from typing import Optional
import json

import pandas as pd
from loguru import logger
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text

from ..db import get_session, get_raw_connection_context
from ..models.schema import PSC


# PSC kind mapping for normalization
PSC_KIND_MAP = {
    "individual-person-with-significant-control": "individual",
    "corporate-entity-person-with-significant-control": "corporate",
    "legal-person-person-with-significant-control": "legal-person",
    "super-secure-person-with-significant-control": "super-secure",
    "individual-beneficial-owner": "individual-bo",
    "corporate-entity-beneficial-owner": "corporate-bo",
    "legal-person-beneficial-owner": "legal-person-bo",
    "super-secure-beneficial-owner": "super-secure-bo",
    "exemptions": "exemption",
}


def _extract_psc_name(record: dict) -> Optional[str]:
    """Extract the PSC name from various possible fields."""
    # Individual PSCs have name_elements
    name_elements = record.get("name_elements")
    if name_elements:
        parts = [
            name_elements.get("title", ""),
            name_elements.get("forename", ""),
            name_elements.get("other_forenames", ""),
            name_elements.get("surname", ""),
        ]
        full_name = " ".join(p.strip() for p in parts if p and p.strip())
        if full_name:
            return full_name

    # Direct name field
    name = record.get("name")
    if name and str(name).strip():
        return str(name).strip()

    # Corporate entity
    corp_name = record.get("name")
    if not corp_name:
        identification = record.get("identification", {}) or {}
        corp_name = identification.get("legal_authority") or identification.get("place_registered")

    return corp_name if corp_name else "UNKNOWN"


def _extract_natures_of_control(record: dict) -> Optional[list[str]]:
    """Extract and normalize natures of control."""
    natures = record.get("natures_of_control")
    if not natures or not isinstance(natures, list):
        return None
    # Clean up the nature strings
    cleaned = []
    for n in natures:
        if isinstance(n, str) and n.strip():
            cleaned.append(n.strip().lower().replace("-", "_"))
    return cleaned if cleaned else None


def _parse_iso_date(val) -> Optional[str]:
    """Parse ISO date string."""
    if not val or (isinstance(val, float) and pd.isna(val)):
        return None
    val_str = str(val).strip()
    if not val_str:
        return None
    try:
        return datetime.fromisoformat(val_str).date()
    except (ValueError, TypeError):
        # Try simpler format
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(val_str, fmt).date()
            except ValueError:
                continue
    return None


def _extract_birth_info(record: dict) -> tuple[Optional[int], Optional[int]]:
    """Extract birth month and year from PSC record."""
    dob = record.get("date_of_birth")
    if not dob or not isinstance(dob, dict):
        return None, None
    month = dob.get("month")
    year = dob.get("year")
    try:
        month = int(month) if month else None
    except (ValueError, TypeError):
        month = None
    try:
        year = int(year) if year else None
    except (ValueError, TypeError):
        year = None
    return month, year


def normalize_psc_record(record: dict, source_file: str = "bulk") -> Optional[dict]:
    """
    Normalize a single PSC JSONL record into a dict matching the pscs table.
    """
    company_number = record.get("company_number", "")
    if not company_number:
        return None

    company_number = str(company_number).strip().zfill(8)

    psc_name = _extract_psc_name(record)
    if not psc_name or psc_name == "UNKNOWN":
        # Skip records with no identifiable name
        return None

    raw_kind = record.get("kind", "unknown")
    psc_kind = PSC_KIND_MAP.get(raw_kind, raw_kind)

    birth_month, birth_year = _extract_birth_info(record)
    natures = _extract_natures_of_control(record)
    notified_on = _parse_iso_date(record.get("notified_on"))
    ceased_on = _parse_iso_date(record.get("ceased_on"))

    return {
        "company_number": company_number,
        "psc_name": psc_name,
        "psc_kind": psc_kind,
        "birth_month": birth_month,
        "birth_year": birth_year,
        "notified_on": notified_on,
        "ceased_on": ceased_on,
        "control_natures": natures,
        "source": "bulk",
        "source_file": source_file,
        "updated_at": datetime.utcnow(),
    }


def normalize_psc_chunk(records: list[dict], source_file: str = "bulk") -> list[dict]:
    """
    Normalize a batch of raw PSC records.

    Args:
        records: List of dicts parsed from JSONL
        source_file: Provenance tag

    Returns:
        List of normalized dicts ready for insert
    """
    normalized = []
    skipped = 0
    for rec in records:
        result = normalize_psc_record(rec, source_file)
        if result:
            normalized.append(result)
        else:
            skipped += 1

    if skipped > 0:
        logger.debug(f"Skipped {skipped} PSC records with missing data")

    return normalized


def insert_psc_batch(records: list[dict], batch_size: int = 5000) -> int:
    """
    Insert normalized PSC records into PostgreSQL.

    PSCs don't have a natural unique key like company_number, so we
    deduplicate on (company_number, psc_name, notified_on) within each batch.

    Returns:
        Number of rows inserted
    """
    if not records:
        return 0

    total = 0
    # Deduplicate within batch
    seen = set()
    unique_records = []
    for r in records:
        key = (r["company_number"], r["psc_name"], str(r.get("notified_on", "")))
        if key not in seen:
            seen.add(key)
            unique_records.append(r)

    with get_session() as session:
        for i in range(0, len(unique_records), batch_size):
            batch = unique_records[i : i + batch_size]

            # Use raw INSERT — PSC rows use auto-increment PK
            # Skip duplicates by checking existence
            stmt = pg_insert(PSC).values(batch)
            stmt = stmt.on_conflict_do_nothing()  # psc_row_id is autoincrement, no natural UK
            result = session.execute(stmt)
            count = result.rowcount
            total += count
            logger.debug(f"Inserted PSC batch {i // batch_size + 1}: {count} rows")

        session.commit()

    logger.info(f"PSC insert complete: {total} rows inserted from {len(unique_records)} unique records")
    return total


def load_psc_jsonl_file(filepath: str, source_file: str = None, chunk_size: int = 50000) -> int:
    """
    Stream-load a PSC JSONL file, normalizing and inserting in chunks.

    Args:
        filepath: Path to the .jsonl file
        source_file: Override source_file provenance tag
        chunk_size: Records per batch

    Returns:
        Total rows inserted
    """
    if source_file is None:
        source_file = filepath.split("/")[-1]

    total = 0
    chunk = []
    line_count = 0

    logger.info(f"Loading PSC JSONL from {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            # PSC bulk data has a wrapper with company_number at top level
            # and data nested under various keys
            if "company_number" in record:
                chunk.append(record)
            elif "data" in record and isinstance(record["data"], dict):
                inner = record["data"]
                inner["company_number"] = record.get("company_number", inner.get("company_number"))
                chunk.append(inner)

            line_count += 1

            if len(chunk) >= chunk_size:
                normalized = normalize_psc_chunk(chunk, source_file)
                inserted = insert_psc_batch(normalized)
                total += inserted
                logger.info(f"Processed {line_count} lines, inserted {total} total PSC records")
                chunk = []

    # Final chunk
    if chunk:
        normalized = normalize_psc_chunk(chunk, source_file)
        inserted = insert_psc_batch(normalized)
        total += inserted

    logger.info(f"PSC load complete: {line_count} lines read, {total} rows inserted from {filepath}")
    return total
