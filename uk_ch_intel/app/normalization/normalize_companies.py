"""
Normalize raw bulk company CSV data into the companies table.

Handles:
- Cleaning company names (strip whitespace, normalize case for matching)
- Parsing SIC codes from delimited strings into arrays
- Normalizing address components
- Deduplication on company_number
- Batch upsert with conflict resolution
"""
from datetime import datetime
from typing import Optional
import re

import pandas as pd
from loguru import logger
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..db import get_session
from ..models.schema import Company


# ── Column mapping from CH bulk CSV to our schema ──────────────────────
BULK_CSV_COLUMN_MAP = {
    "CompanyNumber": "company_number",
    " CompanyNumber": "company_number",  # Some files have leading space
    "CompanyName": "company_name",
    "CompanyStatus": "company_status",
    "CompanyCategory": "company_type",
    "Jurisdiction": "jurisdiction",
    "IncorporationDate": "incorporation_date",
    "DissolutionDate": "dissolution_date",
    "RegAddress.AddressLine1": "address_line_1",
    "RegAddress.AddressLine2": "address_line_2",
    "RegAddress.PostTown": "post_town",
    "RegAddress.County": "county",
    "RegAddress.Country": "country",
    "RegAddress.PostCode": "postal_code",
    "Accounts.NextDueDate": "accounts_next_due",
    "Accounts.LastMadeUpDate": "accounts_last_made_up_to",
    "Returns.NextDueDate": "confirmation_statement_next_due",
    "Returns.LastMadeUpDate": "confirmation_statement_last_made_up_to",
    "SICCode.SicText_1": "sic_1",
    "SICCode.SicText_2": "sic_2",
    "SICCode.SicText_3": "sic_3",
    "SICCode.SicText_4": "sic_4",
}

# SIC columns in order
SIC_COLUMNS = ["sic_1", "sic_2", "sic_3", "sic_4"]


def _parse_sic_codes(row: pd.Series) -> list[str]:
    """Extract SIC code numbers from the SIC text fields."""
    codes = []
    for col in SIC_COLUMNS:
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            # SIC text is like "43210 - Electrical installation"
            match = re.match(r"(\d{4,5})", str(val).strip())
            if match:
                codes.append(match.group(1))
            else:
                # Fallback: store the raw value
                codes.append(str(val).strip()[:10])
    return codes if codes else None


def _build_registered_address(row: pd.Series) -> Optional[str]:
    """Combine address components into a single string."""
    parts = []
    for col in ["address_line_1", "address_line_2", "post_town", "county", "country"]:
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            parts.append(str(val).strip())
    return ", ".join(parts) if parts else None


def _parse_date(val) -> Optional[str]:
    """Parse date from various CH bulk formats."""
    if pd.isna(val) or not str(val).strip():
        return None
    val_str = str(val).strip()
    # Try common formats
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(val_str, fmt).date()
        except ValueError:
            continue
    return None


def normalize_company_chunk(df: pd.DataFrame, source_file: str = "bulk") -> list[dict]:
    """
    Normalize a chunk of raw bulk CSV data into company dicts ready for upsert.

    Args:
        df: DataFrame with raw CH bulk columns (or already renamed)
        source_file: Provenance tag for this batch

    Returns:
        List of dicts matching the companies table schema
    """
    # Rename columns to internal names (handle both original and already-renamed)
    rename_map = {k: v for k, v in BULK_CSV_COLUMN_MAP.items() if k in df.columns}
    if rename_map:
        df = df.rename(columns=rename_map)

    records = []
    now = datetime.utcnow()

    for _, row in df.iterrows():
        company_number = str(row.get("company_number", "")).strip()
        if not company_number or len(company_number) < 1:
            continue

        # Pad company number to 8 chars (CH standard)
        company_number = company_number.zfill(8)

        company_name = str(row.get("company_name", "")).strip()
        if not company_name:
            continue

        sic_codes = _parse_sic_codes(row)
        registered_address = _build_registered_address(row)
        postal_code = str(row.get("postal_code", "")).strip() or None

        record = {
            "company_number": company_number,
            "company_name": company_name,
            "company_status": str(row.get("company_status", "Unknown")).strip(),
            "company_type": str(row.get("company_type", "Unknown")).strip(),
            "jurisdiction": str(row.get("jurisdiction", "")).strip() or "england-wales",
            "incorporation_date": _parse_date(row.get("incorporation_date")),
            "dissolution_date": _parse_date(row.get("dissolution_date")),
            "registered_address": registered_address,
            "postal_code": postal_code,
            "sic_codes": sic_codes,
            "accounts_next_due": _parse_date(row.get("accounts_next_due")),
            "accounts_last_made_up_to": _parse_date(row.get("accounts_last_made_up_to")),
            "confirmation_statement_next_due": _parse_date(row.get("confirmation_statement_next_due")),
            "confirmation_statement_last_made_up_to": _parse_date(row.get("confirmation_statement_last_made_up_to")),
            "source": "bulk",
            "source_file": source_file,
            "updated_at": now,
        }
        records.append(record)

    return records


def upsert_companies_batch(records: list[dict], batch_size: int = 5000) -> tuple[int, int]:
    """
    Upsert normalized company records into PostgreSQL.

    Uses INSERT ... ON CONFLICT DO UPDATE for idempotent bulk loads.

    Returns:
        (inserted_count, updated_count) approximate counts
    """
    if not records:
        return 0, 0

    total_inserted = 0
    total_updated = 0

    with get_session() as session:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            stmt = pg_insert(Company).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["company_number"],
                set_={
                    "company_name": stmt.excluded.company_name,
                    "company_status": stmt.excluded.company_status,
                    "company_type": stmt.excluded.company_type,
                    "jurisdiction": stmt.excluded.jurisdiction,
                    "incorporation_date": stmt.excluded.incorporation_date,
                    "dissolution_date": stmt.excluded.dissolution_date,
                    "registered_address": stmt.excluded.registered_address,
                    "postal_code": stmt.excluded.postal_code,
                    "sic_codes": stmt.excluded.sic_codes,
                    "accounts_next_due": stmt.excluded.accounts_next_due,
                    "accounts_last_made_up_to": stmt.excluded.accounts_last_made_up_to,
                    "confirmation_statement_next_due": stmt.excluded.confirmation_statement_next_due,
                    "confirmation_statement_last_made_up_to": stmt.excluded.confirmation_statement_last_made_up_to,
                    "source": stmt.excluded.source,
                    "source_file": stmt.excluded.source_file,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            result = session.execute(stmt)
            # rowcount for upsert = total rows affected
            affected = result.rowcount
            total_inserted += affected
            logger.debug(f"Upserted company batch {i // batch_size + 1}: {affected} rows")

        session.commit()

    logger.info(f"Company upsert complete: {total_inserted} rows affected")
    return total_inserted, total_updated


def normalize_api_company(api_payload: dict, source: str = "api") -> dict:
    """
    Normalize a single company profile from the CH REST API into a company dict.
    """
    addr = api_payload.get("registered_office_address", {}) or {}
    address_parts = [
        addr.get("address_line_1", ""),
        addr.get("address_line_2", ""),
        addr.get("locality", ""),
        addr.get("region", ""),
        addr.get("country", ""),
    ]
    registered_address = ", ".join(p.strip() for p in address_parts if p.strip()) or None

    sic_codes = api_payload.get("sic_codes")

    accounts = api_payload.get("accounts", {}) or {}
    conf_stmt = api_payload.get("confirmation_statement", {}) or {}

    return {
        "company_number": api_payload.get("company_number", "").zfill(8),
        "company_name": api_payload.get("company_name", ""),
        "company_status": api_payload.get("company_status", "unknown"),
        "company_type": api_payload.get("type", "unknown"),
        "jurisdiction": api_payload.get("jurisdiction", "england-wales"),
        "incorporation_date": api_payload.get("date_of_creation"),
        "dissolution_date": api_payload.get("date_of_cessation"),
        "registered_address": registered_address,
        "postal_code": addr.get("postal_code"),
        "sic_codes": sic_codes,
        "accounts_next_due": accounts.get("next_due"),
        "accounts_last_made_up_to": accounts.get("last_accounts", {}).get("made_up_to"),
        "confirmation_statement_next_due": conf_stmt.get("next_due"),
        "confirmation_statement_last_made_up_to": conf_stmt.get("last_made_up_to"),
        "source": source,
        "source_file": None,
        "updated_at": datetime.utcnow(),
    }
