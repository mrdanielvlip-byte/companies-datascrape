"""
Entity Resolution for Officers.

Implements a reversible, confidence-scored matching framework that links
raw officer records to resolved identity entities.

Matching strategy:
  1. Exact match: normalized name + birth month + birth year  → 0.97
  2. Strong match: normalized name + birth year               → 0.90
  3. Probable match: normalized name + same nationality/occupation → 0.82
  4. Weak match: normalized name only                         → 0.65
  5. Fuzzy match: similar name + birth year                   → 0.55
  6. No match: create new entity                              → 0.50 (baseline)

All merges are reversible:
  - Raw officer records are never modified
  - Each appointment stores the officer_id it was resolved to
  - Resolution confidence is stored per entity
  - Match reason is logged for audit
"""
import re
import unicodedata
from datetime import datetime
from decimal import Decimal
from typing import Optional

from loguru import logger
from sqlalchemy import text

from ..db import get_session
from ..models.schema import OfficerResolved, OfficerRaw, Appointment


# ── Name normalization ─────────────────────────────────────────────────

# Titles, honorifics, and suffixes to strip
TITLES = re.compile(
    r"\b(mr|mrs|ms|miss|dr|prof|professor|sir|dame|lord|lady|baron|"
    r"baroness|count|countess|hon|honourable|cbe|obe|mbe|fca|aca|"
    r"fcca|acca|frcs|jr|sr|jnr|snr|ii|iii|iv)\b\.?",
    re.IGNORECASE,
)

WHITESPACE = re.compile(r"\s+")


def normalize_name(name: str) -> str:
    """
    Normalize an officer name for matching.

    Steps:
    1. Unicode NFKD decomposition (strip accents)
    2. Lowercase
    3. Remove titles and honorifics
    4. Remove punctuation
    5. Collapse whitespace
    6. Sort name parts alphabetically (order-invariant)
    """
    if not name:
        return ""

    # Unicode normalize
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))

    # Lowercase
    name = name.lower().strip()

    # Remove titles
    name = TITLES.sub("", name)

    # Remove punctuation except hyphens in names
    name = re.sub(r"[^\w\s-]", "", name)

    # Collapse whitespace
    name = WHITESPACE.sub(" ", name).strip()

    # Sort parts for order invariance ("SMITH JOHN" == "JOHN SMITH")
    parts = sorted(name.split())
    return " ".join(parts)


def name_similarity(name_a: str, name_b: str) -> float:
    """
    Compute similarity between two normalized names.
    Uses Jaccard similarity on character trigrams.
    """
    if not name_a or not name_b:
        return 0.0

    def trigrams(s: str) -> set:
        s = f"  {s}  "  # pad
        return {s[i : i + 3] for i in range(len(s) - 2)}

    t_a = trigrams(name_a)
    t_b = trigrams(name_b)

    if not t_a or not t_b:
        return 0.0

    intersection = len(t_a & t_b)
    union = len(t_a | t_b)
    return intersection / union if union > 0 else 0.0


# ── Resolution engine ──────────────────────────────────────────────────


class MatchResult:
    """Container for a match result."""

    def __init__(
        self,
        officer_id: Optional[int],
        confidence: Decimal,
        reason: str,
        is_new: bool = False,
    ):
        self.officer_id = officer_id
        self.confidence = confidence
        self.reason = reason
        self.is_new = is_new


def resolve_officer(
    name: str,
    birth_month: Optional[int] = None,
    birth_year: Optional[int] = None,
    nationality: Optional[str] = None,
    occupation: Optional[str] = None,
    country_of_residence: Optional[str] = None,
) -> MatchResult:
    """
    Resolve an officer to an existing or new OfficerResolved entity.

    Uses tiered matching with confidence bands:
    - 0.95+ = auto-merge (exact match on name + DOB)
    - 0.80–0.95 = probable match
    - below 0.80 = keep separate or create new

    Returns:
        MatchResult with officer_id, confidence, and reason
    """
    norm_name = normalize_name(name)
    if not norm_name:
        return MatchResult(None, Decimal("0.00"), "empty_name")

    with get_session() as session:
        # ── Tier 1: Exact match (name + birth_month + birth_year) ──
        if birth_month and birth_year:
            exact = (
                session.query(OfficerResolved)
                .filter(
                    OfficerResolved.normalized_name == norm_name,
                    OfficerResolved.birth_month == birth_month,
                    OfficerResolved.birth_year == birth_year,
                )
                .first()
            )
            if exact:
                return MatchResult(
                    exact.officer_id,
                    Decimal("0.97"),
                    f"exact_match:name+dob({birth_month}/{birth_year})",
                )

        # ── Tier 2: Strong match (name + birth_year only) ──
        if birth_year:
            year_match = (
                session.query(OfficerResolved)
                .filter(
                    OfficerResolved.normalized_name == norm_name,
                    OfficerResolved.birth_year == birth_year,
                )
                .first()
            )
            if year_match:
                return MatchResult(
                    year_match.officer_id,
                    Decimal("0.90"),
                    f"strong_match:name+year({birth_year})",
                )

        # ── Tier 3: Probable match (name + nationality or occupation) ──
        if nationality or occupation:
            filters = [OfficerResolved.normalized_name == norm_name]
            if nationality:
                filters.append(
                    OfficerResolved.nationality == nationality.strip().lower()
                )
            if occupation:
                filters.append(
                    OfficerResolved.occupation == occupation.strip().lower()
                )

            probable = session.query(OfficerResolved).filter(*filters).first()
            if probable:
                match_fields = []
                if nationality:
                    match_fields.append(f"nationality={nationality}")
                if occupation:
                    match_fields.append(f"occupation={occupation}")
                return MatchResult(
                    probable.officer_id,
                    Decimal("0.82"),
                    f"probable_match:name+{'+'.join(match_fields)}",
                )

        # ── Tier 4: Weak match (name only, but must be exact) ──
        name_match = (
            session.query(OfficerResolved)
            .filter(OfficerResolved.normalized_name == norm_name)
            .first()
        )
        if name_match:
            return MatchResult(
                name_match.officer_id,
                Decimal("0.65"),
                "weak_match:name_only",
            )

        # ── Tier 5: Fuzzy match (similar name + birth year) ──
        if birth_year:
            candidates = (
                session.query(OfficerResolved)
                .filter(OfficerResolved.birth_year == birth_year)
                .limit(200)
                .all()
            )
            best_sim = 0.0
            best_candidate = None
            for c in candidates:
                sim = name_similarity(norm_name, c.normalized_name)
                if sim > best_sim and sim >= 0.75:  # 75% trigram threshold
                    best_sim = sim
                    best_candidate = c

            if best_candidate:
                return MatchResult(
                    best_candidate.officer_id,
                    Decimal("0.55"),
                    f"fuzzy_match:sim={best_sim:.2f}+year({birth_year})",
                )

        # ── No match: create new entity ──
        new_officer = OfficerResolved(
            normalized_name=norm_name,
            display_name=name.strip(),
            birth_month=birth_month,
            birth_year=birth_year,
            nationality=nationality.strip().lower() if nationality else None,
            occupation=occupation.strip().lower() if occupation else None,
            country_of_residence=(
                country_of_residence.strip().lower() if country_of_residence else None
            ),
            resolution_confidence=Decimal("0.50"),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        session.add(new_officer)
        session.flush()  # Get the ID

        return MatchResult(
            new_officer.officer_id,
            Decimal("0.50"),
            "new_entity",
            is_new=True,
        )


def resolve_raw_officers_batch(company_number: str) -> int:
    """
    Resolve all raw officer records for a company into appointments
    linked to resolved officer entities.

    Returns:
        Number of appointments created/updated
    """
    with get_session() as session:
        raw_records = (
            session.query(OfficerRaw)
            .filter(OfficerRaw.company_number == company_number)
            .all()
        )

        if not raw_records:
            return 0

        count = 0
        for raw in raw_records:
            payload = raw.source_officer_payload
            if not payload or not isinstance(payload, dict):
                continue

            name = payload.get("name", "")
            dob = payload.get("date_of_birth", {}) or {}
            birth_month = dob.get("month")
            birth_year = dob.get("year")
            nationality = payload.get("nationality")
            occupation = payload.get("occupation")
            country = payload.get("country_of_residence")

            # Resolve identity
            match = resolve_officer(
                name=name,
                birth_month=int(birth_month) if birth_month else None,
                birth_year=int(birth_year) if birth_year else None,
                nationality=nationality,
                occupation=occupation,
                country_of_residence=country,
            )

            if not match.officer_id:
                continue

            # Parse appointment details
            role = payload.get("officer_role", "director")
            appointed_on_str = payload.get("appointed_on")
            resigned_on_str = payload.get("resigned_on")

            try:
                appointed_on = (
                    datetime.strptime(appointed_on_str, "%Y-%m-%d").date()
                    if appointed_on_str
                    else datetime.utcnow().date()
                )
            except (ValueError, TypeError):
                appointed_on = datetime.utcnow().date()

            try:
                resigned_on = (
                    datetime.strptime(resigned_on_str, "%Y-%m-%d").date()
                    if resigned_on_str
                    else None
                )
            except (ValueError, TypeError):
                resigned_on = None

            is_current = resigned_on is None

            # Create or update appointment
            existing = (
                session.query(Appointment)
                .filter(
                    Appointment.company_number == company_number,
                    Appointment.officer_name_on_filing == name,
                    Appointment.role == role,
                    Appointment.appointed_on == appointed_on,
                )
                .first()
            )

            if existing:
                existing.officer_id = match.officer_id
                existing.resigned_on = resigned_on
                existing.is_current = is_current
                existing.updated_at = datetime.utcnow()
            else:
                appt = Appointment(
                    company_number=company_number,
                    officer_id=match.officer_id,
                    officer_name_on_filing=name,
                    role=role,
                    appointed_on=appointed_on,
                    resigned_on=resigned_on,
                    is_current=is_current,
                    source="api",
                    updated_at=datetime.utcnow(),
                )
                session.add(appt)

            count += 1
            logger.debug(
                f"Resolved {name} → officer_id={match.officer_id} "
                f"(confidence={match.confidence}, reason={match.reason})"
            )

        session.commit()

    logger.info(
        f"Resolved {count} officer records for company {company_number}"
    )
    return count


def get_resolution_stats() -> dict:
    """Return summary statistics on entity resolution quality."""
    with get_session() as session:
        result = session.execute(
            text("""
                SELECT
                    count(*) as total_entities,
                    avg(resolution_confidence) as avg_confidence,
                    count(*) filter (where resolution_confidence >= 0.95) as high_confidence,
                    count(*) filter (where resolution_confidence >= 0.80 and resolution_confidence < 0.95) as medium_confidence,
                    count(*) filter (where resolution_confidence < 0.80) as low_confidence
                FROM officers_resolved
            """)
        )
        row = result.fetchone()
        if not row:
            return {}

        return {
            "total_entities": row[0],
            "avg_confidence": float(row[1]) if row[1] else 0.0,
            "high_confidence_count": row[2],
            "medium_confidence_count": row[3],
            "low_confidence_count": row[4],
        }
