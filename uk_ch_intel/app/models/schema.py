from datetime import datetime
from decimal import Decimal
from sqlalchemy import (
    Column,
    String,
    Integer,
    BigInteger,
    Date,
    DateTime,
    Boolean,
    Numeric,
    JSONB,
    ARRAY,
    ForeignKey,
    Index,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Company(Base):
    __tablename__ = "companies"

    company_number = Column(String(8), primary_key=True)
    company_name = Column(String(255), nullable=False, index=True)
    company_status = Column(String(50), nullable=False, index=True)
    company_type = Column(String(50), nullable=False, index=True)
    jurisdiction = Column(String(50), nullable=False)
    incorporation_date = Column(Date, nullable=True, index=True)
    dissolution_date = Column(Date, nullable=True)
    registered_address = Column(Text, nullable=True)
    postal_code = Column(String(20), nullable=True, index=True)
    sic_codes = Column(ARRAY(String), nullable=True)
    accounts_next_due = Column(Date, nullable=True)
    accounts_last_made_up_to = Column(Date, nullable=True)
    confirmation_statement_next_due = Column(Date, nullable=True)
    confirmation_statement_last_made_up_to = Column(Date, nullable=True)
    source = Column(String(50), nullable=False, default="bulk")
    source_file = Column(String(255), nullable=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, index=True)

    # Relationships
    pscs = relationship("PSC", back_populates="company")
    officers_raw = relationship("OfficerRaw", back_populates="company")
    appointments = relationship("Appointment", back_populates="company")
    filings = relationship("Filing", back_populates="company")
    enrichment_jobs = relationship("EnrichmentJob", back_populates="company")
    signals = relationship("CompanySignal", back_populates="company")

    __table_args__ = (
        Index("ix_companies_status_type", "company_status", "company_type"),
        Index("ix_companies_sic_codes", "sic_codes", postgresql_using="gin"),
        Index("ix_companies_name_gin", "company_name", postgresql_using="gin"),
    )


class PSC(Base):
    __tablename__ = "pscs"

    psc_row_id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_number = Column(String(8), ForeignKey("companies.company_number"), nullable=False, index=True)
    psc_name = Column(String(255), nullable=False, index=True)
    psc_kind = Column(String(50), nullable=False, index=True)
    birth_month = Column(Integer, nullable=True)
    birth_year = Column(Integer, nullable=True, index=True)
    notified_on = Column(Date, nullable=True)
    ceased_on = Column(Date, nullable=True)
    control_natures = Column(ARRAY(String), nullable=True)
    source = Column(String(50), nullable=False, default="bulk")
    source_file = Column(String(255), nullable=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, index=True)

    # Relationships
    company = relationship("Company", back_populates="pscs")

    __table_args__ = (
        Index("ix_pscs_company_name", "company_number", "psc_name"),
        Index("ix_pscs_kind", "psc_kind"),
    )


class OfficerRaw(Base):
    __tablename__ = "officers_raw"

    officer_raw_id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_number = Column(String(8), ForeignKey("companies.company_number"), nullable=False, index=True)
    source_officer_payload = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    source_endpoint = Column(String(50), nullable=False, default="officers")

    # Relationships
    company = relationship("Company", back_populates="officers_raw")

    __table_args__ = (
        Index("ix_officers_raw_company_fetched", "company_number", "fetched_at"),
        Index("ix_officers_raw_payload", "source_officer_payload", postgresql_using="gin"),
    )


class OfficerResolved(Base):
    __tablename__ = "officers_resolved"

    officer_id = Column(BigInteger, primary_key=True, autoincrement=True)
    normalized_name = Column(String(255), nullable=False, index=True)
    display_name = Column(String(255), nullable=False)
    birth_month = Column(Integer, nullable=True)
    birth_year = Column(Integer, nullable=True, index=True)
    nationality = Column(String(100), nullable=True)
    occupation = Column(String(255), nullable=True)
    country_of_residence = Column(String(100), nullable=True)
    resolution_confidence = Column(Numeric(5, 2), nullable=False, default=Decimal("0.00"))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, index=True)

    # Relationships
    appointments = relationship("Appointment", back_populates="officer")

    __table_args__ = (
        Index("ix_officers_resolved_name_birth", "normalized_name", "birth_month", "birth_year"),
    )


class Appointment(Base):
    __tablename__ = "appointments"

    appointment_id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_number = Column(String(8), ForeignKey("companies.company_number"), nullable=False, index=True)
    officer_id = Column(BigInteger, ForeignKey("officers_resolved.officer_id"), nullable=True, index=True)
    officer_name_on_filing = Column(String(255), nullable=False, index=True)
    role = Column(String(100), nullable=False, index=True)
    appointed_on = Column(Date, nullable=False, index=True)
    resigned_on = Column(Date, nullable=True, index=True)
    is_current = Column(Boolean, nullable=False, default=True, index=True)
    source = Column(String(50), nullable=False, default="api")
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    company = relationship("Company", back_populates="appointments")
    officer = relationship("OfficerResolved", back_populates="appointments")

    __table_args__ = (
        Index("ix_appointments_company_role_current", "company_number", "role", "is_current"),
        Index("ix_appointments_officer_current", "officer_id", "is_current"),
    )


class Filing(Base):
    __tablename__ = "filings"

    filing_id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_number = Column(String(8), ForeignKey("companies.company_number"), nullable=False, index=True)
    filing_date = Column(Date, nullable=False, index=True)
    category = Column(String(100), nullable=False, index=True)
    type = Column(String(100), nullable=False, index=True)
    description = Column(Text, nullable=True)
    description_values = Column(JSONB, nullable=True)
    transaction_id = Column(String(50), nullable=True, unique=True, index=True)
    source = Column(String(50), nullable=False, default="api")
    fetched_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relationships
    company = relationship("Company", back_populates="filings")

    __table_args__ = (
        Index("ix_filings_company_date", "company_number", "filing_date"),
        Index("ix_filings_category_type", "category", "type"),
    )


class EnrichmentJob(Base):
    __tablename__ = "enrichment_jobs"

    job_id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_number = Column(String(8), ForeignKey("companies.company_number"), nullable=False, index=True)
    job_type = Column(String(50), nullable=False, index=True)
    status = Column(String(20), nullable=False, default="pending", index=True)
    attempt_count = Column(Integer, nullable=False, default=0)
    priority = Column(Integer, nullable=False, default=0, index=True)
    last_error = Column(Text, nullable=True)
    queued_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True, index=True)

    # Relationships
    company = relationship("Company", back_populates="enrichment_jobs")

    __table_args__ = (
        Index("ix_enrichment_jobs_status_type", "status", "job_type"),
        Index("ix_enrichment_jobs_priority_queued", "priority", "queued_at"),
    )


class CompanySignal(Base):
    __tablename__ = "company_signals"

    signal_id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_number = Column(String(8), ForeignKey("companies.company_number"), nullable=False, index=True)
    signal_type = Column(String(100), nullable=False, index=True)
    signal_value = Column(String(255), nullable=True)
    signal_score = Column(Numeric(5, 2), nullable=False, default=Decimal("0.00"))
    explanation = Column(Text, nullable=True)
    computed_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    # Relationships
    company = relationship("Company", back_populates="signals")

    __table_args__ = (
        Index("ix_company_signals_company_type", "company_number", "signal_type"),
        Index("ix_company_signals_score", "signal_score"),
    )


class CompanyEdge(Base):
    __tablename__ = "company_edges"

    edge_id = Column(BigInteger, primary_key=True, autoincrement=True)
    from_type = Column(String(50), nullable=False, index=True)
    from_id = Column(String(50), nullable=False, index=True)
    to_type = Column(String(50), nullable=False, index=True)
    to_id = Column(String(50), nullable=False, index=True)
    edge_type = Column(String(50), nullable=False, index=True)
    weight = Column(Numeric(10, 2), nullable=False, default=Decimal("1.00"))
    metadata = Column(JSONB, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index("ix_company_edges_from", "from_type", "from_id"),
        Index("ix_company_edges_to", "to_type", "to_id"),
        Index("ix_company_edges_type", "edge_type"),
    )


class IngestRun(Base):
    __tablename__ = "ingest_runs"

    run_id = Column(BigInteger, primary_key=True, autoincrement=True)
    ingest_type = Column(String(50), nullable=False, index=True)
    file_name = Column(String(255), nullable=False)
    file_hash = Column(String(64), nullable=True)
    rows_processed = Column(Integer, nullable=False, default=0)
    rows_inserted = Column(Integer, nullable=False, default=0)
    rows_updated = Column(Integer, nullable=False, default=0)
    status = Column(String(20), nullable=False, default="success")
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_ingest_runs_type_date", "ingest_type", "completed_at"),
    )
