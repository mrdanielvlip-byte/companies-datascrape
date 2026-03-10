-- Schema migrations tracking table
CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    description VARCHAR(255) NOT NULL,
    executed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Insert this migration if not already done
INSERT INTO schema_migrations (version, description)
VALUES (1, 'Initial schema creation')
ON CONFLICT DO NOTHING;

-- Companies table
CREATE TABLE IF NOT EXISTS companies (
    company_number VARCHAR(8) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    company_status VARCHAR(50) NOT NULL,
    company_type VARCHAR(50) NOT NULL,
    jurisdiction VARCHAR(50) NOT NULL DEFAULT 'GB',
    incorporation_date DATE,
    dissolution_date DATE,
    registered_address TEXT,
    postal_code VARCHAR(20),
    sic_codes TEXT[],
    accounts_next_due DATE,
    accounts_last_made_up_to DATE,
    confirmation_statement_next_due DATE,
    confirmation_statement_last_made_up_to DATE,
    source VARCHAR(50) NOT NULL DEFAULT 'bulk',
    source_file VARCHAR(255),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Companies indexes
CREATE INDEX IF NOT EXISTS ix_companies_status_type ON companies(company_status, company_type);
CREATE INDEX IF NOT EXISTS ix_companies_postal_code ON companies(postal_code);
CREATE INDEX IF NOT EXISTS ix_companies_incorporation_date ON companies(incorporation_date);
CREATE INDEX IF NOT EXISTS ix_companies_updated_at ON companies(updated_at);
CREATE INDEX IF NOT EXISTS ix_companies_sic_codes ON companies USING GIN(sic_codes);
CREATE INDEX IF NOT EXISTS ix_companies_name_gin ON companies USING GIN(company_name gin_trgm_ops);

-- People with Significant Control (PSCs) table
CREATE TABLE IF NOT EXISTS pscs (
    psc_row_id BIGSERIAL PRIMARY KEY,
    company_number VARCHAR(8) NOT NULL REFERENCES companies(company_number) ON DELETE CASCADE,
    psc_name VARCHAR(255) NOT NULL,
    psc_kind VARCHAR(50) NOT NULL,
    birth_month INTEGER,
    birth_year INTEGER,
    notified_on DATE,
    ceased_on DATE,
    control_natures TEXT[],
    source VARCHAR(50) NOT NULL DEFAULT 'bulk',
    source_file VARCHAR(255),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- PSCs indexes
CREATE INDEX IF NOT EXISTS ix_pscs_company_number ON pscs(company_number);
CREATE INDEX IF NOT EXISTS ix_pscs_psc_name ON pscs(psc_name);
CREATE INDEX IF NOT EXISTS ix_pscs_kind ON pscs(psc_kind);
CREATE INDEX IF NOT EXISTS ix_pscs_birth_year ON pscs(birth_year);
CREATE INDEX IF NOT EXISTS ix_pscs_company_name ON pscs(company_number, psc_name);

-- Raw officers data table (from API)
CREATE TABLE IF NOT EXISTS officers_raw (
    officer_raw_id BIGSERIAL PRIMARY KEY,
    company_number VARCHAR(8) NOT NULL REFERENCES companies(company_number) ON DELETE CASCADE,
    source_officer_payload JSONB NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT NOW(),
    source_endpoint VARCHAR(50) NOT NULL DEFAULT 'officers'
);

-- Officers raw indexes
CREATE INDEX IF NOT EXISTS ix_officers_raw_company_number ON officers_raw(company_number);
CREATE INDEX IF NOT EXISTS ix_officers_raw_fetched_at ON officers_raw(fetched_at);
CREATE INDEX IF NOT EXISTS ix_officers_raw_payload ON officers_raw USING GIN(source_officer_payload);

-- Resolved officers table
CREATE TABLE IF NOT EXISTS officers_resolved (
    officer_id BIGSERIAL PRIMARY KEY,
    normalized_name VARCHAR(255) NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    birth_month INTEGER,
    birth_year INTEGER,
    nationality VARCHAR(100),
    occupation VARCHAR(255),
    country_of_residence VARCHAR(100),
    resolution_confidence NUMERIC(5, 2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Officers resolved indexes
CREATE INDEX IF NOT EXISTS ix_officers_resolved_normalized_name ON officers_resolved(normalized_name);
CREATE INDEX IF NOT EXISTS ix_officers_resolved_birth_year ON officers_resolved(birth_year);
CREATE INDEX IF NOT EXISTS ix_officers_resolved_name_birth ON officers_resolved(normalized_name, birth_month, birth_year);
CREATE INDEX IF NOT EXISTS ix_officers_resolved_updated_at ON officers_resolved(updated_at);

-- Appointments table
CREATE TABLE IF NOT EXISTS appointments (
    appointment_id BIGSERIAL PRIMARY KEY,
    company_number VARCHAR(8) NOT NULL REFERENCES companies(company_number) ON DELETE CASCADE,
    officer_id BIGINT REFERENCES officers_resolved(officer_id) ON DELETE SET NULL,
    officer_name_on_filing VARCHAR(255) NOT NULL,
    role VARCHAR(100) NOT NULL,
    appointed_on DATE NOT NULL,
    resigned_on DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    source VARCHAR(50) NOT NULL DEFAULT 'api',
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Appointments indexes
CREATE INDEX IF NOT EXISTS ix_appointments_company_number ON appointments(company_number);
CREATE INDEX IF NOT EXISTS ix_appointments_officer_id ON appointments(officer_id);
CREATE INDEX IF NOT EXISTS ix_appointments_role ON appointments(role);
CREATE INDEX IF NOT EXISTS ix_appointments_appointed_on ON appointments(appointed_on);
CREATE INDEX IF NOT EXISTS ix_appointments_resigned_on ON appointments(resigned_on);
CREATE INDEX IF NOT EXISTS ix_appointments_is_current ON appointments(is_current);
CREATE INDEX IF NOT EXISTS ix_appointments_company_role_current ON appointments(company_number, role, is_current);
CREATE INDEX IF NOT EXISTS ix_appointments_officer_current ON appointments(officer_id, is_current);

-- Filings table
CREATE TABLE IF NOT EXISTS filings (
    filing_id BIGSERIAL PRIMARY KEY,
    company_number VARCHAR(8) NOT NULL REFERENCES companies(company_number) ON DELETE CASCADE,
    filing_date DATE NOT NULL,
    category VARCHAR(100) NOT NULL,
    type VARCHAR(100) NOT NULL,
    description TEXT,
    description_values JSONB,
    transaction_id VARCHAR(50) UNIQUE,
    source VARCHAR(50) NOT NULL DEFAULT 'api',
    fetched_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Filings indexes
CREATE INDEX IF NOT EXISTS ix_filings_company_number ON filings(company_number);
CREATE INDEX IF NOT EXISTS ix_filings_filing_date ON filings(filing_date);
CREATE INDEX IF NOT EXISTS ix_filings_category ON filings(category);
CREATE INDEX IF NOT EXISTS ix_filings_type ON filings(type);
CREATE INDEX IF NOT EXISTS ix_filings_transaction_id ON filings(transaction_id);
CREATE INDEX IF NOT EXISTS ix_filings_company_date ON filings(company_number, filing_date);
CREATE INDEX IF NOT EXISTS ix_filings_category_type ON filings(category, type);

-- Enrichment jobs queue
CREATE TABLE IF NOT EXISTS enrichment_jobs (
    job_id BIGSERIAL PRIMARY KEY,
    company_number VARCHAR(8) NOT NULL REFERENCES companies(company_number) ON DELETE CASCADE,
    job_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    queued_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);

-- Enrichment jobs indexes
CREATE INDEX IF NOT EXISTS ix_enrichment_jobs_company_number ON enrichment_jobs(company_number);
CREATE INDEX IF NOT EXISTS ix_enrichment_jobs_job_type ON enrichment_jobs(job_type);
CREATE INDEX IF NOT EXISTS ix_enrichment_jobs_status ON enrichment_jobs(status);
CREATE INDEX IF NOT EXISTS ix_enrichment_jobs_status_type ON enrichment_jobs(status, job_type);
CREATE INDEX IF NOT EXISTS ix_enrichment_jobs_priority ON enrichment_jobs(priority);
CREATE INDEX IF NOT EXISTS ix_enrichment_jobs_queued_at ON enrichment_jobs(queued_at);
CREATE INDEX IF NOT EXISTS ix_enrichment_jobs_finished_at ON enrichment_jobs(finished_at);
CREATE INDEX IF NOT EXISTS ix_enrichment_jobs_priority_queued ON enrichment_jobs(priority DESC, queued_at);

-- Company signals
CREATE TABLE IF NOT EXISTS company_signals (
    signal_id BIGSERIAL PRIMARY KEY,
    company_number VARCHAR(8) NOT NULL REFERENCES companies(company_number) ON DELETE CASCADE,
    signal_type VARCHAR(100) NOT NULL,
    signal_value VARCHAR(255),
    signal_score NUMERIC(5, 2) NOT NULL DEFAULT 0.00,
    explanation TEXT,
    computed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Company signals indexes
CREATE INDEX IF NOT EXISTS ix_company_signals_company_number ON company_signals(company_number);
CREATE INDEX IF NOT EXISTS ix_company_signals_signal_type ON company_signals(signal_type);
CREATE INDEX IF NOT EXISTS ix_company_signals_signal_score ON company_signals(signal_score);
CREATE INDEX IF NOT EXISTS ix_company_signals_company_type ON company_signals(company_number, signal_type);
CREATE INDEX IF NOT EXISTS ix_company_signals_computed_at ON company_signals(computed_at);

-- Company edges (graph relationships)
CREATE TABLE IF NOT EXISTS company_edges (
    edge_id BIGSERIAL PRIMARY KEY,
    from_type VARCHAR(50) NOT NULL,
    from_id VARCHAR(50) NOT NULL,
    to_type VARCHAR(50) NOT NULL,
    to_id VARCHAR(50) NOT NULL,
    edge_type VARCHAR(50) NOT NULL,
    weight NUMERIC(10, 2) NOT NULL DEFAULT 1.00,
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Company edges indexes
CREATE INDEX IF NOT EXISTS ix_company_edges_from_type ON company_edges(from_type);
CREATE INDEX IF NOT EXISTS ix_company_edges_from_id ON company_edges(from_id);
CREATE INDEX IF NOT EXISTS ix_company_edges_to_type ON company_edges(to_type);
CREATE INDEX IF NOT EXISTS ix_company_edges_to_id ON company_edges(to_id);
CREATE INDEX IF NOT EXISTS ix_company_edges_edge_type ON company_edges(edge_type);
CREATE INDEX IF NOT EXISTS ix_company_edges_from ON company_edges(from_type, from_id);
CREATE INDEX IF NOT EXISTS ix_company_edges_to ON company_edges(to_type, to_id);

-- Ingest runs tracking
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id BIGSERIAL PRIMARY KEY,
    ingest_type VARCHAR(50) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(64),
    rows_processed INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    rows_updated INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'success',
    error_message TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Ingest runs indexes
CREATE INDEX IF NOT EXISTS ix_ingest_runs_ingest_type ON ingest_runs(ingest_type);
CREATE INDEX IF NOT EXISTS ix_ingest_runs_completed_at ON ingest_runs(completed_at);
CREATE INDEX IF NOT EXISTS ix_ingest_runs_type_date ON ingest_runs(ingest_type, completed_at);

-- Enable trigram extension for text search (if needed)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
