# Architecture Document

## System Design

### Data Flow Pipeline

```
Raw Companies House Data
    ↓
[Bulk CSV Download] → [SHA256 Checksum] → [Chunk Processing (100k rows)]
    ↓
PostgreSQL Staging
    ↓
    ├─→ companies (10M+ records)
    ├─→ pscs (2M+ records)
    └─→ ingest_runs (audit trail)
    ↓
[Enrichment Queue]
    ↓
    ├─→ officers_fetch → officers_raw
    ├─→ company_profile_fetch → companies (upsert)
    └─→ filings_fetch → filings
    ↓
[Normalization Layer]
    ↓
    officers_resolved (deduplicated, scored)
    appointments (with officer_id links)
    ↓
[Signal Computation]
    ↓
    company_signals (7 signal types)
    seller_score (composite 0-100)
    ↓
[Graph Building]
    ↓
    company_edges (officer→company, company↔company, psc→company, officer↔officer)
    ↓
[Analytical Views]
    ↓
    v_likely_sellers
    v_aging_founders
    v_succession_risk
    v_serial_operators
    v_sector_map
    v_family_clusters
```

## Database Schema

### Core Tables (10 tables, 100M+ records)

#### companies
- **PK**: company_number (VARCHAR 8)
- **Purpose**: Master company registry
- **Size**: 10M+ records
- **Key Indexes**: status+type, postal_code, incorporation_date, sic_codes (GIN)

#### officers_resolved
- **PK**: officer_id (BIGSERIAL)
- **Purpose**: Canonical officer identity
- **Size**: 5M+ records
- **Deduplication**: (normalized_name, birth_month, birth_year)
- **Confidence Scoring**: 0.50-0.97
- **Key Indexes**: normalized_name, birth_year, combined name+birth

#### appointments
- **PK**: appointment_id (BIGSERIAL)
- **FKs**: company_number, officer_id
- **Purpose**: Link officers to companies with role and dates
- **Size**: 20M+ records
- **Key Indexes**: company_number+role+current, officer_id+current

#### filings
- **PK**: filing_id (BIGSERIAL)
- **FK**: company_number
- **Purpose**: Filing history with categorization
- **Size**: 50M+ records (growing)
- **Key Indexes**: company_number+date, category+type

#### pscs
- **PK**: psc_row_id (BIGSERIAL)
- **FK**: company_number
- **Purpose**: People with Significant Control
- **Size**: 2M+ records
- **Key Indexes**: company_number, psc_name, birth_year

#### officers_raw
- **PK**: officer_raw_id (BIGSERIAL)
- **FK**: company_number
- **Purpose**: Raw JSON responses from API (audit trail)
- **Size**: Variable (depends on enrichment scope)
- **Key Indexes**: company_number+fetched_at, JSONB GIN index

#### company_signals
- **PK**: signal_id (BIGSERIAL)
- **FK**: company_number
- **Purpose**: Computed seller opportunity signals
- **Size**: 7-8 signals per company (70M+ records)
- **Signal Types**: aging_founder, long_tenure, single_director, founder_era, concentrated_leadership, dormant_succession, complexity_fatigue, seller_score
- **Key Indexes**: company_number+signal_type, signal_score

#### enrichment_jobs
- **PK**: job_id (BIGSERIAL)
- **FK**: company_number
- **Purpose**: Async job queue with retry logic
- **Status**: pending → in_progress → completed/failed
- **Retry Logic**: Max 3 attempts with exponential backoff
- **Key Indexes**: status+job_type, priority+queued_at

#### company_edges
- **PK**: edge_id (BIGSERIAL)
- **Purpose**: Graph relationships (RDF-style triples)
- **Types**: officer→company, company↔company, psc→company, officer↔officer
- **Weight**: Numeric strength (1-5 typically)
- **Key Indexes**: from_type+from_id, to_type+to_id, edge_type

#### ingest_runs
- **PK**: run_id (BIGSERIAL)
- **Purpose**: Audit trail for bulk loads
- **Tracking**: rows processed/inserted/updated, file hash, status
- **Key Indexes**: ingest_type+completed_at

### Views (6 views)

- **v_likely_sellers**: seller_score >= 70, Active companies
- **v_aging_founders**: Director age >= 55
- **v_succession_risk**: Single director + founder era + no recent appointments
- **v_serial_operators**: Officer in 3+ active companies
- **v_sector_map**: Companies grouped by postal region + SIC + avg seller score
- **v_family_clusters**: Companies sharing officer surnames (2+)
- **mv_likely_sellers_materialized**: Materialized view for fast queries

## Module Architecture

### Configuration Module (`app/config.py`)
- Pydantic Settings class
- Environment variable loading
- Validation of enums (EnrichmentScope)
- Singleton pattern via `get_settings()`

### Database Module (`app/db.py`)
- SQLAlchemy engine with NullPool (stateless)
- Session factory with context manager
- Raw psycopg2 connection for COPY operations
- Bulk copy CSV helper function

### Models Module (`app/models/schema.py`)
- SQLAlchemy ORM with proper relationships
- All indexes defined at model level
- JSONB support for flexible data
- Cascade delete rules for referential integrity

### Ingestion Module

#### bulk_companies.py
- HTTP download with progress bar
- SHA256 checksumming
- Pandas chunked reading (100k rows at a time)
- Column mapping from CH CSV format
- Bulk insert via COPY with error handling
- Audit trail in ingest_runs

#### bulk_psc.py
- Download from Companies House PSC endpoint
- JSONL line-by-line parsing
- Upsert logic for idempotence
- Same audit pattern as bulk_companies

#### api_officers.py
- HTTP Basic Auth with API key
- RateLimiter class (token bucket pattern)
- Tenacity retry with exponential backoff
- Pagination handling (35 items per page max)
- Raw JSON storage in officers_raw
- Idempotent job status tracking

#### api_company_profile.py
- Fetch single company profile
- Upsert into companies table
- Updates accounts/confirmation statement dates
- Job status integration

#### api_filings.py
- Paginated filing history (100 items per page)
- Deduplication via transaction_id uniqueness
- Date parsing and categorization
- Job status integration

### Normalization Module (`app/normalization/normalize_officers.py`)

**Algorithm**:
1. Extract officer name from officers_raw JSON
2. Normalize name (remove titles, lowercase)
3. Try exact match: (normalized_name, birth_month, birth_year)
4. Fall back to year-only: (normalized_name, birth_year)
5. Fall back to name-only: (normalized_name)
6. Create new officer_resolved if no match
7. Link via appointment with confidence score
8. Create appointment record

**Confidence Scoring**:
- 0.97: exact name + month + year
- 0.85: exact name + year
- 0.70: exact name only
- 0.50: new record (no match)

### Signal Computation Module (`app/signals/seller_signals.py`)

**Signal Definitions**:

1. **aging_founder** (score 85)
   - Condition: Active director with estimated age >= 55
   - Calculation: current_year - birth_year >= 55

2. **long_tenure** (score 75)
   - Condition: Director appointed >= 15 years ago
   - Calculation: date_difference(now, appointed_on) >= 15 years

3. **single_director** (score 70)
   - Condition: Exactly 1 active director
   - Calculation: COUNT(active directors) = 1

4. **founder_era** (score 60)
   - Condition: Incorporated >= 15 years ago
   - Calculation: date_difference(now, incorporation_date) >= 15 years

5. **concentrated_leadership** (score 65)
   - Condition: <= 2 active officers total
   - Calculation: COUNT(active officers) <= 2

6. **dormant_succession** (score 72)
   - Condition: No director appointments in past 5 years
   - Calculation: MAX(appointed_on) < now - 5 years

7. **complexity_fatigue** (score 68)
   - Condition: Officer linked to 5+ active companies
   - Calculation: COUNT(distinct companies for officer) >= 5

**Seller Score Composite** (0-100):
- Weighted average of signal scores
- Weights:
  - aging_founder: 25%
  - long_tenure: 20%
  - single_director: 18%
  - founder_era: 15%
  - concentrated_leadership: 12%
  - dormant_succession: 18%
  - complexity_fatigue: 10%

### Graph Module (`app/graph/build_edges.py`)

**Edge Types**:

1. **officer→company** (edge_type: "appoints")
   - Weight: appointment_count (capped at 5)
   - Metadata: latest_appointment, appointment_count

2. **psc→company** (edge_type: "owns")
   - Weight: 1.0 (basic), 1.5 (ownership), 2.0 (voting)
   - Metadata: control_natures, notified_on, psc_kind

3. **officer↔officer** (edge_type: "co_serves")
   - Weight: 1.0
   - Metadata: company_number, overlap dates
   - Deduplication: directed by ID ordering

4. **company↔company** (edge_type: "shared_officer")
   - Weight: incremented per shared officer (1.0 per officer)
   - Metadata: officer_id
   - Deduplication: company pair ordering

### Workers Module (`app/workers/queue_worker.py`)

**Job Queue Architecture**:
- Database-backed queue (enrichment_jobs table)
- Priority field for job ordering
- Status tracking: pending → in_progress → completed/failed
- Retry logic: max 3 attempts, exponential backoff
- Dead letter queue: jobs marked failed after 3 attempts

**Job Types**:
- company_profile_fetch
- officers_fetch
- filings_fetch
- signal_recompute
- normalize_officers

**Processing Pattern**:
1. Query pending jobs ordered by priority DESC, queued_at ASC
2. Mark as in_progress, set started_at
3. Execute operation (idempotent check before re-fetch)
4. If success: status = completed, finished_at = now
5. If failure: attempt_count++, retry decision (< 3? requeue : mark failed)

## Performance Characteristics

### Ingestion
- Bulk companies: ~50k records/minute (100k chunks, COPY via psycopg2)
- Bulk PSC: ~30k records/minute (JSONL parsing)
- Memory: ~500MB for 100k chunk processing

### Enrichment
- API calls: 400 req/min (rate limit)
- Officers fetch: ~1 sec per company (35 items/page, paginated)
- Filings fetch: ~2 sec per company (100 items/page, paginated)
- Throughput: ~24 companies/hour per worker at rate limit

### Signal Computation
- Single company: ~50ms (aggregation queries)
- Batch (1000 companies): ~60 seconds
- Graph edges: ~100k edges/minute

### Query Performance
- v_likely_sellers: <1s (indexed signal_score)
- v_aging_founders: <2s (birth_year index)
- v_succession_risk: <3s (multi-table join with subquery)
- Full company table scan: ~5s with aggregates

## Scalability Limits

### Vertical (per host)
- PostgreSQL: 100M+ records (16GB RAM, SSD)
- Redis: 10k jobs in queue (2GB RAM)

### Horizontal (distributed)
- Multi-worker processing: add queue_worker instances (share Redis + PostgreSQL)
- Read replicas: add read-only PostgreSQL followers for analytics
- Bulk ingest: parallelizable by postal code or sector

## Cost Optimization

### API Calls
- Targeted scope: enrich only high-potential companies (10k/month vs 100k)
- Delta updates: only update if data older than 30 days
- Batch processing: group companies by sector to reduce per-company overhead

### Storage
- Bulk data deleted after successful ingest (keep only CSV hash)
- Materialized views: refresh on schedule (nightly)
- Archive filings older than 5 years to separate table

### Compute
- Batch signal computation: once daily instead of per-company
- Job prioritization: favor high-value targets
- Time-of-day scheduling: run bulk ops during off-peak

## Security Considerations

- API key stored in .env (not in code)
- Raw psycopg2 connection closed immediately after COPY
- JSONB payloads sanitized before storage
- No PII in signal explanations
- Database credentials from environment (rotate regularly)

## Data Quality

### Validation Rules
- Company number: 8 chars alphanumeric, uppercase
- Officer name: non-empty, < 255 chars
- Dates: ISO format YYYY-MM-DD, sanity checked
- SIC codes: valid 5-digit codes or null

### Error Handling
- Invalid rows logged with context, skipped
- Failed API calls retry up to 3 times
- Partial ingestion allowed (transaction per company)
- Checksum validation on bulk files

## Testing Strategy

### Unit Tests
- normalize_name() function
- confidence_score_calculation()
- signal_computation() for each type

### Integration Tests
- bulk_ingest with sample CSV
- API client with mock responses
- Database migrations on clean DB

### Performance Tests
- bulk_companies: 100k rows in < 2 minutes
- signal_computation: 10k companies in < 10 minutes
- API rate limiter: exactly 400 req/min

## Monitoring & Observability

### Metrics
- Ingest rate (rows/minute)
- Enrichment backlog (pending job count)
- API success rate (% completed vs failed)
- Signal score distribution

### Logs
- Loguru configured with rotation
- JSON-formatted for parsing
- Trace job IDs across system

### Dashboards (suggested)
- Queue depth by job type
- Enrichment completion rate
- Top sellers by score
- Sector opportunity map
- Officer co-service network

## Operational Runbooks

### Daily Operations
```bash
# Morning: process enrichment queue
python -c "from app.workers.queue_worker import process_pending_jobs; process_pending_jobs(500)"

# Evening: compute signals
python -c "from app.signals.seller_signals import compute_all_signals; compute_all_signals()"

# Refresh materialized views
psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_likely_sellers_materialized"
```

### Weekly
- Review failed jobs: `SELECT * FROM enrichment_jobs WHERE status = 'failed' LIMIT 20`
- Analyze slow queries: `SELECT * FROM pg_stat_statements ORDER BY mean_exec_time DESC`
- Archive completed jobs older than 7 days

### Monthly
- Bulk update all companies: queue all with priority 1
- Rebuild signal scores from scratch
- Tune PostgreSQL settings if needed
- Export seller targets to external system
