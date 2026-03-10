# UK Companies House Intelligence Platform

A production-grade intelligence platform for analyzing UK Companies House data with targeted delta enrichment for PE sourcing and deal analysis.

## Architecture Overview

The platform implements a multi-stage pipeline:

1. **Bulk Data Ingestion**: Download and ingest baseline companies and PSC data from Companies House bulk datasets
2. **Normalized Data Warehouse**: PostgreSQL-based schema with full company, officer, and filing history
3. **Targeted Enrichment**: Queue-based worker system for selective API calls to Companies House API
4. **Signal Engine**: Compute seller opportunity signals based on company structure, leadership, and history
5. **Graph Edges**: Build relationship networks connecting officers, companies, and PSCs

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Companies House API key (from https://developer.companieshouse.gov.uk)
- 10GB+ disk space for bulk data

## Quick Start

### 1. Clone and Setup

```bash
cd /sessions/vibrant-modest-einstein/ch-pe-sourcing/uk_ch_intel
cp .env.example .env
# Edit .env and add your CH_API_KEY
```

### 2. Start Services

```bash
docker-compose up -d
# Wait for containers to be healthy
docker-compose ps
```

### 3. Install Python Dependencies

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Run Bootstrap

```bash
python scripts/bootstrap.py full-bootstrap
```

This will:
- Create all database tables
- Download bulk companies data (500MB+)
- Download bulk PSC data
- Ingest into PostgreSQL
- Queue initial enrichment jobs

## Directory Structure

```
uk_ch_intel/
├── app/
│   ├── config.py              # Configuration management
│   ├── db.py                  # Database connections
│   ├── models/
│   │   ├── schema.py          # SQLAlchemy ORM models
│   │   └── __init__.py
│   ├── ingestion/
│   │   ├── bulk_companies.py  # Bulk companies download & ingest
│   │   ├── bulk_psc.py        # Bulk PSC download & ingest
│   │   ├── api_officers.py    # Officers API client
│   │   ├── api_company_profile.py
│   │   ├── api_filings.py
│   │   └── __init__.py
│   ├── normalization/
│   │   ├── normalize_officers.py  # Officer name resolution
│   │   └── __init__.py
│   ├── signals/
│   │   ├── seller_signals.py      # Seller opportunity signals
│   │   └── __init__.py
│   ├── graph/
│   │   ├── build_edges.py     # Company relationship graph
│   │   └── __init__.py
│   ├── workers/
│   │   ├── queue_worker.py    # Job queue processor
│   │   └── __init__.py
│   ├── analytics/
│   │   ├── views.sql          # SQL analytical views
│   │   └── __init__.py
│   └── __init__.py
├── scripts/
│   ├── bootstrap.py           # Setup and initialization
│   ├── refresh_targets.py     # Queue enrichment jobs
│   └── __init__.py
├── migrations/
│   ├── 001_initial_schema.sql # Database DDL
│   └── __init__.py
├── docker-compose.yml         # Service definitions
├── requirements.txt           # Python dependencies
├── .env.example              # Configuration template
└── README.md                 # This file
```

## Key Operations

### Ingest Bulk Companies Data

```bash
# Download and ingest (slow, one-time only)
python -m scripts.bootstrap ingest-companies

# Or just download
python -m scripts.bootstrap download-bulk-companies
```

Reads 10M+ companies from Companies House CSV, chunks into 100k-row batches, bulk-inserts via COPY.

### Queue Enrichment Jobs

```bash
# For specific SIC codes
python scripts/refresh_targets.py by-sic-codes --sic-codes "62010" --sic-codes "62011" --count 100

# For specific company numbers
python scripts/refresh_targets.py by-company-numbers \
  --company-numbers "00000191" \
  --company-numbers "02009155"

# For postal code regions
python scripts/refresh_targets.py by-postal-codes \
  --postal-codes "SW1" --postal-codes "M1" --count 50

# From JSON file
python scripts/refresh_targets.py from-json-file --json-file targets.json

# For high-potential sellers (seller_score >= 70)
python scripts/refresh_targets.py high-potential-sellers --min-seller-score 75 --count 500
```

### Process Enrichment Queue

```bash
# One-shot processing (processes up to 100 pending jobs)
python -c "from app.workers.queue_worker import process_pending_jobs; process_pending_jobs()"

# Or run continuously with Docker
docker run -it --rm \
  --network ch_intel_network \
  -e DATABASE_URL=postgresql://chuser:chpass@postgres:5432/ch_intel \
  -e REDIS_URL=redis://redis:6379/0 \
  -e CH_API_KEY=your-key \
  -v /path/to/uk_ch_intel:/app \
  python:3.11 \
  python -c "from app.workers.queue_worker import process_pending_jobs; import time; \
             [process_pending_jobs(100) for _ in [None] * 1000 or (time.sleep(30),)]"
```

### Compute Seller Signals

After enriching officers and filings:

```bash
# Compute signals for all companies with enriched data
python -c "from app.signals.seller_signals import compute_all_signals; compute_all_signals()"
```

This computes:
- **aging_founder**: Active director aged 55+
- **long_tenure**: Director appointed 15+ years ago
- **single_director**: Only one active director
- **founder_era**: Incorporated 15+ years ago
- **concentrated_leadership**: ≤2 active officers
- **dormant_succession**: No appointments in past 5 years
- **complexity_fatigue**: Officer linked to 5+ companies
- **seller_score**: Weighted composite (0-100)

### Build Company Graph

After normalizing officers:

```bash
# Build all relationship edges
python -c "from app.graph.build_edges import build_all_edges; build_all_edges()"
```

Creates edges:
- **officer→company**: Appointment relationships
- **psc→company**: Ownership relationships
- **officer↔officer**: Co-service connections
- **company↔company**: Shared officer clusters

## Key Queries

### Find Likely Sellers (High Opportunity Score)

```sql
SELECT * FROM v_likely_sellers
WHERE seller_score >= 70
ORDER BY seller_score DESC
LIMIT 20;
```

### Find Aging Founders

```sql
SELECT * FROM v_aging_founders
WHERE estimated_age >= 60
ORDER BY estimated_age DESC;
```

### Identify Succession Risk

```sql
SELECT * FROM v_succession_risk
ORDER BY years_old DESC
LIMIT 50;
```

### Map Serial Operators

```sql
SELECT * FROM v_serial_operators
WHERE active_company_count >= 3
ORDER BY active_company_count DESC;
```

### Sector Analysis by Region

```sql
SELECT postal_region, primary_sic_code, company_count, avg_seller_score
FROM v_sector_map
WHERE company_count > 10
ORDER BY avg_seller_score DESC;
```

### Family Business Clusters

```sql
SELECT family_name, company_count, company_names
FROM v_family_clusters
WHERE company_count >= 3
ORDER BY company_count DESC;
```

### Queue Status

```sql
SELECT * FROM v_queue_status;
```

## Integration with PE Sourcing Pipeline

This platform can be integrated with your existing deal sourcing workflow:

### 1. Export High-Potential Targets

```bash
python scripts/refresh_targets.py export-targets \
  --output-file targets.json \
  --min-seller-score 75 \
  --count 1000
```

### 2. Load into External System

```python
import json
with open('targets.json') as f:
    targets = json.load(f)
    # Import into Salesforce, HubSpot, etc.
```

### 3. Refresh and Update Scoring

As new companies are enriched, re-run signals:

```bash
python -c "from app.signals.seller_signals import compute_all_signals; compute_all_signals()"
```

### 4. Create Enriched CSV for Brokers/Advisors

```sql
COPY (
  SELECT
    c.company_number,
    c.company_name,
    c.postal_code,
    c.sic_codes[1] as primary_sic,
    (EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM c.incorporation_date))::INT as age_years,
    COUNT(DISTINCT a.officer_id) as officer_count,
    cs.signal_score as seller_score,
    c.accounts_last_made_up_to
  FROM companies c
  LEFT JOIN company_signals cs ON c.company_number = cs.company_number AND cs.signal_type = 'seller_score'
  LEFT JOIN appointments a ON c.company_number = a.company_number AND a.is_current = TRUE
  WHERE c.company_status = 'Active'
    AND cs.signal_score >= 70
  GROUP BY c.company_number, c.company_name, c.postal_code, c.sic_codes, c.incorporation_date, cs.signal_score
  ORDER BY cs.signal_score DESC
) TO '/tmp/seller_targets.csv' WITH (FORMAT CSV, HEADER);
```

## Environment Variables

```env
# Database
DATABASE_URL=postgresql://chuser:chpass@localhost:5432/ch_intel

# Cache/Queue
REDIS_URL=redis://localhost:6379/0

# API Authentication
CH_API_KEY=your-companies-house-api-key

# Enrichment Strategy
ENRICHMENT_SCOPE=targeted  # targeted, delta, or full

# Worker Configuration
MAX_WORKERS=4
API_RATE_LIMIT_PER_MINUTE=400

# Data Storage
BULK_DATA_DIR=./data/raw

# Logging
LOG_LEVEL=INFO
```

## Performance Tuning

### PostgreSQL

For production with large datasets:

```sql
-- In docker-compose.yml or postgresql.conf
shared_buffers = 1GB
effective_cache_size = 3GB
maintenance_work_mem = 256MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 32MB
```

### Bulk Operations

- Increase `chunksize` in `bulk_companies.py` for faster loading (trade memory)
- Parallel workers in queue processor can be increased with `MAX_WORKERS`
- API rate limits can be tuned with `API_RATE_LIMIT_PER_MINUTE`

## Troubleshooting

### "Connection refused" on database

```bash
# Check containers are running
docker-compose ps

# Check logs
docker-compose logs postgres
docker-compose logs redis
```

### Out of memory during bulk ingest

Reduce chunk size in `bulk_companies.py`:

```python
for chunk in pd.read_csv(csv_path, chunksize=50000):  # From 100000
```

### API rate limiting

Increase sleep between requests or reduce `MAX_WORKERS`. The built-in token bucket will handle 429 responses.

### Stale data

Re-run the job queue processor or manually re-queue companies:

```python
from app.workers.queue_worker import queue_enrichment_job
queue_enrichment_job("00000191", "officers_fetch")
```

## Database Maintenance

### Refresh Materialized Views

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_likely_sellers_materialized;
```

### Analyze & Vacuum

```bash
# Connect to database
psql -h localhost -U chuser -d ch_intel

# Analyze query performance
ANALYZE;

# Reclaim space
VACUUM ANALYZE;
```

### Monitor Query Performance

```sql
-- Slow queries
SELECT query, mean_exec_time, calls
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;
```

## API Rate Limits

Companies House API provides 400 requests per minute. The platform:

1. Implements token bucket rate limiting
2. Respects `Retry-After` headers on 429 responses
3. Uses exponential backoff for 500/502/503 errors
4. Queues failed jobs for retry (up to 3 attempts)

## License

This platform is designed for internal PE sourcing operations. Ensure compliance with Companies House data usage terms.

## Support

For issues or questions:

1. Check logs: `docker-compose logs -f`
2. Review queue status: `SELECT * FROM v_queue_status;`
3. Inspect failed jobs: `SELECT * FROM enrichment_jobs WHERE status = 'failed';`
