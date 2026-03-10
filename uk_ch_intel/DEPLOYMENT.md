# Deployment & Operations Guide

## Quick Start (5 minutes)

```bash
# 1. Navigate to platform directory
cd /sessions/vibrant-modest-einstein/ch-pe-sourcing/uk_ch_intel

# 2. Copy and configure environment
cp .env.example .env
# Edit .env and add your CH_API_KEY from https://developer.companieshouse.gov.uk

# 3. Start services (PostgreSQL + Redis)
docker-compose up -d

# 4. Wait for health checks
sleep 10 && docker-compose ps

# 5. Install Python dependencies
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 6. Initialize database
python -m scripts.bootstrap create-tables

# 7. Download bulk data (one-time, ~30 minutes)
python -m scripts.bootstrap download-bulk-companies
python -m scripts.bootstrap download-bulk-psc

# 8. Ingest data (one-time, ~1 hour)
python -m scripts.bootstrap ingest-companies
python -m scripts.bootstrap ingest-psc

# 9. Queue initial enrichment (for a sample)
python scripts/refresh_targets.py high-potential-sellers --count 100

# 10. Process enrichment jobs
python -c "from app.workers.queue_worker import process_pending_jobs; [process_pending_jobs(100) for _ in range(10)]"

# 11. Compute signals
python -c "from app.signals.seller_signals import compute_all_signals; compute_all_signals()"

# 12. Query results
psql -h localhost -U chuser -d ch_intel -c "SELECT * FROM v_likely_sellers LIMIT 10;"
```

## Production Deployment

### Docker Compose (Recommended)

```bash
# Create separate docker-compose.prod.yml with:
# - persistent volumes
# - resource limits
# - restart policies
# - environment variable management

docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### Systemd Service (for continuous worker)

Create `/etc/systemd/system/ch-intel-worker.service`:

```ini
[Unit]
Description=UK Companies House Intelligence Platform Worker
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/path/to/uk_ch_intel
Environment=DATABASE_URL=postgresql://chuser:chpass@localhost:5432/ch_intel
Environment=REDIS_URL=redis://localhost:6379/0
Environment=CH_API_KEY=your-key
ExecStart=/path/to/venv/bin/python -c "from app.workers.queue_worker import process_pending_jobs; import time; __import__('time').sleep(100000000)"
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl daemon-reload
sudo systemctl enable ch-intel-worker
sudo systemctl start ch-intel-worker
```

### Kubernetes Deployment (Advanced)

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ch-intel-enrichment
spec:
  schedule: "*/30 * * * *"  # Every 30 minutes
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: worker
            image: python:3.11
            command:
            - python
            - -c
            - |
              from app.workers.queue_worker import process_pending_jobs
              process_pending_jobs(500)
            env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: ch-intel-secrets
                  key: database-url
            - name: CH_API_KEY
              valueFrom:
                secretKeyRef:
                  name: ch-intel-secrets
                  key: api-key
          restartPolicy: OnFailure
```

## Monitoring

### Check Service Health

```bash
# PostgreSQL
docker-compose exec postgres pg_isready -U chuser

# Redis
docker-compose exec redis redis-cli ping

# Python health check
python -c "from app.db import get_engine; print('DB OK' if get_engine().execute('SELECT 1').fetchone() else 'FAIL')"
```

### Queue Monitoring

```bash
# Check pending jobs
psql -c "SELECT status, COUNT(*) FROM enrichment_jobs GROUP BY status;"

# Monitor enrichment progress
watch -n 5 'psql -c "SELECT status, COUNT(*) FROM enrichment_jobs GROUP BY status;"'

# View failed jobs
psql -c "SELECT job_id, company_number, job_type, last_error FROM enrichment_jobs WHERE status = 'failed' LIMIT 20;"
```

### Signal Quality

```bash
# Distribution of seller scores
psql -c "SELECT 
  ROUND(signal_score / 10) * 10 as score_bucket,
  COUNT(*) as count
FROM company_signals
WHERE signal_type = 'seller_score'
GROUP BY score_bucket
ORDER BY score_bucket;"
```

## Backup & Recovery

### PostgreSQL Backup

```bash
# Full backup
docker-compose exec postgres pg_dump -U chuser ch_intel > backup_$(date +%Y%m%d).sql

# Compressed backup
docker-compose exec postgres pg_dump -U chuser ch_intel | gzip > backup_$(date +%Y%m%d).sql.gz

# Scheduled daily backups
0 2 * * * docker-compose exec postgres pg_dump -U chuser ch_intel | gzip > /backups/ch_intel_$(date +\%Y\%m\%d).sql.gz
```

### Recovery

```bash
# Restore from backup
docker-compose exec -T postgres psql -U chuser -d ch_intel < backup_20240110.sql

# Or with gzip
gunzip -c backup_20240110.sql.gz | docker-compose exec -T postgres psql -U chuser -d ch_intel
```

### Redis Data

```bash
# Redis persistence is enabled by default (AOF)
# Data automatically saved to redis_data volume

# Manual backup
docker-compose exec redis redis-cli BGSAVE
```

## Updates & Maintenance

### Update Code

```bash
# Pull latest changes
git pull

# Update dependencies
pip install -r requirements.txt --upgrade

# Run migrations (if any new migrations)
python -m scripts.bootstrap create-tables
```

### Database Maintenance

```bash
# Analyze query performance
psql -c "ANALYZE;"

# Clean up deleted rows
psql -c "VACUUM ANALYZE;"

# Refresh materialized views
psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_likely_sellers_materialized;"

# Check index bloat
psql -c "SELECT schemaname, tablename, indexname 
         FROM pg_indexes 
         WHERE schemaname NOT IN ('pg_catalog', 'information_schema');"
```

### Clear Old Data

```bash
# Archive and delete old enrichment jobs (older than 30 days)
psql << SQL
BEGIN;
CREATE TABLE enrichment_jobs_archive AS
SELECT * FROM enrichment_jobs
WHERE finished_at < NOW() - INTERVAL '30 days';

DELETE FROM enrichment_jobs
WHERE finished_at < NOW() - INTERVAL '30 days';

COMMIT;
SQL

# Cleanup ingest runs older than 90 days
psql -c "DELETE FROM ingest_runs WHERE completed_at < NOW() - INTERVAL '90 days';"
```

## Troubleshooting

### Out of Disk Space

```bash
# Check usage
docker system df

# Clean up old data
# 1. Archive ingest_runs and company_signals
# 2. Remove old PostgreSQL backups
# 3. Clear Redis cache

# Or expand volume
docker volume create --driver local ch_intel_postgres_data_large
# Then migrate data...
```

### Slow Queries

```bash
# Identify slow queries
psql << SQL
SELECT query, calls, mean_exec_time, max_exec_time
FROM pg_stat_statements
WHERE mean_exec_time > 1000  -- >1 second
ORDER BY mean_exec_time DESC
LIMIT 10;
SQL

# Run ANALYZE on slowest tables
psql -c "ANALYZE companies; ANALYZE appointments; ANALYZE company_signals;"
```

### Memory Issues

```bash
# PostgreSQL using too much memory
# Reduce in docker-compose.yml:
# - shared_buffers: 256MB (was 512MB)
# - effective_cache_size: 1GB (was 3GB)

# Restart containers
docker-compose down
docker-compose up -d
```

### API Rate Limiting

```bash
# If hitting 429 errors:
# 1. Reduce MAX_WORKERS in .env
# 2. Increase API_RATE_LIMIT_PER_MINUTE slightly if your quota allows
# 3. Check queue for retries
psql -c "SELECT COUNT(*) FROM enrichment_jobs WHERE status = 'pending' AND attempt_count > 0;"
```

## Cost Optimization

### Reduce API Calls

```python
# Instead of full enrichment, use delta updates:
# Only enrich companies modified in past 30 days

from app.db import get_session
from app.models import Company
from app.workers.queue_worker import queue_enrichment_jobs

with get_session() as session:
    recent = session.query(Company.company_number).filter(
        Company.updated_at > datetime.utcnow() - timedelta(days=30),
        Company.company_status == 'Active'
    ).limit(1000).all()
    
    company_numbers = [c[0] for c in recent]
    queue_enrichment_jobs(company_numbers, ['officers_fetch', 'filings_fetch'])
```

### Batch Processing

```bash
# Process all jobs in one go rather than continuously
python -c "from app.workers.queue_worker import process_pending_jobs; process_pending_jobs(5000)"

# Then rest for 2 hours to avoid constant API usage
```

### Storage Cleanup

```bash
# Keep only latest 2 weeks of raw bulk data
find /sessions/vibrant-modest-einstein/ch-pe-sourcing/uk_ch_intel/data/raw -name "*.csv" -mtime +14 -delete
find /sessions/vibrant-modest-einstein/ch-pe-sourcing/uk_ch_intel/data/raw -name "*.zip" -mtime +14 -delete
```

## Logging & Debugging

### View Logs

```bash
# Application logs (via loguru)
tail -f logs/ch_intel.log

# Docker logs
docker-compose logs -f postgres
docker-compose logs -f redis
docker-compose logs -f app

# Specific service
docker-compose logs -f --tail=100 postgres
```

### Enable Debug Logging

```bash
# Modify .env
LOG_LEVEL=DEBUG

# Restart services
docker-compose restart
```

### Database Query Logging

```bash
# Connect to postgres with verbose output
psql -h localhost -U chuser -d ch_intel -v VERBOSITY=VERBOSE

# Or enable in PostgreSQL
# ALTER DATABASE ch_intel SET log_statement = 'all';
```

## Integration with CI/CD

### GitHub Actions Example

```yaml
name: Test CH Intel Platform

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15-alpine
        env:
          POSTGRES_USER: chuser
          POSTGRES_PASSWORD: chpass
          POSTGRES_DB: ch_intel
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - run: pip install -r requirements.txt
    - run: python -m scripts.bootstrap create-tables
    - run: python -m pytest tests/
```

## Performance Benchmarks

### Expected Performance

| Operation | Throughput | Duration |
|-----------|-----------|----------|
| Bulk companies ingest | 50k/min | 3+ hours (10M companies) |
| Bulk PSC ingest | 30k/min | 1+ hour (2M records) |
| API officer fetch | ~1.5 sec/company | 24 companies/hour @ 400 req/min |
| API filing fetch | ~2 sec/company | 18 companies/hour @ 400 req/min |
| Signal computation | 50ms/company | 10k companies in ~10 min |
| v_likely_sellers query | <1s | 100k company database |

### Optimization Tips

1. **Parallelize enrichment**: Run multiple worker processes
2. **Batch signal computation**: Once daily instead of per-company
3. **Use materialized views**: For frequently queried data
4. **Archive old jobs**: Keep enrichment_jobs table lean
5. **Partition by postal code**: For very large datasets (100M+ companies)

## Security Checklist

- [ ] .env file is in .gitignore (never commit secrets)
- [ ] Database password is strong (20+ chars, random)
- [ ] API key is rotated monthly
- [ ] PostgreSQL runs with minimal privileges
- [ ] Redis runs without password (if on private network) or with strong password
- [ ] Docker containers run as non-root (optional but recommended)
- [ ] HTTPS used for any external API calls
- [ ] Database backups are encrypted
- [ ] Access logs are monitored

## Contact & Support

For issues or questions:
1. Check logs: `docker-compose logs`
2. Review queue: `psql -c "SELECT * FROM v_queue_status;"`
3. Test database: `python -c "from app.db import get_session; print(get_session())"
