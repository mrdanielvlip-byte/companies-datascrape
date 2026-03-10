import time
from datetime import datetime

from loguru import logger
from redis import Redis
from rq import Queue, Worker
from rq.job import JobStatus

from app.config import get_settings
from app.db import get_session
from app.models import EnrichmentJob
from app.ingestion.api_officers import fetch_officers_for_company
from app.ingestion.api_company_profile import fetch_company_profile
from app.ingestion.api_filings import fetch_filings_for_company
from app.signals.seller_signals import store_signals_for_company
from app.normalization.normalize_officers import normalize_officers_for_company


def get_redis_connection():
    """Get Redis connection."""
    settings = get_settings()
    return Redis.from_url(settings.redis_url, decode_responses=True)


def get_job_queue(queue_name: str = "enrichment"):
    """Get job queue."""
    redis = get_redis_connection()
    return Queue(queue_name, connection=redis)


def process_enrichment_job(job_id: int, company_number: str, job_type: str) -> bool:
    """Process a single enrichment job."""
    logger.info(f"Processing job {job_id}: {job_type} for {company_number}")

    try:
        with get_session() as session:
            job = session.query(EnrichmentJob).filter_by(job_id=job_id).first()

            if not job:
                logger.error(f"Job {job_id} not found")
                return False

            # Check if already completed
            if job.status == "completed":
                logger.info(f"Job {job_id} already completed")
                return True

            # Mark as in_progress
            job.status = "in_progress"
            job.started_at = datetime.utcnow()
            session.commit()

        # Process based on job type
        success = False
        if job_type == "company_profile_fetch":
            success = fetch_company_profile(company_number)
        elif job_type == "officers_fetch":
            success = fetch_officers_for_company(company_number)
        elif job_type == "filings_fetch":
            success = fetch_filings_for_company(company_number)
        elif job_type == "signal_recompute":
            store_signals_for_company(company_number)
            success = True
        elif job_type == "normalize_officers":
            result = normalize_officers_for_company(company_number)
            success = result["processed"] > 0
        else:
            logger.error(f"Unknown job type: {job_type}")
            return False

        # If successful, job is already marked completed by the specific function
        # If not, we need to handle retry
        if not success:
            with get_session() as session:
                job = session.query(EnrichmentJob).filter_by(job_id=job_id).first()
                if job:
                    job.status = "failed"
                    job.attempt_count += 1
                    job.finished_at = datetime.utcnow()

                    # Retry up to 3 times
                    if job.attempt_count < 3:
                        job.status = "pending"
                        job.queued_at = datetime.utcnow()
                        logger.info(f"Re-queuing job {job_id} (attempt {job.attempt_count})")
                    else:
                        logger.error(f"Job {job_id} failed after {job.attempt_count} attempts")

                    session.commit()

        return success

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {e}")

        with get_session() as session:
            job = session.query(EnrichmentJob).filter_by(job_id=job_id).first()
            if job:
                job.status = "failed"
                job.last_error = str(e)
                job.attempt_count += 1
                job.finished_at = datetime.utcnow()

                if job.attempt_count < 3:
                    job.status = "pending"
                    logger.info(f"Re-queuing job {job_id}")

                session.commit()

        return False


def queue_enrichment_job(company_number: str, job_type: str, priority: int = 0):
    """Queue an enrichment job."""
    queue = get_job_queue()

    with get_session() as session:
        # Check if job already exists and is pending
        existing = (
            session.query(EnrichmentJob)
            .filter_by(company_number=company_number, job_type=job_type, status="pending")
            .first()
        )

        if existing:
            logger.debug(f"Job already queued for {company_number} {job_type}")
            return existing.job_id

        # Create new job record
        job = EnrichmentJob(
            company_number=company_number,
            job_type=job_type,
            status="pending",
            priority=priority,
            queued_at=datetime.utcnow(),
        )
        session.add(job)
        session.commit()

        logger.info(f"Queued job {job.job_id}: {job_type} for {company_number}")
        return job.job_id


def queue_enrichment_jobs(
    company_numbers: list[str],
    job_types: list[str] = None,
    priority: int = 0,
):
    """Queue enrichment jobs for multiple companies."""
    if job_types is None:
        job_types = ["company_profile_fetch", "officers_fetch", "filings_fetch"]

    queue = get_job_queue()
    queued_count = 0

    for company_number in company_numbers:
        for job_type in job_types:
            try:
                queue_enrichment_job(company_number, job_type, priority)
                queued_count += 1
            except Exception as e:
                logger.error(f"Error queuing job for {company_number}: {e}")

    logger.info(f"Queued {queued_count} enrichment jobs")
    return queued_count


def process_pending_jobs(max_jobs: int = 100):
    """Process pending enrichment jobs from the database queue."""
    with get_session() as session:
        # Get pending jobs, ordered by priority and queued_at
        pending_jobs = (
            session.query(EnrichmentJob)
            .filter_by(status="pending")
            .order_by(EnrichmentJob.priority.desc(), EnrichmentJob.queued_at)
            .limit(max_jobs)
            .all()
        )

        logger.info(f"Processing {len(pending_jobs)} pending jobs")

        for job in pending_jobs:
            try:
                success = process_enrichment_job(job.job_id, job.company_number, job.job_type)
                if success:
                    logger.info(f"Job {job.job_id} completed successfully")
                else:
                    logger.warning(f"Job {job.job_id} completed with errors")
            except Exception as e:
                logger.error(f"Error processing job {job.job_id}: {e}")

        session.close()


def start_worker(queue_name: str = "enrichment", num_workers: int = 4):
    """Start RQ worker."""
    redis = get_redis_connection()
    queue = Queue(queue_name, connection=redis)

    logger.info(f"Starting RQ worker with {num_workers} workers")

    worker = Worker([queue], connection=redis)
    worker.work()


def get_job_status(job_id: int) -> dict:
    """Get status of a job."""
    with get_session() as session:
        job = session.query(EnrichmentJob).filter_by(job_id=job_id).first()

        if not job:
            return {"status": "not_found"}

        return {
            "job_id": job.job_id,
            "company_number": job.company_number,
            "job_type": job.job_type,
            "status": job.status,
            "attempt_count": job.attempt_count,
            "last_error": job.last_error,
            "queued_at": job.queued_at.isoformat() if job.queued_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        }


def get_queue_stats() -> dict:
    """Get statistics about the enrichment queue."""
    with get_session() as session:
        pending = session.query(EnrichmentJob).filter_by(status="pending").count()
        in_progress = session.query(EnrichmentJob).filter_by(status="in_progress").count()
        completed = session.query(EnrichmentJob).filter_by(status="completed").count()
        failed = session.query(EnrichmentJob).filter_by(status="failed").count()

        return {
            "pending": pending,
            "in_progress": in_progress,
            "completed": completed,
            "failed": failed,
            "total": pending + in_progress + completed + failed,
        }


if __name__ == "__main__":
    # Example: process pending jobs continuously
    while True:
        try:
            process_pending_jobs(max_jobs=50)
            stats = get_queue_stats()
            logger.info(f"Queue stats: {stats}")
            time.sleep(30)  # Process every 30 seconds
        except Exception as e:
            logger.error(f"Error in job processor: {e}")
            time.sleep(60)
