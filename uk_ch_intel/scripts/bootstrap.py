#!/usr/bin/env python
import sys
from pathlib import Path
from datetime import datetime

import click
from loguru import logger
from sqlalchemy import text

from app.config import get_settings
from app.db import get_engine, get_session
from app.ingestion.bulk_companies import ingest_bulk_companies
from app.ingestion.bulk_psc import ingest_bulk_psc
from app.workers.queue_worker import queue_enrichment_jobs
from app.models import Company


def init_db():
    """Initialize database schema."""
    logger.info("Initializing database schema...")

    # Read and execute migration file
    migration_file = Path(__file__).parent.parent / "migrations" / "001_initial_schema.sql"

    if not migration_file.exists():
        logger.error(f"Migration file not found: {migration_file}")
        return False

    try:
        with open(migration_file, "r") as f:
            sql_script = f.read()

        engine = get_engine()
        with engine.connect() as conn:
            # Split into individual statements
            statements = sql_script.split(";")
            for statement in statements:
                if statement.strip():
                    conn.execute(text(statement))
            conn.commit()

        logger.info("Database schema initialized successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return False


@click.group()
def cli():
    """UK Companies House Intelligence Platform bootstrap script."""
    pass


@cli.command()
def create_tables():
    """Create all database tables."""
    click.echo("Creating database tables...")
    if init_db():
        click.echo("Database tables created successfully!")
    else:
        click.echo("Failed to create database tables")
        sys.exit(1)


@cli.command()
@click.option("--force", is_flag=True, help="Force re-download even if file exists")
def download_bulk_companies(force):
    """Download bulk companies data."""
    click.echo("Downloading bulk companies data...")
    try:
        from app.ingestion.bulk_companies import download_bulk_companies
        path = download_bulk_companies(force=force)
        click.echo(f"Companies data downloaded to {path}")
    except Exception as e:
        click.echo(f"Failed to download companies data: {e}")
        sys.exit(1)


@cli.command()
@click.option("--force", is_flag=True, help="Force re-download even if file exists")
def download_bulk_psc(force):
    """Download bulk PSC data."""
    click.echo("Downloading bulk PSC data...")
    try:
        from app.ingestion.bulk_psc import download_bulk_psc
        path = download_bulk_psc(force=force)
        click.echo(f"PSC data downloaded to {path}")
    except Exception as e:
        click.echo(f"Failed to download PSC data: {e}")
        sys.exit(1)


@cli.command()
def ingest_companies():
    """Ingest bulk companies data into database."""
    click.echo("Ingesting bulk companies data...")
    try:
        run = ingest_bulk_companies()
        click.echo(f"Companies ingestion complete:")
        click.echo(f"  Processed: {run.rows_processed}")
        click.echo(f"  Inserted: {run.rows_inserted}")
        click.echo(f"  Updated: {run.rows_updated}")
    except Exception as e:
        click.echo(f"Failed to ingest companies data: {e}")
        sys.exit(1)


@cli.command()
def ingest_psc():
    """Ingest bulk PSC data into database."""
    click.echo("Ingesting bulk PSC data...")
    try:
        run = ingest_bulk_psc()
        click.echo(f"PSC ingestion complete:")
        click.echo(f"  Processed: {run.rows_processed}")
        click.echo(f"  Inserted: {run.rows_inserted}")
        click.echo(f"  Updated: {run.rows_updated}")
    except Exception as e:
        click.echo(f"Failed to ingest PSC data: {e}")
        sys.exit(1)


@cli.command()
@click.option("--sic-codes", multiple=True, help="SIC codes to target (can be specified multiple times)")
@click.option("--count", type=int, default=100, help="Number of companies to queue per SIC code")
def seed_enrichment_jobs(sic_codes, count):
    """Seed enrichment jobs for targeted sectors."""
    click.echo(f"Seeding enrichment jobs for SIC codes: {sic_codes}, count per sector: {count}")

    with get_session() as session:
        if not sic_codes:
            # If no SIC codes specified, queue jobs for all companies with null enrichment
            companies = session.query(Company.company_number).filter(
                Company.company_status == "Active"
            ).limit(count).all()
        else:
            # Queue jobs for companies with specified SIC codes
            companies = []
            for sic_code in sic_codes:
                matched = session.query(Company.company_number).filter(
                    Company.company_status == "Active",
                    Company.sic_codes.contains([sic_code])
                ).limit(count).all()
                companies.extend(matched)

        company_numbers = [c[0] for c in companies]
        click.echo(f"Found {len(company_numbers)} companies to enrich")

        if company_numbers:
            job_types = ["company_profile_fetch", "officers_fetch", "filings_fetch"]
            queued = queue_enrichment_jobs(company_numbers, job_types, priority=5)
            click.echo(f"Queued {queued} enrichment jobs")
        else:
            click.echo("No companies found")


@cli.command()
def full_bootstrap():
    """Run full bootstrap: create tables, download data, ingest, and seed jobs."""
    click.echo("Starting full bootstrap...")

    steps = [
        ("Creating database tables", create_tables),
        ("Downloading bulk companies data", download_bulk_companies),
        ("Ingesting companies data", ingest_companies),
        ("Downloading bulk PSC data", download_bulk_psc),
        ("Ingesting PSC data", ingest_psc),
        ("Seeding enrichment jobs", lambda: seed_enrichment_jobs([], 1000)),
    ]

    for step_name, step_func in steps:
        try:
            click.echo(f"\n{step_name}...")
            if callable(step_func) and step_func.__name__ in ['download_bulk_companies', 'ingest_companies']:
                ctx = click.Context(step_func)
                ctx.invoke(step_func)
            else:
                step_func()
        except Exception as e:
            click.echo(f"Error during {step_name}: {e}")
            click.echo("Bootstrap aborted")
            sys.exit(1)

    click.echo("\nBootstrap complete!")

    # Print summary
    with get_session() as session:
        company_count = session.query(Company).count()
        click.echo(f"\nSummary:")
        click.echo(f"  Total companies: {company_count}")


if __name__ == "__main__":
    cli()
