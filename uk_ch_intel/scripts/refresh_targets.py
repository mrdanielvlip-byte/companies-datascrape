#!/usr/bin/env python
import json
import sys
from pathlib import Path

import click
from loguru import logger

from app.db import get_session
from app.models import Company
from app.workers.queue_worker import queue_enrichment_jobs


@click.group()
def cli():
    """Refresh targeting and queue enrichment for specific companies."""
    pass


@cli.command()
@click.option("--sic-codes", multiple=True, required=True, help="SIC codes to target")
@click.option("--count", type=int, default=100, help="Number of companies per SIC code")
@click.option("--job-types", multiple=True, default=["company_profile_fetch", "officers_fetch", "filings_fetch"],
              help="Job types to queue")
def by_sic_codes(sic_codes, count, job_types):
    """Queue enrichment jobs for companies with specific SIC codes."""
    click.echo(f"Queuing enrichment for SIC codes: {sic_codes}")

    with get_session() as session:
        all_companies = []

        for sic_code in sic_codes:
            companies = session.query(Company.company_number).filter(
                Company.company_status == "Active",
                Company.sic_codes.contains([sic_code])
            ).limit(count).all()

            company_numbers = [c[0] for c in companies]
            all_companies.extend(company_numbers)

            click.echo(f"  SIC {sic_code}: {len(company_numbers)} companies")

        unique_companies = list(set(all_companies))
        click.echo(f"Total unique companies: {len(unique_companies)}")

        if unique_companies:
            queued = queue_enrichment_jobs(unique_companies, list(job_types), priority=5)
            click.echo(f"Queued {queued} enrichment jobs")
        else:
            click.echo("No companies found")


@cli.command()
@click.option("--company-numbers", multiple=True, required=True, help="Company numbers to refresh")
@click.option("--job-types", multiple=True, default=["company_profile_fetch", "officers_fetch", "filings_fetch"],
              help="Job types to queue")
def by_company_numbers(company_numbers, job_types):
    """Queue enrichment jobs for specific company numbers."""
    click.echo(f"Queuing enrichment for {len(company_numbers)} company numbers")

    with get_session() as session:
        # Verify companies exist
        existing = session.query(Company).filter(
            Company.company_number.in_(company_numbers)
        ).count()

        click.echo(f"Found {existing} companies in database")

        if existing > 0:
            queued = queue_enrichment_jobs(list(company_numbers), list(job_types), priority=5)
            click.echo(f"Queued {queued} enrichment jobs")
        else:
            click.echo("No companies found")


@cli.command()
@click.option("--postal-codes", multiple=True, required=True, help="Postal code prefixes (e.g., 'SW1', 'M1')")
@click.option("--count", type=int, default=50, help="Number of companies per postal code")
@click.option("--job-types", multiple=True, default=["company_profile_fetch", "officers_fetch", "filings_fetch"],
              help="Job types to queue")
def by_postal_codes(postal_codes, count, job_types):
    """Queue enrichment jobs for companies in specific postal code regions."""
    click.echo(f"Queuing enrichment for postal codes: {postal_codes}")

    with get_session() as session:
        all_companies = []

        for postal_code in postal_codes:
            companies = session.query(Company.company_number).filter(
                Company.company_status == "Active",
                Company.postal_code.startswith(postal_code)
            ).limit(count).all()

            company_numbers = [c[0] for c in companies]
            all_companies.extend(company_numbers)

            click.echo(f"  Postal {postal_code}: {len(company_numbers)} companies")

        unique_companies = list(set(all_companies))
        click.echo(f"Total unique companies: {len(unique_companies)}")

        if unique_companies:
            queued = queue_enrichment_jobs(unique_companies, list(job_types), priority=4)
            click.echo(f"Queued {queued} enrichment jobs")


@cli.command()
@click.option("--json-file", type=click.Path(exists=True), required=True,
              help="JSON file with company targets")
@click.option("--job-types", multiple=True, default=["company_profile_fetch", "officers_fetch", "filings_fetch"],
              help="Job types to queue")
def from_json_file(json_file, job_types):
    """Queue enrichment jobs from a JSON file."""
    click.echo(f"Loading targets from {json_file}")

    try:
        with open(json_file, "r") as f:
            data = json.load(f)

        if isinstance(data, list):
            company_numbers = data
        elif isinstance(data, dict):
            # Support different JSON structures
            company_numbers = data.get("company_numbers") or data.get("companies") or []
        else:
            click.echo("Invalid JSON structure")
            sys.exit(1)

        click.echo(f"Found {len(company_numbers)} companies in JSON file")

        if company_numbers:
            queued = queue_enrichment_jobs(company_numbers, list(job_types), priority=5)
            click.echo(f"Queued {queued} enrichment jobs")
        else:
            click.echo("No companies found in JSON file")

    except json.JSONDecodeError as e:
        click.echo(f"Failed to parse JSON file: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}")
        sys.exit(1)


@cli.command()
@click.option("--min-seller-score", type=float, default=70.0, help="Minimum seller score threshold")
@click.option("--count", type=int, default=500, help="Max number of companies to queue")
@click.option("--job-types", multiple=True, default=["officers_fetch", "filings_fetch"],
              help="Job types to queue")
def high_potential_sellers(min_seller_score, count, job_types):
    """Queue jobs for high-potential seller companies."""
    click.echo(f"Queuing enrichment for sellers with score >= {min_seller_score}")

    with get_session() as session:
        from app.models import CompanySignal

        # Get companies with high seller scores
        high_scorers = session.query(Company.company_number).join(
            CompanySignal, CompanySignal.company_number == Company.company_number
        ).filter(
            CompanySignal.signal_type == "seller_score",
            CompanySignal.signal_score >= min_seller_score,
            Company.company_status == "Active"
        ).limit(count).all()

        company_numbers = [c[0] for c in high_scorers]
        click.echo(f"Found {len(company_numbers)} high-potential sellers")

        if company_numbers:
            # Use higher priority for high-potential targets
            queued = queue_enrichment_jobs(company_numbers, list(job_types), priority=8)
            click.echo(f"Queued {queued} enrichment jobs (priority: 8)")


@cli.command()
@click.option("--output-file", type=click.Path(), default="targets_to_refresh.json",
              help="Output file for targets")
@click.option("--min-seller-score", type=float, default=70.0, help="Minimum seller score")
@click.option("--count", type=int, default=1000, help="Max results")
def export_targets(output_file, min_seller_score, count):
    """Export high-potential target companies to JSON file."""
    click.echo(f"Exporting targets with seller_score >= {min_seller_score}")

    with get_session() as session:
        from app.models import CompanySignal

        targets = session.query(
            Company.company_number,
            Company.company_name,
            Company.postal_code,
            Company.sic_codes,
            CompanySignal.signal_score
        ).join(
            CompanySignal, CompanySignal.company_number == Company.company_number
        ).filter(
            CompanySignal.signal_type == "seller_score",
            CompanySignal.signal_score >= min_seller_score,
            Company.company_status == "Active"
        ).order_by(
            CompanySignal.signal_score.desc()
        ).limit(count).all()

        target_list = [
            {
                "company_number": t[0],
                "company_name": t[1],
                "postal_code": t[2],
                "sic_codes": t[3],
                "seller_score": float(t[4]) if t[4] else 0.0
            }
            for t in targets
        ]

        with open(output_file, "w") as f:
            json.dump(target_list, f, indent=2)

        click.echo(f"Exported {len(target_list)} targets to {output_file}")


if __name__ == "__main__":
    cli()
