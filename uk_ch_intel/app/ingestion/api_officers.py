import base64
import json
import time
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlencode

import httpx
import requests
from loguru import logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from app.db import get_session
from app.models import OfficerRaw, EnrichmentJob
from app.config import get_settings


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, rate_per_minute: int):
        self.rate_per_minute = rate_per_minute
        self.tokens = rate_per_minute
        self.last_update = time.time()

    def wait(self):
        """Wait until we have a token available."""
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.rate_per_minute, self.tokens + elapsed * (self.rate_per_minute / 60))
        self.last_update = now

        if self.tokens < 1:
            sleep_time = (1 - self.tokens) * (60 / self.rate_per_minute)
            logger.debug(f"Rate limit: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
            self.tokens = 0
        else:
            self.tokens -= 1


class CompaniesHouseAPIClient:
    """Client for Companies House API with retry logic and rate limiting."""

    def __init__(self, api_key: str, rate_limit_per_minute: int = 400):
        self.api_key = api_key
        self.base_url = "https://api.companieshouse.gov.uk"
        self.rate_limiter = RateLimiter(rate_limit_per_minute)
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create authenticated session."""
        session = requests.Session()
        auth_string = base64.b64encode(f"{self.api_key}:".encode()).decode()
        session.headers.update({"Authorization": f"Basic {auth_string}"})
        return session

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    )
    def _make_request(self, url: str, params: dict = None) -> dict:
        """Make API request with retry logic."""
        self.rate_limiter.wait()

        try:
            response = self.session.get(url, params=params, timeout=30)

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Sleeping for {retry_after}s")
                time.sleep(retry_after)
                return self._make_request(url, params)

            # Handle server errors
            if response.status_code in (500, 502, 503):
                logger.warning(f"Server error {response.status_code}, retrying...")
                raise requests.exceptions.RequestException(f"Server error {response.status_code}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def get_officers(self, company_number: str) -> list[dict]:
        """Fetch all officers for a company."""
        url = f"{self.base_url}/company/{company_number}/officers"
        all_officers = []
        items_per_page = 35

        try:
            page = 0
            while True:
                params = {"items_per_page": items_per_page, "start_index": page * items_per_page}
                logger.debug(f"Fetching officers for {company_number}, page {page}")

                data = self._make_request(url, params)

                if not data.get("items"):
                    break

                all_officers.extend(data["items"])
                page += 1

                # Check if there are more results
                if page * items_per_page >= data.get("total_results", 0):
                    break

            logger.info(f"Fetched {len(all_officers)} officers for company {company_number}")
            return all_officers

        except Exception as e:
            logger.error(f"Error fetching officers for {company_number}: {e}")
            raise

    def get_company_profile(self, company_number: str) -> dict:
        """Fetch company profile data."""
        url = f"{self.base_url}/company/{company_number}"
        logger.debug(f"Fetching profile for {company_number}")
        return self._make_request(url)

    def get_filing_history(self, company_number: str) -> list[dict]:
        """Fetch all filings for a company."""
        url = f"{self.base_url}/company/{company_number}/filing-history"
        all_filings = []
        items_per_page = 100

        try:
            page = 0
            while True:
                params = {"items_per_page": items_per_page, "start_index": page * items_per_page}
                logger.debug(f"Fetching filings for {company_number}, page {page}")

                data = self._make_request(url, params)

                if not data.get("items"):
                    break

                all_filings.extend(data["items"])
                page += 1

                if page * items_per_page >= data.get("total_results", 0):
                    break

            logger.info(f"Fetched {len(all_filings)} filings for company {company_number}")
            return all_filings

        except Exception as e:
            logger.error(f"Error fetching filings for {company_number}: {e}")
            raise


def fetch_officers_for_company(company_number: str) -> bool:
    """Fetch and store officers for a company."""
    settings = get_settings()
    client = CompaniesHouseAPIClient(settings.ch_api_key, settings.api_rate_limit_per_minute)

    with get_session() as session:
        try:
            # Update job status to in_progress
            job = (
                session.query(EnrichmentJob)
                .filter_by(company_number=company_number, job_type="officers_fetch", status="pending")
                .first()
            )
            if job:
                job.status = "in_progress"
                job.started_at = datetime.utcnow()
                session.commit()

            # Fetch officers
            officers = client.get_officers(company_number)

            # Store raw response
            for officer in officers:
                officer_raw = OfficerRaw(
                    company_number=company_number,
                    source_officer_payload=officer,
                    fetched_at=datetime.utcnow(),
                    source_endpoint="officers",
                )
                session.add(officer_raw)

            session.commit()

            # Update job status
            if job:
                job.status = "completed"
                job.finished_at = datetime.utcnow()
                session.commit()

            logger.info(f"Stored {len(officers)} officers for company {company_number}")
            return True

        except Exception as e:
            logger.error(f"Error fetching officers for {company_number}: {e}")

            # Update job with error
            if job:
                job.status = "failed"
                job.last_error = str(e)
                job.attempt_count += 1
                job.finished_at = datetime.utcnow()
                session.commit()

            return False


def fetch_officers_batch(company_numbers: list[str], max_workers: int = 4) -> dict:
    """Fetch officers for multiple companies."""
    results = {"succeeded": 0, "failed": 0, "skipped": 0}

    for company_number in company_numbers:
        try:
            if fetch_officers_for_company(company_number):
                results["succeeded"] += 1
            else:
                results["failed"] += 1
        except Exception as e:
            logger.error(f"Batch error for {company_number}: {e}")
            results["failed"] += 1

    logger.info(f"Batch fetch results: {results}")
    return results


if __name__ == "__main__":
    # Example usage
    from app.config import get_settings

    settings = get_settings()
    client = CompaniesHouseAPIClient(settings.ch_api_key)

    # Test with a known company number
    try:
        officers = client.get_officers("00000191")  # Example company
        print(f"Found {len(officers)} officers")
    except Exception as e:
        print(f"Error: {e}")
