"""
concurrent_pipeline.py — Thread-pool concurrency + rate limiter for PE pipeline

Provides:
  1. RateLimiter  — Token-bucket limiter that respects Companies House API quotas
                    across all keys (600 req / 5 min per key).
  2. process_batch — Runs a per-company function across a thread pool with rate
                     limiting and progress reporting.

Usage in enrichment modules:
    from concurrent_pipeline import process_batch, get_rate_limiter

    def enrich_one(company: dict) -> dict:
        # ... per-company logic (API calls etc.) ...
        return enriched_company

    def run_concurrent(max_workers: int = 8):
        companies = json.load(open(filtered_path))
        results = process_batch(
            items=companies,
            func=enrich_one,
            max_workers=max_workers,
            description="Enriching",
        )
        # ... save results ...

Thread-safe: all rate-limiter operations use threading.Lock.
The rate limiter is a singleton — initialise once at pipeline startup via
init_rate_limiter(key_count), then every module shares the same bucket.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


# ── Rate Limiter (Token Bucket) ──────────────────────────────────────────────

class RateLimiter:
    """
    Token-bucket rate limiter for Companies House API.

    Companies House allows 600 requests per 5 minutes per API key.
    With N keys in rotation, the effective budget is N × 600 / 300 = N × 2 req/sec.

    We use a conservative 85% of theoretical max to avoid 429s:
        target_rps = key_count × 2 × 0.85

    The bucket refills at target_rps tokens per second, with a burst
    allowance of 2× the per-second rate (to absorb small bursts without
    blocking unnecessarily).
    """

    def __init__(self, key_count: int = 1):
        self._key_count = max(1, key_count)
        # 600 req / 300 sec = 2 req/sec per key; use 85% to stay safe
        self._rps = self._key_count * 2.0 * 0.85
        self._burst = max(10, int(self._rps * 3))  # burst allowance
        self._tokens = float(self._burst)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._total_acquired = 0
        self._total_waited = 0.0

    def acquire(self, tokens: int = 1):
        """
        Block until `tokens` are available, then consume them.
        Called before each API request.
        """
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    self._total_acquired += tokens
                    return

            # Not enough tokens — sleep briefly and retry
            sleep_time = tokens / self._rps
            self._total_waited += sleep_time
            time.sleep(sleep_time)

    def _refill(self):
        """Add tokens based on elapsed time since last refill."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self._burst, self._tokens + elapsed * self._rps)

    def stats(self) -> dict:
        return {
            "key_count": self._key_count,
            "target_rps": round(self._rps, 1),
            "burst": self._burst,
            "total_requests": self._total_acquired,
            "total_wait_secs": round(self._total_waited, 1),
        }


# ── Singleton rate limiter ────────────────────────────────────────────────────

_limiter: RateLimiter | None = None
_limiter_lock = threading.Lock()


def init_rate_limiter(key_count: int = 1) -> RateLimiter:
    """Initialise the global rate limiter. Call once at pipeline startup."""
    global _limiter
    with _limiter_lock:
        _limiter = RateLimiter(key_count)
        print(f"  ⚡ Concurrent pipeline: rate limiter initialised "
              f"({key_count} key(s), {_limiter._rps:.1f} req/s target, "
              f"burst={_limiter._burst})")
    return _limiter


def get_rate_limiter() -> RateLimiter:
    """Get the global rate limiter (initialise with 1 key if not yet done)."""
    global _limiter
    if _limiter is None:
        init_rate_limiter(1)
    return _limiter


def rate_limited_sleep(base_sleep: float = 0.05):
    """
    Replace bare time.sleep() calls in enrichment modules.
    Acquires 1 token from the rate limiter, which will block if
    we're approaching the rate limit. Falls back to a small sleep
    if the limiter isn't overloaded.
    """
    limiter = get_rate_limiter()
    limiter.acquire(1)


# ── Batch processor ──────────────────────────────────────────────────────────

def process_batch(
    items: list,
    func,
    max_workers: int = 8,
    description: str = "Processing",
    preserve_order: bool = True,
) -> list:
    """
    Process a list of items concurrently using a thread pool.

    Args:
        items:          List of items (e.g. company dicts) to process.
        func:           Callable that takes one item and returns a result.
                        Must be thread-safe.
        max_workers:    Number of concurrent threads (default 8).
        description:    Label for progress logging.
        preserve_order: If True, results are returned in the same order as items.

    Returns:
        List of results (same length as items). Failed items return None.
    """
    total = len(items)
    if total == 0:
        return []

    # Scale workers to item count — no point having 8 threads for 3 items
    actual_workers = min(max_workers, total)

    print(f"  ⚡ {description}: {total} items × {actual_workers} workers (concurrent)")

    results = [None] * total if preserve_order else []
    completed = 0
    failed = 0
    lock = threading.Lock()

    def _wrapped(index_item):
        idx, item = index_item
        try:
            return idx, func(item)
        except Exception as e:
            company_name = ""
            if isinstance(item, dict):
                company_name = item.get("company_name", item.get("company_number", ""))
            print(f"    ⚠ {description} error ({company_name}): {e}")
            return idx, None

    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        futures = {
            executor.submit(_wrapped, (i, item)): i
            for i, item in enumerate(items)
        }

        for future in as_completed(futures):
            idx, result = future.result()
            if preserve_order:
                results[idx] = result
            else:
                results.append(result)

            with lock:
                completed += 1
                if result is None:
                    failed += 1
                if completed % 25 == 0 or completed == total:
                    pct = completed / total * 100
                    limiter_stats = get_rate_limiter().stats()
                    print(f"    [{completed}/{total}] {pct:.0f}% done "
                          f"({limiter_stats['total_requests']} API calls, "
                          f"{limiter_stats['total_wait_secs']:.1f}s rate-limit wait)")

    if failed:
        print(f"  ⚠ {description}: {failed}/{total} items failed")

    return results
