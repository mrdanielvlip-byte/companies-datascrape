"""
api_keys.py — Multi-key rotator for Companies House API

Loads all CH API keys from .ch_api_key file and COMPANIES_HOUSE_API_KEY env var,
then provides round-robin rotation to spread requests across keys and multiply
the effective rate limit (600 req / 5 min per key).

Usage:
    from api_keys import get_auth

    r = requests.get(url, auth=get_auth(), timeout=10)

Each call to get_auth() returns the next (key, "") tuple in rotation.
Thread-safe via threading.Lock.
"""

import os
import threading
from pathlib import Path

_KEY_FILE = Path(__file__).parent / ".ch_api_key"
_keys: list[str] = []
_index = 0
_lock = threading.Lock()


def _load_all_keys() -> list[str]:
    """
    Load all Companies House API keys from:
      1. .ch_api_key file — lines containing COMPANIES_HOUSE_API_KEY= (supports
         multiple lines for multiple keys, e.g. COMPANIES_HOUSE_API_KEY_2=xxx)
      2. COMPANIES_HOUSE_API_KEY environment variable
      3. COMPANIES_HOUSE_API_KEY_2, _3, etc. environment variables
    Returns deduplicated list of keys.
    """
    found = []

    # File-based keys
    if _KEY_FILE.exists():
        for line in _KEY_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "COMPANIES_HOUSE_API_KEY" in line and "=" in line:
                val = line.split("=", 1)[1].strip()
                if val:
                    found.append(val)

    # Environment variable keys
    base = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
    if base:
        found.append(base)
    # Support _2, _3, ... _10
    for i in range(2, 11):
        extra = os.environ.get(f"COMPANIES_HOUSE_API_KEY_{i}", "")
        if extra:
            found.append(extra)

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for k in found:
        if k not in seen:
            seen.add(k)
            unique.append(k)

    return unique


def init():
    """Initialise the key pool. Call once at pipeline startup."""
    global _keys
    _keys = _load_all_keys()
    if _keys:
        print(f"  🔑 API key pool: {len(_keys)} key(s) loaded "
              f"(effective rate limit: {len(_keys) * 600} req / 5 min)")
    else:
        print("  ⚠ No Companies House API keys found!")


def get_auth() -> tuple[str, str]:
    """Return the next (api_key, "") auth tuple in round-robin rotation."""
    global _index
    if not _keys:
        return ("", "")
    with _lock:
        key = _keys[_index % len(_keys)]
        _index += 1
    return (key, "")


def get_single_key() -> str:
    """Return the first API key (for non-rotated usage like SIC validation)."""
    return _keys[0] if _keys else ""


def key_count() -> int:
    """Return the number of keys in the pool."""
    return len(_keys)
