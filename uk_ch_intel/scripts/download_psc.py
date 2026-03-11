#!/usr/bin/env python3
"""
Download PSC (Persons with Significant Control) bulk data from Companies House.

Downloads the 31-part snapshot (~2.1 GB total) into datasets/psc_bulk/.
Each part is ~69 MB. After downloading, run `python scripts/query.py setup`
to load the data into your local DuckDB database.

Usage:
    python scripts/download_psc.py                # Download all 31 parts
    python scripts/download_psc.py --parts 1-5    # Download parts 1-5 only
    python scripts/download_psc.py --check        # Check what's already downloaded
    python scripts/download_psc.py --date 2026-03-11  # Specific snapshot date

The PSC snapshot is refreshed daily before 10am GMT at:
    https://download.companieshouse.gov.uk/en_pscdata.html
"""
import argparse
import hashlib
import os
import sys
import time
from datetime import date
from pathlib import Path

try:
    import requests
except ImportError:
    print("Install requests: pip install requests")
    sys.exit(1)


BASE_URL = "https://download.companieshouse.gov.uk"
TOTAL_PARTS = 31
DEFAULT_OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "datasets", "psc_bulk"
)


def get_snapshot_date() -> str:
    """Get today's date formatted for the snapshot filename."""
    return date.today().isoformat()


def download_part(part_num: int, snapshot_date: str, output_dir: str) -> bool:
    """
    Download a single PSC snapshot part.

    Returns True if successful, False otherwise.
    """
    filename = f"psc-snapshot-{snapshot_date}_{part_num}of{TOTAL_PARTS}.zip"
    url = f"{BASE_URL}/{filename}"
    output_path = os.path.join(output_dir, filename)

    # Skip if already downloaded
    if os.path.exists(output_path):
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"  ✓ Part {part_num}/{TOTAL_PARTS} already exists ({size_mb:.0f} MB)")
        return True

    print(f"  Downloading part {part_num}/{TOTAL_PARTS}: {filename}...")

    try:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192 * 16):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    pct = downloaded / total_size * 100
                    mb = downloaded / (1024 * 1024)
                    print(f"\r    {mb:.0f} MB / {total_size / (1024*1024):.0f} MB ({pct:.0f}%)", end="", flush=True)

        print()  # newline after progress
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"  ✓ Part {part_num} complete ({size_mb:.0f} MB)")
        return True

    except requests.exceptions.HTTPError as e:
        print(f"\n  ✗ HTTP error for part {part_num}: {e}")
        # Clean up partial download
        if os.path.exists(output_path):
            os.remove(output_path)
        return False

    except Exception as e:
        print(f"\n  ✗ Error downloading part {part_num}: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)
        return False


def check_downloads(output_dir: str, snapshot_date: str):
    """Check which parts are already downloaded."""
    print(f"Checking {output_dir} for snapshot date {snapshot_date}:\n")
    total_size = 0
    found = 0

    for i in range(1, TOTAL_PARTS + 1):
        filename = f"psc-snapshot-{snapshot_date}_{i}of{TOTAL_PARTS}.zip"
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath):
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            total_size += size_mb
            found += 1
            print(f"  ✓ Part {i:2d}: {size_mb:.0f} MB")
        else:
            print(f"  ✗ Part {i:2d}: missing")

    print(f"\n{found}/{TOTAL_PARTS} parts downloaded ({total_size:.0f} MB total)")

    # Also check for any other PSC files
    other_files = [f for f in os.listdir(output_dir)
                   if f.endswith(".zip") and snapshot_date not in f]
    if other_files:
        print(f"\nOther PSC files found (older snapshots):")
        for f in sorted(other_files):
            size_mb = os.path.getsize(os.path.join(output_dir, f)) / (1024 * 1024)
            print(f"  {f} ({size_mb:.0f} MB)")


def parse_parts_range(parts_str: str) -> list[int]:
    """Parse a parts range string like '1-5' or '1,3,5' into a list of ints."""
    parts = []
    for segment in parts_str.split(","):
        segment = segment.strip()
        if "-" in segment:
            start, end = segment.split("-", 1)
            parts.extend(range(int(start), int(end) + 1))
        else:
            parts.append(int(segment))
    return sorted(set(p for p in parts if 1 <= p <= TOTAL_PARTS))


def main():
    parser = argparse.ArgumentParser(
        description="Download PSC bulk data from Companies House"
    )
    parser.add_argument(
        "--parts", type=str, default=None,
        help="Parts to download, e.g. '1-5' or '1,3,7-10'. Default: all 31."
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Snapshot date (YYYY-MM-DD). Default: today."
    )
    parser.add_argument(
        "--output", type=str, default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory. Default: {DEFAULT_OUTPUT_DIR}"
    )
    parser.add_argument(
        "--check", action="store_true",
        help="Check which parts are already downloaded."
    )
    args = parser.parse_args()

    snapshot_date = args.date or get_snapshot_date()
    output_dir = args.output

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    if args.check:
        check_downloads(output_dir, snapshot_date)
        return

    # Determine which parts to download
    if args.parts:
        parts = parse_parts_range(args.parts)
    else:
        parts = list(range(1, TOTAL_PARTS + 1))

    print(f"PSC Bulk Data Download")
    print(f"  Snapshot date: {snapshot_date}")
    print(f"  Parts: {len(parts)} of {TOTAL_PARTS}")
    print(f"  Estimated size: ~{len(parts) * 69} MB")
    print(f"  Output: {output_dir}")
    print()

    successes = 0
    failures = 0

    for part_num in parts:
        ok = download_part(part_num, snapshot_date, output_dir)
        if ok:
            successes += 1
        else:
            failures += 1
        # Small delay between downloads to be polite
        if part_num < parts[-1]:
            time.sleep(0.5)

    print(f"\nDone: {successes} downloaded, {failures} failed")

    if successes > 0:
        print(f"\nNext step: load into DuckDB with:")
        print(f"  cd uk_ch_intel")
        print(f"  python scripts/query.py setup")


if __name__ == "__main__":
    main()
