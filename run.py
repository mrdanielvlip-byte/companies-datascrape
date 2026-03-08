"""
run.py — End-to-end PE deal-sourcing pipeline

Usage:
    python run.py                    # Full run: search → enrich → Excel
    python run.py --search-only      # Only pull companies from Companies House
    python run.py --enrich-only      # Only enrich (requires raw JSON from previous run)
    python run.py --excel-only       # Only rebuild Excel (requires enriched JSON)
    python run.py --config my_config # Use a different config module (e.g. configs/plumbing.py)

The API key is read from .ch_api_key in the same directory.
Intermediate data is saved to output/ so any step can be re-run independently.
"""

import argparse
import importlib
import sys
import os


def main():
    parser = argparse.ArgumentParser(description="Companies House PE pipeline")
    parser.add_argument("--search-only", action="store_true")
    parser.add_argument("--enrich-only", action="store_true")
    parser.add_argument("--excel-only",  action="store_true")
    parser.add_argument("--config",      default="config",
                        help="Config module to use (default: config)")
    args = parser.parse_args()

    # Allow swapping config via --config flag
    cfg_module = importlib.import_module(args.config)
    sys.modules["config"] = cfg_module

    import ch_search
    import ch_enrich
    import build_excel

    # Reload to pick up swapped config
    importlib.reload(ch_search)
    importlib.reload(ch_enrich)
    importlib.reload(build_excel)

    os.makedirs(cfg_module.OUTPUT_DIR, exist_ok=True)

    if args.search_only:
        ch_search.run()
        return

    if args.enrich_only:
        # Apply name filter before enriching
        _filter(cfg_module)
        ch_enrich.run()
        return

    if args.excel_only:
        build_excel.run()
        return

    # Full run
    ch_search.run()
    _filter(cfg_module)
    ch_enrich.run()
    build_excel.run()

    print(f"\n✓ Pipeline complete. Output: {cfg_module.OUTPUT_DIR}/{cfg_module.EXCEL_OUTPUT}")


def _filter(cfg):
    """Remove companies that slipped through the name-search fuzzy matching."""
    import json

    raw_path = os.path.join(cfg.OUTPUT_DIR, cfg.RAW_JSON)
    with open(raw_path) as f:
        companies = json.load(f)

    before = len(companies)
    filtered = [
        c for c in companies
        if _is_genuine(c.get("company_name", ""), cfg)
    ]

    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.FILTERED_JSON)
    with open(out_path, "w") as f:
        json.dump(filtered, f, indent=2)

    print(f"\nFilter: {before} → {len(filtered)} companies  (saved → {out_path})")


def _is_genuine(name: str, cfg) -> bool:
    n = name.lower()
    if any(ex in n for ex in cfg.EXCLUDE_TERMS):
        return False
    if any(ex in n for ex in cfg.EXCLUDE_SUBSECTORS):
        return False
    return any(kw in n for kw in cfg.INCLUDE_STEMS)


if __name__ == "__main__":
    main()
