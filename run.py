"""
run.py — End-to-end PE deal-sourcing pipeline

Pipeline steps:
  1. Search     ch_search.py    — SIC sweep + name search → raw_companies.json
  2. Filter     (inline)        — remove false positives → filtered_companies.json
  3. Enrich     ch_enrich.py    — directors, PSC, charges, dealability, scoring → enriched_companies.json
  4. Financials ch_financials.py— accounts data + 3-model revenue/EBITDA estimation
  5. Contacts   ch_contacts.py  — website identification + email inference (top N)
  6. Bolt-on    bolt_on.py      — sector adjacency + fragmentation analysis
  7. Excel      build_excel.py  — 6-sheet workbook output

Usage:
  python run.py --sector "fire safety"                  # ← NEW: full pipeline, auto SIC discovery
  python run.py --sector "electrical contractors"       # ← any freetext sector description
  python run.py --sector "waste management" --save-config configs/waste_mgmt.py

  python run.py                                         # Full pipeline with default config.py
  python run.py --config configs.plumbing_hvac          # Different pre-built sector config
  python run.py --search-only                           # Steps 1–2 only
  python run.py --enrich-only                           # Step 3 only (requires filtered JSON)
  python run.py --financials-only                       # Step 4 only
  python run.py --contacts-only                         # Step 5 only
  python run.py --excel-only                            # Steps 6–7 only
  python run.py --skip-contacts                         # Skip contact enrichment (faster)

  # Combine --sector with step flags:
  python run.py --sector "drainage and sewerage" --search-only
  python run.py --sector "industrial cleaning" --skip-contacts

API key is read from .ch_api_key in project root.
"""

import argparse
import importlib
import sys
import os
import json


def load_api_key() -> str | None:
    """Load Companies House API key from .ch_api_key file."""
    key_file = os.path.join(os.path.dirname(__file__), ".ch_api_key")
    if os.path.exists(key_file):
        with open(key_file) as f:
            for line in f:
                if "COMPANIES_HOUSE_API_KEY" in line:
                    return line.split("=", 1)[1].strip()
    return None


def load_config(config_name: str):
    cfg = importlib.import_module(config_name)
    sys.modules["config"] = cfg
    return cfg


def load_discovered_config(sector_description: str, validate: bool = False) -> object:
    """
    Use sic_discovery to auto-generate a config from a freetext sector description.
    Injects the result into sys.modules["config"] so all pipeline modules can import it.
    """
    from sic_discovery import discover
    api_key = load_api_key()
    cfg = discover(sector_description, api_key=api_key, validate=validate)
    sys.modules["config"] = cfg
    return cfg


def filter_companies(cfg):
    raw_path = os.path.join(cfg.OUTPUT_DIR, cfg.RAW_JSON)
    with open(raw_path) as f:
        companies = json.load(f)

    before   = len(companies)
    filtered = [c for c in companies if _is_genuine(c.get("company_name", ""), cfg)]
    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.FILTERED_JSON)
    with open(out_path, "w") as f:
        json.dump(filtered, f, indent=2)
    print(f"Filter: {before} → {len(filtered)} companies  (saved → {out_path})")
    return filtered


def _is_genuine(name: str, cfg) -> bool:
    n = name.lower()
    if any(ex in n for ex in cfg.EXCLUDE_TERMS):
        return False
    if any(ex in n for ex in getattr(cfg, "EXCLUDE_SUBSECTORS", [])):
        return False
    # If no stems defined, include everything
    stems = getattr(cfg, "INCLUDE_STEMS", [])
    if not stems:
        return True
    return any(kw in n for kw in stems)


def main():
    parser = argparse.ArgumentParser(
        description="Companies House PE pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py --sector "fire safety"
  python run.py --sector "electrical contractors" --skip-contacts
  python run.py --sector "waste management" --save-config configs/waste.py
  python run.py --config configs.plumbing_hvac
  python run.py  (uses default config.py — calibration sector)
        """,
    )

    # ── Sector / config source ────────────────────────────────────────────────
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--sector",
        metavar="DESCRIPTION",
        help='Free-text sector description. Auto-discovers SIC codes. '
             'Example: --sector "fire safety and protection systems"',
    )
    source_group.add_argument(
        "--config",
        default="config",
        metavar="MODULE",
        help="Config module path (default: config). Example: --config configs.plumbing_hvac",
    )

    # ── Step flags ────────────────────────────────────────────────────────────
    parser.add_argument("--search-only",     action="store_true", help="Steps 1–2 only")
    parser.add_argument("--enrich-only",     action="store_true", help="Step 3 only")
    parser.add_argument("--financials-only", action="store_true", help="Step 4 only")
    parser.add_argument("--contacts-only",   action="store_true", help="Step 5 only")
    parser.add_argument("--excel-only",      action="store_true", help="Steps 6–7 only")
    parser.add_argument("--skip-contacts",   action="store_true", help="Skip contact enrichment (faster)")
    parser.add_argument("--no-disify",       action="store_true", help="Skip Disify email verification (faster but unverified)")

    # ── Discovery options ─────────────────────────────────────────────────────
    parser.add_argument(
        "--validate-sic",
        action="store_true",
        help="Validate discovered SIC codes against CH API (slower but more accurate)",
    )
    parser.add_argument(
        "--save-config",
        metavar="FILE",
        help="Save the auto-discovered config to a Python file (e.g. configs/my_sector.py)",
    )

    args = parser.parse_args()

    # ── Load config ───────────────────────────────────────────────────────────
    if args.sector:
        cfg = load_discovered_config(args.sector, validate=args.validate_sic)
        if args.save_config:
            from sic_discovery import save_config_file
            saved = save_config_file(cfg, args.save_config)
            print(f"\n  Config saved → {saved}")
    else:
        cfg = load_config(args.config)

    os.makedirs(cfg.OUTPUT_DIR, exist_ok=True)

    def reload(module_name):
        mod = importlib.import_module(module_name)
        importlib.reload(mod)
        return mod

    print(f"\n{'='*65}")
    print(f"  Companies House PE Pipeline — {cfg.SECTOR_LABEL}")
    print(f"{'='*65}\n")

    # ── Individual step flags ─────────────────────────────────────────────────

    if args.search_only:
        search = reload("ch_search")
        search.run()
        filter_companies(cfg)
        return

    if args.enrich_only:
        enrich = reload("ch_enrich")
        enrich.run()
        return

    if args.financials_only:
        fin = reload("ch_financials")
        fin.run()
        return

    if args.contacts_only:
        contacts = reload("ch_contacts")
        contacts.run()
        return

    if args.excel_only:
        bolt = reload("bolt_on")
        bolt.run()
        excel = reload("build_excel")
        excel.run()
        return

    # ── Full pipeline ─────────────────────────────────────────────────────────

    print("Step 1/7 — Search")
    search = reload("ch_search")
    search.run()

    print("\nStep 2/7 — Filter")
    filter_companies(cfg)

    print("\nStep 3/7 — Enrich (directors, PSC, charges, scoring)")
    enrich = reload("ch_enrich")
    enrich.run()

    print("\nStep 4/7 — Financial estimation")
    fin = reload("ch_financials")
    fin.run()

    if not args.skip_contacts:
        run_disify = not args.no_disify
        label = "website + email inference + Disify verification" if run_disify else "website + email inference (no Disify)"
        print(f"\nStep 5/7 — Contact intelligence ({label})")
        contacts = reload("ch_contacts")
        contacts.run(run_disify=run_disify)
    else:
        print("\nStep 5/7 — Contact intelligence SKIPPED (--skip-contacts)")

    print("\nStep 6/7 — Bolt-on analysis")
    bolt = reload("bolt_on")
    bolt.run()

    print("\nStep 7/7 — Build Excel")
    excel = reload("build_excel")
    out   = excel.run()

    print(f"\n{'='*65}")
    print(f"  ✓ Pipeline complete")
    print(f"  Output: {out}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
