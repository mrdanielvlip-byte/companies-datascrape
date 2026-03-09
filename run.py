"""
run.py — End-to-end PE deal-sourcing pipeline

Pipeline steps:
  1.  Search        ch_search.py        — SIC sweep + name search → raw_companies.json
  2.  Filter        (inline)            — remove false positives → filtered_companies.json
  3.  Enrich        ch_enrich.py        — directors, PSC, charges, dealability, scoring
  4.  Financials    ch_financials.py    — accounts data + 3-model revenue/EBITDA estimation
  5.  Contacts      ch_contacts.py      — website identification + email inference (Disify verified)
  6.  Sell Signals  sell_signals.py     — exit readiness: late filings, director churn, Sell Intent Score
  7.  Contracts     contracts_finder.py — government contract intelligence (Contracts Finder + FTS)
  8.  Digital       digital_health.py   — domain age, LinkedIn, job postings, website health
  9.  Accreditations accreditations.py  — CQC, Environment Agency, ICO, ISO/CHAS/Gas Safe detection
  10. Bolt-on       bolt_on.py          — sector adjacency + fragmentation analysis
  11. Excel         build_excel.py      — 9-sheet workbook output

Usage:
  python run.py --sector "fire safety"                  # Full pipeline, auto SIC discovery
  python run.py --sector "electrical contractors"       # Any freetext sector description
  python run.py --sector "waste management" --save-config configs/waste_mgmt.py

  python run.py                                         # Full pipeline with default config.py
  python run.py --config configs.plumbing_hvac          # Different pre-built sector config
  python run.py --search-only                           # Steps 1–2 only
  python run.py --enrich-only                           # Step 3 only (requires filtered JSON)
  python run.py --financials-only                       # Step 4 only
  python run.py --contacts-only                         # Step 5 only
  python run.py --excel-only                            # Steps 10–11 only
  python run.py --skip-contacts                         # Skip contact enrichment (faster)
  python run.py --skip-extras                           # Skip steps 6–9 (faster pipeline)

  # Skip individual enrichment steps:
  python run.py --no-sell-signals
  python run.py --no-contracts
  python run.py --no-digital
  python run.py --no-accreditations

  # Combine flags:
  python run.py --sector "drainage and sewerage" --search-only
  python run.py --sector "industrial cleaning" --skip-contacts --skip-extras

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
  python run.py --sector "fire safety"                           # Full 11-step pipeline
  python run.py --sector "electrical contractors" --skip-contacts
  python run.py --sector "waste management" --skip-extras        # Skip steps 6-9 (faster)
  python run.py --sector "IT managed services" --no-contracts    # Skip gov contracts only
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
    parser.add_argument("--excel-only",      action="store_true", help="Steps 10–11 only (bolt-on + Excel)")
    parser.add_argument("--skip-contacts",   action="store_true", help="Skip contact enrichment (faster)")
    parser.add_argument("--no-disify",       action="store_true", help="Skip Disify email verification")
    parser.add_argument("--skip-extras",     action="store_true", help="Skip steps 6–9 (sell signals, contracts, digital, accreditations)")
    parser.add_argument("--no-sell-signals", action="store_true", help="Skip sell intent signal analysis (step 6)")
    parser.add_argument("--no-contracts",    action="store_true", help="Skip government contracts lookup (step 7)")
    parser.add_argument("--no-digital",      action="store_true", help="Skip digital health assessment (step 8)")
    parser.add_argument("--no-accreditations", action="store_true", help="Skip accreditation enrichment (step 9)")

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

    skip_extras = args.skip_extras

    print("Step 1/11 — Search")
    search = reload("ch_search")
    search.run()

    print("\nStep 2/11 — Filter")
    filter_companies(cfg)

    print("\nStep 3/11 — Enrich (directors, PSC, charges, acquisition scoring)")
    enrich = reload("ch_enrich")
    enrich.run()

    print("\nStep 4/11 — Financial estimation (3-model revenue + EBITDA)")
    fin = reload("ch_financials")
    fin.run()

    if not args.skip_contacts:
        run_disify = not args.no_disify
        label = "Disify verified" if run_disify else "unverified (no Disify)"
        print(f"\nStep 5/11 — Contact intelligence (website + email inference, {label})")
        contacts = reload("ch_contacts")
        contacts.run(run_disify=run_disify)
    else:
        print("\nStep 5/11 — Contact intelligence SKIPPED (--skip-contacts)")

    # ── Enhanced intelligence steps (6–9) ────────────────────────────────────

    if not skip_extras and not args.no_sell_signals:
        print("\nStep 6/11 — Sell intent signals (late filings, director churn, age/tenure)")
        sell = reload("sell_signals")
        sell.run()
    else:
        print("\nStep 6/11 — Sell signals SKIPPED")

    if not skip_extras and not args.no_contracts:
        print("\nStep 7/11 — Government contracts (Contracts Finder + Find a Tender)")
        contracts = reload("contracts_finder")
        contracts.run()
    else:
        print("\nStep 7/11 — Government contracts SKIPPED")

    if not skip_extras and not args.no_digital:
        print("\nStep 8/11 — Digital health (domain age, LinkedIn, job postings)")
        digital = reload("digital_health")
        digital.run()
    else:
        print("\nStep 8/11 — Digital health SKIPPED")

    if not skip_extras and not args.no_accreditations:
        print("\nStep 9/11 — Accreditations (CQC, Environment Agency, ICO, ISO/CHAS)")
        accreds = reload("accreditations")
        accreds.run()
    else:
        print("\nStep 9/11 — Accreditations SKIPPED")

    print("\nStep 10/11 — Bolt-on sector adjacency analysis")
    bolt = reload("bolt_on")
    bolt.run()

    print("\nStep 11/11 — Build Excel (9-sheet workbook)")
    excel = reload("build_excel")
    out   = excel.run()

    print(f"\n{'='*65}")
    print(f"  ✓ Pipeline complete")
    print(f"  Output: {out}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
