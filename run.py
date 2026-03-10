"""
run.py — End-to-end PE deal-sourcing pipeline

Pipeline steps:
  1.  Search        ch_search.py        — SIC sweep + name search → raw_companies.json
                    OR reg_sources.py   — regulatory register discovery (--reg-source)
  2.  Filter        (inline)            — remove false positives → filtered_companies.json
  3.  Enrich        ch_enrich.py        — directors, PSC, charges, dealability, scoring
  4.  Financials    ch_financials.py    — accounts data + 8-model revenue/EBITDA estimation
                                         Models: Employee RPE, Asset Turnover, Staff Cost,
                                         Net Asset, Location, Director Hybrid,
                                         Debtor Book (Model 7 — highest accuracy for B2B),
                                         Debt Capacity (Model 8 — floor estimate)
  4b. Accounts OCR  ch_accounts_ocr.py — CH Document API + Tesseract OCR: pulls actual P&L
                                         and balance sheet from filed PDF accounts; replaces
                                         estimates with real Tier 1 figures where available.
                                         Tier A (full/medium/group/small): actual turnover + PBT
                                         Tier B (total-exemption/abridged): real net assets + debtors
  5.  Contacts      ch_contacts.py      — website identification + email inference (Disify verified)
  6.  Sell Signals  sell_signals.py     — exit readiness: late filings, director churn,
                                         Sell Intent Score (A–D) + Seller Likelihood (E–J)
                                         E: Ownership concentration  F: Succession gap
                                         G: Long ownership (>15yr)   H: Revenue trajectory
                                         I: No recent hiring          J: Director reduction
  7.  Contracts     contracts_finder.py — government contract intelligence (Contracts Finder + FTS)
  8.  Digital       digital_health.py   — domain age, LinkedIn, job postings, website health
  9.  Accreditations accreditations.py  — regulatory registers + ISO/CHAS/Gas Safe detection
  10. Bolt-on       bolt_on.py          — sector adjacency + fragmentation analysis
  11. Competitor Map competitor_map.py  — 10 closest geographic/operational competitors per company
                                         PE-backed competitor flags, roll-up opportunity scoring
  12. Acq Score     acquisition_score.py — 5-dimension acquisition attractiveness score (0–100)
                                         Fragmentation(20) + Recurring(20) + OpsImprovement(20)
                                         + BoltOn(20) + Exit Attractiveness(20)
  13. Excel         build_excel.py      — 10-sheet workbook output

Report depth presets (choose one, default is --full):
  python run.py --sector "lift maintenance" --quick     # Quick: Steps 1-4 + Excel only (~5x faster; skips OCR)
  python run.py --sector "lift maintenance" --full      # Full: all steps inc. OCR of filed accounts (default)

Usage:
  python run.py --sector "fire safety"                  # Full pipeline, auto SIC discovery
  python run.py --sector "electrical contractors"       # Any freetext sector description
  python run.py --sector "waste management" --save-config configs/waste_mgmt.py

  # Register-first discovery (Step 1 via regulatory register instead of SIC sweep):
  python run.py --reg-source EA_WASTE    --reg-query "drainage"
  python run.py --reg-source EA_CARRIERS --reg-query ""
  python run.py --reg-source CQC         --reg-query "domiciliary care"
  python run.py --reg-source FCA         --reg-query "mortgage"
  python run.py --list-registers                        # Show all available registers

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
  python run.py --reg-source EA_WASTE --reg-query "drainage" --skip-extras

API key is read from .ch_api_key in project root.
Register API keys (optional — add to .ch_api_key):
  CQC_API_KEY=your_key_here
  FCA_SUBSCRIPTION_KEY=your_key_here
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


def _build_reg_config(register_key: str):
    """
    Build a minimal config module object for register-first discovery.
    Uses the register's sector hints and name to populate config fields.
    """
    from reg_sources import REGISTER_CATALOGUE
    import types

    entry = REGISTER_CATALOGUE.get(register_key)
    if not entry:
        print(f"Unknown register: {register_key!r}")
        sys.exit(1)

    cfg_mod = types.ModuleType("config")

    # Label
    cfg_mod.SECTOR_LABEL   = entry["name"]
    cfg_mod.OUTPUT_DIR     = "output"

    # JSON filenames
    cfg_mod.RAW_JSON       = "raw_companies.json"
    cfg_mod.FILTERED_JSON  = "filtered_companies.json"
    cfg_mod.ENRICHED_JSON  = "enriched_companies.json"
    cfg_mod.EXCEL_OUTPUT   = f"pe_pipeline_{register_key.lower()}.xlsx"

    # SIC codes from register hints (used by ch_enrich, ch_financials etc.)
    cfg_mod.SIC_CODES      = entry.get("sic_hints", [])
    cfg_mod.NAME_QUERIES   = []

    # Filtering — permissive for register-sourced companies (already pre-qualified)
    cfg_mod.EXCLUDE_TERMS      = ["holding", "holdings", "group plc", "listed"]
    cfg_mod.EXCLUDE_SUBSECTORS = []
    cfg_mod.INCLUDE_STEMS      = []   # empty = include all

    # Pipeline limits
    cfg_mod.FINANCIALS_TOP_N      = 100
    cfg_mod.CONTACTS_TOP_N        = 75
    cfg_mod.SELL_SIGNALS_TOP_N    = 100
    cfg_mod.CONTRACTS_TOP_N       = 50
    cfg_mod.DIGITAL_TOP_N         = 75
    cfg_mod.ACCREDITATIONS_TOP_N  = 75
    cfg_mod.BOLT_ON_TOP_N         = 50

    sys.modules["config"] = cfg_mod
    return cfg_mod


def _run_register_discovery(register_key: str, reg_query: str, cfg) -> list:
    """
    Step 1 (register-first mode): Query the regulatory register, cross-reference
    with Companies House, write raw_companies.json and filtered_companies.json.
    Returns the list of filtered companies.
    """
    from reg_sources import discover, REGISTER_CATALOGUE

    api_key = load_api_key()
    companies = discover(
        register_key=register_key,
        keyword=reg_query,
        ch_api_key=api_key,
    )

    if not companies:
        print(f"\n  No companies found via {register_key}. Exiting.")
        sys.exit(0)

    os.makedirs(cfg.OUTPUT_DIR, exist_ok=True)

    raw_path = os.path.join(cfg.OUTPUT_DIR, cfg.RAW_JSON)
    with open(raw_path, "w") as f:
        json.dump(companies, f, indent=2)
    print(f"  Raw: {len(companies)} companies saved → {raw_path}")

    # Filter (register-sourced companies are already pre-qualified; apply basic filter)
    before   = len(companies)
    filtered = [c for c in companies if _is_genuine(c.get("company_name", ""), cfg)]
    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.FILTERED_JSON)
    with open(out_path, "w") as f:
        json.dump(filtered, f, indent=2)
    print(f"  Filter: {before} → {len(filtered)} companies → {out_path}")

    return filtered


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
    source_group.add_argument(
        "--reg-source",
        metavar="REGISTER",
        help='Use a regulatory register as the primary discovery source instead of '
             'SIC sweep. Example: --reg-source EA_WASTE. '
             'Use --list-registers to see all options.',
    )

    parser.add_argument(
        "--reg-query",
        metavar="KEYWORD",
        default="",
        help='Keyword to search the regulatory register. Pass "" for all entries. '
             'Example: --reg-query "drainage"',
    )
    parser.add_argument(
        "--list-registers",
        action="store_true",
        help="List all available regulatory registers and exit",
    )

    # ── Step flags ────────────────────────────────────────────────────────────
    # ── Report depth preset ───────────────────────────────────────────────────
    depth_group = parser.add_mutually_exclusive_group()
    depth_group.add_argument(
        "--quick",
        action="store_true",
        help=(
            "Quick report mode: Steps 1–4 + Excel only (search, filter, enrich, financials). "
            "Skips accounts OCR, contacts, sell signals, contracts, digital health, accreditations. "
            "~5x faster. Ideal for initial sector screening."
        ),
    )
    depth_group.add_argument(
        "--full",
        action="store_true",
        help=(
            "Full detailed report mode (default): all 10 steps including contacts, sell signals, "
            "government contracts, digital health and accreditation enrichment. "
            "Produces the richest output but takes significantly longer."
        ),
    )

    parser.add_argument("--search-only",       action="store_true", help="Steps 1-2 only")
    parser.add_argument("--enrich-only",       action="store_true", help="Step 3 only")
    parser.add_argument("--financials-only",   action="store_true", help="Step 4 only")
    parser.add_argument("--accounts-ocr-only", action="store_true", help="Step 4b only — download + OCR filed accounts PDFs")
    parser.add_argument("--contacts-only",     action="store_true", help="Step 5 only")
    parser.add_argument("--no-accounts-ocr",   action="store_true", help="Skip Step 4b (accounts PDF OCR). Use with --quick or if OCR already done.")
    parser.add_argument("--excel-only",      action="store_true", help="Steps 10–11 only (bolt-on + Excel)")
    parser.add_argument("--skip-contacts",   action="store_true", help="Skip contact enrichment (faster)")
    parser.add_argument("--no-disify",       action="store_true", help="Skip Disify email verification")
    parser.add_argument("--skip-extras",     action="store_true", help="Skip steps 6–9 (sell signals, contracts, digital, accreditations)")
    parser.add_argument("--no-sell-signals", action="store_true", help="Skip sell intent signal analysis (step 6)")
    parser.add_argument("--no-contracts",    action="store_true", help="Skip government contracts lookup (step 7)")
    parser.add_argument("--no-digital",      action="store_true", help="Skip digital health assessment (step 8)")
    parser.add_argument("--no-accreditations", action="store_true", help="Skip accreditation enrichment (step 9)")

    parser.add_argument(
        "--local-db",
        action="store_true",
        help="Use local SQLite database for Step 1 search instead of Companies House API. "
             "Dramatically faster (~1s vs 10+ min). Requires: python build_local_db.py",
    )
    parser.add_argument(
        "--smart",
        action="store_true",
        help="Interactive guided search: auto-discovers SIC codes, asks filtering criteria "
             "(age, region, charges), shows count estimate, then runs pipeline. "
             "Requires --sector and local DB. Best starting point for new sectors.",
    )

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

    # ── --list-registers: print register catalogue and exit ───────────────────
    if args.list_registers:
        from reg_sources import list_registers
        list_registers()
        sys.exit(0)

    # ── Load config ───────────────────────────────────────────────────────────
    if args.sector:
        cfg = load_discovered_config(args.sector, validate=args.validate_sic)
        if args.save_config:
            from sic_discovery import save_config_file
            saved = save_config_file(cfg, args.save_config)
            print(f"\n  Config saved → {saved}")
    elif args.reg_source:
        # Register-first mode: build a minimal config from the register metadata
        cfg = _build_reg_config(args.reg_source)
    elif args.smart and args.sector:
        cfg = load_discovered_config(args.sector, validate=False)
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
        if args.local_db:
            local = reload("local_search")
            local.run()
        else:
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

    if getattr(args, "accounts_ocr_only", False):
        ocr = reload("ch_accounts_ocr")
        ocr.run()
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

    # --quick preset: skip contacts + all extra enrichment steps (5x faster)
    if args.quick:
        args.skip_contacts = True
        args.skip_extras   = True
        print("  [QUICK MODE]  Steps 1-4 + Excel only. Use --full for contacts, sell signals,")
        print("                contracts, digital health and accreditations.\n")

    skip_extras = args.skip_extras

    if args.smart:
        # ── Smart guided search ───────────────────────────────────────────────
        print(f"Step 1/11 — Smart Sector Search  ⚡")
        smart = reload("smart_search")
        result = smart.run_interactive(
            sector=args.sector or cfg.SECTOR_LABEL,
            non_interactive=False,
        )
        if not result:
            return
        print("\nStep 2/11 — Filter (applied during smart search)")
    elif args.reg_source:
        # ── Register-first discovery ──────────────────────────────────────────
        reg_query = getattr(args, "reg_query", "")
        print(f"Step 1/11 — Register Discovery ({args.reg_source}: '{reg_query}')")
        _run_register_discovery(args.reg_source, reg_query, cfg)
        print("\nStep 2/11 — Filter (applied during register discovery)")
    elif args.local_db:
        # ── Local SQLite DB search (fast, no API calls) ───────────────────────
        print("Step 1/11 — Local DB Search  ⚡ (SQLite, no API)")
        local = reload("local_search")
        local.run()
        print("\nStep 2/11 — Filter (applied during local search)")
    else:
        # ── Standard SIC sweep via Companies House API ────────────────────────
        print("Step 1/11 — Search (Companies House API)")
        search = reload("ch_search")
        search.run()

        print("\nStep 2/11 — Filter")
        filter_companies(cfg)

    print("\nStep 3/11 — Enrich (directors, PSC, charges, acquisition scoring)")
    enrich = reload("ch_enrich")
    enrich.run()

    print("\nStep 4/11 — Financial estimation (PE 6-model triangulation: Employee, Asset, Staff Cost, Net Asset, Location, Director Hybrid)")
    fin = reload("ch_financials")
    fin.run()

    # Step 4b — Accounts OCR (skipped in --quick mode or with --no-accounts-ocr)
    skip_ocr = args.quick or getattr(args, "no_accounts_ocr", False)
    if not skip_ocr:
        print("\nStep 4b/13 — Accounts OCR (CH Document API + Tesseract: pull actual P&L from filed PDFs)")
        ocr = reload("ch_accounts_ocr")
        ocr.run(resume=True)
    else:
        print("\nStep 4b/13 — Accounts OCR SKIPPED (--quick mode; use --full or omit --no-accounts-ocr)")

    if not args.skip_contacts:
        run_disify = not args.no_disify
        label = "Disify verified" if run_disify else "unverified (no Disify)"
        print(f"\nStep 5/13 — Contact intelligence (website + email inference, {label})")
        contacts = reload("ch_contacts")
        contacts.run(run_disify=run_disify)
    else:
        print("\nStep 5/13 — Contact intelligence SKIPPED (--skip-contacts)")

    # ── Enhanced intelligence steps (6–9) ────────────────────────────────────

    if not skip_extras and not args.no_sell_signals:
        print("\nStep 6/13 — Sell intent signals (late filings, director churn, age/tenure)")
        sell = reload("sell_signals")
        sell.run()
    else:
        print("\nStep 6/13 — Sell signals SKIPPED")

    if not skip_extras and not args.no_contracts:
        print("\nStep 7/13 — Government contracts (Contracts Finder + Find a Tender)")
        contracts = reload("contracts_finder")
        contracts.run()
    else:
        print("\nStep 7/13 — Government contracts SKIPPED")

    if not skip_extras and not args.no_digital:
        print("\nStep 8/13 — Digital health (domain age, LinkedIn, job postings)")
        digital = reload("digital_health")
        digital.run()
    else:
        print("\nStep 8/13 — Digital health SKIPPED")

    if not skip_extras and not args.no_accreditations:
        print("\nStep 9/13 — Accreditations (CQC, Environment Agency, ICO, ISO/CHAS)")
        accreds = reload("accreditations")
        accreds.run()
    else:
        print("\nStep 9/13 — Accreditations SKIPPED")

    print("\nStep 10/13 — Bolt-on sector adjacency analysis")
    bolt = reload("bolt_on")
    bolt.run()

    # ── Step 11: Competitor Map (geographic + operational) ────────────────────
    if not skip_extras:
        print("\nStep 11/13 — Competitor mapping (10 closest rivals per company, PE-backed flags)")
        comp_map = reload("competitor_map")
        comp_map.run()
    else:
        print("\nStep 11/13 — Competitor mapping SKIPPED (--quick mode)")

    # ── Step 12: Acquisition Attractiveness Score ──────────────────────────────
    if not skip_extras:
        print("\nStep 12/13 — Acquisition attractiveness scoring (5-dimension, 0–100)")
        acq = reload("acquisition_score")
        acq.run()
    else:
        print("\nStep 12/13 — Acquisition scoring SKIPPED (--quick mode)")

    print("\nStep 13/13 — Build Excel (10-sheet workbook)")
    excel = reload("build_excel")
    out   = excel.run()

    print(f"\n{'='*65}")
    print(f"  ✓ Pipeline complete")
    print(f"  Output: {out}")
    print(f"{'='*65}\n")

    # ── Email notification ────────────────────────────────────────────────────
    # Sends completion email with Excel attached (≤10 MB) or as a path reference.
    # Requires MAIL_USERNAME + MAIL_PASSWORD env vars or .mail_config file.
    # Silently skips if credentials not configured.
    try:
        from notify import send_completion_email
        import json, pathlib

        # Build a basic summary from enriched JSON if available
        summary = {}
        enriched_path = pathlib.Path("data/sectors") / f"{cfg.SECTOR_SLUG}_enriched.json"
        if enriched_path.exists():
            try:
                companies = json.loads(enriched_path.read_text())
                from acquisition_score import TIER_LABELS
                tier1 = sum(1 for c in companies if c.get("acquisition_score", {}).get("tier") == "Tier 1")
                tier2 = sum(1 for c in companies if c.get("acquisition_score", {}).get("tier") == "Tier 2")
                summary = {
                    "total":     len(companies),
                    "tier1":     tier1,
                    "tier2":     tier2,
                    "family":    sum(1 for c in companies if c.get("family_business") or c.get("is_family_company")),
                    "directors": sum(len(c.get("directors", [])) for c in companies),
                }
            except Exception:
                pass

        sector_label = getattr(cfg, "SECTOR_LABEL", getattr(cfg, "SECTOR_SLUG", "Unknown Sector")).replace("_", " ").title()
        send_completion_email(
            excel_path=out,
            sector=sector_label,
            summary=summary,
        )
    except Exception as e:
        print(f"[notify] Email step failed (non-fatal): {e}")


if __name__ == "__main__":
    main()
