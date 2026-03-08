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
  python run.py                           # Full pipeline
  python run.py --search-only             # Step 1–2 only
  python run.py --enrich-only             # Step 3 only (requires filtered JSON)
  python run.py --financials-only         # Step 4 only
  python run.py --contacts-only           # Step 5 only
  python run.py --excel-only              # Steps 6–7 only
  python run.py --config configs.plumbing_hvac   # Different sector
  python run.py --skip-contacts           # Skip contact enrichment (faster)

API key is read from .ch_api_key in project root.
GitHub token is read from .github_token in project root (optional).
"""

import argparse
import importlib
import sys
import os
import json


def load_config(config_name: str):
    cfg = importlib.import_module(config_name)
    sys.modules["config"] = cfg
    return cfg


def filter_companies(cfg):
    raw_path = os.path.join(cfg.OUTPUT_DIR, cfg.RAW_JSON)
    with open(raw_path) as f:
        companies = json.load(f)

    before   = len(companies)
    filtered = [c for c in companies if _is_genuine(c.get("company_name",""), cfg)]
    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.FILTERED_JSON)
    with open(out_path, "w") as f:
        json.dump(filtered, f, indent=2)
    print(f"Filter: {before} → {len(filtered)} companies  (saved → {out_path})")
    return filtered


def _is_genuine(name: str, cfg) -> bool:
    n = name.lower()
    if any(ex in n for ex in cfg.EXCLUDE_TERMS):
        return False
    if any(ex in n for ex in cfg.EXCLUDE_SUBSECTORS):
        return False
    return any(kw in n for kw in cfg.INCLUDE_STEMS)


def main():
    parser = argparse.ArgumentParser(description="Companies House PE pipeline")
    parser.add_argument("--search-only",     action="store_true")
    parser.add_argument("--enrich-only",     action="store_true")
    parser.add_argument("--financials-only", action="store_true")
    parser.add_argument("--contacts-only",   action="store_true")
    parser.add_argument("--excel-only",      action="store_true")
    parser.add_argument("--skip-contacts",   action="store_true",
                        help="Skip contact/website enrichment (faster run)")
    parser.add_argument("--config", default="config",
                        help="Config module (default: config)")
    args = parser.parse_args()

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
        print("\nStep 5/7 — Contact intelligence (website + email inference)")
        contacts = reload("ch_contacts")
        contacts.run()
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
