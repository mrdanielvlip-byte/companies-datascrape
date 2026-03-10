"""
Batch CH API enrichment for lift maintenance sector.
Pulls directors, DOBs, balance sheet, charges for all companies.
Saves results to /tmp/lift_enriched.json with resume support.
"""
import sys, json, time, os, sqlite3, requests
sys.path.insert(0, "/sessions/vibrant-modest-einstein/ch-pe-sourcing")

import ch_enrich, ch_financials
from revenue_estimate import estimate_revenue

DB    = "/sessions/vibrant-modest-einstein/ch-pe-sourcing/data/companies_house.db"
OUT   = "/tmp/lift_enriched.json"
DELAY = 0.35   # seconds between API calls (~170 companies/min, well under 600/5min limit)

# Load API keys
ch_enrich.AUTH     = (ch_enrich.load_api_key(), "")
ch_financials.AUTH = (ch_financials.load_api_key(), "")

# Load companies from sector_cache
con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row
companies = con.execute("""
    SELECT sc.company_number, sc.company_name, sc.sic1,
           sc.postcode, sc.address_town, sc.address_county,
           sc.incorporation_date, sc.company_age_years
    FROM sector_cache sc
    WHERE sc.sector = 'lift_maintenance'
      AND sc.company_number IS NOT NULL
      AND sc.company_number != ''
    ORDER BY sc.company_age_years DESC
""").fetchall()
con.close()
print(f"Companies to enrich: {len(companies)}", flush=True)

# Resume support — load already-processed results
if os.path.exists(OUT):
    with open(OUT) as f:
        done = json.load(f)
    done_nums = {d["company_number"] for d in done}
    print(f"Resuming — {len(done)} already done", flush=True)
else:
    done = []
    done_nums = set()

sic_map = {
    "43290":"Other construction installation","33190":"Repair of other equipment",
    "71129":"Other engineering activities","43999":"Other specialised construction",
    "33120":"Repair of machinery","28220":"Mfg lifting & handling equipment",
    "33200":"Installation of industrial machinery","82990":"Other business support",
}

total  = len(companies)
errors = 0

for i, row in enumerate(companies):
    cn = row["company_number"]
    if cn in done_nums:
        continue

    name = row["company_name"]
    age  = row["company_age_years"] or 0

    try:
        # Directors
        dirs_raw = ch_enrich.get_directors(cn)
        time.sleep(DELAY)

        # Balance sheet / financials
        bs = ch_financials.get_balance_sheet(cn)
        time.sleep(DELAY)

        # Charges (optional, best-effort)
        charges = {}
        try:
            charges = ch_financials.get_charges(cn)
            time.sleep(DELAY * 0.5)
        except Exception:
            pass

        # Accounts history — last 3 years of filings (period ends + available figures)
        accounts_history = []
        try:
            accounts_history = ch_financials.get_accounts_history(cn, years=3)
            time.sleep(DELAY * 0.5)
        except Exception:
            pass

        # Revenue estimate
        pe_input = {
            "company_name":  name,
            "sic1":          row["sic1"] or "",
            "num_sites":     1,
            "total_assets":  bs.get("total_assets"),
            "total_employees": bs.get("total_employees"),
            "staff_costs":   bs.get("staff_costs"),
            "net_assets":    bs.get("net_assets"),
            "director_salary": bs.get("director_emoluments"),
        }
        pe = estimate_revenue(pe_input)
        pe_d = pe.to_dict()

        # Succession scoring
        succ = ch_enrich.succession_score(dirs_raw) if dirs_raw else {}
        family = ch_enrich.detect_family(name, dirs_raw) if dirs_raw else {}

        # Build director list
        # NOTE: dirs_raw comes from ch_enrich.get_directors() which already processes
        # the CH API response — field names are "role", "appointed", "dob_year", "age"
        # (not the raw CH API names officer_role / appointed_on / date_of_birth).
        directors = []
        for d in dirs_raw:
            directors.append({
                "name":         d.get("name", ""),
                "role":         d.get("role", ""),           # get_directors() renames officer_role → role
                "appointed":    d.get("appointed", ""),      # get_directors() renames appointed_on → appointed
                "dob_year":     d.get("dob_year"),           # get_directors() extracts date_of_birth.year
                "age_est":      d.get("age"),                # get_directors() computes calc_age(dob) → age
                "years_active": d.get("years_active", 0),   # get_directors() computes tenure
                "occupation":   d.get("occupation", ""),    # CH occupation field (job title)
                "nationality":  d.get("nationality", ""),
            })

        rec = {
            "company_number":     cn,
            "company_name":       name,
            "sic1":               row["sic1"] or "",
            "sic_desc":           sic_map.get(row["sic1"] or "", "Other"),
            "postcode":           row["postcode"] or "",
            "town":               row["address_town"] or "",
            "county":             row["address_county"] or "",
            "incorporation_date": row["incorporation_date"] or "",
            "age_years":          round(age, 1),
            "directors":          directors,
            "director_count":     len(directors),
            "oldest_director_age": max((d["age_est"] for d in directors if d["age_est"]), default=None),
            "family_business":    family.get("is_family", False),
            "family_names":       family.get("family_name", ""),
            "succession_score":   succ.get("score", 0),
            "succession_risk":    succ.get("risk", "Unknown"),
            "bs": {
                "total_assets":      bs.get("total_assets"),
                "net_assets":        bs.get("net_assets"),
                "total_liabilities": bs.get("total_liabilities"),
                "cash":              bs.get("cash"),
                "total_employees":   bs.get("total_employees"),
                "accounts_type":     bs.get("accounts_type"),
                "period_end":        bs.get("period_end"),
                "staff_costs":       bs.get("staff_costs"),
                "director_emoluments": bs.get("director_emoluments"),
            },
            "charges": charges,
            "rev_low":   pe_d["revenue_low"],
            "rev_base":  pe_d["revenue_base"],
            "rev_high":  pe_d["revenue_high"],
            "ebitda_base": pe_d["ebitda_base"],
            "confidence":  pe_d["confidence_label"],
            "models_used": pe_d.get("models_used", []),
            "accounts_history": accounts_history,
            "ch_url": f"https://find-and-update.company-information.service.gov.uk/company/{cn}",
        }

        # Employee estimate — best available source
        emp_count, emp_source = ch_financials.estimate_employees(rec)
        if emp_count is not None:
            rec["estimated_employees"]        = emp_count
            rec["estimated_employees_source"] = emp_source
        done.append(rec)
        done_nums.add(cn)

    except Exception as e:
        errors += 1
        done.append({
            "company_number": cn, "company_name": name,
            "age_years": age, "error": str(e),
            "postcode": row["postcode"] or "", "sic1": row["sic1"] or "",
        })
        done_nums.add(cn)

    # Save checkpoint every 50
    if len(done) % 50 == 0:
        with open(OUT, "w") as f:
            json.dump(done, f)
        processed = i + 1
        pct = processed / total * 100
        print(f"  [{processed}/{total}  {pct:.0f}%]  done={len(done)}  errors={errors}", flush=True)

# Final save
with open(OUT, "w") as f:
    json.dump(done, f)
print(f"\nDone. Total: {len(done)}  Errors: {errors}")
