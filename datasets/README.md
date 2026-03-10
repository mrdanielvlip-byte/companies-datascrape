# datasets/

Raw and enriched company data exports for the PE Deal-Sourcing pipeline.
These are the **unfiltered source datasets** preserved for future queries, re-scoring, and cross-referencing.

---

## Contents

### `lift_maintenance/`
| File | Rows | Description |
|------|------|-------------|
| `UK_Lift_Maintenance_Companies_March2026.xlsx` | 1,089 | Full lift & elevator sector output — all companies found via SIC codes, LEIA member list, and CH search. Includes All Companies, Prime Targets, Competitor Maps, Directors Register, and Family Businesses sheets. |

**Key columns (All Companies sheet):**
`Company No`, `Company Name`, `Postcode`, `Town`, `SIC`, `Rev (Actual/Est)`, `Net Assets`, `Employees`, `Acq Score`, `Acq Tier`, `Sell Band`, `Family Co`, `Source`

---

### `roofing/`
| File | Rows | Description |
|------|------|-------------|
| `Roofing_PE_Pipeline_March2026.xlsx` | 629 | Full roofing sector PE pipeline. Includes PE Pipeline, Top 30 Profiles, Director Contacts, Financial Estimates, Bolt-On Analysis, Sell Signals, Gov Contracts. |

**Key columns (PE Pipeline sheet):**
`Reg. No.`, `Company Name`, `Acq. Score`, `Grade`, `SI Band`, `Family`, `Revenue`, `Net Assets`

---

### `directors_enrichment/`
| File | Rows | Description |
|------|------|-------------|
| `directors_55plus.json` | 554 | CH API live enrichment — all active directors aged 55+ across both sectors. One row per director. |
| `directors_55plus.csv`  | 554 | Same data in CSV format for easy querying in pandas/Excel/SQL. |

**Key columns:**
`sector`, `company_number`, `company_name`, `town`, `postcode`, `revenue`, `net_assets`, `acq_score`, `acq_tier`, `sell_band`, `family_co`, `name`, `role`, `age`, `dob_year`, `dob_month`, `tenure_yrs`, `nationality`

**Coverage:** 385 unique companies across Lift Maintenance (459 director rows) and Roofing (95 director rows).
Age range: 55–90, average 62.4. DOBs sourced live from Companies House API (month + year only).

---

## Quick queries (pandas)

```python
import pandas as pd

# All companies with a director aged 70+
df = pd.read_csv("datasets/directors_enrichment/directors_55plus.csv")
old_directors = df[df["age"] >= 70][["company_name", "sector", "age", "name", "tenure_yrs", "revenue"]]

# Family companies with director aged 65+
succession_risk = df[(df["family_co"] == "Yes") & (df["age"] >= 65)]

# Lift maintenance, Tier 1 companies only
lift = pd.read_excel("datasets/lift_maintenance/UK_Lift_Maintenance_Companies_March2026.xlsx",
                     sheet_name="All Companies")
tier1 = lift[lift["Acq Tier"].str.startswith("Tier 1", na=False)]

# Roofing — all companies with sell signal
roof = pd.read_excel("datasets/roofing/Roofing_PE_Pipeline_March2026.xlsx",
                     sheet_name="Sell Signals")
```

---

## Data provenance

| Dataset | Source | Run date | Companies |
|---------|--------|----------|-----------|
| Lift Maintenance | CH API + SIC 43290/33120/28221/81100 + LEIA member list | March 2026 | 1,089 |
| Roofing | CH API + SIC 43910/41201/43999 | March 2026 | 629 |
| Directors 55+ | CH `/company/{n}/officers` API (live enrichment) | March 2026 | 1,716 companies queried |

> **Note:** The lift maintenance file is from the most enriched run (including Competitor Maps with 81 entries and 8 Prime Targets). Two earlier lift runs are archived locally but not committed as they contain less enriched data.
