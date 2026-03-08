"""
Example config: UK Plumbing & HVAC installers — owner-managed SMEs
Copy this file and edit to target any sector.
"""

SECTOR_LABEL = "UK Plumbing, Heating & HVAC"

SIC_CODES = [
    "43220",   # Plumbing, heat and air-conditioning installation
    "43210",   # Electrical installation
    "43290",   # Other construction installation
    "33120",   # Repair of machinery
]

NAME_QUERIES = [
    "plumbing services",
    "heating services",
    "hvac services",
    "air conditioning services",
    "mechanical services",
]

INCLUDE_STEMS = [
    "plumb", "heating", "hvac", "air condition", "mechanical services",
    "refrigerat", "ventilat", "boiler",
]

EXCLUDE_TERMS = [
    "dental", "recruitment", "marketing", "photography", "estate agent",
    "nursery", "funeral", "fashion", "cleaning", "landscaping",
    "accountant", "solicitor",
]

EXCLUDE_SUBSECTORS = []

REVENUE_PER_HEAD_LOW  = 55_000
REVENUE_PER_HEAD_MID  = 70_000
REVENUE_PER_HEAD_HIGH = 85_000
ASSET_TURNOVER_RATIO  = 1.8
EBITDA_MARGIN_LOW     = 0.08
EBITDA_MARGIN_BASE    = 0.12
EBITDA_MARGIN_HIGH    = 0.18

TARGET_REVENUE_MIN  = 5_000_000
TARGET_REVENUE_MAX  = 30_000_000
TARGET_EBITDA_MIN   = 500_000
TARGET_EBITDA_MAX   = 5_000_000
FOUNDER_AGE_FLOOR   = 55

SCORE_WEIGHTS = {
    "scale_fit":           25,
    "founder_retirement":  20,
    "succession_weakness": 20,
    "independence":        15,
    "sector_fragmentation": 10,
    "operational_signals":  10,
}

OUTPUT_DIR    = "output"
RAW_JSON      = "raw_companies.json"
FILTERED_JSON = "filtered_companies.json"
ENRICHED_JSON = "enriched_companies.json"
EXCEL_OUTPUT  = "PE_Pipeline.xlsx"
