"""
Example config: UK Fire & Security services
"""

SECTOR_LABEL = "UK Fire Protection & Security Services"

SIC_CODES = [
    "80200",   # Security systems service activities
    "43210",   # Electrical installation
    "71200",   # Technical testing and analysis
    "33200",   # Installation of industrial machinery
]

NAME_QUERIES = [
    "fire protection",
    "fire safety",
    "fire suppression",
    "fire systems",
    "security systems",
    "intruder alarm",
    "access control",
]

INCLUDE_STEMS = [
    "fire protect", "fire safety", "fire suppress", "fire system",
    "fire alarm", "sprinkler", "security system", "intruder alarm",
    "access control", "cctv install", "fire & security",
]

EXCLUDE_TERMS = [
    "dental", "recruitment", "marketing", "photography", "estate agent",
    "nursery", "funeral", "fashion", "cleaning", "landscaping",
    "accountant", "solicitor", "catering",
]

EXCLUDE_SUBSECTORS = []

REVENUE_PER_HEAD_LOW  = 60_000
REVENUE_PER_HEAD_MID  = 78_000
REVENUE_PER_HEAD_HIGH = 95_000
ASSET_TURNOVER_RATIO  = 2.0
EBITDA_MARGIN_LOW     = 0.10
EBITDA_MARGIN_BASE    = 0.15
EBITDA_MARGIN_HIGH    = 0.22

TARGET_REVENUE_MIN  = 5_000_000
TARGET_REVENUE_MAX  = 30_000_000
TARGET_EBITDA_MIN   = 750_000
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
