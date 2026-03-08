"""
Sector configuration for the Companies House PE deal-sourcing pipeline.
Swap this file (or pass a different config path) to run a different sector search.
"""

# ── API ───────────────────────────────────────────────────────────────────────
# Key is loaded from .ch_api_key file in the script root directory.
# Format of .ch_api_key:
#   COMPANIES_HOUSE_API_KEY=your-key-here

# ── SECTOR DEFINITION ─────────────────────────────────────────────────────────
SECTOR_LABEL = "UK Calibration & Metrology Laboratories"

# SIC codes to sweep (all active companies with any of these codes are pulled)
SIC_CODES = [
    "71200",   # Technical testing and analysis
    "33190",   # Repair of other equipment
    "33130",   # Repair of electronic and optical equipment
    "26511",   # Manufacture of electronic measuring equipment
    "26513",   # Manufacture of industrial process control equipment
    "74909",   # Other professional, scientific and technical activities n.e.c.
    "71122",   # Engineering related scientific and technical consulting activities
    "33140",   # Repair of electrical equipment
]

# Direct name search keywords (hits Companies House search endpoint)
NAME_QUERIES = [
    "calibration",
    "calibration laboratory",
    "calibration services",
    "calibration lab",
    "metrology",
    "metrology services",
]

# Keywords that must appear in company name to be retained after name search
# (prevents false positives from fuzzy search results)
INCLUDE_STEMS = [
    "calibrat",
    "metrolog",
    "ndt",
    "non-destruct",
    "nondestructiv",
    "dimensional measurement",
    "gauge calibr",
    "torque calibr",
    "pressure calibr",
    "instrument calibr",
    "test equipment",
]

# Company name substrings that disqualify a result
EXCLUDE_TERMS = [
    "dental", "dentist", "catering", "restaurant", "hair", "beauty",
    "tattoo", "photography", "photographer", "driving school", "estate agent",
    "nursery", "funeral", "fashion", "cleaning", "recruitment", "marketing",
    "painting", "plumbing", "landscaping", "letting", "accountant",
    "solicitor", "flooring", "building services", "construction",
    "civil engineering", "architecture", "pathology", "veterinary",
    "pharmaceutical manufacturing",
]

# Exclude ADAS (automotive sensor calibration) — different sub-sector
EXCLUDE_SUBSECTORS = [
    "adas",  # Advanced Driver Assistance Systems calibration
]

# ── FINANCIAL BENCHMARKS (edit per sector) ───────────────────────────────────
REVENUE_PER_HEAD_LOW  = 60_000   # £/employee — pessimistic
REVENUE_PER_HEAD_MID  = 75_000   # £/employee — base case
REVENUE_PER_HEAD_HIGH = 92_000   # £/employee — optimistic
ASSET_TURNOVER_RATIO  = 2.0      # Revenue ≈ total_assets × this ratio
EBITDA_MARGIN_LOW     = 0.10
EBITDA_MARGIN_BASE    = 0.15
EBITDA_MARGIN_HIGH    = 0.20

# ── PE TARGET CRITERIA ───────────────────────────────────────────────────────
TARGET_REVENUE_MIN  = 5_000_000   # £5M
TARGET_REVENUE_MAX  = 30_000_000  # £30M
TARGET_EBITDA_MIN   = 1_000_000   # £1M
TARGET_EBITDA_MAX   = 5_000_000   # £5M
FOUNDER_AGE_FLOOR   = 55          # Flag founders/directors aged 55+

# ── ACQUISITION SCORE WEIGHTS (must sum to 100) ──────────────────────────────
SCORE_WEIGHTS = {
    "scale_fit":          25,  # Company size vs target range
    "founder_retirement": 20,  # Max director age signal
    "succession_weakness":20,  # Overall succession score
    "independence":       15,  # Not PE-backed
    "sector_fragmentation":10, # Fixed for this sector
    "operational_signals": 10, # Company age, UKAS, etc.
}

# ── OUTPUT ────────────────────────────────────────────────────────────────────
OUTPUT_DIR         = "output"
RAW_JSON           = "raw_companies.json"
FILTERED_JSON      = "filtered_companies.json"
ENRICHED_JSON      = "enriched_companies.json"
EXCEL_OUTPUT       = "PE_Pipeline.xlsx"
