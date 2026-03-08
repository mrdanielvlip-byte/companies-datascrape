"""
config.py — Sector configuration for the Companies House PE deal-sourcing pipeline.

Swap this file (or pass --config configs.my_sector) to target any sector.
"""

# ── API ───────────────────────────────────────────────────────────────────────
# Key is loaded from .ch_api_key file in the project root.
# Format: COMPANIES_HOUSE_API_KEY=your-key-here

# ── SECTOR DEFINITION ─────────────────────────────────────────────────────────
SECTOR_LABEL = "UK Calibration & Metrology Laboratories"

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

NAME_QUERIES = [
    "calibration",
    "calibration laboratory",
    "calibration services",
    "calibration lab",
    "metrology",
    "metrology services",
]

INCLUDE_STEMS = [
    "calibrat", "metrolog", "ndt", "non-destruct", "nondestructiv",
    "dimensional measurement", "gauge calibr", "torque calibr",
    "pressure calibr", "instrument calibr", "test equipment",
]

EXCLUDE_TERMS = [
    "dental", "dentist", "catering", "restaurant", "hair", "beauty",
    "tattoo", "photography", "photographer", "driving school", "estate agent",
    "nursery", "funeral", "fashion", "cleaning", "recruitment", "marketing",
    "painting", "plumbing", "landscaping", "letting", "accountant",
    "solicitor", "flooring", "building services", "construction",
    "civil engineering", "architecture", "pathology", "veterinary",
    "pharmaceutical manufacturing",
]

EXCLUDE_SUBSECTORS = [
    "adas",   # Advanced Driver Assistance Systems — different sub-sector
]

# ── SECTOR BENCHMARK TABLE ────────────────────────────────────────────────────
# Source: sector research, Tier 4 derived estimates
# Edit these for each new sector config

SECTOR_BENCHMARKS = {
    # Revenue per employee (£/head)
    "revenue_per_head_low":   60_000,
    "revenue_per_head_base":  75_000,
    "revenue_per_head_high":  92_000,

    # Asset turnover ratio (Revenue / Total Assets)
    "asset_turnover_ratio":   2.0,

    # Revenue per operating site (£ — if location data available)
    "revenue_per_site":       800_000,

    # EBITDA margins
    "ebitda_margin_low":   0.10,
    "ebitda_margin_base":  0.15,
    "ebitda_margin_high":  0.20,

    # Market size and fragmentation
    "estimated_market_size_gbp": 2_000_000_000,  # £2bn UK calibration market
    "estimated_top5_market_share": 0.25,           # ~25% — highly fragmented
    "sector_b2b_score": 95,                        # % of revenue from B2B customers
}

# Alias for backwards compatibility with ch_financials.py
REVENUE_PER_HEAD_LOW  = SECTOR_BENCHMARKS["revenue_per_head_low"]
REVENUE_PER_HEAD_MID  = SECTOR_BENCHMARKS["revenue_per_head_base"]
REVENUE_PER_HEAD_HIGH = SECTOR_BENCHMARKS["revenue_per_head_high"]
ASSET_TURNOVER_RATIO  = SECTOR_BENCHMARKS["asset_turnover_ratio"]

EBITDA_MARGIN_LOW  = SECTOR_BENCHMARKS["ebitda_margin_low"]
EBITDA_MARGIN_BASE = SECTOR_BENCHMARKS["ebitda_margin_base"]
EBITDA_MARGIN_HIGH = SECTOR_BENCHMARKS["ebitda_margin_high"]

# ── PE TARGET CRITERIA ───────────────────────────────────────────────────────
TARGET_REVENUE_MIN  = 5_000_000    # £5M
TARGET_REVENUE_MAX  = 30_000_000   # £30M
TARGET_EBITDA_MIN   = 1_000_000    # £1M
TARGET_EBITDA_MAX   = 5_000_000    # £5M
FOUNDER_AGE_FLOOR   = 55

# ── ACQUISITION SCORE WEIGHTS ─────────────────────────────────────────────────
# Per institutional PE spec — must conceptually sum to 100%
# Implemented as dimension multipliers in ch_enrich.py:
#   Scale & Financial     × 0.30
#   Market Attractiveness × 0.20
#   Ownership & Succession× 0.30
#   Dealability Signals   × 0.20

SCORE_WEIGHTS = {
    "scale_financial":      0.30,
    "market_attractiveness":0.20,
    "ownership_succession": 0.30,
    "dealability_signals":  0.20,
}

# Score interpretation thresholds
SCORE_THRESHOLDS = {
    "prime":      80,   # Prime acquisition target
    "high":       65,   # High priority
    "medium":     50,   # Medium priority
    # < 50 = Intelligence record only
}

# ── MARKET ATTRACTIVENESS SCORE ───────────────────────────────────────────────
# Fixed score for this sector (0–100). Used in market attractiveness dimension.
# UK calibration: highly fragmented, B2B, recurring contracts → score 78
MARKET_ATTRACTIVENESS_SCORE = 78

# ── CONTACT ENRICHMENT ────────────────────────────────────────────────────────
# Number of top-ranked companies to attempt contact/website enrichment on
CONTACT_ENRICH_TOP_N = 50

# ── BOLT-ON ADJACENCIES ───────────────────────────────────────────────────────
# Sector-specific bolt-on opportunity map
BOLT_ON_ADJACENCIES = [
    {
        "cluster":          "Dimensional Inspection & NDT",
        "rationale":        "Shared customer base, complementary equipment, natural upsell path",
        "bolt_on_services": ["3D scanning", "CMM inspection", "ultrasonic NDT", "radiographic testing"],
        "sic_codes":        ["71200", "71122"],
        "opportunity_score": 9,
    },
    {
        "cluster":          "Instrument Repair & Asset Management",
        "rationale":        "Recurring revenue; customers already own calibrated equipment",
        "bolt_on_services": ["instrument repair", "preventive maintenance", "equipment lifecycle management"],
        "sic_codes":        ["33130", "33190", "33140"],
        "opportunity_score": 8,
    },
    {
        "cluster":          "Compliance & Quality Consulting",
        "rationale":        "Higher-margin advisory on top of technical services; ISO 17025 consulting",
        "bolt_on_services": ["UKAS accreditation support", "ISO 17025 consulting", "quality management systems"],
        "sic_codes":        ["74909", "71122"],
        "opportunity_score": 7,
    },
    {
        "cluster":          "Environmental & Emissions Testing",
        "rationale":        "Growing regulatory demand; adjacent technical capability",
        "bolt_on_services": ["emissions testing", "environmental monitoring", "noise measurement"],
        "sic_codes":        ["71200"],
        "opportunity_score": 6,
    },
]

# ── OUTPUT ────────────────────────────────────────────────────────────────────
OUTPUT_DIR    = "output"
RAW_JSON      = "raw_companies.json"
FILTERED_JSON = "filtered_companies.json"
ENRICHED_JSON = "enriched_companies.json"
EXCEL_OUTPUT  = "PE_Pipeline.xlsx"
