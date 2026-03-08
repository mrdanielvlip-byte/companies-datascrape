# Companies House PE Deal-Sourcing Pipeline

Institutional-grade UK SME acquisition intelligence platform. Sweeps the Companies House API by SIC code and keyword, enriches every company with director data, charges, dealability signals, financial estimates, and contact intelligence — then scores each against a 4-dimension PE acquisition model.

Designed to replicate the analytical rigour of Dun & Bradstreet / Creditsafe, optimised specifically for PE buy-and-build target identification.

---

## Pipeline overview

```
Step 1  ch_search.py       SIC sweep + name search → all active companies
Step 2  (filter)           Remove false positives using keyword rules
Step 3  ch_enrich.py       Directors, PSC, charges, dealability signals, acquisition score
Step 4  ch_financials.py   Accounts metadata + 3-model revenue/EBITDA estimation
Step 5  ch_contacts.py     Website identification + email pattern inference
Step 6  bolt_on.py         Sector adjacency + market fragmentation analysis
Step 7  build_excel.py     6-sheet Excel workbook output
```

---

## Acquisition scoring model

```
Acquisition Score =
  Scale & Financial      × 30%
  Market Attractiveness  × 20%
  Ownership & Succession × 30%
  Dealability Signals    × 20%
```

| Score   | Classification          |
|---------|-------------------------|
| 80–100  | Prime acquisition target |
| 65–79   | High priority           |
| 50–64   | Medium priority         |
| < 50    | Intelligence record only |

All companies remain in the dataset regardless of score.

---

## Data reliability tiers

| Tier   | Source                                                            |
|--------|-------------------------------------------------------------------|
| Tier 1 | Companies House official registry (officers, PSC, charges, filings) |
| Tier 2 | Structured industry datasets (UKAS, Contracts Finder)            |
| Tier 3 | Verified corporate websites                                       |
| Tier 4 | Derived estimates (financial models)                              |

Every output value carries its source tier. Where conflicts exist, Tier 1 takes precedence.

---

## Output — Excel workbook (6 sheets)

| Sheet               | Contents                                                          |
|---------------------|-------------------------------------------------------------------|
| PE Pipeline         | All companies ranked by acquisition score with auto-filter        |
| Top 30 Profiles     | Detailed intelligence cards: directors, signals, score breakdown  |
| Director Contacts   | Email patterns, website URL, confidence ratings per director      |
| Financial Estimates | Revenue/EBITDA low/base/high with model formula shown             |
| Bolt-On Analysis    | Market fragmentation index + sector adjacency recommendations     |
| Summary Stats       | Pipeline KPIs: grade distribution, succession, dealability        |

---

## Setup

```bash
pip install -r requirements.txt
```

Create `.ch_api_key` in the project root:
```
COMPANIES_HOUSE_API_KEY=your-key-here
```

Free API key: https://developer.company-information.service.gov.uk/

---

## Usage

```bash
# Full pipeline
python run.py

# Different sector
python run.py --config configs.plumbing_hvac
python run.py --config configs.fire_security

# Skip contact enrichment (faster)
python run.py --skip-contacts

# Individual steps
python run.py --search-only        # Steps 1–2
python run.py --enrich-only        # Step 3
python run.py --financials-only    # Step 4
python run.py --contacts-only      # Step 5
python run.py --excel-only         # Steps 6–7
```

---

## Creating a new sector config

Copy `config.py` to `configs/my_sector.py` and edit these key fields:

| Field                       | Description                                    |
|-----------------------------|------------------------------------------------|
| `SECTOR_LABEL`              | Display name                                   |
| `SIC_CODES`                 | List of relevant SIC codes                     |
| `NAME_QUERIES`              | Keyword phrases for CH name search             |
| `INCLUDE_STEMS`             | Substrings that must appear in company name    |
| `EXCLUDE_TERMS`             | Substrings that disqualify a company           |
| `SECTOR_BENCHMARKS`         | Revenue/head, asset turnover, EBITDA margins   |
| `MARKET_ATTRACTIVENESS_SCORE` | Fixed sector score 0–100                     |
| `BOLT_ON_ADJACENCIES`       | Sector-specific service adjacency map          |

Then run: `python run.py --config configs.my_sector`

---

## Financial models

- **Employee model**: `Revenue = Employees × Revenue per Employee`
- **Asset model**: `Revenue = Total Assets × Sector Asset Turnover Ratio`
- **Location model**: `Revenue = Sites × Revenue per Site Benchmark`
- **EBITDA**: `EBITDA = Revenue × Sector EBITDA Margin`

All estimates are Tier 4 (derived). Low / base / high ranges provided. For most UK SMEs, only balance sheet data is publicly available (Total Exemption accounts — turnover not disclosed).

---

## Director contact intelligence

For each company (top N by acquisition score):
1. Identifies official website via DuckDuckGo + domain pattern inference
2. Scrapes homepage and contact pages for phone numbers and email addresses
3. Infers director email patterns using company domain
4. Scores confidence: **High** (verified) / **Medium** (domain confirmed) / **Low** (inferred)

---

## Included sector configs

| File                       | Sector                                |
|----------------------------|---------------------------------------|
| `config.py`                | Calibration & Metrology Laboratories  |
| `configs/plumbing_hvac.py` | Plumbing, Heating & HVAC              |
| `configs/fire_security.py` | Fire Protection & Security Systems    |

---

## Security

- `.ch_api_key` is **gitignored** — never committed
- `output/` is gitignored — re-run the pipeline to regenerate
