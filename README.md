# Companies House PE Deal-Sourcing Pipeline

Automated pipeline for identifying UK SME acquisition targets in any sector using the Companies House public API. Scores every active company by succession risk, founder retirement signal, PE independence, and operational maturity.

## What it does

1. **Search** — sweeps Companies House by SIC code + keyword name search across all active companies
2. **Filter** — removes false positives (dental labs, recruiters, etc.) using configurable keyword rules
3. **Enrich** — pulls directors, PSC (shareholders), and company age for every match via the API
4. **Score** — applies a 6-dimension weighted acquisition score (0–100) to each company
5. **Export** — produces a formatted Excel workbook with ranked pipeline, top-30 profiles, and summary stats

## Setup

```bash
pip install -r requirements.txt
```

Create a `.ch_api_key` file in the project root:

```
COMPANIES_HOUSE_API_KEY=your-key-here
```

Get a free API key at: https://developer.company-information.service.gov.uk/

## Usage

```bash
# Full pipeline (search → enrich → Excel)
python run.py

# Use a different sector config
python run.py --config configs.plumbing_hvac
python run.py --config configs.fire_security

# Run individual steps
python run.py --search-only      # Pull raw companies
python run.py --enrich-only      # Enrich (uses existing raw JSON)
python run.py --excel-only       # Rebuild Excel only
```

Output is saved to `output/PE_Pipeline.xlsx`.

## Targeting a new sector

Copy `config.py` to `configs/my_sector.py` and edit:

| Field | Description |
|---|---|
| `SECTOR_LABEL` | Display name for reports |
| `SIC_CODES` | List of relevant SIC codes |
| `NAME_QUERIES` | Keyword phrases for name search |
| `INCLUDE_STEMS` | Substrings that must appear in company name |
| `EXCLUDE_TERMS` | Substrings that disqualify a company |
| `REVENUE_PER_HEAD_*` | Revenue/employee benchmarks (£) |
| `EBITDA_MARGIN_*` | Sector EBITDA margin range |
| `SCORE_WEIGHTS` | Acquisition score dimension weights (must sum to 100) |

Then run:
```bash
python run.py --config configs.my_sector
```

## Acquisition scoring model

| Dimension | Weight | Signal |
|---|---|---|
| Scale fit | 25% | Company age as maturity proxy |
| Founder retirement | 20% | Max director age |
| Succession weakness | 20% | Director count + age distribution |
| Independence | 15% | Not PE-backed (PSC check) |
| Sector fragmentation | 10% | Fixed for each sector |
| Operational signals | 10% | Company age, stability |

**Grade thresholds:** A+ ≥85 · A ≥80 · B+ ≥75 · B ≥70 · C ≥60 · D <60

## Example sectors already configured

- `config.py` — Calibration & Metrology Laboratories
- `configs/plumbing_hvac.py` — Plumbing, Heating & HVAC
- `configs/fire_security.py` — Fire Protection & Security Systems

## Notes

- The `.ch_api_key` file is excluded from git via `.gitignore` — never commit your key
- Output JSON and Excel are also excluded — re-run the pipeline to regenerate
- Rate limiting: the script uses 50–100ms delays between API calls; a full 300-company run takes ~3–4 minutes
