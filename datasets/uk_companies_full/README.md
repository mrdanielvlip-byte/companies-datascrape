# UK Companies House — Full Company Register

**Source:** [Companies House Free Data Product](https://download.companieshouse.gov.uk/en_output.html)
**Snapshot date:** 2 March 2026
**Total companies:** 5,677,276

---

## Files

| File | Size | Rows |
|------|------|------|
| `BasicCompanyData-2026-03-02-part1_7.zip` | 70 MB | 850,000 |
| `BasicCompanyData-2026-03-02-part2_7.zip` | 71 MB | 850,000 |
| `BasicCompanyData-2026-03-02-part3_7.zip` | 71 MB | 850,000 |
| `BasicCompanyData-2026-03-02-part4_7.zip` | 71 MB | 850,000 |
| `BasicCompanyData-2026-03-02-part5_7.zip` | 71 MB | 850,000 |
| `BasicCompanyData-2026-03-02-part6_7.zip` | 71 MB | 850,000 |
| `BasicCompanyData-2026-03-02-part7_7.zip` | 48 MB | 577,277 |

Each ZIP contains a single CSV of the same name. No password required.

---

## Columns

| Column | Description |
|--------|-------------|
| `CompanyName` | Registered company name |
| `CompanyNumber` | 8-digit CH identifier |
| `RegAddress.*` | Registered address fields (PostTown, PostCode, Country, etc.) |
| `CompanyCategory` | e.g. Private Limited Company, LLP, PLC |
| `CompanyStatus` | Active, Dissolved, Liquidation, etc. |
| `IncorporationDate` | Date registered at CH |
| `DissolutionDate` | Date dissolved (blank if active) |
| `SICCode.SicText_1–4` | Up to 4 SIC codes with description |
| `Accounts.*` | Accounts reference dates and category |
| `Mortgages.*` | Charge counts |
| `PreviousName_1–10` | Previous company names with change dates |
| `ConfStmtNextDueDate` | Confirmation statement due date |
| `URI` | CH public URI |

---

## Quick queries (pandas)

```python
import pandas as pd, zipfile, io

def load_all_parts(base_path="datasets/uk_companies_full"):
    dfs = []
    for i in range(1, 8):
        fname = f"{base_path}/BasicCompanyData-2026-03-02-part{i}_7.zip"
        with zipfile.ZipFile(fname) as z:
            csv_name = fname.split("/")[-1].replace(".zip", ".csv")
            with z.open(csv_name) as f:
                dfs.append(pd.read_csv(f, dtype=str, low_memory=False))
    return pd.concat(dfs, ignore_index=True)

df = load_all_parts()
df.columns = df.columns.str.strip()

# All active companies
active = df[df["CompanyStatus"] == "Active"]

# Filter by SIC — e.g. all lift/elevator companies
lift = df[df["SICCode.SicText_1"].str.contains("43290|28221", na=False)]

# Filter by postcode region
london = df[df["RegAddress.PostCode"].str.startswith("SW", na=False)]

# Companies incorporated in last 5 years
df["IncorporationDate"] = pd.to_datetime(df["IncorporationDate"], errors="coerce")
recent = df[df["IncorporationDate"] >= "2020-01-01"]
```

---

## Officers / Directors

**Companies House does not publish a bulk directors export.** Director data must be queried
via the CH API per company (`/company/{number}/officers`). The `uk_ch_intel/` platform
in this repo handles this at scale using a rate-limited queue worker.

For the 1,716 lift maintenance + roofing companies already in our pipeline, pre-enriched
director data (554 directors aged 55+) is available in `datasets/directors_enrichment/`.

---

## Refreshing this data

CH publishes a new snapshot on the first working day of each month.

```bash
# Re-download latest snapshot (update date in filename)
BASE="https://download.companieshouse.gov.uk"
DATE="2026-04-01"  # update to current month
for i in 1 2 3 4 5 6 7; do
  curl -L "$BASE/BasicCompanyData-${DATE}-part${i}_7.zip" \
       -o "datasets/uk_companies_full/BasicCompanyData-${DATE}-part${i}_7.zip"
done
```

Check the current filename at: https://download.companieshouse.gov.uk/en_output.html
