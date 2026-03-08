"""
build_excel.py — Generate PE pipeline Excel workbook from enriched JSON

Sheets:
  1. PE Pipeline       — all companies ranked by acquisition score
  2. Top 30 Profiles   — detailed per-company intelligence cards
  3. Director Contacts — contact intelligence for top companies
  4. Financials        — revenue/EBITDA estimates and balance sheet
  5. Bolt-On Analysis  — sector adjacency and roll-up opportunities
  6. Summary Stats     — pipeline KPIs
"""

import json
import os
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

import config as cfg

# ── Colour palette ─────────────────────────────────────────────────────────
NAVY   = "1F3864"
BLUE   = "2E75B6"
WHITE  = "FFFFFF"
ALT    = "EBF3FB"
GREEN  = "E2EFDA"
RED    = "FCE4D6"
AMBER  = "FFEB9C"
GREY   = "F2F2F2"
DKGREY = "404040"

THIN = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)

def fill(hex_color):
    return PatternFill("solid", fgColor=hex_color)

def score_fill(score):
    if score >= 80: return fill("375623")   # dark green = Prime
    if score >= 65: return fill("70AD47")   # green = High
    if score >= 50: return fill(AMBER)      # amber = Medium
    return fill("FF7070")                    # red = Intel only

def score_font_color(score):
    return WHITE if score >= 65 else "000000"

def cell(ws, row, col, value, bg=None, fg="000000", bold=False,
         align="left", wrap=False, size=9, border=True):
    c = ws.cell(row=row, column=col, value=value)
    if bg:
        c.fill = fill(bg)
    c.font = Font(name="Arial", size=size, bold=bold, color=fg)
    c.alignment = Alignment(horizontal=align, vertical="center", wrap_text=wrap)
    if border:
        c.border = THIN
    return c

def title_row(ws, row, cols_span, text, bg=NAVY, size=13):
    ws.merge_cells(f"A{row}:{get_column_letter(cols_span)}{row}")
    ws.row_dimensions[row].height = 28
    c = ws.cell(row=row, column=1, value=text)
    c.fill = fill(bg)
    c.font = Font(name="Arial", bold=True, size=size, color=WHITE)
    c.alignment = Alignment(horizontal="center", vertical="center")

def sub_row(ws, row, cols_span, text, bg=BLUE):
    ws.merge_cells(f"A{row}:{get_column_letter(cols_span)}{row}")
    ws.row_dimensions[row].height = 18
    c = ws.cell(row=row, column=1, value=text)
    c.fill = fill(bg)
    c.font = Font(name="Arial", italic=True, size=9, color=WHITE)
    c.alignment = Alignment(horizontal="center", vertical="center")


# ── Sheet 1: Full pipeline ────────────────────────────────────────────────────

PIPELINE_COLS = [
    ("Rank", 5), ("Reg. No.", 12), ("Company Name", 48), ("Incorp.", 11),
    ("Age", 7), ("Dirs", 5), ("Max Age", 8), ("Avg Age", 8),
    ("Succ.", 7), ("Deal.", 7), ("Acq. Score", 10), ("Grade", 10),
    ("PE", 5), ("Family", 6),
    ("Scale", 7), ("Market", 7), ("Own./Succ.", 10), ("Dealability", 11),
    ("Charges", 8), ("SIC", 22),
]

def build_pipeline(wb, companies):
    ws = wb.active
    ws.title = "PE Pipeline"
    n = len(PIPELINE_COLS)

    title_row(ws, 1, n, f"{cfg.SECTOR_LABEL}  —  PE Pipeline  ({len(companies)} companies)  |  March 2026")
    sub_row(ws, 2, n,
            f"Acquisition Score = Scale & Financial(30%) + Market Attractiveness(20%) + "
            f"Ownership & Succession(30%) + Dealability(20%)  |  Source: Companies House API  |  All data Tier 1")

    ws.row_dimensions[3].height = 36
    for ci, (label, width) in enumerate(PIPELINE_COLS, 1):
        cell(ws, 3, ci, label, bg=NAVY, fg=WHITE, bold=True, align="center", wrap=True)
        ws.column_dimensions[get_column_letter(ci)].width = width

    for i, c in enumerate(companies, 1):
        row = i + 3
        bg  = ALT if i % 2 == 0 else None
        acq = c["acquisition_score"]
        comp= c.get("acq_components", {})
        ss  = c.get("succession", {})
        deal= c.get("dealability", {})
        ch  = c.get("charges", {})

        cell(ws, row, 1,  i,                              bg=bg, align="center", bold=True)
        cell(ws, row, 2,  c["company_number"],            bg=bg)
        cell(ws, row, 3,  c["company_name"],              bg=bg)
        cell(ws, row, 4,  (c.get("date_of_creation") or "")[:4], bg=bg, align="center")
        cell(ws, row, 5,  c.get("company_age_years", 0), bg=bg, align="center")
        cell(ws, row, 6,  c.get("director_count", 0),    bg=bg, align="center")
        cell(ws, row, 7,  ss.get("max_age") or "-",      bg=bg, align="center")
        cell(ws, row, 8,  ss.get("avg_age") or "-",      bg=bg, align="center")
        cell(ws, row, 9,  ss.get("total", 0),            bg=bg, align="center")
        cell(ws, row, 10, deal.get("score", 0),          bg=bg, align="center")

        # Acquisition score
        for col in (11, 12):
            val = acq if col == 11 else c.get("acquisition_grade", "")
            cx  = ws.cell(row=row, column=col, value=val)
            cx.fill      = score_fill(acq)
            cx.font      = Font(name="Arial", size=9, bold=True, color=score_font_color(acq))
            cx.alignment = Alignment(horizontal="center", vertical="center")
            cx.border    = THIN

        cell(ws, row, 13, "⚠" if c.get("pe_backed") else "-",
             bg=RED if c.get("pe_backed") else bg, align="center")
        cell(ws, row, 14, "✓" if c.get("is_family") else "-",
             bg=GREEN if c.get("is_family") else bg, align="center")

        cell(ws, row, 15, comp.get("scale_financial", 0),       bg=bg, align="center")
        cell(ws, row, 16, comp.get("market_attractiveness", 0), bg=bg, align="center")
        cell(ws, row, 17, comp.get("ownership_succession", 0),  bg=bg, align="center")
        cell(ws, row, 18, comp.get("dealability", 0),           bg=bg, align="center")
        cell(ws, row, 19, ch.get("outstanding_charges", "-"),   bg=bg, align="center")
        cell(ws, row, 20, ", ".join(c.get("sic_codes", [])),    bg=bg, size=8)

    ws.freeze_panes = "A4"
    ws.auto_filter.ref = f"A3:{get_column_letter(n)}{len(companies)+3}"


# ── Sheet 2: Top 30 profiles ──────────────────────────────────────────────────

def build_top30(wb, companies):
    ws  = wb.create_sheet("Top 30 Profiles")
    top = companies[:30]
    n   = 14

    title_row(ws, 1, n, "TOP 30 PE ACQUISITION TARGETS — INTELLIGENCE PROFILES")

    row = 2
    for rank, c in enumerate(top, 1):
        acq  = c["acquisition_score"]
        ss   = c.get("succession", {})
        deal = c.get("dealability", {})
        comp = c.get("acq_components", {})
        ch   = c.get("charges", {})

        ws.row_dimensions[row].height = 20
        ws.merge_cells(f"A{row}:{get_column_letter(n)}{row}")
        hdr = ws.cell(row=row, column=1,
                      value=f"#{rank}  {c['company_name']}  |  Reg: {c['company_number']}"
                            f"  |  Score: {acq}  |  {c.get('acquisition_grade','')}")
        hdr.fill      = score_fill(acq)
        hdr.font      = Font(name="Arial", bold=True, size=10, color=score_font_color(acq))
        hdr.alignment = Alignment(horizontal="left", vertical="center")
        row += 1

        # Metrics grid
        meta_labels = ["Incorp.", "Age", "Directors", "Max Dir. Age",
                       "Succ. Score", "Deal. Score", "PE Backed", "Charges",
                       "Scale", "Market", "Own./Succ.", "Dealability", "Family", "SIC"]
        meta_vals = [
            (c.get("date_of_creation") or "")[:4],
            f"{c.get('company_age_years',0)} yrs",
            c.get("director_count", 0),
            ss.get("max_age") or "N/A",
            ss.get("total", 0),
            deal.get("score", 0),
            "YES ⚠" if c.get("pe_backed") else "No",
            ch.get("outstanding_charges", "N/A"),
            comp.get("scale_financial", 0),
            comp.get("market_attractiveness", 0),
            comp.get("ownership_succession", 0),
            comp.get("dealability", 0),
            "YES" if c.get("is_family") else "No",
            ", ".join(c.get("sic_codes", [])),
        ]
        for ci, (lbl, val) in enumerate(zip(meta_labels, meta_vals), 1):
            cell(ws, row,   ci, lbl, bg=BLUE, fg=WHITE, bold=True, align="center", size=8)
            cell(ws, row+1, ci, val, bg=ALT,  align="center", size=9)
        row += 2

        # Succession detail
        if ss.get("total"):
            ws.merge_cells(f"A{row}:{get_column_letter(n)}{row}")
            sh = ws.cell(row=row, column=1,
                         value=f"  Succession Detail  |  Age Score: {ss.get('age_score',0)}  "
                               f"|  Director Score: {ss.get('dir_score',0)}  "
                               f"|  Distribution: {ss.get('dist_score',0)}  "
                               f"|  Governance Penalty: {ss.get('governance_penalty',0)}"
                               f"  |  Formula: {ss.get('formula','')}")
            sh.font      = Font(name="Arial", size=8, italic=True)
            sh.alignment = Alignment(horizontal="left", vertical="center")
            sh.border    = THIN
            row += 1

        # Dealability signals
        signals = deal.get("signals", [])
        if signals:
            ws.merge_cells(f"A{row}:{get_column_letter(n)}{row}")
            sig_text = "  Dealability Signals:  " + "  |  ".join(
                f"{s['type']} ({s.get('date','')[:7]})" for s in signals[:4])
            sc = ws.cell(row=row, column=1, value=sig_text)
            sc.font      = Font(name="Arial", size=8, italic=True, color="1F3864")
            sc.alignment = Alignment(horizontal="left", vertical="center")
            sc.border    = THIN
            row += 1

        # Directors
        dirs = c.get("directors", [])
        if dirs:
            ws.merge_cells(f"A{row}:{get_column_letter(n)}{row}")
            dh = ws.cell(row=row, column=1, value="Directors / Officers  (Tier 1 — Companies House)")
            dh.fill      = fill(DKGREY)
            dh.font      = Font(name="Arial", bold=True, size=8, color=WHITE)
            dh.alignment = Alignment(horizontal="left", vertical="center")
            row += 1
            for d in dirs[:6]:
                age_str = f"Age ~{d['age']}" if d.get("age") else "Age unknown"
                txt = (f"  {d['name'].title()}  |  {age_str}  "
                       f"|  Appointed: {(d.get('appointed') or '')[:7]}  "
                       f"|  Tenure: {d.get('years_active',0):.1f} yrs  "
                       f"|  {(d.get('occupation') or '')[:40]}")
                ws.merge_cells(f"A{row}:{get_column_letter(n)}{row}")
                dc = ws.cell(row=row, column=1, value=txt)
                dc.font      = Font(name="Arial", size=8)
                dc.alignment = Alignment(horizontal="left", vertical="center")
                dc.border    = THIN
                row += 1

        ws.row_dimensions[row].height = 8
        row += 1

    for ci in range(1, n + 1):
        ws.column_dimensions[get_column_letter(ci)].width = 16
    ws.column_dimensions["A"].width = 65
    ws.column_dimensions["N"].width = 22


# ── Sheet 3: Director contacts ────────────────────────────────────────────────

def build_contacts(wb, companies):
    ws = wb.create_sheet("Director Contacts")
    n  = 10

    title_row(ws, 1, n, "DIRECTOR CONTACT INTELLIGENCE")
    sub_row(ws, 2, n,
            "High = verified on site  |  Medium = pattern inferred from confirmed domain  "
            "|  Low = domain unconfirmed  |  Data tier shown per record")

    headers = ["Rank", "Company", "Director Name", "Role", "Age",
               "Best Email", "Confidence", "Email Pattern", "Website", "Data Tier"]
    widths  = [6, 40, 28, 20, 7, 38, 12, 30, 35, 25]

    ws.row_dimensions[3].height = 20
    for ci, (h, w) in enumerate(zip(headers, widths), 1):
        cell(ws, 3, ci, h, bg=NAVY, fg=WHITE, bold=True, align="center")
        ws.column_dimensions[get_column_letter(ci)].width = w

    row = 4
    for rank, c in enumerate(companies[:50], 1):
        contacts = c.get("contacts", {})
        dir_contacts = contacts.get("director_contacts", [])
        website = contacts.get("website", {})
        site_url = website.get("website_url", "") or ""

        if not dir_contacts:
            # Still show the company row even without contact data
            bg = ALT if rank % 2 == 0 else None
            cell(ws, row, 1, rank,              bg=bg, align="center")
            cell(ws, row, 2, c["company_name"], bg=bg)
            cell(ws, row, 3, "No contact data enriched", bg=bg, fg="888888")
            cell(ws, row, 9, site_url,          bg=bg)
            row += 1
            continue

        for di, d in enumerate(dir_contacts):
            bg = ALT if rank % 2 == 0 else None
            conf = d.get("email_confidence", "None")
            conf_bg = GREEN if conf == "High" else (AMBER if conf == "Medium" else (RED if conf == "Low" else bg))

            cell(ws, row, 1, rank if di == 0 else "",  bg=bg, align="center")
            cell(ws, row, 2, c["company_name"] if di == 0 else "", bg=bg)
            cell(ws, row, 3, d.get("name", ""),        bg=bg)
            cell(ws, row, 4, d.get("role", ""),        bg=bg)
            cell(ws, row, 5, d.get("age") or "-",      bg=bg, align="center")
            cell(ws, row, 6, d.get("best_email", ""),  bg=conf_bg)
            cell(ws, row, 7, conf,                     bg=conf_bg, align="center", bold=True)
            patterns = d.get("email_patterns", [])
            cell(ws, row, 8, patterns[0].get("pattern","") if patterns else "", bg=bg)
            cell(ws, row, 9, site_url if di == 0 else "", bg=bg)
            cell(ws, row, 10, d.get("data_tier",""),   bg=bg, size=8)
            row += 1

    ws.freeze_panes = "A4"


# ── Sheet 4: Financials ───────────────────────────────────────────────────────

def build_financials(wb, companies):
    ws = wb.create_sheet("Financial Estimates")
    n  = 14

    title_row(ws, 1, n, "FINANCIAL ESTIMATION — Revenue & EBITDA Models")
    sub_row(ws, 2, n,
            "Employee Model: Revenue = Employees × Rev/Head  |  "
            "Asset Model: Revenue = Total Assets × Turnover Ratio  |  "
            "All estimates Tier 4 — derived  |  Actual turnover not publicly available for Total Exemption filers")

    headers = ["Rank", "Company", "Accts Type", "Period End",
               "Rev Low (£)", "Rev Base (£)", "Rev High (£)", "Confidence",
               "EBITDA Low", "EBITDA Base", "EBITDA High",
               "Net Assets", "Charges", "Formula / Source"]
    widths  = [6, 42, 18, 12, 14, 14, 14, 12, 13, 13, 13, 14, 9, 45]

    ws.row_dimensions[3].height = 20
    for ci, (h, w) in enumerate(zip(headers, widths), 1):
        cell(ws, 3, ci, h, bg=NAVY, fg=WHITE, bold=True, align="center", wrap=True)
        ws.column_dimensions[get_column_letter(ci)].width = w

    def fmt(v):
        return f"£{v:,.0f}" if v else "-"

    row = 4
    for rank, c in enumerate(companies, 1):
        bg  = ALT if rank % 2 == 0 else None
        fin = c.get("financials", {})
        rev = fin.get("revenue_estimate", {})
        ebitda = fin.get("ebitda_estimate", {})
        bs  = fin.get("balance_sheet", {})
        ch  = c.get("charges", fin.get("charges", {}))
        ratios = fin.get("balance_sheet_ratios", {})

        cell(ws, row, 1,  rank,                          bg=bg, align="center")
        cell(ws, row, 2,  c["company_name"],             bg=bg)
        cell(ws, row, 3,  bs.get("accounts_type","N/A"), bg=bg, size=8)
        cell(ws, row, 4,  bs.get("period_end","")[:7],  bg=bg, align="center")
        cell(ws, row, 5,  fmt(rev.get("revenue_low")),  bg=bg, align="right")
        cell(ws, row, 6,  fmt(rev.get("revenue_base")), bg=bg, align="right", bold=True)
        cell(ws, row, 7,  fmt(rev.get("revenue_high")), bg=bg, align="right")
        cell(ws, row, 8,  rev.get("confidence","None"), bg=bg, align="center")
        cell(ws, row, 9,  fmt(ebitda.get("ebitda_low")),  bg=bg, align="right")
        cell(ws, row, 10, fmt(ebitda.get("ebitda_base")), bg=bg, align="right", bold=True)
        cell(ws, row, 11, fmt(ebitda.get("ebitda_high")), bg=bg, align="right")
        cell(ws, row, 12, fmt(ratios.get("net_assets")),  bg=bg, align="right")
        cell(ws, row, 13, ch.get("outstanding_charges","-"), bg=bg, align="center")
        cell(ws, row, 14, rev.get("formula",""), bg=bg, size=8)
        row += 1

    ws.freeze_panes = "A4"
    ws.auto_filter.ref = f"A3:{get_column_letter(n)}{len(companies)+3}"


# ── Sheet 5: Bolt-on analysis ─────────────────────────────────────────────────

def build_bolt_on(wb, bolt_on_data: dict):
    ws = wb.create_sheet("Bolt-On Analysis")

    title_row(ws, 1, 8, "BOLT-ON OPPORTUNITY ANALYSIS — Sector Adjacency & Roll-Up Map")

    # Market fragmentation
    row = 3
    frag = bolt_on_data.get("market_fragmentation", {})
    ws.merge_cells(f"A{row}:H{row}")
    fr = ws.cell(row=row, column=1,
                 value=f"  Market Fragmentation Index: {frag.get('fragmentation_index','-')}  |  "
                       f"{frag.get('interpretation','')}  |  "
                       f"Formula: {frag.get('formula','')}  |  "
                       f"Total Companies Analysed: {frag.get('total_companies_analysed','-')}")
    fr.font      = Font(name="Arial", bold=True, size=10, color=WHITE)
    fr.fill      = fill(NAVY)
    fr.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[row].height = 22
    row += 2

    # Bolt-on recommendations
    for rec in bolt_on_data.get("bolt_on_recommendations", []):
        ws.merge_cells(f"A{row}:H{row}")
        rh = ws.cell(row=row, column=1,
                     value=f"  {rec['cluster']}  —  Opportunity Score: {rec['opportunity_score']}/10")
        rh.font      = Font(name="Arial", bold=True, size=10, color=WHITE)
        rh.fill      = fill(BLUE)
        rh.alignment = Alignment(horizontal="left", vertical="center")
        ws.row_dimensions[row].height = 20
        row += 1

        cell(ws, row, 1, "Rationale",      bg=GREY, bold=True, size=9)
        ws.merge_cells(f"B{row}:H{row}")
        cell(ws, row, 2, rec["rationale"],  bg=None, size=9)
        row += 1

        cell(ws, row, 1, "Bolt-On Services", bg=GREY, bold=True, size=9)
        ws.merge_cells(f"B{row}:H{row}")
        cell(ws, row, 2, "  •  ".join(rec["bolt_on_services"]), bg=None, size=9)
        row += 1

        cell(ws, row, 1, "Example Targets", bg=GREY, bold=True, size=9)
        targets_text = "  |  ".join(
            f"{t['name']} (Score: {t['score']})" for t in rec.get("example_targets", [])[:5]
        )
        ws.merge_cells(f"B{row}:H{row}")
        cell(ws, row, 2, targets_text or "—", bg=ALT, size=9)
        row += 1

        ws.row_dimensions[row].height = 8
        row += 1

    # SIC clusters
    row += 1
    ws.merge_cells(f"A{row}:H{row}")
    sc = ws.cell(row=row, column=1, value="SIC CODE DISTRIBUTION")
    sc.font  = Font(name="Arial", bold=True, size=10, color=WHITE)
    sc.fill  = fill(DKGREY)
    sc.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[row].height = 20
    row += 1

    for cluster in bolt_on_data.get("sic_clusters", []):
        cell(ws, row, 1, cluster["sic_code"],          bg=GREY, bold=True, align="center")
        cell(ws, row, 2, f"{cluster['company_count']} companies", bg=GREY)
        ws.merge_cells(f"C{row}:H{row}")
        cell(ws, row, 3, ", ".join(cluster["example_companies"][:5]), bg=None, size=8)
        row += 1

    for ci, w in enumerate([14, 45, 45, 1, 1, 1, 1, 1], 1):
        ws.column_dimensions[get_column_letter(ci)].width = w if w > 1 else 5


# ── Sheet 6: Summary stats ────────────────────────────────────────────────────

def build_summary(wb, companies):
    ws = wb.create_sheet("Summary Stats")
    title_row(ws, 1, 4, "PIPELINE SUMMARY STATISTICS")

    acq_scores = [c["acquisition_score"] for c in companies]

    stats = [
        ("PIPELINE OVERVIEW", ""),
        ("Total companies found",           len(companies)),
        ("Prime targets  (score ≥ 80)",     sum(1 for s in acq_scores if s >= 80)),
        ("High priority  (65–79)",          sum(1 for s in acq_scores if 65 <= s < 80)),
        ("Medium priority  (50–64)",        sum(1 for s in acq_scores if 50 <= s < 65)),
        ("Intelligence only  (< 50)",       sum(1 for s in acq_scores if s < 50)),
        ("", ""),
        ("OWNERSHIP & SUCCESSION", ""),
        ("PE-backed (flagged for exclusion)",  sum(1 for c in companies if c.get("pe_backed"))),
        ("Family / owner-managed",             sum(1 for c in companies if c.get("is_family"))),
        ("Solo director (key-person risk)",    sum(1 for c in companies if c.get("director_count") == 1)),
        ("Max director age 65+",               sum(1 for c in companies if (c.get("succession") or {}).get("max_age",0) >= 65)),
        ("Max director age 55+",               sum(1 for c in companies if (c.get("succession") or {}).get("max_age",0) >= 55)),
        ("Succession score ≥ 80",              sum(1 for c in companies if (c.get("succession") or {}).get("total",0) >= 80)),
        ("", ""),
        ("COMPANY AGE", ""),
        ("Incorporated pre-2000  (25+ yrs)",   sum(1 for c in companies if c.get("company_age_years",0) >= 25)),
        ("Incorporated 2000–2009",             sum(1 for c in companies if 15 <= c.get("company_age_years",0) < 25)),
        ("Incorporated 2010–2019",             sum(1 for c in companies if 5 <= c.get("company_age_years",0) < 15)),
        ("Incorporated 2020+",                 sum(1 for c in companies if c.get("company_age_years",0) < 5)),
        ("", ""),
        ("DEALABILITY", ""),
        ("With dealability signals",           sum(1 for c in companies if c.get("dealability",{}).get("signal_count",0) > 0)),
        ("With outstanding charges (debt)",    sum(1 for c in companies if c.get("charges",{}).get("outstanding_charges",0) > 0)),
        ("Clean charge register",              sum(1 for c in companies if c.get("charges",{}).get("outstanding_charges",0) == 0)),
    ]

    for ri, (label, val) in enumerate(stats, 2):
        ws.row_dimensions[ri].height = 18
        if val == "":   # section header
            cell(ws, ri, 1, label, bg=NAVY, fg=WHITE, bold=True, size=10, align="left")
            ws.merge_cells(f"A{ri}:B{ri}")
        else:
            bg = ALT if ri % 2 == 0 else None
            cell(ws, ri, 1, label, bg=bg, bold=False, size=10)
            cell(ws, ri, 2, val,   bg=bg, bold=True, fg=NAVY, align="center", size=10)

    ws.column_dimensions["A"].width = 38
    ws.column_dimensions["B"].width = 12


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(enriched_path) as f:
        companies = json.load(f)

    bolt_on_path = os.path.join(cfg.OUTPUT_DIR, "bolt_on_analysis.json")
    bolt_on_data = {}
    if os.path.exists(bolt_on_path):
        with open(bolt_on_path) as f:
            bolt_on_data = json.load(f)

    wb = Workbook()
    build_pipeline(wb, companies)
    build_top30(wb, companies)
    build_contacts(wb, companies)
    build_financials(wb, companies)
    if bolt_on_data:
        build_bolt_on(wb, bolt_on_data)
    build_summary(wb, companies)

    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.EXCEL_OUTPUT)
    wb.save(out_path)
    print(f"Saved → {out_path}")
    return out_path


if __name__ == "__main__":
    run()
