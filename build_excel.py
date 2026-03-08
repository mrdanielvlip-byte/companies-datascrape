"""
build_excel.py — Generate PE pipeline Excel workbook from enriched JSON
Produces PE_Pipeline.xlsx in output/
"""

import json
import os
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

import config as cfg


# ── Style constants ───────────────────────────────────────────────────────────

NAVY   = "1F3864"
BLUE   = "2E75B6"
WHITE  = "FFFFFF"
ALT    = "EBF3FB"
GREEN  = "E2EFDA"
RED    = "FCE4D6"
AMBER  = "FFEB9C"
DKGREY = "404040"

THIN = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)


def fill(hex_color: str) -> PatternFill:
    return PatternFill("solid", fgColor=hex_color)


def score_fill(score: int) -> PatternFill:
    if score >= 85: return fill("375623")
    if score >= 80: return fill("70AD47")
    if score >= 75: return fill("A9D18E")
    if score >= 70: return fill(AMBER)
    if score >= 60: return fill("FFD966")
    return fill("FF7070")


def score_font_color(score: int) -> str:
    return WHITE if score >= 80 else "000000"


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


# ── Sheet 1: Full pipeline ────────────────────────────────────────────────────

PIPELINE_COLS = [
    ("Rank",         5),  ("Reg. No.",    12), ("Company Name",  48),
    ("Incorporated", 11), ("Age (yrs)",   10), ("Directors",     10),
    ("Max Age",       9), ("Avg Age",      9), ("Succ. Score",   11),
    ("Acq. Score",   11), ("Grade",        9), ("PE Backed",     10),
    ("Family",        9), ("Scale",        8), ("Fdr Ret.",       8),
    ("Succession",   10), ("Independence", 13),("Frag.",          8),
    ("Ops",           7),
]

def build_pipeline(wb, companies):
    ws = wb.active
    ws.title = "PE Pipeline"
    n = len(PIPELINE_COLS)

    title_row(ws, 1, n, f"{cfg.SECTOR_LABEL} — PE Pipeline  ({len(companies)} companies)  |  March 2026")
    title_row(ws, 2, n,
              f"SIC codes: {', '.join(cfg.SIC_CODES)}  |  Source: Companies House API",
              bg=BLUE, size=9)

    ws.row_dimensions[3].height = 36
    for ci, (label, width) in enumerate(PIPELINE_COLS, 1):
        cell(ws, 3, ci, label, bg=NAVY, fg=WHITE, bold=True, align="center", wrap=True)
        ws.column_dimensions[get_column_letter(ci)].width = width

    for i, c in enumerate(companies, 1):
        row = i + 3
        alt_bg = ALT if i % 2 == 0 else None
        acq    = c["acquisition_score"]
        comp   = c.get("acq_components", {})
        ss     = c.get("succession", {})

        cell(ws, row,  1, i,                            bg=alt_bg, align="center", bold=True)
        cell(ws, row,  2, c["company_number"],          bg=alt_bg)
        cell(ws, row,  3, c["company_name"],            bg=alt_bg)
        cell(ws, row,  4, (c.get("date_of_creation") or "")[:4], bg=alt_bg, align="center")
        cell(ws, row,  5, c.get("company_age_years", 0), bg=alt_bg, align="center")
        cell(ws, row,  6, c.get("director_count", 0),  bg=alt_bg, align="center")
        cell(ws, row,  7, ss.get("max_age") or "-",    bg=alt_bg, align="center")
        cell(ws, row,  8, ss.get("avg_age") or "-",    bg=alt_bg, align="center")
        cell(ws, row,  9, ss.get("total", 0),          bg=alt_bg, align="center")

        # Acquisition score — colour coded
        for col in (10, 11):
            val = acq if col == 10 else c.get("acquisition_grade", "")
            cx  = ws.cell(row=row, column=col, value=val)
            cx.fill      = score_fill(acq)
            cx.font      = Font(name="Arial", size=9, bold=True, color=score_font_color(acq))
            cx.alignment = Alignment(horizontal="center", vertical="center")
            cx.border    = THIN

        cell(ws, row, 12, "YES ⚠" if c.get("pe_backed") else "-",
             bg=RED if c.get("pe_backed") else alt_bg, align="center")
        cell(ws, row, 13, "YES" if c.get("is_family") else "-",
             bg=GREEN if c.get("is_family") else alt_bg, align="center")
        cell(ws, row, 14, comp.get("scale", 0),             bg=alt_bg, align="center")
        cell(ws, row, 15, comp.get("founder_retirement", 0), bg=alt_bg, align="center")
        cell(ws, row, 16, comp.get("succession", 0),         bg=alt_bg, align="center")
        cell(ws, row, 17, comp.get("independence", 0),       bg=alt_bg, align="center")
        cell(ws, row, 18, comp.get("fragmentation", 0),      bg=alt_bg, align="center")
        cell(ws, row, 19, comp.get("ops", 0),                bg=alt_bg, align="center")

    ws.freeze_panes = "A4"
    ws.auto_filter.ref = f"A3:{get_column_letter(n)}{len(companies)+3}"


# ── Sheet 2: Top 30 profiles ──────────────────────────────────────────────────

def build_top30(wb, companies):
    ws  = wb.create_sheet("Top 30 Profiles")
    top = companies[:30]
    n   = 12

    title_row(ws, 1, n, "TOP 30 PE ACQUISITION TARGETS — DETAILED PROFILES")

    row = 2
    for rank, c in enumerate(top, 1):
        acq = c["acquisition_score"]
        ws.row_dimensions[row].height = 20
        ws.merge_cells(f"A{row}:{get_column_letter(n)}{row}")
        hdr = ws.cell(row=row, column=1,
                      value=f"#{rank}  {c['company_name']}  |  Reg: {c['company_number']}"
                            f"  |  Score: {acq}  |  Grade: {c.get('acquisition_grade','')}")
        hdr.fill      = score_fill(acq)
        hdr.font      = Font(name="Arial", bold=True, size=10, color=score_font_color(acq))
        hdr.alignment = Alignment(horizontal="left", vertical="center")
        row += 1

        meta_labels = ["Incorp.", "Age", "Directors", "Max Dir. Age",
                       "Succ. Score", "PE Backed", "Family/OM", "SIC Codes"]
        ss = c.get("succession", {})
        meta_vals = [
            (c.get("date_of_creation") or "")[:4],
            f"{c.get('company_age_years',0)} yrs",
            c.get("director_count", 0),
            ss.get("max_age") or "N/A",
            ss.get("total", 0),
            "YES ⚠" if c.get("pe_backed") else "No",
            "YES" if c.get("is_family") else "No",
            ", ".join(c.get("sic_codes", [])),
        ]
        for ci, (lbl, val) in enumerate(zip(meta_labels, meta_vals), 1):
            cell(ws, row,   ci, lbl, bg=BLUE, fg=WHITE, bold=True, align="center", size=8)
            cell(ws, row+1, ci, val, bg=ALT,  align="center", size=9)
        row += 2

        dirs = c.get("directors", [])
        if dirs:
            ws.merge_cells(f"A{row}:{get_column_letter(n)}{row}")
            dh = ws.cell(row=row, column=1, value="Directors / Officers")
            dh.fill      = fill(DKGREY)
            dh.font      = Font(name="Arial", bold=True, size=8, color=WHITE)
            dh.alignment = Alignment(horizontal="left", vertical="center")
            row += 1
            for d in dirs[:6]:
                age_str = f"Age ~{d['age']}" if d.get("age") else "Age unknown"
                txt = (f"  {d['name'].title()}  |  {age_str}  "
                       f"|  Appointed: {(d.get('appointed') or '')[:4]}  "
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
    ws.column_dimensions["A"].width = 60
    ws.column_dimensions["H"].width = 25


# ── Sheet 3: Summary stats ────────────────────────────────────────────────────

def build_summary(wb, companies):
    ws = wb.create_sheet("Summary Stats")
    title_row(ws, 1, 4, "PIPELINE SUMMARY STATISTICS")

    stats = [
        ("Total companies found",        len(companies)),
        ("A+ grade  (score ≥ 85)",        sum(1 for c in companies if c["acquisition_score"] >= 85)),
        ("A grade   (80–84)",             sum(1 for c in companies if 80 <= c["acquisition_score"] < 85)),
        ("B+ grade  (75–79)",             sum(1 for c in companies if 75 <= c["acquisition_score"] < 80)),
        ("B grade   (70–74)",             sum(1 for c in companies if 70 <= c["acquisition_score"] < 75)),
        ("C grade   (60–69)",             sum(1 for c in companies if 60 <= c["acquisition_score"] < 70)),
        ("D grade   (< 60)",              sum(1 for c in companies if c["acquisition_score"] < 60)),
        ("PE-backed (flagged)",           sum(1 for c in companies if c.get("pe_backed"))),
        ("Family / owner-managed",        sum(1 for c in companies if c.get("is_family"))),
        ("Solo directors",                sum(1 for c in companies if c.get("director_count") == 1)),
        ("Incorporated pre-2000",         sum(1 for c in companies if c.get("company_age_years", 0) >= 25)),
        ("Max director age 65+",          sum(1 for c in companies if (c.get("succession") or {}).get("max_age", 0) >= 65)),
        ("Succession score ≥ 80",         sum(1 for c in companies if (c.get("succession") or {}).get("total", 0) >= 80)),
    ]

    for ri, (label, val) in enumerate(stats, 2):
        ws.row_dimensions[ri].height = 18
        bg = ALT if ri % 2 == 0 else None
        cell(ws, ri, 1, label, bg=bg, bold=True, size=10)
        cell(ws, ri, 2, val,   bg=bg, bold=True, fg=NAVY, align="center", size=10)

    ws.column_dimensions["A"].width = 35
    ws.column_dimensions["B"].width = 15


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(enriched_path) as f:
        companies = json.load(f)

    wb = Workbook()
    build_pipeline(wb, companies)
    build_top30(wb, companies)
    build_summary(wb, companies)

    out_path = os.path.join(cfg.OUTPUT_DIR, cfg.EXCEL_OUTPUT)
    wb.save(out_path)
    print(f"Saved → {out_path}")
    return out_path


if __name__ == "__main__":
    run()
