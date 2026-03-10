"""
V Squared Sector Search Analysis — Streamlit Web App

Features:
- Multiple concurrent searches, each in its own tab
- Runs persist on GitHub Actions — closing the browser never stops a search
- Run history reloaded from GitHub API on every page open (nothing lost)
- Per-search email notification address
- Auto-refresh while runs are active
"""

import time
import json
import zipfile
import io
import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timezone

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="V Squared Sector Search Analysis",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Config ─────────────────────────────────────────────────────────────────────
GITHUB_TOKEN   = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO    = st.secrets.get("GITHUB_REPO", "mrdanielvlip-byte/companies-datascrape")
API_BASE       = "https://api.github.com"
HEADERS        = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
WORKFLOW_QUICK    = "pe_sourcing.yml"
WORKFLOW_DEEP     = "lift_maintenance_ocr.yml"
WORKFLOW_ESTIMATE = "estimate.yml"
DEFAULT_EMAIL     = "daniellipinski@mac.com"

# ── Session state ──────────────────────────────────────────────────────────────
if "pinned_runs" not in st.session_state:
    st.session_state.pinned_runs = []
if "active_tab" not in st.session_state:
    st.session_state.active_tab = 0
if "cached_runs" not in st.session_state:
    st.session_state.cached_runs = {}       # run_id → run dict
if "last_fetch" not in st.session_state:
    st.session_state.last_fetch = 0
if "run_inputs_store" not in st.session_state:
    st.session_state.run_inputs_store = {}  # run_id → inputs dict (sector, mode, region…)
if "pending_trigger" not in st.session_state:
    st.session_state.pending_trigger = None # inputs saved just before workflow dispatch
# Estimate state
if "estimate_triggered" not in st.session_state:
    st.session_state.estimate_triggered = False # True the moment Preview is clicked
if "estimate_run_id" not in st.session_state:
    st.session_state.estimate_run_id = None     # run_id once GitHub creates it
if "estimate_sector" not in st.session_state:
    st.session_state.estimate_sector = ""
if "estimate_result" not in st.session_state:
    st.session_state.estimate_result = None     # parsed estimate.json once complete
if "estimate_confirmed" not in st.session_state:
    st.session_state.estimate_confirmed = False # user pressed "Yes, run full search"


# ── GitHub API helpers ─────────────────────────────────────────────────────────

def gh(path, method="GET", body=None):
    url = f"{API_BASE}{path}"
    try:
        if method == "POST":
            r = requests.post(url, headers=HEADERS, json=body, timeout=15)
        elif method == "PATCH":
            r = requests.patch(url, headers=HEADERS, json=body, timeout=15)
        else:
            r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 204:
            return {}
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": str(e)}


def trigger_workflow(workflow_file, inputs):
    result = gh(f"/repos/{GITHUB_REPO}/actions/workflows/{workflow_file}/dispatches",
                method="POST", body={"ref": "main", "inputs": inputs})
    return "_error" not in result


def get_recent_runs(limit=20):
    """Fetch recent runs from both workflows, merged and sorted by date."""
    runs = []
    for wf in (WORKFLOW_QUICK, WORKFLOW_DEEP):
        data = gh(f"/repos/{GITHUB_REPO}/actions/workflows/{wf}/runs?per_page={limit}")
        for r in data.get("workflow_runs", []):
            r["_workflow_label"] = "Sector Search" if wf == WORKFLOW_QUICK else "Deep OCR"
            runs.append(r)
    runs.sort(key=lambda r: r["created_at"], reverse=True)
    return runs[:limit]


def get_run(run_id):
    cached = st.session_state.cached_runs.get(run_id)
    if cached and cached.get("status") == "completed":
        return cached   # completed runs don't change
    data = gh(f"/repos/{GITHUB_REPO}/actions/runs/{run_id}")
    if "_error" not in data:
        st.session_state.cached_runs[run_id] = data
    return data


def get_artifacts(run_id):
    data = gh(f"/repos/{GITHUB_REPO}/actions/runs/{run_id}/artifacts")
    return [a for a in data.get("artifacts", []) if not a.get("expired")]


def download_artifact(artifact_id):
    url = f"{API_BASE}/repos/{GITHUB_REPO}/actions/artifacts/{artifact_id}/zip"
    try:
        r = requests.get(url, headers=HEADERS, allow_redirects=True, timeout=60)
        r.raise_for_status()
        return r.content
    except Exception:
        return None


def cancel_run(run_id):
    gh(f"/repos/{GITHUB_REPO}/actions/runs/{run_id}/cancel", method="POST")


def fetch_estimate_result(run_id) -> dict | None:
    """Download and parse estimate.json from a completed estimate workflow run."""
    import zipfile, io
    artifacts = get_artifacts(run_id)
    art = next((a for a in artifacts if a["name"] == "sector-estimate"), None)
    if not art:
        return None
    raw = download_artifact(art["id"])
    if not raw:
        return None
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            with z.open("estimate.json") as f:
                return json.loads(f.read())
    except Exception:
        return None


# ── Formatting helpers ─────────────────────────────────────────────────────────

def status_icon(status, conclusion):
    if status in ("queued", "in_progress"):
        return "⏳"
    if conclusion == "success":
        return "✅"
    if conclusion == "cancelled":
        return "🚫"
    if conclusion in ("failure", "timed_out"):
        return "❌"
    return "❓"


def fmt_duration(started, completed=None):
    try:
        start = datetime.strptime(started, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        end   = (datetime.strptime(completed, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                 if completed else datetime.now(timezone.utc))
        s = int((end - start).total_seconds())
        if s < 60:   return f"{s}s"
        if s < 3600: return f"{s//60}m {s%60}s"
        return f"{s//3600}h {(s%3600)//60}m"
    except Exception:
        return "—"


def run_display_name(run):
    name = run.get("display_title") or run.get("name") or f"Run {run.get('id', '?')}"
    return name[:50]


# ── Excel preview helper ──────────────────────────────────────────────────────

GRADE_COLOURS = {
    "Prime":             "#375623",   # dark green
    "High":              "#1F5C99",   # dark blue
    "Medium":            "#7B5B00",   # amber
    "Intelligence Only": "#555555",   # grey
}
GRADE_BG = {
    "Prime":             "#E2EFDA",
    "High":              "#DDEEFF",
    "Medium":            "#FFF2CC",
    "Intelligence Only": "#EEEEEE",
}

def parse_excel_preview(zip_bytes: bytes) -> dict | None:
    """
    Extract the PE Pipeline sheet from the downloaded zip artifact.
    Returns a dict with grade_counts, top_companies DataFrame, and summary stats.
    """
    try:
        from openpyxl import load_workbook
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            xlsx_names = [n for n in z.namelist() if n.endswith(".xlsx")]
            if not xlsx_names:
                return None
            xlsx_data = z.read(xlsx_names[0])

        wb = load_workbook(io.BytesIO(xlsx_data), read_only=True, data_only=True)

        if "PE Pipeline" not in wb.sheetnames:
            return None

        ws = wb["PE Pipeline"]
        rows = list(ws.iter_rows(values_only=True))

        # Row 3 (index 2) is the header row
        if len(rows) < 4:
            return None

        headers = [str(h).strip() if h else "" for h in rows[2]]

        # Find key columns by header name
        def col(name):
            try:
                return headers.index(name)
            except ValueError:
                return None

        ci_name   = col("Company Name")
        ci_grade  = col("Grade")
        ci_score  = col("Acq. Score")
        ci_rev    = col("Rev. Base £")
        ci_ebitda = col("EBITDA £")
        ci_epct   = col("EBITDA %")
        ci_sell   = col("SI Band")
        ci_family = col("Family")
        ci_pe     = col("PE")
        ci_dirs   = col("Dirs")
        ci_age    = col("Age")

        data_rows = rows[3:]   # skip title, subtitle, header
        records = []
        for r in data_rows:
            if not r or not any(r):
                continue
            name = r[ci_name] if ci_name is not None else ""
            if not name:
                continue
            records.append({
                "Company":     str(name),
                "Grade":       str(r[ci_grade]  or "") if ci_grade  is not None else "",
                "Score":       r[ci_score]  if ci_score  is not None else None,
                "Rev. Base":   str(r[ci_rev]    or "") if ci_rev    is not None else "",
                "EBITDA £":    str(r[ci_ebitda] or "") if ci_ebitda is not None else "",
                "EBITDA %":    str(r[ci_epct]   or "") if ci_epct   is not None else "",
                "Sell Intent": str(r[ci_sell]   or "") if ci_sell   is not None else "",
                "Family":      str(r[ci_family] or "") if ci_family is not None else "",
                "PE":          str(r[ci_pe]     or "") if ci_pe     is not None else "",
                "Dirs":        r[ci_dirs]  if ci_dirs  is not None else None,
                "Age (yr)":    r[ci_age]   if ci_age   is not None else None,
            })

        df = pd.DataFrame(records)
        if df.empty:
            return None

        # Grade distribution
        grade_counts = df["Grade"].value_counts().to_dict()
        grade_order  = ["Prime", "High", "Medium", "Intelligence Only"]
        grade_counts = {g: grade_counts.get(g, 0) for g in grade_order}

        # Top companies — Prime first, then High, sorted by score
        df["_score_num"] = pd.to_numeric(df["Score"], errors="coerce").fillna(0)
        grade_rank = {"Prime": 0, "High": 1, "Medium": 2, "Intelligence Only": 3}
        df["_grade_rank"] = df["Grade"].map(grade_rank).fillna(9)
        top = df.sort_values(["_grade_rank", "_score_num"], ascending=[True, False]).head(25)
        display_cols = ["Company", "Grade", "Score", "Rev. Base", "EBITDA £",
                        "EBITDA %", "Sell Intent", "Family", "Dirs", "Age (yr)"]
        top_df = top[display_cols].reset_index(drop=True)
        top_df.index = top_df.index + 1   # 1-based rank

        return {"grade_counts": grade_counts, "top_df": top_df, "total": len(df)}

    except Exception:
        return None


def show_excel_preview(zip_bytes: bytes, run_id: str):
    """Render grade distribution metrics + top companies table in Streamlit."""
    preview = parse_excel_preview(zip_bytes)
    if not preview:
        return

    st.divider()
    st.markdown("#### 📊 Results Preview")

    gc   = preview["grade_counts"]
    total = preview["total"]

    # Grade metric cards
    mcols = st.columns(4)
    for i, grade in enumerate(["Prime", "High", "Medium", "Intelligence Only"]):
        count = gc.get(grade, 0)
        pct   = f"{count/total*100:.0f}%" if total > 0 else "0%"
        with mcols[i]:
            st.markdown(
                f"<div style='background:{GRADE_BG[grade]};border-radius:8px;padding:12px 16px;"
                f"text-align:center;border-left:4px solid {GRADE_COLOURS[grade]}'>"
                f"<div style='font-size:28px;font-weight:700;color:{GRADE_COLOURS[grade]}'>{count}</div>"
                f"<div style='font-size:12px;font-weight:600;color:{GRADE_COLOURS[grade]}'>{grade}</div>"
                f"<div style='font-size:11px;color:#888'>{pct} of {total}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f"**Top targets** (sorted by grade then acquisition score)  ·  {total} companies total")

    # Colour-code Grade column
    def style_grade(val):
        bg = GRADE_BG.get(val, "#FFFFFF")
        fg = GRADE_COLOURS.get(val, "#000000")
        return f"background-color:{bg};color:{fg};font-weight:600"

    styled = (
        preview["top_df"]
        .style
        .applymap(style_grade, subset=["Grade"])
        .format({"Score": lambda x: str(int(x)) if pd.notna(x) and x != "" else "-",
                 "Dirs":  lambda x: str(int(x)) if pd.notna(x) and x != "" else "-",
                 "Age (yr)": lambda x: f"{x:.0f}" if pd.notna(x) and x != "" else "-"})
    )
    st.dataframe(styled, use_container_width=True, height=600)


# ── Load all recent runs (auto-pin any active ones) ───────────────────────────

@st.cache_data(ttl=30)
def load_all_runs():
    return get_recent_runs(20)


all_runs = load_all_runs()

# Match a pending trigger to the newest run we haven't catalogued yet
if st.session_state.pending_trigger:
    pt = st.session_state.pending_trigger
    triggered_at = pt.get("triggered_at", "")
    for r in all_runs[:5]:   # check the 5 most recent runs
        rid = r["id"]
        if rid not in st.session_state.run_inputs_store:
            # Only associate if run was created after (or within 10s before) trigger
            try:
                run_created = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
                trig_time   = datetime.fromisoformat(triggered_at.replace("Z", "+00:00"))
                if (run_created - trig_time).total_seconds() > -10:
                    st.session_state.run_inputs_store[rid] = pt
                    st.session_state.pending_trigger = None
                    break
            except Exception:
                pass

# Auto-pin any runs that are currently active (queued/in_progress)
for r in all_runs:
    if r["status"] in ("queued", "in_progress") and r["id"] not in st.session_state.pinned_runs:
        st.session_state.pinned_runs.append(r["id"])

# Build the run lookup dict
run_lookup = {r["id"]: r for r in all_runs}
for rid in st.session_state.pinned_runs:
    if rid not in run_lookup:
        fetched = get_run(rid)
        if "_error" not in fetched:
            run_lookup[rid] = fetched


# ── Header ─────────────────────────────────────────────────────────────────────

st.markdown("""
<h1 style='margin-bottom:0'>🔍 V Squared Sector Search Analysis</h1>
<p style='color:gray;margin-top:4px'>UK SME Acquisition Intelligence Platform &nbsp;·&nbsp; Searches run on GitHub — closing this browser never stops them</p>
""", unsafe_allow_html=True)

if not GITHUB_TOKEN:
    st.error("⚠️ GITHUB_TOKEN not configured in Streamlit secrets.")
    st.stop()

st.divider()


# ── Build tab list: New Search + one tab per pinned run ────────────────────────

pinned = st.session_state.pinned_runs  # ordered list of run_ids

def tab_name_for_run(rid):
    """Use stored sector name if available, else display_title (set by run-name: in YAML)."""
    inputs = st.session_state.run_inputs_store.get(rid, {})
    if inputs.get("sector"):
        return inputs["sector"]
    run = run_lookup.get(rid, {})
    return run_display_name(run) if run else f"Run {rid}"

tab_labels = ["➕ New Search"]
for rid in pinned:
    run  = run_lookup.get(rid, {})
    icon = status_icon(run.get("status", ""), run.get("conclusion"))
    name = tab_name_for_run(rid)
    tab_labels.append(f"{icon} {name[:28]}")

tabs = st.tabs(tab_labels)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 0 — New Search
# ══════════════════════════════════════════════════════════════════════════════

def _reset_estimate():
    st.session_state.estimate_triggered = False
    st.session_state.estimate_run_id    = None
    st.session_state.estimate_result    = None
    st.session_state.estimate_sector    = ""
    st.session_state.estimate_confirmed = False

with tabs[0]:
    st.subheader("New Sector Search")
    st.caption("Each search runs independently on GitHub — you can start multiple and close this window at any time.")

    # ── PHASE 1: Search form ───────────────────────────────────────────────────
    # Hide form as soon as Preview is clicked (estimate_triggered), not just
    # when we have a run_id — prevents the race condition where GitHub takes
    # longer than 4s to create the run and the form reappears.
    estimate_active = (
        st.session_state.estimate_triggered
        or st.session_state.estimate_run_id is not None
        or st.session_state.estimate_result is not None
        or st.session_state.estimate_confirmed
    )

    if not estimate_active:
        with st.form("new_search", clear_on_submit=False):
            col1, col2 = st.columns([2, 1])
            with col1:
                sector = st.text_input(
                    "Sector description *",
                    placeholder='e.g. "fire safety systems", "HVAC contractors", "pest control"',
                    help="Free text — the pipeline auto-discovers SIC codes using curated maps and fuzzy matching.",
                )
            with col2:
                notify_email = st.text_input(
                    "Send results to",
                    value=DEFAULT_EMAIL,
                    help="Email address to notify when this specific search finishes.",
                )

            col3, _ = st.columns([1, 2])
            with col3:
                mode = st.radio(
                    "Report depth",
                    ["quick", "full"],
                    captions=["~10 min · search + enrich + financials", "~60 min · adds sell signals, contracts, digital health"],
                    index=0,
                )

            with st.expander("Advanced filters (optional)"):
                fc1, fc2 = st.columns(2)
                with fc1:
                    region = st.selectbox("Region", [
                        "", "London", "South East", "South West", "East of England",
                        "East Midlands", "West Midlands", "Yorkshire and The Humber",
                        "North West", "North East", "Wales", "Scotland", "Northern Ireland",
                    ], index=0, help="Leave blank for all UK.")
                with fc2:
                    min_revenue = st.selectbox("Minimum revenue",
                        ["", "250000", "500000", "1000000", "2000000", "5000000"],
                        format_func=lambda x: "No minimum" if x == "" else f"£{int(x):,}",
                        index=0)

            st.divider()
            deep_col, preview_col, _ = st.columns([1, 1, 1])
            with deep_col:
                run_deep = st.checkbox("Deep OCR run (~5.5 hrs, most thorough)")
            with preview_col:
                preview_clicked = st.form_submit_button(
                    "🔍 Preview Companies", use_container_width=True,
                    help="Run a quick ~2 min estimate to see company count and SIC accuracy before committing to a full search."
                )

        if preview_clicked:
            if not sector.strip():
                st.error("Please enter a sector description.")
            else:
                # ── Mark as triggered immediately so form disappears on rerun ──
                st.session_state.estimate_triggered = True
                st.session_state.estimate_sector    = sector.strip()
                st.session_state._estimate_email    = notify_email
                st.session_state._estimate_mode     = mode
                st.session_state._estimate_region   = region
                st.session_state._estimate_rev      = min_revenue
                st.session_state._estimate_deep     = run_deep
                ok = trigger_workflow(WORKFLOW_ESTIMATE, {"sector": sector.strip()})
                if not ok:
                    _reset_estimate()
                    st.error("Failed to trigger estimate. Check your GitHub token in secrets.")
                else:
                    st.rerun()

    # ── PHASE 2: Estimate in progress (triggered but may not have run_id yet) ──
    elif (st.session_state.estimate_triggered or st.session_state.estimate_run_id) \
            and st.session_state.estimate_result is None \
            and not st.session_state.estimate_confirmed:

        st.info(f"🔍 Estimating company universe for **{st.session_state.estimate_sector}** …")

        # If we don't have a run_id yet, poll GitHub until the run appears
        if st.session_state.estimate_run_id is None:
            wf_runs = gh(f"/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_ESTIMATE}/runs?per_page=1")
            runs = wf_runs.get("workflow_runs", [])
            if runs:
                st.session_state.estimate_run_id = runs[0]["id"]
                st.session_state.estimate_triggered = True  # keep active
            else:
                st.progress(0.02, text="Waiting for GitHub to start the estimate job…")
                st.caption("Auto-checks every 5 s")
                if st.button("✕ Cancel", key="cancel_est_wait"):
                    _reset_estimate()
                    st.rerun()
                time.sleep(5)
                st.rerun()
        else:
            run_id = st.session_state.estimate_run_id
            run    = get_run(run_id)
            status = run.get("status", "unknown")
            conc   = run.get("conclusion")

            if status in ("queued", "in_progress"):
                started_str = run.get("run_started_at") or run.get("created_at", "")
                try:
                    started_dt  = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
                    elapsed_sec = (datetime.now(timezone.utc) - started_dt).total_seconds()
                    pct     = min(elapsed_sec / 120, 0.95)
                    remain  = max(0, int((120 - elapsed_sec) // 60))
                    bar_txt = f"~{int(elapsed_sec//60)}m {int(elapsed_sec%60)}s elapsed · ~{remain} min remaining"
                except Exception:
                    pct, bar_txt = 0.05, "Starting up…"

                st.progress(pct, text=bar_txt)
                st.caption("Auto-refreshes every 10 s")
                if st.button("✕ Cancel estimate", key="cancel_est"):
                    cancel_run(run_id)
                    _reset_estimate()
                    st.rerun()
                time.sleep(10)
                st.rerun()

            elif status == "completed" and conc == "success":
                result = fetch_estimate_result(run_id)
                if result:
                    st.session_state.estimate_result = result
                    st.rerun()
                else:
                    st.error("Estimate finished but could not read results. Try again.")
                    if st.button("Start over", key="est_retry"):
                        _reset_estimate()
                        st.rerun()
            else:
                st.error(f"Estimate ended with: **{conc or status}**. Try again or use a different sector description.")
                if st.button("Start over", key="est_fail"):
                    _reset_estimate()
                    st.rerun()

    # ── PHASE 3: Show estimate results and ask to confirm ─────────────────────
    elif st.session_state.estimate_result is not None \
            and not st.session_state.estimate_confirmed:
        r          = st.session_state.estimate_result
        sector     = r.get("sector", st.session_state.estimate_sector)
        total      = r.get("total_companies", 0)
        acc_pct    = r.get("accuracy_pct", 0)
        acc_lbl    = r.get("accuracy_label", "")
        source     = r.get("match_source", "fuzzy")
        sic_bkd    = r.get("sic_breakdown", [])
        samples    = r.get("sample_companies", [])
        api_errors = r.get("api_errors", 0)

        # Accuracy colour
        if acc_pct >= 80:
            acc_colour = "🟢"
        elif acc_pct >= 60:
            acc_colour = "🟡"
        else:
            acc_colour = "🔴"

        st.success(f"**Estimate complete for: {sector}**")

        if api_errors > 0:
            st.warning(
                f"⚠️ {api_errors} SIC code(s) couldn't be counted due to API rate limits — "
                "the total shown may be **understated**. The full search will not be affected."
            )

        # ── Top metrics ────────────────────────────────────────────────────────
        m1, m2, m3 = st.columns(3)
        m1.metric("Estimated companies", f"{total:,}")
        m2.metric("Sector accuracy", f"{acc_pct}%")
        m3.metric("SIC match method", "Curated map" if source == "curated" else "Fuzzy match")

        st.caption(f"{acc_colour} {acc_lbl}")
        st.divider()

        # ── SIC code breakdown ─────────────────────────────────────────────────
        st.markdown("**Matched SIC codes**")
        for s in sic_bkd:
            pct_of_total = (s["count"] / total * 100) if total else 0
            bar = "█" * int(pct_of_total / 5)   # rough bar, max 20 chars at 100%
            st.markdown(
                f"`{s['code']}` &nbsp; {s['description']} &nbsp; "
                f"— **{s['count']:,}** companies &nbsp; `{bar}` {pct_of_total:.0f}%"
            )

        # ── Sample companies ───────────────────────────────────────────────────
        if samples:
            st.divider()
            st.markdown("**Sample companies found**")
            st.caption("  ·  ".join(samples[:10]))

        st.divider()

        # ── Confirm / restart ─────────────────────────────────────────────────
        st.markdown("**Would you like to run a full search on this sector?**")
        yes_col, no_col = st.columns([1, 1])
        with yes_col:
            if st.button("✅ Yes — Run Full Search", type="primary", use_container_width=True):
                st.session_state.estimate_confirmed = True
                st.rerun()
        with no_col:
            if st.button("✏️ No — Change Sector", use_container_width=True):
                _reset_estimate()
                st.rerun()

    # ── PHASE 4: Confirmed — trigger the real pipeline ─────────────────────────
    elif st.session_state.estimate_confirmed:
        sector       = st.session_state.estimate_sector
        notify_email = st.session_state.get("_estimate_email", DEFAULT_EMAIL)
        mode         = st.session_state.get("_estimate_mode", "quick")
        region       = st.session_state.get("_estimate_region", "")
        min_revenue  = st.session_state.get("_estimate_rev", "")
        run_deep     = st.session_state.get("_estimate_deep", False)

        with st.spinner(f"Launching full pipeline for '{sector}'…"):
            if run_deep:
                ok = trigger_workflow(WORKFLOW_DEEP, {
                    "sector": sector, "notify_email": notify_email,
                })
                label = f"{sector} (Deep OCR)"
                st.session_state.pending_trigger = {
                    "sector": sector, "mode": "Deep OCR", "region": "",
                    "min_revenue": "", "notify_email": notify_email, "is_deep": True,
                    "triggered_at": datetime.now(timezone.utc).isoformat(),
                }
            else:
                ok = trigger_workflow(WORKFLOW_QUICK, {
                    "sector": sector, "mode": mode, "region": region,
                    "min_revenue": min_revenue, "notify_email": notify_email,
                })
                label = sector
                st.session_state.pending_trigger = {
                    "sector": sector, "mode": mode, "region": region,
                    "min_revenue": min_revenue, "notify_email": notify_email, "is_deep": False,
                    "triggered_at": datetime.now(timezone.utc).isoformat(),
                }

        if ok:
            st.success(f"✅ Full pipeline triggered for **{label}**.")
            st.info(f"📧 You'll get an email at **{notify_email}** when it's done.")
            _reset_estimate()
            time.sleep(4)
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Failed to trigger pipeline. Check your GitHub token in secrets.")
            st.session_state.estimate_confirmed = False

    # ── Recent runs summary table ──────────────────────────────────────────────
    st.divider()
    st.subheader("All Runs")
    st.caption("Click **Pin as tab** to open a run in its own tab for detailed status and download.")

    if not all_runs:
        st.info("No runs yet — trigger one above.")
    else:
        for run in all_runs[:15]:
            rid     = run["id"]
            status  = run["status"]
            conc    = run.get("conclusion")
            icon    = status_icon(status, conc)
            name    = run_display_name(run)
            label   = run.get("_workflow_label", "")
            date    = run["created_at"][:10]
            dur     = fmt_duration(run["created_at"], run.get("updated_at") if status == "completed" else None)

            c1, c2, c3 = st.columns([5, 1, 1])
            with c1:
                st.markdown(f"**{icon} {name}** &nbsp; `{label}` &nbsp; {date} · {dur}")
            with c2:
                if rid not in st.session_state.pinned_runs:
                    if st.button("Pin tab", key=f"pin_{rid}", use_container_width=True):
                        st.session_state.pinned_runs.insert(0, rid)
                        st.rerun()
                else:
                    st.caption("✓ Pinned")
            with c3:
                st.markdown(f"[GitHub ↗]({run['html_url']})")


# ══════════════════════════════════════════════════════════════════════════════
# TABS 1+ — Individual run tabs
# ══════════════════════════════════════════════════════════════════════════════

for tab_idx, run_id in enumerate(pinned):
    with tabs[tab_idx + 1]:

        run = run_lookup.get(run_id)
        if not run:
            st.warning(f"Could not load run {run_id}")
            continue

        # Refresh live run data
        if run.get("status") != "completed":
            run = get_run(run_id)
            run_lookup[run_id] = run

        # Guard against API error dicts
        if run.get("_error") or "created_at" not in run:
            st.error(f"Could not load run data (ID {run_id}). GitHub API may be temporarily unavailable.")
            if st.button("Remove tab", key=f"rm_{run_id}"):
                st.session_state.pinned_runs.remove(run_id)
                st.rerun()
            continue

        status  = run.get("status", "unknown")
        conc    = run.get("conclusion")
        icon    = status_icon(status, conc)
        name    = run_display_name(run)
        label   = run.get("_workflow_label", "Run")
        dur     = fmt_duration(run["created_at"], run.get("updated_at") if status == "completed" else None)
        run_url = run.get("html_url", "")

        # ── Run header ─────────────────────────────────────────────────────────
        hcol1, hcol2, hcol3 = st.columns([5, 1, 1])
        with hcol1:
            st.subheader(f"{icon} {tab_name_for_run(run_id)}")
            # Show search criteria as inline tags
            inputs = st.session_state.run_inputs_store.get(run_id, {})
            tags = []
            if inputs.get("mode"):        tags.append(f"📋 {inputs['mode']}")
            if inputs.get("region"):      tags.append(f"📍 {inputs['region']}")
            if inputs.get("min_revenue"): tags.append(f"💰 £{int(inputs['min_revenue']):,}+ revenue")
            if inputs.get("notify_email"):tags.append(f"📧 {inputs['notify_email']}")
            if tags:
                st.caption("  ·  ".join(tags))
            else:
                st.caption(f"{label} · {run['created_at'][:10]} · {dur} · {conc or status}")
        with hcol2:
            st.link_button("View on GitHub", run_url, use_container_width=True)
        with hcol3:
            if st.button("🗑 Delete Tab", key=f"close_{run_id}", use_container_width=True,
                         help="Remove this tab. The run itself keeps going on GitHub."):
                st.session_state.pinned_runs.remove(run_id)
                st.rerun()

        st.divider()

        # ── Status card ────────────────────────────────────────────────────────
        if status == "completed" and conc == "success":
            st.success("✅ Run completed successfully")

            artifacts = get_artifacts(run_id)
            excel_arts = [a for a in artifacts if a["name"] in ("pe-sourcing-results", "lift-maintenance-excel")]

            if excel_arts:
                art = excel_arts[0]
                size_kb = art["size_in_bytes"] // 1024
                st.markdown(f"**📊 Excel report ready** — {art['name']} ({size_kb} KB)")
                # Cache the downloaded artifact bytes in session state so
                # clicking "Download" once lets both the save button and
                # the preview render without a second API call.
                cache_key = f"_artifact_bytes_{run_id}"
                if st.button(f"⬇ Download & Preview Excel", key=f"dl_{run_id}", type="primary"):
                    with st.spinner("Downloading from GitHub…"):
                        data = download_artifact(art["id"])
                    if data:
                        st.session_state[cache_key] = data

                if st.session_state.get(cache_key):
                    data = st.session_state[cache_key]
                    st.download_button(
                        label="💾 Save to your computer",
                        data=data,
                        file_name=f"PE_Sourcing_{run['created_at'][:10]}.zip",
                        mime="application/zip",
                        key=f"save_{run_id}",
                    )
                    show_excel_preview(data, run_id)
            else:
                st.info("No Excel artifact found — it may have expired (30-day retention).")

        elif status == "completed" and conc in ("failure", "timed_out"):
            st.error(f"❌ Run failed ({conc}). [Check logs on GitHub →]({run_url})")
            st.info("GitHub Actions logs will show what went wrong. You can re-trigger from New Search.")

        elif status == "completed" and conc == "cancelled":
            st.warning("🚫 Run was cancelled.")

        elif status in ("queued", "in_progress"):
            # ── Estimated progress bar ─────────────────────────────────────────
            is_deep   = run.get("_workflow_label", "") == "Deep OCR"
            est_mins  = 330 if is_deep else 90          # expected total minutes
            est_secs  = est_mins * 60

            started_str = run.get("run_started_at") or run.get("created_at", "")
            if started_str and status == "in_progress":
                try:
                    started_dt  = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
                    elapsed_sec = (datetime.now(timezone.utc) - started_dt).total_seconds()
                    progress    = min(elapsed_sec / est_secs, 0.97)   # cap at 97% until done
                    elapsed_min = int(elapsed_sec // 60)
                    remain_min  = max(0, int((est_secs - elapsed_sec) // 60))
                    bar_text    = (
                        f"~{elapsed_min} min elapsed · ~{remain_min} min remaining "
                        f"(estimated {est_mins} min total)"
                    )
                except Exception:
                    progress, bar_text = 0.05, "Starting up…"
            elif status == "queued":
                progress, bar_text = 0.02, "Queued — waiting for a GitHub Actions runner…"
            else:
                progress, bar_text = 0.05, "Starting up…"

            st.progress(progress, text=bar_text)
            st.caption(f"Auto-refreshes every 30 s &nbsp;·&nbsp; [Watch live logs on GitHub →]({run_url})")

            # Cancel button
            if st.button("🛑 Cancel this run", key=f"cancel_{run_id}"):
                cancel_run(run_id)
                st.warning("Cancellation requested…")
                time.sleep(3)
                st.cache_data.clear()
                st.rerun()

        # ── Run metadata ───────────────────────────────────────────────────────
        with st.expander("Run details"):
            st.json({
                "run_id":     run_id,
                "status":     status,
                "conclusion": conc,
                "started":    run.get("created_at"),
                "updated":    run.get("updated_at"),
                "duration":   dur,
                "github_url": run_url,
                "workflow":   run.get("_workflow_label", ""),
            })


# ── Auto-refresh if any run is still active ────────────────────────────────────
has_active = any(
    run_lookup.get(rid, {}).get("status") in ("queued", "in_progress")
    for rid in pinned
)
if has_active:
    time.sleep(30)
    st.cache_data.clear()
    st.rerun()
