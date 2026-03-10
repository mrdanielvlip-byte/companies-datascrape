"""
PE Deal Sourcing Intelligence Platform — Streamlit Web App

Lets you search any UK sector, configure filters, trigger the pipeline
on GitHub Actions, monitor progress, and download the Excel report.

Setup (Streamlit Cloud):
  1. Fork / connect your GitHub repo at share.streamlit.io
  2. Add secrets in the Streamlit Cloud dashboard:
       GITHUB_TOKEN  = ghp_xxxx   (your GitHub personal access token)
       GITHUB_REPO   = mrdanielvlip-byte/companies-datascrape
       RESEND_API_KEY = re_xxxx   (optional — already in GitHub secrets)

Local dev:
  pip install streamlit requests
  Create .streamlit/secrets.toml:
    GITHUB_TOKEN   = "ghp_xxxx"
    GITHUB_REPO    = "mrdanielvlip-byte/companies-datascrape"
  streamlit run streamlit_app.py
"""

import time
import base64
import streamlit as st
import requests
from datetime import datetime

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PE Deal Sourcing",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Config from Streamlit secrets ─────────────────────────────────────────────
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO  = st.secrets.get("GITHUB_REPO", "mrdanielvlip-byte/companies-datascrape")
API_BASE     = "https://api.github.com"
HEADERS      = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept":        "application/vnd.github+json",
}

# Workflow file names
WORKFLOW_QUICK  = "pe_sourcing.yml"          # Generic sector search (quick/full, ~15-90 min)
WORKFLOW_DEEP   = "lift_maintenance_ocr.yml" # Deep OCR run for lift maintenance (~5.5 hr)

# ── Session state init ─────────────────────────────────────────────────────────
if "active_runs" not in st.session_state:
    st.session_state.active_runs = []   # list of run dicts {id, sector, started, workflow}
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = 0


# ── GitHub API helpers ─────────────────────────────────────────────────────────

def gh_get(path: str) -> dict | list | None:
    try:
        r = requests.get(f"{API_BASE}{path}", headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"GitHub API error: {e}")
        return None


def trigger_workflow(workflow_file: str, inputs: dict) -> bool:
    url  = f"{API_BASE}/repos/{GITHUB_REPO}/actions/workflows/{workflow_file}/dispatches"
    body = {"ref": "main", "inputs": inputs}
    try:
        r = requests.post(url, headers=HEADERS, json=body, timeout=15)
        return r.status_code == 204
    except Exception as e:
        st.error(f"Failed to trigger workflow: {e}")
        return False


def get_recent_runs(workflow_file: str, limit: int = 10) -> list:
    data = gh_get(f"/repos/{GITHUB_REPO}/actions/workflows/{workflow_file}/runs?per_page={limit}")
    return data.get("workflow_runs", []) if data else []


def get_run(run_id: int) -> dict | None:
    return gh_get(f"/repos/{GITHUB_REPO}/actions/runs/{run_id}")


def get_artifacts(run_id: int) -> list:
    data = gh_get(f"/repos/{GITHUB_REPO}/actions/runs/{run_id}/artifacts")
    return data.get("artifacts", []) if data else []


def download_artifact(artifact_id: int) -> bytes | None:
    """Download artifact zip via GitHub API — returns raw bytes."""
    url = f"{API_BASE}/repos/{GITHUB_REPO}/actions/artifacts/{artifact_id}/zip"
    try:
        r = requests.get(url, headers=HEADERS, allow_redirects=True, timeout=60)
        r.raise_for_status()
        return r.content
    except Exception as e:
        st.error(f"Download failed: {e}")
        return None


def cancel_run(run_id: int) -> bool:
    url = f"{API_BASE}/repos/{GITHUB_REPO}/actions/runs/{run_id}/cancel"
    try:
        r = requests.post(url, headers=HEADERS, timeout=15)
        return r.status_code == 202
    except Exception:
        return False


def status_icon(status: str, conclusion: str | None) -> str:
    if status == "in_progress" or status == "queued":
        return "⏳"
    if conclusion == "success":
        return "✅"
    if conclusion == "cancelled":
        return "🚫"
    if conclusion in ("failure", "timed_out"):
        return "❌"
    return "❓"


def fmt_duration(started: str, completed: str | None) -> str:
    try:
        start = datetime.strptime(started, "%Y-%m-%dT%H:%M:%SZ")
        end   = datetime.strptime(completed, "%Y-%m-%dT%H:%M:%SZ") if completed else datetime.utcnow()
        secs  = int((end - start).total_seconds())
        if secs < 60:
            return f"{secs}s"
        if secs < 3600:
            return f"{secs // 60}m {secs % 60}s"
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    except Exception:
        return "—"


# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🏢 PE Deal Sourcing")
    st.caption("UK SME Acquisition Intelligence")
    st.divider()

    if not GITHUB_TOKEN:
        st.error("⚠️ GitHub token not configured.\nAdd GITHUB_TOKEN to Streamlit secrets.")
    else:
        st.success("✓ Connected to GitHub")
        st.caption(f"`{GITHUB_REPO}`")

    st.divider()
    st.markdown("**How it works**")
    st.markdown(
        "1. Enter a sector below\n"
        "2. Set your filters\n"
        "3. Hit **Run Pipeline**\n"
        "4. Get emailed when done\n"
        "5. Download Excel report"
    )
    st.divider()
    st.markdown("**Deep OCR Run** (Lift Maintenance only)")
    st.caption("Full 542-company OCR batch with actual P&L from filed accounts. Takes up to 5.5 hours.")
    if st.button("🔬 Run Deep OCR", use_container_width=True, disabled=not GITHUB_TOKEN):
        ok = trigger_workflow(WORKFLOW_DEEP, {})
        if ok:
            st.success("Deep OCR run triggered!")
        else:
            st.error("Failed to trigger.")


# ── Main layout ────────────────────────────────────────────────────────────────

st.header("New Sector Search")

with st.form("search_form"):
    col1, col2 = st.columns([2, 1])

    with col1:
        sector = st.text_input(
            "Sector description",
            placeholder='e.g. "fire safety systems", "HVAC contractors", "pest control"',
            help="Free text — the pipeline auto-discovers relevant SIC codes.",
        )
        notify_email = st.text_input(
            "Notify email",
            value="daniellipinski@mac.com",
            help="Email address to send results to when the run completes.",
        )

    with col2:
        mode = st.radio(
            "Report depth",
            options=["quick", "full"],
            index=0,
            help="Quick: search + enrich + financials (~10 min). Full: adds sell signals, contracts, digital health (~60 min).",
        )

    with st.expander("Advanced filters (optional)"):
        fcol1, fcol2 = st.columns(2)
        with fcol1:
            region = st.selectbox(
                "Region",
                options=[
                    "", "London", "South East", "South West", "East of England",
                    "East Midlands", "West Midlands", "Yorkshire and The Humber",
                    "North West", "North East", "Wales", "Scotland", "Northern Ireland",
                ],
                index=0,
                help="Leave blank for all UK.",
            )
        with fcol2:
            min_revenue = st.selectbox(
                "Minimum revenue",
                options=["", "250000", "500000", "1000000", "2000000", "5000000"],
                format_func=lambda x: "No minimum" if x == "" else f"£{int(x):,}",
                index=0,
            )

    submitted = st.form_submit_button(
        "🚀 Run Pipeline",
        use_container_width=True,
        type="primary",
        disabled=not GITHUB_TOKEN,
    )

if submitted:
    if not sector.strip():
        st.error("Please enter a sector description.")
    else:
        with st.spinner(f"Triggering pipeline for **{sector}**…"):
            inputs = {
                "sector":        sector.strip(),
                "mode":          mode,
                "region":        region,
                "min_revenue":   min_revenue,
                "notify_email":  notify_email,
            }
            ok = trigger_workflow(WORKFLOW_QUICK, inputs)
        if ok:
            st.success(f"✅ Pipeline triggered for **{sector}** ({mode} mode). You'll get an email at {notify_email} when it's done.")
            time.sleep(3)   # give GitHub a moment to register the run
            st.rerun()
        else:
            st.error("Failed to trigger pipeline. Check your GitHub token.")


# ── Recent Runs ────────────────────────────────────────────────────────────────

st.divider()
st.header("Recent Runs")

col_refresh, col_spacer = st.columns([1, 5])
with col_refresh:
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()

# Load runs from both workflows
runs_quick = get_recent_runs(WORKFLOW_QUICK, limit=8)
runs_deep  = get_recent_runs(WORKFLOW_DEEP,  limit=4)

# Tag them
for r in runs_quick:
    r["_workflow_label"] = "Sector Search"
for r in runs_deep:
    r["_workflow_label"] = "Deep OCR"

all_runs = sorted(runs_quick + runs_deep, key=lambda r: r["created_at"], reverse=True)[:12]

if not all_runs:
    st.info("No runs yet. Trigger a pipeline above.")
else:
    for run in all_runs:
        run_id     = run["id"]
        status     = run["status"]
        conclusion = run.get("conclusion")
        icon       = status_icon(status, conclusion)
        label      = run.get("_workflow_label", "")
        created    = run["created_at"][:10]
        duration   = fmt_duration(run["created_at"], run.get("updated_at"))
        run_url    = run["html_url"]

        # Try to get sector name from run inputs (available via API)
        display_name = run.get("display_title") or run.get("name") or f"Run {run_id}"

        with st.container(border=True):
            head_col, action_col = st.columns([4, 1])

            with head_col:
                st.markdown(f"**{icon} {display_name}**  `{label}`")
                status_text = conclusion or status
                st.caption(f"{created}  ·  {duration}  ·  {status_text}")

            with action_col:
                # Cancel button for in-progress runs
                if status in ("in_progress", "queued"):
                    if st.button("Cancel", key=f"cancel_{run_id}", use_container_width=True):
                        if cancel_run(run_id):
                            st.success("Cancelling…")
                            time.sleep(2)
                            st.rerun()

                # Download button for successful runs
                if status == "completed" and conclusion == "success":
                    artifacts = get_artifacts(run_id)
                    excel_artifacts = [a for a in artifacts if not a["expired"] and
                                       a["name"] in ("pe-sourcing-results", "lift-maintenance-excel")]
                    if excel_artifacts:
                        art = excel_artifacts[0]
                        if st.button("⬇ Download", key=f"dl_{run_id}", use_container_width=True, type="primary"):
                            with st.spinner("Downloading…"):
                                data = download_artifact(art["id"])
                            if data:
                                st.download_button(
                                    label="💾 Save Excel",
                                    data=data,
                                    file_name=f"PE_Sourcing_{created}.zip",
                                    mime="application/zip",
                                    key=f"save_{run_id}",
                                )

            # Progress bar for running jobs
            if status == "in_progress":
                st.progress(0.0, text="Running… (auto-refresh to update)")

            # GitHub link
            st.markdown(f"[View on GitHub →]({run_url})")


# ── Auto-refresh for active runs ───────────────────────────────────────────────
has_active = any(r["status"] in ("in_progress", "queued") for r in all_runs)
if has_active:
    st.info("⏳ A run is in progress. This page will refresh automatically every 30 seconds.")
    time.sleep(30)
    st.rerun()
