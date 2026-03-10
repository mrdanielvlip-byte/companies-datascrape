"""
PE Deal Sourcing Intelligence Platform — Streamlit Web App

Features:
- Multiple concurrent searches, each in its own tab
- Runs persist on GitHub Actions — closing the browser never stops a search
- Run history reloaded from GitHub API on every page open (nothing lost)
- Per-search email notification address
- Auto-refresh while runs are active
"""

import time
import json
import streamlit as st
import requests
from datetime import datetime, timezone

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PE Deal Sourcing",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Config ─────────────────────────────────────────────────────────────────────
GITHUB_TOKEN   = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO    = st.secrets.get("GITHUB_REPO", "mrdanielvlip-byte/companies-datascrape")
API_BASE       = "https://api.github.com"
HEADERS        = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
WORKFLOW_QUICK = "pe_sourcing.yml"
WORKFLOW_DEEP  = "lift_maintenance_ocr.yml"
DEFAULT_EMAIL  = "daniellipinski@mac.com"

# ── Session state ──────────────────────────────────────────────────────────────
if "pinned_runs" not in st.session_state:
    # List of run_ids the user has explicitly pinned as tabs
    st.session_state.pinned_runs = []
if "active_tab" not in st.session_state:
    st.session_state.active_tab = 0
if "cached_runs" not in st.session_state:
    st.session_state.cached_runs = {}   # run_id → run dict, avoids re-fetching
if "last_fetch" not in st.session_state:
    st.session_state.last_fetch = 0


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
    name = run.get("display_title") or run.get("name") or f"Run {run['id']}"
    return name[:50]


# ── Load all recent runs (auto-pin any active ones) ───────────────────────────

@st.cache_data(ttl=30)
def load_all_runs():
    return get_recent_runs(20)


all_runs = load_all_runs()

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
<h1 style='margin-bottom:0'>🏢 PE Deal Sourcing</h1>
<p style='color:gray;margin-top:4px'>UK SME Acquisition Intelligence Platform &nbsp;·&nbsp; Searches run on GitHub — closing this browser never stops them</p>
""", unsafe_allow_html=True)

if not GITHUB_TOKEN:
    st.error("⚠️ GITHUB_TOKEN not configured in Streamlit secrets.")
    st.stop()

st.divider()


# ── Build tab list: New Search + one tab per pinned run ────────────────────────

pinned = st.session_state.pinned_runs  # ordered list of run_ids

tab_labels = ["➕ New Search"]
for rid in pinned:
    run = run_lookup.get(rid, {})
    icon = status_icon(run.get("status", ""), run.get("conclusion"))
    name = run_display_name(run) if run else f"Run {rid}"
    tab_labels.append(f"{icon} {name[:30]}")

tabs = st.tabs(tab_labels)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 0 — New Search
# ══════════════════════════════════════════════════════════════════════════════

with tabs[0]:
    st.subheader("New Sector Search")
    st.caption("Each search runs independently on GitHub — you can start multiple and close this window at any time.")

    with st.form("new_search", clear_on_submit=True):
        col1, col2 = st.columns([2, 1])

        with col1:
            sector = st.text_input(
                "Sector description *",
                placeholder='e.g. "fire safety systems", "HVAC contractors", "pest control"',
                help="Free text — the pipeline auto-discovers SIC codes from your description.",
            )
        with col2:
            notify_email = st.text_input(
                "Send results to",
                value=DEFAULT_EMAIL,
                help="Email address to notify when this specific search finishes.",
            )

        col3, col4 = st.columns([1, 2])
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
        deep_col, run_col = st.columns([1, 2])
        with deep_col:
            run_deep = st.checkbox("Deep OCR run (Lift Maintenance only, ~5.5 hrs)")
        with run_col:
            submitted = st.form_submit_button("🚀 Run Pipeline", type="primary", use_container_width=True)

    if submitted:
        if not sector.strip() and not run_deep:
            st.error("Please enter a sector description.")
        else:
            with st.spinner("Triggering pipeline on GitHub…"):
                if run_deep:
                    ok = trigger_workflow(WORKFLOW_DEEP, {})
                    label = "Lift Maintenance Deep OCR"
                else:
                    ok = trigger_workflow(WORKFLOW_QUICK, {
                        "sector":       sector.strip(),
                        "mode":         mode,
                        "region":       region,
                        "min_revenue":  min_revenue,
                        "notify_email": notify_email,
                    })
                    label = sector.strip()

            if ok:
                st.success(f"✅ Pipeline triggered for **{label}**. A new tab will appear shortly — refresh the page.")
                st.info("📧 You'll get an email at **" + notify_email + "** when it's done. You can safely close this window.")
                time.sleep(4)
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("Failed to trigger. Check your GitHub token in secrets.")

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
            st.subheader(f"{icon} {name}")
            st.caption(f"{label} · {run['created_at'][:10]} · {dur} · {conc or status}")
        with hcol2:
            st.link_button("View on GitHub", run_url, use_container_width=True)
        with hcol3:
            if st.button("✕ Close tab", key=f"close_{run_id}", use_container_width=True):
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
                if st.button(f"⬇ Download Excel", key=f"dl_{run_id}", type="primary"):
                    with st.spinner("Downloading from GitHub…"):
                        data = download_artifact(art["id"])
                    if data:
                        st.download_button(
                            label="💾 Save to your computer",
                            data=data,
                            file_name=f"PE_Sourcing_{run['created_at'][:10]}.zip",
                            mime="application/zip",
                            key=f"save_{run_id}",
                        )
            else:
                st.info("No Excel artifact found — it may have expired (30-day retention).")

        elif status == "completed" and conc in ("failure", "timed_out"):
            st.error(f"❌ Run failed ({conc}). [Check logs on GitHub →]({run_url})")
            st.info("GitHub Actions logs will show what went wrong. You can re-trigger from New Search.")

        elif status == "completed" and conc == "cancelled":
            st.warning("🚫 Run was cancelled.")

        elif status in ("queued", "in_progress"):
            st.info(f"⏳ Run is **{status}** — this window auto-refreshes every 30s.")
            st.progress(0.0, text="Running on GitHub's servers in the background…")
            st.markdown(f"[Watch live logs on GitHub →]({run_url})")

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
