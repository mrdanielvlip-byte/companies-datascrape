"""
notify.py — Email notification module for the PE deal-sourcing pipeline.

Uses Resend (resend.com) — free tier, no personal email credentials needed.
Sends the Excel file as an attachment (if ≤ 10 MB) or with a link.

Setup (one-time, 2 minutes):
  1. Go to resend.com → sign up free
  2. Dashboard → API Keys → Create API Key
  3. Add as GitHub secret:  RESEND_API_KEY = re_xxxxxxxxxxxx
  4. For local runs: add to .mail_config:
       RESEND_API_KEY=re_xxxxxxxxxxxx

That's it — no SMTP, no personal credentials.

Usage from other scripts:
  from notify import send_completion_email
  send_completion_email(
      excel_path="output/UK_Lift_Maintenance_Companies_March2026.xlsx",
      sector="Lift Maintenance",
      summary={"total": 542, "tier1": 12, "tier2": 34},
  )

Standalone:
  python notify.py --file output/UK_Lift_Maintenance_Companies_March2026.xlsx --sector "Lift Maintenance"
  python notify.py --test
"""

import os
import json
import base64
import argparse
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_RECIPIENT  = "daniellipinski@mac.com"
RESEND_FROM        = "PE Deal Sourcing <onboarding@resend.dev>"   # Resend's shared domain (free)
RESEND_API_URL     = "https://api.resend.com/emails"
MAX_ATTACH_BYTES   = 10 * 1024 * 1024   # 10 MB


# ── Config loader ──────────────────────────────────────────────────────────────

def _load_config() -> dict:
    """Load config from .mail_config file then env vars (env vars win)."""
    cfg = {}
    config_path = Path(__file__).parent / ".mail_config"
    if config_path.exists():
        for line in config_path.read_text().splitlines():
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                cfg[k.strip()] = v.strip()
    for key in ("RESEND_API_KEY", "NOTIFY_EMAIL"):
        val = os.environ.get(key)
        if val:
            cfg[key] = val
    return cfg


# ── Main send function ─────────────────────────────────────────────────────────

def send_completion_email(
    excel_path=None,
    sector: str = "Unknown Sector",
    summary: dict | None = None,
    subject: str | None = None,
    extra_message: str = "",
) -> bool:
    """
    Send a completion email via Resend.

    Args:
        excel_path:    Path to the Excel file to attach.
        sector:        Sector name for subject line and body.
        summary:       Dict with keys: total, tier1, tier2, family, directors.
        subject:       Override the auto-generated subject line.
        extra_message: Extra text appended to the body.

    Returns:
        True on success, False on failure (never raises).
    """
    cfg       = _load_config()
    api_key   = cfg.get("RESEND_API_KEY", "")
    recipient = cfg.get("NOTIFY_EMAIL", DEFAULT_RECIPIENT)

    if not api_key:
        print("[notify] ⚠️  RESEND_API_KEY not set — email skipped.")
        print("[notify]    Sign up free at resend.com, get an API key, add it as:")
        print("[notify]    • GitHub secret:  RESEND_API_KEY")
        print("[notify]    • Local file:     .mail_config → RESEND_API_KEY=re_xxxx")
        return False

    # ── Build content ──────────────────────────────────────────────────────────
    now        = datetime.now().strftime("%d %b %Y %H:%M")
    excel_path = Path(excel_path) if excel_path else None
    file_size  = excel_path.stat().st_size if excel_path and excel_path.exists() else 0
    file_name  = excel_path.name if excel_path else None

    if subject is None:
        subject = f"✅ {sector} Deal Sourcing — Report Ready ({now})"

    if summary:
        summary_block = (
            f"Pipeline Summary\n"
            f"  Companies processed : {summary.get('total', '?')}\n"
            f"  Tier 1 targets      : {summary.get('tier1', '?')}\n"
            f"  Tier 2 targets      : {summary.get('tier2', '?')}\n"
            f"  Family businesses   : {summary.get('family', '?')}\n"
            f"  Directors identified: {summary.get('directors', '?')}\n"
        )
    else:
        summary_block = ""

    if file_size and file_size <= MAX_ATTACH_BYTES:
        attach_note = f"📎 Excel attached: {file_name} ({file_size / 1024:.0f} KB)"
    elif excel_path:
        attach_note = f"📁 File: {file_name} ({file_size / 1024 / 1024:.1f} MB — see GitHub Actions artifact)"
    else:
        attach_note = "⚠️  No Excel file found."

    body_text = (
        f"Your {sector} intelligence report has finished processing.\n\n"
        f"{summary_block}\n"
        f"{attach_note}\n\n"
        f"Sheets included:\n"
        f"  - Summary Dashboard\n"
        f"  - All Companies (with acquisition scores)\n"
        f"  - Prime Targets (Tier 1 & 2)\n"
        f"  - Competitor Maps\n"
        f"  - Directors Register\n"
        f"  - Family Businesses\n\n"
        f"Generated: {now}\n"
        f"{('─' * 40 + chr(10) + extra_message) if extra_message else ''}"
        f"— PE Deal Sourcing Pipeline\n"
    )

    # ── Build Resend payload ───────────────────────────────────────────────────
    payload = {
        "from":    RESEND_FROM,
        "to":      [recipient],
        "subject": subject,
        "text":    body_text,
    }

    # Attach Excel if within size limit
    if excel_path and excel_path.exists() and file_size <= MAX_ATTACH_BYTES:
        with open(excel_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")
        payload["attachments"] = [{
            "filename": file_name,
            "content":  encoded,
        }]

    # ── POST to Resend API ─────────────────────────────────────────────────────
    try:
        data    = json.dumps(payload).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        }
        req  = urllib.request.Request(RESEND_API_URL, data=data, headers=headers, method="POST")
        resp = urllib.request.urlopen(req, timeout=30)
        body = json.loads(resp.read().decode())
        print(f"[notify] ✅ Email sent to {recipient}  (id: {body.get('id', '?')})")
        return True
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        print(f"[notify] ❌ Resend API error {e.code}: {err_body}")
        if e.code == 401:
            print("[notify]    Check your RESEND_API_KEY is correct.")
        return False
    except Exception as e:
        print(f"[notify] ❌ Unexpected error: {e}")
        return False


# ── Standalone CLI ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Send a deal-sourcing completion email via Resend.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Setup:
  1. Sign up free at resend.com
  2. Dashboard → API Keys → Create API Key
  3. Add to .mail_config in the repo root:
       RESEND_API_KEY=re_xxxxxxxxxxxx

Examples:
  python notify.py --file output/UK_Lift_Maintenance_Companies_March2026.xlsx --sector "Lift Maintenance"
  python notify.py --test
  python notify.py --file output/report.xlsx --to someone@example.com
        """,
    )
    parser.add_argument("--file",   metavar="PATH",  help="Path to the Excel file to send")
    parser.add_argument("--sector", metavar="NAME",  default="Deal Sourcing", help="Sector name for subject line")
    parser.add_argument("--to",     metavar="EMAIL", help=f"Override recipient (default: {DEFAULT_RECIPIENT})")
    parser.add_argument("--test",   action="store_true", help="Send a test email without attachment")
    args = parser.parse_args()

    if args.to:
        os.environ["NOTIFY_EMAIL"] = args.to

    ok = send_completion_email(
        excel_path=None if args.test else args.file,
        sector=args.sector or "Test",
        extra_message="This is a test email from the PE deal-sourcing pipeline." if args.test else "",
    )
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
