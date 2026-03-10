"""
notify.py — Email notification module for the PE deal-sourcing pipeline.

Sends a completion email with the Excel file attached (if ≤ 10 MB)
or a download link when the file is too large.

Configuration (any of these methods, checked in order):
  1. Environment variables:
       NOTIFY_EMAIL      — recipient address (default: daniellipinski@mac.com)
       MAIL_USERNAME     — SMTP login / sender address
       MAIL_PASSWORD     — SMTP password (iCloud: app-specific password)
       MAIL_SERVER       — SMTP host        (default: smtp.mail.me.com)
       MAIL_PORT         — SMTP port        (default: 587)
  2. A .mail_config file in the repo root (KEY=VALUE per line, same names as above)
  3. Hardcoded fallback for recipient only (daniellipinski@mac.com)

iCloud app-specific password:
  appleid.apple.com → Sign-In & Security → App-Specific Passwords → +
  Use that generated password as MAIL_PASSWORD.

Usage from other scripts:
  from notify import send_completion_email
  send_completion_email(
      excel_path="output/UK_Lift_Maintenance_Companies_March2026.xlsx",
      sector="Lift Maintenance",
      summary={"total": 542, "tier1": 12, "tier2": 34},
  )

Or standalone:
  python notify.py --file output/UK_Lift_Maintenance_Companies_March2026.xlsx --sector "Lift Maintenance"
"""

import os
import smtplib
import argparse
import mimetypes
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path
from datetime import datetime

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_RECIPIENT = "daniellipinski@mac.com"
DEFAULT_SERVER    = "smtp.mail.me.com"
DEFAULT_PORT      = 587
MAX_ATTACH_BYTES  = 10 * 1024 * 1024   # 10 MB — attach directly; above this, link only


# ── Config loader ─────────────────────────────────────────────────────────────

def _load_mail_config() -> dict:
    """
    Load SMTP credentials from env vars, then .mail_config file.
    Returns a dict with keys: username, password, server, port, recipient.
    """
    cfg = {}

    # 1. Try .mail_config file
    config_path = Path(__file__).parent / ".mail_config"
    if config_path.exists():
        for line in config_path.read_text().splitlines():
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                cfg[k.strip()] = v.strip()

    # 2. Env vars override file
    for env_key, cfg_key in [
        ("MAIL_USERNAME", "MAIL_USERNAME"),
        ("MAIL_PASSWORD", "MAIL_PASSWORD"),
        ("MAIL_SERVER",   "MAIL_SERVER"),
        ("MAIL_PORT",     "MAIL_PORT"),
        ("NOTIFY_EMAIL",  "NOTIFY_EMAIL"),
    ]:
        val = os.environ.get(env_key)
        if val:
            cfg[cfg_key] = val

    return cfg


# ── Main send function ─────────────────────────────────────────────────────────

def send_completion_email(
    excel_path: str | Path | None = None,
    sector: str = "Unknown Sector",
    summary: dict | None = None,
    subject: str | None = None,
    extra_message: str = "",
) -> bool:
    """
    Send a completion email with the Excel report.

    Args:
        excel_path:     Path to the Excel file to attach / reference.
        sector:         Sector name for the subject line and body.
        summary:        Dict with keys like total, tier1, tier2, family, directors.
        subject:        Override the auto-generated subject line.
        extra_message:  Additional text appended to the email body.

    Returns:
        True if sent successfully, False otherwise (logs reason, never raises).
    """
    cfg = _load_mail_config()

    username  = cfg.get("MAIL_USERNAME", "")
    password  = cfg.get("MAIL_PASSWORD", "")
    server    = cfg.get("MAIL_SERVER", DEFAULT_SERVER)
    port      = int(cfg.get("MAIL_PORT", DEFAULT_PORT))
    recipient = cfg.get("NOTIFY_EMAIL", DEFAULT_RECIPIENT)

    if not username or not password:
        print("[notify] ⚠️  MAIL_USERNAME / MAIL_PASSWORD not set — email skipped.")
        print("[notify]    Set them in .mail_config or as environment variables.")
        return False

    # ── Build email content ────────────────────────────────────────────────────
    now        = datetime.now().strftime("%d %b %Y %H:%M")
    excel_path = Path(excel_path) if excel_path else None
    file_size  = excel_path.stat().st_size if excel_path and excel_path.exists() else 0
    file_name  = excel_path.name if excel_path else "N/A"

    if subject is None:
        subject = f"✅ {sector} Deal Sourcing — Report Ready ({now})"

    # Summary block
    if summary:
        total     = summary.get("total", "?")
        tier1     = summary.get("tier1", "?")
        tier2     = summary.get("tier2", "?")
        family    = summary.get("family", "?")
        directors = summary.get("directors", "?")
        summary_block = (
            f"Pipeline Summary\n"
            f"  Companies processed : {total}\n"
            f"  Tier 1 targets      : {tier1}\n"
            f"  Tier 2 targets      : {tier2}\n"
            f"  Family businesses   : {family}\n"
            f"  Directors identified: {directors}\n"
        )
    else:
        summary_block = ""

    attach_note = (
        f"📎 Excel attached: {file_name} ({file_size / 1024:.0f} KB)"
        if file_size and file_size <= MAX_ATTACH_BYTES
        else f"📁 File too large to attach ({file_size / 1024 / 1024:.1f} MB). "
             f"Find it at: {excel_path}" if excel_path
        else "⚠️  No Excel file found."
    )

    body = f"""Your {sector} intelligence report has finished processing.

{summary_block}
{attach_note}

Sheets included:
  - Summary Dashboard
  - All Companies (with acquisition scores)
  - Prime Targets (Tier 1 & 2)
  - Competitor Maps
  - Directors Register
  - Family Businesses

Generated: {now}
{("─" * 40 + chr(10) + extra_message) if extra_message else ""}
— PE Deal Sourcing Pipeline
"""

    # ── Build MIME message ─────────────────────────────────────────────────────
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = f"PE Deal Sourcing <{username}>"
    msg["To"]      = recipient
    msg["Date"]    = formatdate()
    msg.set_content(body)

    # Attach Excel if small enough
    if excel_path and excel_path.exists() and file_size <= MAX_ATTACH_BYTES:
        mime_type, _ = mimetypes.guess_type(str(excel_path))
        maintype, subtype = (mime_type or "application/octet-stream").split("/", 1)
        with open(excel_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype=maintype,
                subtype=subtype,
                filename=excel_path.name,
            )

    # ── Send ──────────────────────────────────────────────────────────────────
    try:
        print(f"[notify] Connecting to {server}:{port} …")
        with smtplib.SMTP(server, port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(username, password)
            smtp.send_message(msg)
        print(f"[notify] ✅ Email sent to {recipient}")
        return True
    except smtplib.SMTPAuthenticationError:
        print("[notify] ❌ Authentication failed — check MAIL_USERNAME / MAIL_PASSWORD.")
        print("[notify]    iCloud users: use an app-specific password, not your Apple ID password.")
        print("[notify]    Generate at: appleid.apple.com → Sign-In & Security → App-Specific Passwords")
        return False
    except smtplib.SMTPException as e:
        print(f"[notify] ❌ SMTP error: {e}")
        return False
    except Exception as e:
        print(f"[notify] ❌ Unexpected error: {e}")
        return False


# ── Standalone CLI ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Send a deal-sourcing completion email with an Excel attachment.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Setup:
  Create a .mail_config file in the repo root:
    MAIL_USERNAME=daniellipinski@mac.com
    MAIL_PASSWORD=xxxx-xxxx-xxxx-xxxx   # iCloud app-specific password

Examples:
  python notify.py --file output/UK_Lift_Maintenance_Companies_March2026.xlsx --sector "Lift Maintenance"
  python notify.py --file output/report.xlsx --sector "Fire Safety" --to someone@example.com
  python notify.py --test   # sends a test email without an attachment
        """,
    )
    parser.add_argument("--file",   metavar="PATH",   help="Path to the Excel file to send")
    parser.add_argument("--sector", metavar="NAME",   default="Deal Sourcing", help="Sector name for subject line")
    parser.add_argument("--to",     metavar="EMAIL",  help=f"Override recipient (default: {DEFAULT_RECIPIENT})")
    parser.add_argument("--test",   action="store_true", help="Send a test email without attachment")
    args = parser.parse_args()

    if args.to:
        os.environ["NOTIFY_EMAIL"] = args.to

    if args.test:
        ok = send_completion_email(
            sector=args.sector or "Test",
            extra_message="This is a test email from the PE deal-sourcing pipeline.",
        )
    else:
        ok = send_completion_email(
            excel_path=args.file,
            sector=args.sector,
        )

    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
