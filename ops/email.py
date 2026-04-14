"""
Email notifications via Resend.
Only used for opt-in access request outcome notifications.
"""
import logging
import os

import resend

logger = logging.getLogger("pipeline_ops")

resend.api_key = os.environ.get("RESEND_API_KEY", "")

FROM_ADDRESS = "noreply@cartracker.info"


def send_access_approved(to_email: str, role: str) -> None:
    role_label = role.replace("_", " ").title()
    destination = {
        "admin": "https://cartracker.info/admin",
        "power_user": "https://cartracker.info/admin",
        "observer": "https://cartracker.info/admin",
        "viewer": "https://cartracker.info/dashboard",
    }.get(role, "https://cartracker.info")

    try:
        resend.Emails.send({
            "from": FROM_ADDRESS,
            "to": to_email,
            "subject": "Your CarTracker access request was approved",
            "html": f"""
                <p>Your access request has been approved.</p>
                <p>You have been granted <strong>{role_label}</strong> access.</p>
                <p><a href="{destination}">Go to CarTracker →</a></p>
                <hr>
                <small>This email was sent because you opted in to notifications when submitting
                your request. Your email address has now been deleted from our records.</small>
            """,
        })
    except Exception:
        logger.warning("Failed to send approval email to %s", to_email)


def send_access_denied(to_email: str) -> None:
    try:
        resend.Emails.send({
            "from": FROM_ADDRESS,
            "to": to_email,
            "subject": "Your CarTracker access request was not approved",
            "html": """
                <p>Your access request was reviewed and was not approved at this time.</p>
                <p>If you think this is a mistake, please reach out directly.</p>
                <hr>
                <small>This email was sent because you opted in to notifications when submitting
                your request. Your email address has now been deleted from our records.</small>
            """,
        })
    except Exception:
        logger.warning("Failed to send denial email to %s", to_email)
