import json
import os

import requests

from ..utils.logging import log


def send_slack_alert(message: str):
    """
    Send a message to Slack webhook
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        log(" Slack webhook not configured!")
        return

    payload = {"text": message}
    resp = requests.post(
        webhook_url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )
    if resp.status_code != 200:
        log(f"Failed to send Slack message: {resp.status_code} {resp.text}")
    else:
        log(" Slack message sent successfully.")
