import os
import hmac
import hashlib
import time
import json
import requests
from fastapi import FastAPI, Request, Response
from dotenv import load_dotenv

load_dotenv()

SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
SLACK_BOT_TOKEN      = os.getenv("SLACK_BOT_TOKEN")

app = FastAPI()

def verify_slack_signature(request_body: bytes, timestamp: str, signature: str) -> bool:
    """Verify the request actually came from Slack."""
    if abs(time.time() - int(timestamp)) > 60 * 5:
        return False
    sig_basestring = f"v0:{timestamp}:{request_body.decode('utf-8')}"
    computed = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode(),
        sig_basestring.encode(),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(computed, signature)

def update_slack_message(channel: str, ts: str, text: str):
    """Update an existing Slack message after button click."""
    requests.post(
        "https://slack.com/api/chat.update",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        json={
            "channel": channel,
            "ts": ts,
            "text": text,
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": text}
                }
            ]
        }
    )

@app.post("/slack/actions")
async def handle_action(request: Request):
    """Receive Approve/Reject button clicks from Slack."""
    body_bytes = await request.body()

    # Verify it's really from Slack
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")

    if not verify_slack_signature(body_bytes, timestamp, signature):
        return Response(content="Unauthorized", status_code=401)

    # Parse the payload
    from urllib.parse import parse_qs
    parsed = parse_qs(body_bytes.decode())
    payload = json.loads(parsed["payload"][0])

    action_id  = payload["actions"][0]["action_id"]
    channel    = payload["container"]["channel_id"]
    message_ts = payload["container"]["message_ts"]
    user       = payload["user"]["name"]

    if action_id == "approve_action":
        print(f"\n[APPROVED] by @{user} — executing remediation...")
        update_slack_message(
            channel, message_ts,
            f"✅ *Action APPROVED* by @{user}\n"
            f"🔧 Executing remediation: restarting affected pod...\n"
            f"📋 RCA report will be generated after resolution."
        )
        # Phase 3 will plug real K8s commands here
        print("  >> Would restart pod here (Phase 3)")

    elif action_id == "reject_action":
        print(f"\n[REJECTED] by @{user} — standing down.")
        update_slack_message(
            channel, message_ts,
            f"❌ *Action REJECTED* by @{user}\n"
            f"👀 Incident logged. Manual investigation required."
        )

    return Response(content="", status_code=200)


@app.get("/health")
async def health():
    return {"status": "InfraGPT webhook server running"}