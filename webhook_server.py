import os
import hmac
import hashlib
import time
import json
import requests
from fastapi import FastAPI, Request, Response
from dotenv import load_dotenv
from langgraph_agent import run_remediation

load_dotenv()

SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
SLACK_BOT_TOKEN      = os.getenv("SLACK_BOT_TOKEN")

app = FastAPI()

# Store pending actions temporarily in memory
pending_actions = {}

def verify_slack_signature(request_body: bytes, timestamp: str, signature: str) -> bool:
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
    body_bytes = await request.body()

    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")

    if not verify_slack_signature(body_bytes, timestamp, signature):
        return Response(content="Unauthorized", status_code=401)

    from urllib.parse import parse_qs
    parsed = parse_qs(body_bytes.decode())
    payload = json.loads(parsed["payload"][0])

    action_id  = payload["actions"][0]["action_id"]
    channel    = payload["container"]["channel_id"]
    message_ts = payload["container"]["message_ts"]
    user       = payload["user"]["name"]

    if action_id == "approve_action":
        print(f"\n[APPROVED] by @{user} — launching ReAct agent...")

        # Update Slack immediately so user knows it's running
        update_slack_message(
            channel, message_ts,
            f"⚙️ *Action APPROVED by @{user}*\n"
            f"🤖 LangGraph ReAct agent is executing remediation...\n"
            f"_This may take 20-30 seconds_"
        )

        # Get the anomaly context stored from the alert
        anomaly_context = pending_actions.get(message_ts, {
            "service": "payment-service",
            "severity": "P1",
            "root_cause": "Multiple signals spiking simultaneously",
            "action": "Restart the affected pod"
        })

        # Run the agent in background
        import threading
        def run_agent():
            try:
                result = run_remediation(anomaly_context)
                # Update Slack with agent result
                update_slack_message(
                    channel, message_ts,
                    f"✅ *Remediation Complete — approved by @{user}*\n\n"
                    f"*Agent Summary:*\n{result[:500]}"
                )
                print(f"[AGENT DONE] Slack updated with result.")
            except Exception as e:
                update_slack_message(
                    channel, message_ts,
                    f"⚠️ *Remediation failed:* {str(e)}"
                )
                print(f"[AGENT ERROR] {e}")

        threading.Thread(target=run_agent, daemon=True).start()

    elif action_id == "reject_action":
        print(f"\n[REJECTED] by @{user} — standing down.")
        update_slack_message(
            channel, message_ts,
            f"❌ *Action REJECTED by @{user}*\n"
            f"👀 Incident logged. Manual investigation required."
        )

    return Response(content="", status_code=200)


def store_pending_action(message_ts: str, anomaly_context: dict):
    """Called from consumer.py to store context for later approval."""
    pending_actions[message_ts] = anomaly_context


@app.get("/health")
async def health():
    return {"status": "InfraGPT webhook server running"}