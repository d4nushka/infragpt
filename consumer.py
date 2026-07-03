import json
import time
import os
import requests
import numpy as np
from collections import deque
from kafka import KafkaConsumer
from dotenv import load_dotenv
from llm_agent import analyze_anomaly

load_dotenv()
SLACK_WEBHOOK   = os.getenv("SLACK_WEBHOOK_URL")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

WINDOW_SIZE    = 12
Z_THRESHOLD    = 2.5
MIN_SIGNALS    = 2
ALERT_COOLDOWN = 60

history = {
    "cpu_percent":         deque(maxlen=WINDOW_SIZE),
    "latency_p99_ms":      deque(maxlen=WINDOW_SIZE),
    "error_rate_percent":  deque(maxlen=WINDOW_SIZE),
    "pod_restarts":        deque(maxlen=WINDOW_SIZE),
}


def z_score(value, hist):
    if len(hist) < 5:
        return 0.0
    arr = np.array(hist)
    std = arr.std()
    if std < 0.0001:
        return 0.0
    return round((value - arr.mean()) / std, 2)


def analyze(metric):
    scores    = {k: z_score(metric[k], history[k]) for k in history}
    triggered = [k for k, z in scores.items() if z > Z_THRESHOLD]
    return len(triggered) >= MIN_SIGNALS, scores, triggered


def update_history(metric):
    for key in history:
        history[key].append(metric[key])


def send_slack_alert_with_buttons(metric, scores, triggered, llm_result):
    severity_emoji = {"P0": "🔴", "P1": "🟠", "P2": "🟡"}.get(
        llm_result["severity"], "🟠"
    )
    signals_text = "`, `".join(triggered)

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{severity_emoji} {llm_result['severity']} Anomaly — InfraGPT"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Service:* `{metric['service']}`\n"
                        f"*Triggered signals:* `{signals_text}`"
                    )
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*CPU*\n{metric['cpu_percent']}% (z={scores['cpu_percent']})"},
                    {"type": "mrkdwn", "text": f"*Latency p99*\n{metric['latency_p99_ms']}ms (z={scores['latency_p99_ms']})"},
                    {"type": "mrkdwn", "text": f"*Error rate*\n{metric['error_rate_percent']}% (z={scores['error_rate_percent']})"},
                    {"type": "mrkdwn", "text": f"*Pod restarts*\n{metric['pod_restarts']} (z={scores['pod_restarts']})"},
                ]
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*🧠 LLM Root Cause Analysis*\n"
                        f"*Probable cause:* {llm_result['root_cause']}\n"
                        f"*Recommended action:* {llm_result['action']}\n\n"
                        f"_{llm_result['explanation']}_"
                    )
                }
            },
            {"type": "divider"},
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "✅ Approve"},
                        "style": "primary",
                        "action_id": "approve_action",
                        "confirm": {
                            "title": {"type": "plain_text", "text": "Confirm Action"},
                            "text": {"type": "mrkdwn", "text": f"Execute: *{llm_result['action']}*?"},
                            "confirm": {"type": "plain_text", "text": "Yes, do it"},
                            "deny": {"type": "plain_text", "text": "Cancel"}
                        }
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "❌ Reject"},
                        "style": "danger",
                        "action_id": "reject_action"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": "InfraGPT Phase 2 — LLM-powered analysis — human-in-the-loop"}
                ]
            }
        ]
    }

    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=5)
    if resp.status_code == 200:
        print("  Slack alert with buttons sent!")
    else:
        print(f"  Slack alert failed: {resp.status_code} — {resp.text}")


if __name__ == "__main__":
    print("Connecting to Kafka topic 'metrics-raw'...")
    consumer = KafkaConsumer(
        'metrics-raw',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='infragpt-detector-v2',
        consumer_timeout_ms=-1
    )
    print("Listening. Waiting for metrics...\n")

    last_alert_time = 0

    for message in consumer:
        metric = message.value
        is_anomaly, scores, triggered = analyze(metric)
        update_history(metric)

        status = "ANOMALY" if is_anomaly else "ok     "
        print(
            f"[{status}] "
            f"cpu_z={scores['cpu_percent']:5.2f}  "
            f"lat_z={scores['latency_p99_ms']:5.2f}  "
            f"err_z={scores['error_rate_percent']:5.2f}  "
            f"rst_z={scores['pod_restarts']:5.2f}"
        )

        if is_anomaly:
            now = time.time()
            if (now - last_alert_time) > ALERT_COOLDOWN:
                print("  Calling LLM for analysis...")
                llm_result = analyze_anomaly(metric, scores, triggered)
                print(f"  LLM says: {llm_result['severity']} — {llm_result['root_cause']}")
                send_slack_alert_with_buttons(metric, scores, triggered, llm_result)
                last_alert_time = now
            else:
                remaining = int(ALERT_COOLDOWN - (now - last_alert_time))
                print(f"  (cooldown — next alert in {remaining}s)")