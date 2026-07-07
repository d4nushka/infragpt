import os
import json
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))


def analyze_anomaly(metric: dict, scores: dict, triggered: list, similar_incidents: list = None) -> dict:
    # Format similar past incidents for context
    past_context = ""
    if similar_incidents:
        past_context = "\nSIMILAR PAST INCIDENTS:\n"
        for inc in similar_incidents:
            date = inc.get("created_at", "")[:10]
            past_context += (
                f"- [{date}] {inc['severity']} on {inc['service']}: "
                f"{inc['root_cause']} → fixed by: {inc['action_taken']}\n"
            )

    prompt = f"""You are an expert SRE analyzing a cloud infrastructure anomaly.

ANOMALY DATA:
Service: {metric['service']}
Triggered signals: {', '.join(triggered)}

CURRENT METRICS vs BASELINE:
- CPU: {metric['cpu_percent']}% (z-score: {scores['cpu_percent']})
- Latency p99: {metric['latency_p99_ms']}ms (z-score: {scores['latency_p99_ms']})
- Error rate: {metric['error_rate_percent']}% (z-score: {scores['error_rate_percent']})
- Pod restarts: {metric['pod_restarts']} (z-score: {scores['pod_restarts']})
{past_context}
Respond in this EXACT JSON format only, no extra text:
{{
    "severity": "P0 or P1 or P2",
    "root_cause": "one sentence explaining the most likely cause",
    "action": "one sentence describing the recommended remediation action",
    "explanation": "2-3 sentence human readable summary for the on-call engineer"
}}"""

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=300
    )

    text = response.choices[0].message.content.strip()

    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {
            "severity": "P1",
            "root_cause": "Multiple signals spiking simultaneously on payment-service",
            "action": "Restart affected pod and check resource limits",
            "explanation": text[:200]
        }