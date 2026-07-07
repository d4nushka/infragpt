# InfraGPT

I built this because I was frustrated with how monitoring works today.

Every alert system I've used does the same thing — it waits for a metric to cross a threshold, then sends you a noisy Slack ping at 2am. You wake up, stare at a dashboard, and spend 20 minutes figuring out what actually happened and what to do about it.

InfraGPT does something different. It watches multiple signals at once, reasons about what's wrong, tells you what it thinks caused it, and asks if you want it to fix it. If you say yes, it does.

---

## What it actually does

When your infrastructure behaves abnormally, InfraGPT:

1. Detects anomalies using z-score based multi-signal correlation — not just "CPU > 80%"
2. Searches its memory of past incidents to see if this has happened before
3. Calls an LLM to diagnose the probable root cause using current metrics + historical context
4. Sends a structured Slack alert with its analysis and recommended action
5. Waits for a human to approve or reject before doing anything
6. On approval, runs a LangGraph ReAct agent that interacts with your Kubernetes cluster to fix it
7. Stores the resolved incident back into vector memory so future diagnoses get smarter

The key insight is that a single spiking metric is noise. Three metrics spiking simultaneously is a real incident. This is how experienced SREs think — InfraGPT mimics that reasoning.

---

## The stack

- **Apache Kafka** — event streaming backbone, metrics flow through a real-time topic
- **Python + NumPy** — z-score anomaly detection with sliding window baselines
- **Groq (LLaMA 3.1)** — LLM for root cause analysis and severity classification
- **Supabase + pgvector** — vector database for semantic incident memory
- **sentence-transformers** — local embeddings, no API cost
- **Slack Block Kit** — structured alerts with interactive Approve/Reject buttons
- **FastAPI + ngrok** — webhook server to receive Slack button callbacks
- **LangGraph ReAct** — autonomous agent loop: observe → reason → act → verify
- **K3d** — lightweight local Kubernetes for pod management
- **Docker + Docker Compose** — entire local setup runs for $0

---

## Why multi-signal detection matters

A naive alerting system fires when CPU crosses 80%. A senior SRE ignores that alert — CPU spikes happen all the time during deploys.

What actually matters is when CPU spikes *and* latency climbs *and* error rate jumps *at the same time*. That combination means something. InfraGPT uses z-scores to measure how far each metric is from its recent baseline, and only triggers when at least two signals cross the threshold simultaneously.

This cuts out noise almost entirely.

---

## Why vector memory matters

The first time your payment service OOMKills, the LLM has to reason about it from scratch. The fifth time, InfraGPT has seen this before — it finds the three most similar past incidents in Supabase and injects them into the prompt. The diagnosis is faster and more accurate. The system genuinely gets smarter over time.

---

## Project structure
---
infragpt/
├── simulator.py          # generates fake metrics with injected spikes
├── consumer.py           # Kafka consumer, anomaly detection, orchestration
├── llm_agent.py          # Groq integration for root cause analysis
├── langgraph_agent.py    # LangGraph ReAct agent for K8s remediation
├── incident_memory.py    # Supabase vector memory — store and search incidents
├── webhook_server.py     # FastAPI server for Slack button callbacks
├── k8s-deployments.yml   # fake microservice deployments for local K8s
├── docker-compose.yml    # Kafka + Zookeeper local setup
└── .env.example          # required environment variables
---

## Running it locally

### Prerequisites

- Python 3.11+
- Docker Desktop
- k3d (`winget install k3d`)
- kubectl (`winget install kubectl`)
- Free accounts: Slack, Groq, Supabase, ngrok

### Setup

```bash
git clone https://github.com/d4nushka/infragpt.git
cd infragpt
python -m venv venv
venv\Scripts\activate
pip install kafka-python==2.0.5 requests numpy python-dotenv groq fastapi uvicorn \
            langgraph langchain langchain-groq supabase sentence-transformers
```

Copy `.env.example` to `.env` and fill in your keys.

### Start everything

```bash
# Start Kafka
docker compose up -d

# Start K3d cluster
k3d cluster start infragpt

# Deploy fake microservices
kubectl apply -f k8s-deployments.yml

# Terminal 1 — webhook server
uvicorn webhook_server:app --port 8000

# Terminal 2 — ngrok tunnel
ngrok http 8000

# Terminal 3 — metric simulator
python simulator.py

# Terminal 4 — anomaly consumer
python consumer.py
```

Update your Slack app's Interactivity URL to `https://your-ngrok-url/slack/actions`.

---

## How the alert looks

When an anomaly is detected, InfraGPT posts to Slack with:
- Severity level (P0 / P1 / P2) with colour indicator
- Current metric values and their z-scores
- LLM root cause analysis with probable cause and recommended action
- Similar past incidents retrieved from vector memory
- Approve and Reject buttons

Clicking Approve triggers the LangGraph agent. It lists your pods, identifies the affected one, restarts it, and verifies the deployment recovered. The Slack message updates with what it did.

---

## What I learned building this

Building this on a machine with 5.89GB usable RAM while running Kafka, K3d, an LLM pipeline, and a vector database simultaneously taught me more about resource constraints and system design tradeoffs than any tutorial ever did.

The hardest part wasn't the LLM integration or the Kubernetes agent — it was the multi-signal detection logic. Getting the z-score window size, threshold, and minimum signal count right so the system fires on real incidents without spamming on noise took a lot of iteration.

---

## What's next

- Auto-generate structured RCA reports after each resolved incident
- Multi-cloud drift detection using Terraform state comparison
- Cost optimization agent that analyzes AWS spend and suggests right-sizing
- Deploy on real cloud infrastructure with Confluent Cloud + Railway

---

## Built by

Anushka Das  
BTech CSE, VIT Bhopal — Cloud Computing & Automation  
[github.com/d4nushka](https://github.com/d4nushka)