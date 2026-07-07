import os
import json
from datetime import datetime
from supabase import create_client
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()

# ── Supabase Client Initialization ─────────────────────────
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# ── Embedding Model Setup ──────────────────────────────────
# Local execution, zero API costs.
print("Loading embedding model...")
embedder = SentenceTransformer("all-MiniLM-L6-v2")
print("Embedding model ready.")


def embed_text(text: str) -> list:
    """Convert text string into a 384-dimensional vector array."""
    return embedder.encode(text).tolist()


def store_incident(metric: dict, scores: dict, triggered: list, llm_result: dict):
    """
    Store a resolved incident in Supabase with its embedding configuration.
    Invoked immediately following an active mitigation approval loop.
    """
    # Construct a highly dense semantic context string for the vector space
    summary = (
        f"Service: {metric['service']}. "
        f"Severity: {llm_result['severity']}. "
        f"Root cause: {llm_result['root_cause']}. "
        f"Action: {llm_result['action']}. "
        f"Signals: {', '.join(triggered)}. "
        f"CPU: {metric['cpu_percent']}%, "
        f"Latency: {metric['latency_p99_ms']}ms, "
        f"Error rate: {metric['error_rate_percent']}%"
    )

    embedding = embed_text(summary)

    data = {
        "service":              metric["service"],
        "severity":             llm_result["severity"],
        "root_cause":           llm_result["root_cause"],
        "action_taken":         llm_result["action"],  # Maps to your SQL column
        "triggered_signals":    triggered,
        "cpu_percent":          metric["cpu_percent"],
        "latency_p99_ms":       metric["latency_p99_ms"],
        "error_rate_percent":   metric["error_rate_percent"],
        "pod_restarts":         metric["pod_restarts"],
        "resolved":             True,
        "embedding":            embedding
    }

    try:
        result = supabase.table("incidents").insert(data).execute()
        print(f"  Incident stored in Supabase (id: {result.data[0]['id'][:8]}...)")
        return result.data[0]["id"]
    except Exception as e:
        print(f"  Failed to store incident: {e}")
        return None


def find_similar_incidents(metric: dict, triggered: list, top_k: int = 3) -> list:
    """
    Search Supabase for past incidents similar to the current live anomaly telemetry.
    Returns top_k most structurally aligned past database rows.
    """
    # Build query layout matching the incoming telemetry profile
    query_text = (
        f"Service: {metric['service']}. "
        f"Signals: {', '.join(triggered)}. "
        f"CPU: {metric['cpu_percent']}%, "
        f"Latency: {metric['latency_p99_ms']}ms, "
        f"Error rate: {metric['error_rate_percent']}%"
    )

    query_embedding = embed_text(query_text)

    try:
        # Executes the RPC pattern matching function inside PostgreSQL
        result = supabase.rpc(
            "match_incidents",
            {
                "query_embedding": query_embedding,
                "match_threshold": 0.4,  # Optimized threshold level for MiniLM structures
                "match_count": top_k
            }
        ).execute()

        return result.data if result.data else []
    except Exception as e:
        print(f"  Similarity search failed: {e}")
        return []


def format_similar_incidents(similar: list) -> str:
    """Format similar incidents into a clean list for injection into the LLM system prompt."""
    if not similar:
        return "No similar past incidents found."

    lines = []
    for inc in similar:
        # Extract date properties safely from timestamp returns
        date = inc.get("created_at", "")[:10] if inc.get("created_at") else "Recent"
        lines.append(
            f"- [{date}] {inc['severity']} on {inc['service']}: "
            f"{inc['root_cause']} → fixed by: {inc['action_taken']}"
        )
    return "\n".join(lines)


# ── Standalone Component Verification Test ─────────────────
if __name__ == "__main__":
    test_metric = {
        "service": "payment-service",
        "cpu_percent": 88.5,
        "latency_p99_ms": 2400.0,
        "error_rate_percent": 11.2,
        "pod_restarts": 3
    }
    test_llm_result = {
        "severity": "P1",
        "root_cause": "Memory pressure causing pod OOMKilled",
        "action": "Restart affected pod and increase memory limits",
        "explanation": "High memory usage detected across multiple signals."
    }
    test_triggered = ["cpu_percent", "latency_p99_ms", "error_rate_percent"]

    print("\n--- Running Component Test ---")
    print("Storing test incident...")
    incident_id = store_incident(test_metric, {}, test_triggered, test_llm_result)
    print(f"Stored with ID: {incident_id}")

    print("\nSearching for similar incidents...")
    similar = find_similar_incidents(test_metric, test_triggered, top_k=3)
    print(f"Found {len(similar)} similar incidents:")
    print(format_similar_incidents(similar))