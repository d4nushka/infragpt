import os
import subprocess
import json
from langchain_groq import ChatGroq
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from dotenv import load_dotenv

load_dotenv()

# ── Explicit kubeconfig so kubectl works in all contexts ───
KUBECONFIG = r"C:\Users\Anushka Das\.config\k3d\kubeconfig-infragpt.yaml"
KUBECTL_ENV = {**os.environ, "KUBECONFIG": KUBECONFIG}

# ── LLM setup ──────────────────────────────────────────────
llm = ChatGroq(
    api_key=os.getenv("GROQ_API_KEY"),
    model="llama-3.1-8b-instant",
    temperature=0
)

# ── Tools ──────────────────────────────────────────────────

@tool
def get_pod_list(namespace: str = "default") -> str:
    """Get list of all running pods in the Kubernetes cluster."""
    try:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-n", namespace, "-o", "json"],
            capture_output=True, text=True, timeout=30,
            env=KUBECTL_ENV
        )
        if result.returncode != 0:
            return f"Error getting pods: {result.stderr}"

        data = json.loads(result.stdout)
        pods = []
        for item in data.get("items", []):
            name = item["metadata"]["name"]
            status = item["status"]["phase"]
            restarts = sum(
                cs.get("restartCount", 0)
                for cs in item["status"].get("containerStatuses", [])
            )
            pods.append(f"{name} | status={status} | restarts={restarts}")

        return "\n".join(pods) if pods else "No pods found"
    except Exception as e:
        return f"Error: {str(e)}"


@tool
def restart_pod(pod_name: str, namespace: str = "default") -> str:
    """Delete a pod to force Kubernetes to restart it.
    Use this when a pod is crashing or showing high error rates."""
    try:
        result = subprocess.run(
            ["kubectl", "delete", "pod", pod_name, "-n", namespace],
            capture_output=True, text=True, timeout=30,
            env=KUBECTL_ENV
        )
        if result.returncode == 0:
            return f"Successfully deleted pod {pod_name}. Kubernetes will restart it automatically."
        else:
            return f"Error deleting pod: {result.stderr}"
    except Exception as e:
        return f"Error: {str(e)}"


@tool
def scale_deployment(deployment_name: str, replicas: int, namespace: str = "default") -> str:
    """Scale a deployment up or down.
    Use this when a service needs more instances to handle load."""
    try:
        result = subprocess.run(
            ["kubectl", "scale", "deployment", deployment_name,
             f"--replicas={replicas}", "-n", namespace],
            capture_output=True, text=True, timeout=30,
            env=KUBECTL_ENV
        )
        if result.returncode == 0:
            return f"Successfully scaled {deployment_name} to {replicas} replicas."
        else:
            return f"Error scaling deployment: {result.stderr}"
    except Exception as e:
        return f"Error: {str(e)}"


@tool
def get_deployment_status(deployment_name: str, namespace: str = "default") -> str:
    """Check the status of a specific deployment after remediation."""
    try:
        result = subprocess.run(
            ["kubectl", "get", "deployment", deployment_name,
             "-n", namespace, "-o", "json"],
            capture_output=True, text=True, timeout=30,
            env=KUBECTL_ENV
        )
        if result.returncode != 0:
            return f"Error: {result.stderr}"

        data = json.loads(result.stdout)
        spec_replicas = data["spec"]["replicas"]
        ready = data["status"].get("readyReplicas", 0)
        available = data["status"].get("availableReplicas", 0)

        return (
            f"Deployment {deployment_name}: "
            f"desired={spec_replicas}, ready={ready}, available={available}"
        )
    except Exception as e:
        return f"Error: {str(e)}"


# ── Build the ReAct agent ──────────────────────────────────
tools = [get_pod_list, restart_pod, scale_deployment, get_deployment_status]
agent = create_react_agent(llm, tools)


def run_remediation(anomaly_context: dict) -> str:
    service  = anomaly_context.get("service", "payment-service")
    severity = anomaly_context.get("severity", "P1")
    cause    = anomaly_context.get("root_cause", "Unknown")
    action   = anomaly_context.get("action", "Investigate and remediate")

    prompt = f"""You are an autonomous SRE agent. An anomaly has been detected and approved for remediation.

INCIDENT DETAILS:
- Service: {service}
- Severity: {severity}
- Root cause: {cause}
- Recommended action: {action}

Your job:
1. First get the list of pods to understand the current state
2. Find the pod(s) related to {service}
3. Execute the appropriate remediation (restart the affected pod OR scale the deployment)
4. Verify the fix worked by checking deployment status
5. Return a concise summary of what you did and the final state

Be decisive. Execute the remediation now."""

    print(f"\n[AGENT] Starting ReAct loop for {service}...")

    result = agent.invoke({
        "messages": [{"role": "user", "content": prompt}]
    })

    final_message = result["messages"][-1].content.strip()

    # Strip "Summary:" prefix if LLM adds it
    if final_message.lower().startswith("summary:"):
        final_message = final_message[8:].strip()

    print(f"[AGENT] Completed: {final_message[:200]}")
    return final_message


# ── Standalone test ────────────────────────────────────────
if __name__ == "__main__":
    test_context = {
        "service": "payment-service",
        "severity": "P1",
        "root_cause": "High CPU and memory causing pod instability",
        "action": "Restart the affected payment-service pod"
    }
    result = run_remediation(test_context)
    print("\n=== AGENT RESULT ===")
    print(result)