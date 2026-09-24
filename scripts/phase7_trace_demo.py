"""Run a fake coding workflow and verify its real Tempo trace and redaction."""

import asyncio
import json
import subprocess
from pathlib import Path
from time import monotonic
from typing import Any

import httpx
from trace_workflow import fetch_trace, search_workflow

WORKFLOW_SENTINELS = (
    "SUPER_SECRET_PROMPT_VALUE",
    "PRIVATE_SOURCE_CONTENT",
    "FAKE_API_KEY_123",
)
REQUIRED = {
    "api.workflow.start": "api_start",
    "kafka.publish": "kafka_publish",
    "kafka.consume": "kafka_consume",
    "execution.claim": "execution_claim",
    "execution.run": "execution_attempt",
    "agent.run": "agent_run",
    "agent.turn": "agent_turn",
    "model.call": "model_call",
    "tool.call": "tool_call",
    "coding.apply_patch": "patch",
    "coding.run_tests": "tests",
    "sandbox.command": "sandbox_command",
    "approval.wait": "approval_wait",
    "approval.decision": "approval_decision",
    "git.commit": "git_commit",
}


async def run_workflow(
    api_url: str = "http://127.0.0.1:8000",
) -> tuple[str, str, dict[str, Any]]:
    async with httpx.AsyncClient(base_url=api_url, timeout=15) as client:
        created = await client.post(
            "/api/v1/workflows",
            json={
                "workflow_type": "coding-demo",
                "steps": [
                    {
                        "name": "fix-discount",
                        "step_type": "coding_agent",
                        "input": {
                            "repository": "fixture:discount_service",
                            "task": "Fix the discount bug in TASK.md and run tests. "
                            + " ".join(WORKFLOW_SENTINELS),
                            "test_command": "pytest -q",
                            "provider": "fake",
                        },
                    }
                ],
            },
        )
        created.raise_for_status()
        workflow_id: str = created.json()["id"]
        started = await client.post(f"/api/v1/workflows/{workflow_id}/start")
        started.raise_for_status()
        deadline = monotonic() + 120
        approval_id: str | None = None
        while monotonic() < deadline:
            response = await client.get(f"/api/v1/workflows/{workflow_id}")
            response.raise_for_status()
            workflow: dict[str, Any] = response.json()
            if workflow["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                if workflow["status"] != "SUCCEEDED":
                    raise AssertionError(f"Coding workflow ended {workflow['status']}")
                break
            approvals = await client.get("/api/v1/approvals", params={"limit": 100})
            approvals.raise_for_status()
            for item in approvals.json():
                if item["workflow_id"] == workflow_id and item["status"] == "PENDING":
                    approval_id = item["id"]
                    decided = await client.post(f"/api/v1/approvals/{approval_id}/approve")
                    decided.raise_for_status()
            await asyncio.sleep(0.25)
        else:
            raise TimeoutError(f"Coding workflow {workflow_id} did not finish")
        coding = await client.get(f"/api/v1/workflows/{workflow_id}/coding")
        coding.raise_for_status()
        workspaces = coding.json()
        if len(workspaces) != 1 or not workspaces[0]["final_git_head"] or not approval_id:
            raise AssertionError("Missing workspace commit or approval")
        return workflow_id, approval_id, workspaces[0]


async def locate_trace(workflow_id: str, tempo_url: str) -> tuple[str, list[dict[str, Any]]]:
    deadline = monotonic() + 45
    while monotonic() < deadline:
        for trace_id in search_workflow(workflow_id, tempo_url):
            spans = fetch_trace(trace_id, tempo_url)
            names = {item["name"] for item in spans}
            if REQUIRED.keys() <= names:
                return trace_id, spans
        await asyncio.sleep(1)
    raise AssertionError(f"Tempo trace for {workflow_id} lacks required stages")


async def main() -> None:
    workflow_id, approval_id, workspace = await run_workflow()
    trace_id, spans = await locate_trace(workflow_id, "http://127.0.0.1:3200")
    text = json.dumps(spans)
    if any(value in text for value in WORKFLOW_SENTINELS):
        raise AssertionError("Sentinel leaked into exported span data")
    services = sorted({item["service"] for item in spans})
    required_services = {
        "continuum-api",
        "continuum-dispatcher",
        "continuum-event-worker",
        "continuum-executor",
    }
    if not required_services <= set(services):
        raise AssertionError(f"Missing trace services: {required_services - set(services)}")
    if not any(
        item["name"] == "agent.run" and "continuum.agent_run.id" in item["attributes"]
        for item in spans
    ):
        raise AssertionError("AgentRun marker lacks durable run identity")
    if not any(
        item["name"] == "execution.run" and "continuum.step.type" in item["attributes"]
        for item in spans
    ):
        raise AssertionError("Execution marker lacks step type")
    revision = (
        await asyncio.to_thread(subprocess.check_output, ["git", "rev-parse", "HEAD"], text=True)
    ).strip()
    summary = {
        "source_base_commit": revision,
        "workflow_id": workflow_id,
        "trace_id": trace_id,
        "span_count": len(spans),
        "services_present": services,
        "required_span_categories_present": sorted(REQUIRED.values()),
        "workflow_status": "SUCCEEDED",
        "model_provider": "fake",
        "tool_count": sum(item["name"] == "tool.call" for item in spans),
        "sandbox_commands": sum(item["name"] == "sandbox.command" for item in spans),
        "approval_id": approval_id,
        "commit_sha": workspace["final_git_head"],
        "recovered": False,
        "redaction_sentinels_absent": True,
    }
    path = Path("benchmarks/results/phase7-trace-demo.json")
    await asyncio.to_thread(path.write_text, json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
