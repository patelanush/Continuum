"""Prove Collector loss is outside the workflow correctness path."""

import asyncio
import json
from pathlib import Path
from time import monotonic

import httpx
from trace_workflow import fetch_trace, search_workflow


async def compose(*arguments: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "--profile",
        "observability",
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode:
        raise RuntimeError((stdout + stderr).decode()[-2000:])


async def one(client: httpx.AsyncClient) -> tuple[str, int]:
    created = await client.post(
        "/api/v1/workflows",
        json={
            "workflow_type": "coding-demo",
            "steps": [{"name": "collector-outage", "step_type": "noop", "input": {}}],
        },
    )
    created.raise_for_status()
    workflow_id = created.json()["id"]
    started = await client.post(f"/api/v1/workflows/{workflow_id}/start")
    started.raise_for_status()
    deadline = monotonic() + 60
    while monotonic() < deadline:
        response = await client.get(f"/api/v1/workflows/{workflow_id}")
        response.raise_for_status()
        if response.json()["status"] == "SUCCEEDED":
            attempts = await client.get(f"/api/v1/workflows/{workflow_id}/attempts")
            attempts.raise_for_status()
            return workflow_id, len(attempts.json())
        await asyncio.sleep(0.2)
    raise TimeoutError(workflow_id)


async def main() -> None:
    await compose("stop", "otel-collector")
    try:
        async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=15) as client:
            outage_results = await asyncio.gather(*(one(client) for _ in range(5)))
    finally:
        await compose("start", "otel-collector")
    if any(count != 1 for _, count in outage_results):
        raise AssertionError("Collector outage changed execution attempt count")
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=15) as client:
        resumed_workflow, resumed_attempts = await one(client)
    deadline = monotonic() + 40
    trace_id = ""
    while monotonic() < deadline:
        for candidate in search_workflow(resumed_workflow, "http://127.0.0.1:3200"):
            names = {item["name"] for item in fetch_trace(candidate, "http://127.0.0.1:3200")}
            if {"api.workflow.start", "kafka.publish", "kafka.consume", "execution.run"} <= names:
                trace_id = candidate
                break
        if trace_id:
            break
        await asyncio.sleep(1)
    if not trace_id:
        raise AssertionError("Telemetry did not resume after Collector restart")
    summary = {
        "scenario": "telemetry-backend-outage",
        "fault": "OpenTelemetry Collector stopped during five workflows",
        "workflows_during_outage": len(outage_results),
        "outage_workflows_succeeded": len(outage_results),
        "outage_attempt_counts": [count for _, count in outage_results],
        "post_restart_workflow_id": resumed_workflow,
        "post_restart_attempt_count": resumed_attempts,
        "post_restart_trace_id": trace_id,
        "correctness_impact": "none observed",
    }
    path = Path("benchmarks/results/phase7-telemetry-outage.json")
    await asyncio.to_thread(path.write_text, json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
