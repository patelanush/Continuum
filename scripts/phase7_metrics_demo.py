"""Compare Prometheus counter deltas with 20 completed durable workflows."""

import asyncio
import json
from pathlib import Path
from time import monotonic

import httpx


async def query(client: httpx.AsyncClient, expression: str) -> float:
    response = await client.get("http://127.0.0.1:9090/api/v1/query", params={"query": expression})
    response.raise_for_status()
    results = response.json()["data"]["result"]
    return float(results[0]["value"][1]) if results else 0.0


async def one(client: httpx.AsyncClient, semaphore: asyncio.Semaphore) -> str:
    async with semaphore:
        created = await client.post(
            "/api/v1/workflows",
            json={
                "workflow_type": "coding-demo",
                "steps": [{"name": "metrics", "step_type": "noop", "input": {}}],
            },
        )
        created.raise_for_status()
        workflow_id: str = created.json()["id"]
        started = await client.post(f"/api/v1/workflows/{workflow_id}/start")
        started.raise_for_status()
        deadline = monotonic() + 60
        while monotonic() < deadline:
            result = await client.get(f"/api/v1/workflows/{workflow_id}")
            result.raise_for_status()
            status = result.json()["status"]
            if status == "SUCCEEDED":
                return workflow_id
            if status in {"FAILED", "CANCELLED"}:
                raise AssertionError(f"Workflow {workflow_id} ended {status}")
            await asyncio.sleep(0.2)
        raise TimeoutError(workflow_id)


async def main() -> None:
    expressions = {
        "started": "sum(continuum_workflows_started_total)",
        "completed": 'sum(continuum_workflows_completed_total{status="succeeded"})',
        "steps": 'sum(continuum_steps_completed_total{status="succeeded",step_type="noop"})',
    }
    async with httpx.AsyncClient(timeout=15) as prometheus:
        before = {name: await query(prometheus, expr) for name, expr in expressions.items()}
        async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=15) as api:
            semaphore = asyncio.Semaphore(5)
            workflow_ids = await asyncio.gather(*(one(api, semaphore) for _ in range(20)))
        deadline = monotonic() + 30
        while monotonic() < deadline:
            after = {name: await query(prometheus, expr) for name, expr in expressions.items()}
            deltas = {name: round(after[name] - before[name]) for name in expressions}
            if all(value == 20 for value in deltas.values()):
                break
            await asyncio.sleep(1)
        else:
            raise AssertionError(f"Prometheus deltas did not match durable count: {deltas}")
    summary = {
        "expected_workflows": 20,
        "durable_succeeded_workflows": len(workflow_ids),
        "prometheus_before": before,
        "prometheus_after": after,
        "observed_deltas": deltas,
        "verified": True,
    }
    path = Path("benchmarks/results/phase7-metrics-demo.json")
    await asyncio.to_thread(path.write_text, json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
