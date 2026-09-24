"""Crash a running executor and verify the persisted recovery trace in Tempo."""

import asyncio
import json
from pathlib import Path
from time import monotonic
from typing import Any

import httpx
from trace_workflow import fetch_trace, search_workflow


async def prom(expression: str) -> float:
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(
            "http://127.0.0.1:9090/api/v1/query", params={"query": expression}
        )
        response.raise_for_status()
        rows = response.json()["data"]["result"]
        return float(rows[0]["value"][1]) if rows else 0.0


async def docker(*arguments: str) -> str:
    process = await asyncio.create_subprocess_exec(
        "docker",
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode:
        raise RuntimeError(f"docker {arguments[0]} failed: {stderr.decode()[-1000:]}")
    return stdout.decode().strip()


async def attempts(client: httpx.AsyncClient, workflow_id: str) -> list[dict[str, Any]]:
    response = await client.get(f"/api/v1/workflows/{workflow_id}/attempts")
    response.raise_for_status()
    return response.json()  # type: ignore[no-any-return]


async def main() -> None:
    before_recoveries = await prom("sum(continuum_recoveries_total)")
    before_expirations = await prom("sum(continuum_lease_expirations_total)")
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=15) as client:
        created = await client.post(
            "/api/v1/workflows",
            json={
                "workflow_type": "coding-demo",
                "steps": [
                    {
                        "name": "recoverable-work",
                        "step_type": "slow_noop",
                        "input": {"duration_ms": 10000},
                    }
                ],
            },
        )
        created.raise_for_status()
        workflow_id = created.json()["id"]
        started = await client.post(f"/api/v1/workflows/{workflow_id}/start")
        started.raise_for_status()
        deadline = monotonic() + 60
        first: dict[str, Any] | None = None
        while monotonic() < deadline:
            first = next(
                (
                    item
                    for item in await attempts(client, workflow_id)
                    if item["status"] == "RUNNING"
                ),
                None,
            )
            if first:
                break
            await asyncio.sleep(0.1)
        if first is None:
            raise TimeoutError("First attempt was never claimed")
        # Allow the short attempt marker to leave the batch processor before SIGKILL.
        await asyncio.sleep(2)
        ids = (await docker("compose", "ps", "-q", "executor")).splitlines()
        container_id = ""
        for candidate in ids:
            hostname = await docker("inspect", "--format", "{{.Config.Hostname}}", candidate)
            if first["executor_id"].startswith(hostname):
                container_id = candidate
                break
        if not container_id:
            raise AssertionError("Could not locate the executor owning the attempt")
        await docker("kill", "--signal=KILL", container_id)
        try:
            await docker("start", container_id)
            deadline = monotonic() + 100
            while monotonic() < deadline:
                response = await client.get(f"/api/v1/workflows/{workflow_id}")
                response.raise_for_status()
                status = response.json()["status"]
                if status == "SUCCEEDED":
                    break
                if status in {"FAILED", "CANCELLED"}:
                    raise AssertionError(f"Recovered workflow ended {status}")
                await asyncio.sleep(0.5)
            else:
                raise TimeoutError("Recovered workflow did not finish")
        finally:
            state = await docker("inspect", "--format", "{{.State.Running}}", container_id)
            if state != "true":
                await docker("start", container_id)
        records = await attempts(client, workflow_id)
        if [item["status"] for item in records] != ["EXPIRED", "SUCCEEDED"]:
            raise AssertionError(f"Unexpected attempt history: {records}")
    deadline = monotonic() + 45
    required = {
        "execution.run",
        "recovery.expire_attempt",
        "recovery.create_replacement",
        "execution.result",
    }
    trace_id = ""
    spans: list[dict[str, Any]] = []
    while monotonic() < deadline:
        for candidate in search_workflow(workflow_id, "http://127.0.0.1:3200"):
            candidate_spans = fetch_trace(candidate, "http://127.0.0.1:3200")
            names = {item["name"] for item in candidate_spans}
            if (
                required <= names
                and sum(item["name"] == "execution.run" for item in candidate_spans) >= 2
            ):
                trace_id, spans = candidate, candidate_spans
                break
        if trace_id:
            break
        await asyncio.sleep(1)
    if not trace_id:
        raise AssertionError("Tempo lacks the correlated attempt and recovery stages")
    deadline = monotonic() + 30
    while monotonic() < deadline:
        after_recoveries = await prom("sum(continuum_recoveries_total)")
        after_expirations = await prom("sum(continuum_lease_expirations_total)")
        if (
            after_recoveries - before_recoveries >= 1
            and after_expirations - before_expirations >= 1
        ):
            break
        await asyncio.sleep(1)
    else:
        raise AssertionError("Recovery and lease expiration metrics did not increase")
    summary = {
        "fault": "SIGKILL executor during slow_noop",
        "workflow_id": workflow_id,
        "trace_id": trace_id,
        "span_count": len(spans),
        "services_present": sorted({item["service"] for item in spans}),
        "attempt_1": {"id": records[0]["id"], "status": records[0]["status"]},
        "attempt_2": {"id": records[1]["id"], "status": records[1]["status"]},
        "recovery_spans_present": sorted(required),
        "workflow_status": "SUCCEEDED",
        "recovery_metric_delta": round(after_recoveries - before_recoveries),
        "lease_expiration_metric_delta": round(after_expirations - before_expirations),
    }
    path = Path("benchmarks/results/phase7-recovery-trace.json")
    await asyncio.to_thread(path.write_text, json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
