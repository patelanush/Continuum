"""Repeatable local Compose crash, heartbeat, and distributed-workload demonstrations.

Run after `docker compose up --build -d --scale worker=3 --scale executor=3`.
This script intentionally SIGKILLs only the executor owning its test attempt.
"""

import argparse
import asyncio
import json
import subprocess
from collections.abc import Awaitable, Callable
from typing import Any, cast
from uuid import UUID, uuid4

import httpx

API = "http://127.0.0.1:8000"
PAYMENTS = "http://localhost:8001"


async def wait_for[T](probe: Callable[[], Awaitable[T | None]], *, wait_timeout: float = 90) -> T:
    deadline = asyncio.get_running_loop().time() + wait_timeout
    while asyncio.get_running_loop().time() < deadline:
        result = await probe()
        if result is not None:
            return result
        await asyncio.sleep(0.1)
    raise TimeoutError(f"condition not met within {wait_timeout} seconds")


async def create_and_start(
    client: httpx.AsyncClient, steps: list[dict[str, Any]]
) -> dict[str, Any]:
    created = await client.post(
        "/api/v1/workflows", json={"workflow_type": "phase3-demo", "steps": steps}
    )
    created.raise_for_status()
    workflow = cast(dict[str, Any], created.json())
    started = await client.post(f"/api/v1/workflows/{workflow['id']}/start")
    started.raise_for_status()
    return workflow


async def attempts(client: httpx.AsyncClient, workflow_id: str) -> list[dict[str, Any]]:
    response = await client.get(f"/api/v1/workflows/{workflow_id}/attempts")
    response.raise_for_status()
    return cast(list[dict[str, Any]], response.json())


async def workflow(client: httpx.AsyncClient, workflow_id: str) -> dict[str, Any]:
    response = await client.get(f"/api/v1/workflows/{workflow_id}")
    response.raise_for_status()
    return cast(dict[str, Any], response.json())


def kill_owner(executor_id: str) -> str:
    listed = subprocess.run(
        ["docker", "compose", "ps", "-q", "executor"],
        check=True,
        capture_output=True,
        text=True,
    )
    for container_id in listed.stdout.splitlines():
        inspected = subprocess.run(
            ["docker", "inspect", "--format", "{{.Config.Hostname}}", container_id],
            check=True,
            capture_output=True,
            text=True,
        )
        if executor_id.startswith(inspected.stdout.strip()):
            subprocess.run(["docker", "kill", "--signal=KILL", container_id], check=True)
            return container_id
    raise RuntimeError(f"Could not locate Compose executor owning {executor_id}")


def restart_container(container_id: str) -> None:
    subprocess.run(["docker", "start", container_id], check=True, capture_output=True, text=True)


async def running_attempt(client: httpx.AsyncClient, workflow_id: str) -> dict[str, Any]:
    async def probe() -> dict[str, Any] | None:
        return next(
            (item for item in await attempts(client, workflow_id) if item["status"] == "RUNNING"),
            None,
        )

    return await wait_for(probe)


async def succeeded(client: httpx.AsyncClient, workflow_id: str) -> dict[str, Any]:
    async def probe() -> dict[str, Any] | None:
        state = await workflow(client, workflow_id)
        if state["status"] == "FAILED":
            raise AssertionError(f"Workflow failed: {state}")
        return state if state["status"] == "SUCCEEDED" else None

    return await wait_for(probe, wait_timeout=90)


async def side_effect_crash(
    client: httpx.AsyncClient, payments: httpx.AsyncClient
) -> dict[str, Any]:
    before = (await payments.get("/refunds/count")).json()["count"]
    created = await create_and_start(
        client,
        [
            {
                "name": "refund",
                "step_type": "mock_refund",
                "input": {
                    "customer_id": f"demo-{uuid4()}",
                    "amount": "49.99",
                    "delay_after_commit_ms": 20000,
                },
            }
        ],
    )
    workflow_id = created["id"]
    step_id = UUID(created["steps"][0]["id"])
    key = f"continuum:{step_id}"
    first = await running_attempt(client, workflow_id)

    async def refund_committed() -> dict[str, Any] | None:
        response = await payments.get(f"/refunds/by-idempotency-key/{key}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return cast(dict[str, Any], response.json())

    refund = await wait_for(refund_committed, wait_timeout=30)
    current = await attempts(client, workflow_id)
    assert current[0]["status"] == "RUNNING", "executor finalized before crash injection"
    killed = kill_owner(first["executor_id"])
    try:
        final = await succeeded(client, workflow_id)
        records = await attempts(client, workflow_id)
        after = (await payments.get("/refunds/count")).json()["count"]
    finally:
        restart_container(killed)
    assert [item["status"] for item in records] == ["EXPIRED", "SUCCEEDED"]
    assert records[0]["executor_id"] != records[1]["executor_id"]
    assert after == before + 1
    assert final["steps"][0]["status"] == "SUCCEEDED"
    assert final["steps"][0]["output"]["refund_id"] == refund["refund_id"]
    return {
        "workflow_id": workflow_id,
        "killed_container": killed[:12],
        "attempts": [item["status"] for item in records],
        "executors": [item["executor_id"] for item in records],
        "refund_id": refund["refund_id"],
        "refund_count_delta": after - before,
        "workflow_status": final["status"],
    }


async def pure_crash(client: httpx.AsyncClient) -> dict[str, Any]:
    created = await create_and_start(
        client,
        [{"name": "slow", "step_type": "slow_noop", "input": {"duration_ms": 12000}}],
    )
    first = await running_attempt(client, created["id"])
    killed = kill_owner(first["executor_id"])
    try:
        final = await succeeded(client, created["id"])
        records = await attempts(client, created["id"])
    finally:
        restart_container(killed)
    assert [item["status"] for item in records] == ["EXPIRED", "SUCCEEDED"]
    return {
        "workflow_id": created["id"],
        "killed_container": killed[:12],
        "attempts": [item["status"] for item in records],
        "workflow_status": final["status"],
    }


async def heartbeat_demo(client: httpx.AsyncClient) -> dict[str, Any]:
    created = await create_and_start(
        client,
        [{"name": "healthy-slow", "step_type": "slow_noop", "input": {"duration_ms": 25000}}],
    )
    first = await running_attempt(client, created["id"])
    final = await succeeded(client, created["id"])
    records = await attempts(client, created["id"])
    assert len(records) == 1 and records[0]["status"] == "SUCCEEDED"
    assert records[0]["last_heartbeat_at"] is not None
    assert records[0]["last_heartbeat_at"] > first["started_at"]
    return {
        "workflow_id": created["id"],
        "attempt_count": len(records),
        "workflow_status": final["status"],
    }


async def batch_demo(client: httpx.AsyncClient) -> dict[str, Any]:
    steps = [{"name": f"step-{index}", "step_type": "noop"} for index in range(5)]
    created = await asyncio.gather(*(create_and_start(client, steps) for _ in range(20)))
    ids = [item["id"] for item in created]
    completed = await asyncio.gather(*(succeeded(client, item) for item in ids))
    records = await asyncio.gather(*(attempts(client, item) for item in ids))
    all_attempts = [item for group in records for item in group]
    executors = {item["executor_id"] for item in all_attempts}
    assert len(completed) == 20
    assert all(len(group) == 5 for group in records)
    assert all(item["status"] == "SUCCEEDED" for item in all_attempts)
    assert all(all(step["status"] == "SUCCEEDED" for step in flow["steps"]) for flow in completed)
    assert len(executors) >= 2
    return {
        "workflows": len(completed),
        "steps": len(all_attempts),
        "executor_ids": sorted(executors),
    }


async def main(mode: str) -> None:
    async with (
        httpx.AsyncClient(base_url=API, timeout=10) as client,
        httpx.AsyncClient(base_url=PAYMENTS, timeout=10) as payments,
    ):
        results: dict[str, Any] = {}
        if mode in {"all", "side-effect"}:
            results["side_effect_crash"] = await side_effect_crash(client, payments)
        if mode in {"all", "pure"}:
            results["pure_crash"] = await pure_crash(client)
        if mode in {"all", "heartbeat"}:
            results["heartbeat"] = await heartbeat_demo(client)
        if mode in {"all", "batch"}:
            results["batch"] = await batch_demo(client)
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=["all", "side-effect", "pure", "heartbeat", "batch"],
        nargs="?",
        default="all",
    )
    asyncio.run(main(parser.parse_args().mode))
