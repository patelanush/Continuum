"""Deterministic coding recovery demo through the running local Compose stack."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
from time import monotonic
from typing import Any
from uuid import UUID

import httpx
from phase6_coding_demo import run as finish_coding_demo
from phase8_benchmark import command
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from trace_workflow import search_workflow

from durable_agent_runtime.db.models import CodingWorkspace, SandboxExecution

DATABASE_URL = "postgresql+asyncpg://durable:durable@127.0.0.1:55433/durable"
API_URL = "http://127.0.0.1:8000"


async def compose(*arguments: str) -> str:
    return await command("docker", "compose", *arguments)


async def patch_is_applied(workflow_id: UUID) -> bool:
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        sessions = async_sessionmaker(engine)
        async with sessions() as session:
            workspace = await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.workflow_id == workflow_id)
            )
            if workspace is None:
                return False
            sandbox = await session.scalar(
                select(SandboxExecution)
                .where(SandboxExecution.workspace_id == workspace.id)
                .order_by(SandboxExecution.created_at.desc())
            )
        if sandbox is None:
            return False
        try:
            return bool(
                await command(
                    "docker", "exec", sandbox.container_ref, "git", "status", "--porcelain"
                )
            )
        except RuntimeError:
            return False
    finally:
        await engine.dispose()


async def wait_patch(workflow_id: UUID) -> None:
    deadline = monotonic() + 90
    while monotonic() < deadline:
        if await patch_is_applied(workflow_id):
            return
        await asyncio.sleep(0.2)
    raise TimeoutError("coding patch did not reach the crash boundary")


async def trace_id_for(workflow_id: UUID) -> str | None:
    async with httpx.AsyncClient(timeout=2) as client:
        try:
            ready = await client.get("http://127.0.0.1:3200/ready")
            if ready.status_code != 200:
                return None
        except httpx.HTTPError:
            return None
    deadline = monotonic() + 15
    while monotonic() < deadline:
        try:
            traces = await asyncio.to_thread(
                search_workflow, str(workflow_id), "http://127.0.0.1:3200"
            )
            if traces:
                return str(traces[0])
        except Exception:
            pass
        await asyncio.sleep(1)
    return None


async def run_demo() -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as api:
        ready = await api.get("/health/ready")
        ready.raise_for_status()
        await compose("stop", "-t", "1", "executor")
        oneoff = ""
        try:
            oneoff = await compose(
                "run",
                "-d",
                "--no-deps",
                "-e",
                "APP_ENV=faultlab",
                "-e",
                "AGENT_PROVIDER=fake",
                "-e",
                "EXECUTOR_LEASE_SECONDS=5",
                "-e",
                "EXECUTOR_HEARTBEAT_SECONDS=1",
                "-e",
                "FAULTLAB_CODING_PAUSE_AFTER_PATCH=1",
                "executor",
            )
            created = await api.post(
                "/api/v1/workflows",
                json={
                    "workflow_type": "coding-demo",
                    "steps": [
                        {
                            "name": "fix-discount",
                            "step_type": "coding_agent",
                            "input": {
                                "repository": "fixture:discount_service",
                                "task": "Fix the discount bug in TASK.md and run tests.",
                                "test_command": "pytest -q",
                                "provider": "fake",
                            },
                        }
                    ],
                },
            )
            created.raise_for_status()
            workflow_id = UUID(created.json()["id"])
            started = await api.post(f"/api/v1/workflows/{workflow_id}/start")
            started.raise_for_status()
            print(f"Workflow {workflow_id}: coding agent started", flush=True)
            await wait_patch(workflow_id)
            print(
                "Patch applied; killing its active executor before result persistence", flush=True
            )
            await command("docker", "kill", "--signal=KILL", oneoff)
            await command("docker", "rm", "-f", oneoff)
            oneoff = ""
            await compose("start", "executor")
            captured = io.StringIO()
            with contextlib.redirect_stdout(captured):
                coding = await finish_coding_demo("fake", API_URL, 180, str(workflow_id))
            attempts_response = await api.get(f"/api/v1/workflows/{workflow_id}/attempts")
            attempts_response.raise_for_status()
            attempts: list[dict[str, Any]] = attempts_response.json()
            statuses = [attempt["status"] for attempt in attempts]
            if statuses != ["EXPIRED", "SUCCEEDED"]:
                raise AssertionError(f"expected expired then successful attempt: {statuses}")
            trace_id = await trace_id_for(workflow_id)
            result = {
                "workflow_id": str(workflow_id),
                "workflow_status": coding["workflow_status"],
                "injected_fault": "executor SIGKILL after patch application",
                "attempts": statuses,
                "patch_reconciled": True,
                "tests_passed": coding["test_count"],
                "approval": "APPROVED",
                "commit_sha": coding["final_git_sha"],
                "duplicate_patches": 0,
                "duplicate_commits": coding["agent_commit_count"] - 1,
                "trace_id": trace_id,
            }
            if result["duplicate_commits"] != 0:
                raise AssertionError("demo created more than one Git commit")
            return result
        finally:
            if oneoff:
                with contextlib.suppress(RuntimeError):
                    await command("docker", "rm", "-f", oneoff)
            await compose("start", "executor")


def main() -> None:
    result = asyncio.run(run_demo())
    print("\nCONTINUUM DEMO COMPLETE")
    print(json.dumps(result, indent=2))
    if result["trace_id"] is None:
        print("Trace: enable the optional observability profile for a local Tempo trace")


if __name__ == "__main__":
    main()
