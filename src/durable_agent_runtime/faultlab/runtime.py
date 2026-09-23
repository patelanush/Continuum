"""Isolated Compose control and read-only durable-state probes for experiments."""

import asyncio
import os
import subprocess
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from durable_agent_runtime.db.models import (
    ConsumedEvent,
    ExecutionAttempt,
    OutboxEvent,
    StateTransition,
    Workflow,
    WorkflowStep,
)
from durable_agent_runtime.domain.enums import ExecutionAttemptStatus

PROJECT = "continuum-faultlab"
REPO_ROOT = Path(__file__).resolve().parents[3]


class DockerController:
    """Only operates on FaultLab's exact Compose project and its containers."""

    def __init__(self, *, project: str = PROJECT) -> None:
        if project != PROJECT:
            raise ValueError("FaultLab only controls the isolated continuum-faultlab project")
        self.project = project
        self.environment = {
            **os.environ,
            "APP_ENV": "faultlab",
            "API_PORT": "28000",
            "MOCK_PAYMENTS_PORT": "18001",
            "POSTGRES_PORT": "55435",
            "PAYMENTS_POSTGRES_PORT": "55436",
            "KAFKA_PORT": "19093",
            "EXECUTOR_LEASE_SECONDS": "5",
            "EXECUTOR_HEARTBEAT_SECONDS": "1",
            "EXECUTOR_POLL_INTERVAL_SECONDS": "0.1",
            "RECOVERY_SCAN_INTERVAL_SECONDS": "0.2",
            "OUTBOX_POLL_INTERVAL": "0.1",
            "OUTBOX_PUBLISH_LEASE_SECONDS": "11",
            "AGENT_PROVIDER": "fake",
            "MODEL_TIMEOUT_SECONDS": "5",
            "MODEL_MAX_ATTEMPTS": "3",
            "AGENT_MAX_TURNS": "8",
            "CODING_AGENT_MAX_TURNS": "12",
            "SANDBOX_VOLUME_PREFIX": self.project,
            "CODING_COMMAND_TIMEOUT_SECONDS": "10",
            "FAULTLAB_DISPATCHER_PAUSE_AFTER_ACK": "0",
            "FAULTLAB_PAUSE_AFTER_CLAIM": "0",
        }

    async def command(self, *args: str, env: dict[str, str] | None = None) -> str:
        process = await asyncio.create_subprocess_exec(
            *args,
            cwd=REPO_ROOT,
            env={**self.environment, **(env or {})},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            output, errors = await asyncio.wait_for(process.communicate(), timeout=180)
        except TimeoutError as exc:
            process.kill()
            await process.communicate()
            raise RuntimeError(f"Command {args[:4]} exceeded 180 seconds") from exc
        if process.returncode != 0:
            raise RuntimeError(
                f"Command {args[:4]} failed ({process.returncode}): "
                f"{errors.decode(errors='replace')[-2000:]}"
            )
        return output.decode().strip()

    async def compose(self, *args: str, env: dict[str, str] | None = None) -> str:
        return await self.command("docker", "compose", "-p", self.project, *args, env=env)

    async def up(self, *, executors: int = 3) -> None:
        await self.compose(
            "up",
            "--build",
            "-d",
            "--wait",
            "--scale",
            "worker=3",
            "--scale",
            f"executor={executors}",
        )

    async def clean(self) -> None:
        label = f"label=continuum.project={self.project}"
        containers = (await self.command("docker", "ps", "-aq", "--filter", label)).splitlines()
        for container_id in containers:
            await self.command("docker", "rm", "-f", container_id)
        volumes = (
            await self.command("docker", "volume", "ls", "-q", "--filter", label)
        ).splitlines()
        for volume in volumes:
            if not volume.startswith(f"{self.project}-ws-"):
                raise RuntimeError("Refusing to remove an unexpected FaultLab volume")
            await self.command("docker", "volume", "rm", volume)
        await self.compose("down", "--volumes", "--remove-orphans")

    async def service_containers(self, service: str, *, include_stopped: bool = False) -> list[str]:
        arguments = ("ps", "-a", "-q", service) if include_stopped else ("ps", "-q", service)
        return (await self.compose(*arguments)).splitlines()

    async def owner_container(self, executor_id: str) -> str:
        for container_id in await self.service_containers("executor"):
            hostname = await self.command(
                "docker", "inspect", "--format", "{{.Config.Hostname}}", container_id
            )
            if executor_id.startswith(hostname):
                return container_id
        raise RuntimeError(f"No FaultLab executor owns {executor_id}")

    async def kill_owner(self, executor_id: str) -> str:
        container_id = await self.owner_container(executor_id)
        await self.command("docker", "kill", "--signal=KILL", container_id)
        return container_id

    async def restart_container(self, container_id: str) -> None:
        await self.command("docker", "start", container_id)

    async def run_oneoff(self, service: str, *, variables: dict[str, str]) -> str:
        arguments: list[str] = ["run", "-d", "--no-deps"]
        for name, value in variables.items():
            if not name.startswith("FAULTLAB_"):
                raise ValueError("Only FaultLab hooks can be set on one-off containers")
            arguments.extend(["-e", f"{name}={value}"])
        arguments.append(service)
        return await self.compose(*arguments)

    async def remove_oneoff(self, container_id: str) -> None:
        await self.command("docker", "rm", "-f", container_id)


async def wait_for[T](
    probe: Callable[[], Awaitable[T | None]], *, wait_timeout: float = 40, interval: float = 0.1
) -> T:
    deadline = asyncio.get_running_loop().time() + wait_timeout
    while asyncio.get_running_loop().time() < deadline:
        value = await probe()
        if value is not None:
            return value
        await asyncio.sleep(interval)
    raise TimeoutError(f"Condition not met within {wait_timeout} seconds")


class FaultLabRuntime:
    def __init__(self, docker: DockerController) -> None:
        self.docker = docker
        self.api = httpx.AsyncClient(base_url="http://127.0.0.1:28000", timeout=10)
        self.payments = httpx.AsyncClient(base_url="http://127.0.0.1:18001", timeout=10)
        self.engine = create_async_engine(
            "postgresql+asyncpg://durable:durable@127.0.0.1:55435/durable",
            pool_pre_ping=True,
        )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        self.producer = AIOKafkaProducer(
            bootstrap_servers="127.0.0.1:19093", acks="all", enable_idempotence=True
        )

    async def __aenter__(self) -> "FaultLabRuntime":
        await self.producer.start()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.producer.stop()
        await self.api.aclose()
        await self.payments.aclose()
        await self.engine.dispose()

    async def create_start(self, steps: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
        created = await self.api.post(
            "/api/v1/workflows", json={"workflow_type": f"faultlab-{label}", "steps": steps}
        )
        created.raise_for_status()
        workflow = cast(dict[str, Any], created.json())
        started = await self.api.post(f"/api/v1/workflows/{workflow['id']}/start")
        started.raise_for_status()
        return workflow

    async def workflow(self, workflow_id: UUID) -> dict[str, Any]:
        response = await self.api.get(f"/api/v1/workflows/{workflow_id}")
        response.raise_for_status()
        return cast(dict[str, Any], response.json())

    async def attempts(self, workflow_id: UUID) -> list[dict[str, Any]]:
        response = await self.api.get(f"/api/v1/workflows/{workflow_id}/attempts")
        response.raise_for_status()
        return cast(list[dict[str, Any]], response.json())

    async def wait_attempt(
        self, workflow_id: UUID, *, status: str = "RUNNING", number: int = 1
    ) -> dict[str, Any]:
        async def probe() -> dict[str, Any] | None:
            return next(
                (
                    item
                    for item in await self.attempts(workflow_id)
                    if item["status"] == status and item["attempt_number"] == number
                ),
                None,
            )

        return await wait_for(probe)

    async def assert_active_lease(self, attempt_id: UUID) -> None:
        """Verify the fault is injected before PostgreSQL considers ownership expired."""
        async with self.sessions() as session:
            live = await session.scalar(
                select(ExecutionAttempt.id).where(
                    ExecutionAttempt.id == attempt_id,
                    ExecutionAttempt.status == ExecutionAttemptStatus.RUNNING,
                    ExecutionAttempt.lease_expires_at > func.clock_timestamp(),
                )
            )
        if live is None:
            raise AssertionError(f"Fault injection missed active lease for {attempt_id}")

    async def wait_terminal(self, workflow_id: UUID, *, wait_timeout: float = 40) -> dict[str, Any]:
        async def probe() -> dict[str, Any] | None:
            current = await self.workflow(workflow_id)
            return current if current["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"} else None

        return await wait_for(probe, wait_timeout=wait_timeout)

    async def refund(self, key: str) -> dict[str, Any] | None:
        response = await self._payments_get(f"/refunds/by-idempotency-key/{key}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return cast(dict[str, Any], response.json())

    async def wait_refund(self, key: str) -> dict[str, Any]:
        return await wait_for(lambda: self.refund(key))

    async def refund_count(self, customer_id: str) -> int:
        response = await self._payments_get("/refunds/count", params={"customer_id": customer_id})
        response.raise_for_status()
        return int(response.json()["count"])

    async def _payments_get(
        self, path: str, *, params: dict[str, str] | None = None
    ) -> httpx.Response:
        # A post-crash observation can hit a just-closed HTTP keep-alive connection.
        # Retry only this read-only probe, not the effect or its correctness assertion.
        for attempt in range(3):
            try:
                return await self.payments.get(path, params=params)
            except httpx.TransportError:
                if attempt == 2:
                    raise
                await asyncio.sleep(0.1)
        raise AssertionError("Unreachable read-only probe retry state")

    async def outbox(self, workflow_id: UUID) -> list[OutboxEvent]:
        async with self.sessions() as session:
            return list(
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.workflow_id == workflow_id)
                    .order_by(OutboxEvent.created_at, OutboxEvent.id)
                )
            )

    async def wait_committed_offset(
        self, topic: str, partition: int, offset: int, *, group: str = "continuum-workers-v1"
    ) -> None:
        observer = AIOKafkaConsumer(
            bootstrap_servers="127.0.0.1:19093", group_id=group, enable_auto_commit=False
        )
        await observer.start()
        try:
            target = TopicPartition(topic, partition)

            async def probe() -> bool | None:
                committed = await observer.committed(target)
                return True if committed is not None and committed > offset else None

            await wait_for(probe)
        finally:
            await observer.stop()

    async def snapshot(self, workflow_id: UUID) -> dict[str, Any]:
        async with self.sessions() as session:
            workflow = await session.get(Workflow, workflow_id)
            if workflow is None:
                raise AssertionError(f"Workflow {workflow_id} disappeared")
            steps = list(
                await session.scalars(
                    select(WorkflowStep)
                    .where(WorkflowStep.workflow_id == workflow_id)
                    .order_by(WorkflowStep.position)
                )
            )
            attempts = list(
                await session.scalars(
                    select(ExecutionAttempt)
                    .where(ExecutionAttempt.workflow_id == workflow_id)
                    .order_by(ExecutionAttempt.step_id, ExecutionAttempt.attempt_number)
                )
            )
            outbox = list(
                await session.scalars(
                    select(OutboxEvent).where(OutboxEvent.workflow_id == workflow_id)
                )
            )
            transitions = list(
                await session.scalars(
                    select(StateTransition).where(StateTransition.workflow_id == workflow_id)
                )
            )
            consumed = int(
                await session.scalar(
                    select(func.count())
                    .select_from(ConsumedEvent)
                    .where(ConsumedEvent.workflow_id == workflow_id)
                )
                or 0
            )
            return {
                "workflow": workflow,
                "steps": steps,
                "attempts": attempts,
                "outbox": outbox,
                "transitions": transitions,
                "consumed_count": consumed,
            }


def git_state() -> tuple[str, bool]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    )
    return commit, dirty
