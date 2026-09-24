"""Run isolated, API-driven scaling, mixed-agent, and failure-under-load experiments.

The runner never calls workflow services directly. It submits through FastAPI,
waits for durable terminal state, and reads PostgreSQL only for measurements and
independent correctness assertions. ``prepare`` creates a disposable Compose
project; ``clean`` removes only that project's containers and volumes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import subprocess
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from phase8_stats import aggregate_runs, distribution, throughput
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from durable_agent_runtime.db.models import (
    AgentRun,
    AgentToolCall,
    AgentTurn,
    ApprovalRequest,
    CodingWorkspace,
    ConsumedEvent,
    ExecutionAttempt,
    ModelCall,
    OutboxEvent,
    SandboxCommand,
    StateTransition,
    Workflow,
    WorkflowStep,
    WorkspaceCheckpoint,
)
from durable_agent_runtime.domain.enums import ApprovalStatus

PROJECT = "continuum-bench"
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = "postgresql+asyncpg://durable:durable@127.0.0.1:55437/durable"
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}


def now() -> str:
    return datetime.now(UTC).isoformat()


def git_sha() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def git_dirty() -> bool:
    return bool(
        subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
    )


@dataclass(frozen=True)
class BenchSettings:
    api_url: str = "http://127.0.0.1:38000"
    payments_url: str = "http://127.0.0.1:38001"
    database_url: str = DEFAULT_DB
    kafka_bootstrap: str = "127.0.0.1:19094"
    project: str = PROJECT
    timeout_seconds: float = 360

    def __post_init__(self) -> None:
        if self.project != PROJECT:
            raise ValueError("benchmark controls only the continuum-bench project")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout must be positive")


def compose_env(*, telemetry: bool = False) -> dict[str, str]:
    return {
        **os.environ,
        "API_PORT": "38000",
        "MOCK_PAYMENTS_PORT": "38001",
        "POSTGRES_PORT": "55437",
        "PAYMENTS_POSTGRES_PORT": "55438",
        "KAFKA_PORT": "19094",
        "SANDBOX_VOLUME_PREFIX": PROJECT,
        "AGENT_PROVIDER": "fake",
        "EXECUTOR_LEASE_SECONDS": "5",
        "EXECUTOR_HEARTBEAT_SECONDS": "1",
        "EXECUTOR_POLL_INTERVAL_SECONDS": "0.1",
        "RECOVERY_SCAN_INTERVAL_SECONDS": "0.2",
        "OUTBOX_POLL_INTERVAL": "0.1",
        "OTEL_ENABLED": str(telemetry).lower(),
    }


async def command(
    *args: str, env: dict[str, str] | None = None, command_timeout_seconds: int = 300
) -> str:
    process = await asyncio.create_subprocess_exec(
        *args,
        cwd=ROOT,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, error = await asyncio.wait_for(
            process.communicate(), timeout=command_timeout_seconds
        )
    except TimeoutError:
        process.kill()
        await process.communicate()
        raise
    if process.returncode:
        raise RuntimeError(f"{args[:4]} failed: {error.decode(errors='replace')[-2000:]}")
    return output.decode().strip()


async def compose(*args: str, telemetry: bool = False, command_timeout_seconds: int = 300) -> str:
    return await command(
        "docker",
        "compose",
        "-p",
        PROJECT,
        *args,
        env=compose_env(telemetry=telemetry),
        command_timeout_seconds=command_timeout_seconds,
    )


async def prepare(*, executors: int = 1, workers: int = 3) -> None:
    await compose(
        "up",
        "--build",
        "-d",
        "--wait",
        "--scale",
        f"worker={workers}",
        "--scale",
        f"executor={executors}",
        command_timeout_seconds=600,
    )
    await command(
        "uv",
        "run",
        "alembic",
        "upgrade",
        "head",
        env={
            **compose_env(),
            "DATABASE_URL": DEFAULT_DB,
        },
    )


async def scale(executors: int, workers: int) -> None:
    if executors < 1 or workers < 1:
        raise ValueError("at least one executor and event worker are required")
    await compose(
        "up",
        "-d",
        "--wait",
        "--no-build",
        "--scale",
        f"worker={workers}",
        "--scale",
        f"executor={executors}",
    )
    for service, expected in (("executor", executors), ("worker", workers)):
        ids = (await compose("ps", "-q", service)).splitlines()
        if len(ids) != expected:
            raise AssertionError(f"{service}: expected {expected} containers, found {len(ids)}")


async def clean() -> None:
    await compose("down", "--volumes", "--remove-orphans")
    volumes = (
        await command(
            "docker", "volume", "ls", "-q", "--filter", f"label=continuum.project={PROJECT}"
        )
    ).splitlines()
    for volume in volumes:
        if not volume.startswith(f"{PROJECT}-ws-"):
            raise RuntimeError(f"unexpected benchmark workspace volume: {volume}")
        await command("docker", "volume", "rm", "-f", volume)


@dataclass(frozen=True)
class WorkItem:
    workflow_id: UUID
    kind: str
    customer_id: str | None = None


def specification(kind: str, ordinal: int, experiment_id: str) -> tuple[dict[str, Any], str | None]:
    customer = f"bench-{experiment_id}-{ordinal}" if kind in {"refund", "support"} else None
    if kind == "scaling":
        steps = [{"name": "measured-work", "step_type": "slow_noop", "input": {"duration_ms": 250}}]
    elif kind == "normal":
        steps = [
            {"name": "first", "step_type": "noop", "input": {}},
            {"name": "second", "step_type": "noop", "input": {}},
        ]
    elif kind == "support":
        steps = [
            {
                "name": "support",
                "step_type": "support_agent",
                "input": {
                    "customer_id": customer,
                    "amount": "4.99",
                    "request": "Refund a duplicate charge.",
                    "provider": "fake",
                },
            }
        ]
    elif kind == "refund":
        steps = [
            {
                "name": "refund",
                "step_type": "mock_refund",
                "input": {
                    "customer_id": customer,
                    "amount": "4.99",
                    "delay_before_commit_ms": 100,
                },
            }
        ]
    elif kind == "coding":
        steps = [
            {
                "name": "coding",
                "step_type": "coding_agent",
                "input": {
                    "repository": "fixture:discount_service",
                    "task": "Fix the discount bug in TASK.md and run tests.",
                    "test_command": "pytest -q",
                    "provider": "fake",
                },
            }
        ]
    elif kind == "slow":
        steps = [{"name": "slow", "step_type": "slow_noop", "input": {"duration_ms": 5000}}]
    else:
        raise ValueError(f"unknown workload kind: {kind}")
    return {
        "workflow_type": f"benchmark-{kind}",
        "input": {"experiment_id": experiment_id},
        "steps": steps,
    }, customer


def workload_kinds(benchmark_type: str, count: int) -> list[str]:
    if count < 1:
        raise ValueError("workflow count must be positive")
    if benchmark_type == "scaling":
        return ["scaling"] * count
    if benchmark_type == "mixed":
        pattern = ["normal"] * 8 + ["support"] * 5 + ["refund"] * 4 + ["coding"] * 3
    elif benchmark_type == "failure-load":
        pattern = ["slow"] * 10 + ["refund"] * 5 + ["support"] * 5
    else:
        raise ValueError(f"unknown benchmark type: {benchmark_type}")
    return [pattern[index % len(pattern)] for index in range(count)]


async def submit(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    kind: str,
    ordinal: int,
    experiment_id: str,
) -> WorkItem:
    async with semaphore:
        body, customer = specification(kind, ordinal, experiment_id)
        response = await client.post("/api/v1/workflows", json=body)
        response.raise_for_status()
        workflow_id = UUID(response.json()["id"])
        started = await client.post(f"/api/v1/workflows/{workflow_id}/start")
        started.raise_for_status()
        return WorkItem(workflow_id, kind, customer)


async def topic_partitions(bootstrap: str, topic: str) -> set[int]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        details = await admin.describe_topics([topic])
        if not details or details[0]["error_code"]:
            raise AssertionError(f"Kafka topic unavailable: {topic}")
        return {int(partition["partition"]) for partition in details[0]["partitions"]}
    finally:
        await admin.close()


async def dlq_offsets(bootstrap: str) -> tuple[int, int]:
    partitions = await topic_partitions(bootstrap, "continuum.dead-letter.v1")
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap, enable_auto_commit=False, group_id=None
    )
    await consumer.start()
    try:
        positions = await consumer.end_offsets(
            [TopicPartition("continuum.dead-letter.v1", partition) for partition in partitions]
        )
        return len(partitions), sum(positions.values())
    finally:
        with suppress(asyncio.CancelledError):
            await consumer.stop()


async def kafka_partitions(bootstrap: str) -> int:
    return len(await topic_partitions(bootstrap, "continuum.step.ready.v1"))


async def approve_pending(session: AsyncSession, client: httpx.AsyncClient, ids: list[UUID]) -> int:
    rows = (
        (
            await session.execute(
                select(ApprovalRequest.id).where(
                    ApprovalRequest.workflow_id.in_(ids), ApprovalRequest.status == "PENDING"
                )
            )
        )
        .scalars()
        .all()
    )
    for approval_id in rows:
        for retry in range(3):
            try:
                response = await client.post(f"/api/v1/approvals/{approval_id}/approve")
                if response.is_success:
                    break
                if response.status_code != 409:
                    response.raise_for_status()
            except httpx.TransportError:
                if retry == 2:
                    raise
            status = await session.scalar(
                select(ApprovalRequest.status).where(ApprovalRequest.id == approval_id)
            )
            if status == ApprovalStatus.APPROVED:
                break
            if retry == 2:
                raise RuntimeError(f"approval {approval_id} did not complete after retries")
            await asyncio.sleep(0.2)
    return len(rows)


async def wait_terminal(
    sessions: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    ids: list[UUID],
    *,
    timeout_seconds: float,
    approvals: bool,
) -> int:
    deadline = monotonic() + timeout_seconds
    decisions = 0
    while monotonic() < deadline:
        async with sessions() as session:
            if approvals:
                decisions += await approve_pending(session, client, ids)
            rows = (
                await session.execute(
                    select(Workflow.id, Workflow.status).where(Workflow.id.in_(ids))
                )
            ).all()
        if len(rows) == len(ids) and all(row.status.value in TERMINAL for row in rows):
            return decisions
        await asyncio.sleep(0.2)
    raise TimeoutError(
        f"{sum(row.status.value not in TERMINAL for row in rows)} workflows remained active"
    )


def elapsed_ms(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return max(0.0, (end - start).total_seconds() * 1000)


def timings(rows: list[float | None]) -> dict[str, float | int | None]:
    return distribution([value for value in rows if value is not None])


async def durable_result(
    sessions: async_sessionmaker[AsyncSession],
    items: list[WorkItem],
    payments: httpx.AsyncClient,
    *,
    dlq_delta: int,
) -> dict[str, Any]:
    ids = [item.workflow_id for item in items]
    async with sessions() as session:
        workflows = list(
            (await session.scalars(select(Workflow).where(Workflow.id.in_(ids)))).all()
        )
        steps = list(
            (
                await session.scalars(select(WorkflowStep).where(WorkflowStep.workflow_id.in_(ids)))
            ).all()
        )
        attempts = list(
            (
                await session.scalars(
                    select(ExecutionAttempt).where(ExecutionAttempt.workflow_id.in_(ids))
                )
            ).all()
        )
        events = list(
            (
                await session.scalars(select(OutboxEvent).where(OutboxEvent.workflow_id.in_(ids)))
            ).all()
        )
        transitions = list(
            (
                await session.scalars(
                    select(StateTransition).where(StateTransition.workflow_id.in_(ids))
                )
            ).all()
        )
        runs = list(
            (await session.scalars(select(AgentRun).where(AgentRun.workflow_id.in_(ids)))).all()
        )
        run_ids = [run.id for run in runs]
        turns = (
            list(
                (
                    await session.scalars(
                        select(AgentTurn).where(AgentTurn.agent_run_id.in_(run_ids))
                    )
                ).all()
            )
            if run_ids
            else []
        )
        turn_ids = [turn.id for turn in turns]
        model_calls = (
            list(
                (
                    await session.scalars(
                        select(ModelCall).where(ModelCall.agent_turn_id.in_(turn_ids))
                    )
                ).all()
            )
            if turn_ids
            else []
        )
        tool_calls = (
            list(
                (
                    await session.scalars(
                        select(AgentToolCall).where(AgentToolCall.agent_run_id.in_(run_ids))
                    )
                ).all()
            )
            if run_ids
            else []
        )
        workspaces = list(
            (
                await session.scalars(
                    select(CodingWorkspace).where(CodingWorkspace.workflow_id.in_(ids))
                )
            ).all()
        )
        workspace_ids = [workspace.id for workspace in workspaces]
        commands = (
            list(
                (
                    await session.scalars(
                        select(SandboxCommand).where(SandboxCommand.workspace_id.in_(workspace_ids))
                    )
                ).all()
            )
            if workspace_ids
            else []
        )
        checkpoints = (
            list(
                (
                    await session.scalars(
                        select(WorkspaceCheckpoint).where(
                            WorkspaceCheckpoint.workspace_id.in_(workspace_ids)
                        )
                    )
                ).all()
            )
            if workspace_ids
            else []
        )
        approvals = list(
            (
                await session.scalars(
                    select(ApprovalRequest).where(ApprovalRequest.workflow_id.in_(ids))
                )
            ).all()
        )
    by_workflow = {workflow.id: workflow for workflow in workflows}
    if len(by_workflow) != len(items):
        raise AssertionError("benchmark workflows missing from durable state")
    duplicates = Counter(
        (
            transition.entity_type.value,
            transition.entity_id,
            transition.from_status,
            transition.to_status,
        )
        for transition in transitions
    )
    duplicate_transitions = sum(count - 1 for count in duplicates.values() if count > 1)
    refund_count = 0
    duplicate_side_effects = 0
    lost_side_effects = 0
    for item in items:
        if item.customer_id is None:
            continue
        response = await payments.get("/refunds/count", params={"customer_id": item.customer_id})
        response.raise_for_status()
        count = int(response.json()["count"])
        refund_count += count
        duplicate_side_effects += max(0, count - 1)
        lost_side_effects += max(0, 1 - count)
    for workspace in workspaces:
        if not workspace.final_git_head:
            raise AssertionError(f"coding workspace {workspace.id} lacks final commit")
        if (
            sum(
                command.command_type == "git_commit" and command.status.value == "SUCCEEDED"
                for command in commands
                if command.workspace_id == workspace.id
            )
            != 1
        ):
            raise AssertionError("coding workspace has zero or multiple successful commit records")
        if (
            sum(
                checkpoint.reason == "PATCH_APPLIED"
                for checkpoint in checkpoints
                if checkpoint.workspace_id == workspace.id
            )
            != 1
        ):
            raise AssertionError("coding workspace has zero or multiple patch checkpoints")
        if not any(
            command.command_type == "run_tests" and command.exit_code == 0
            for command in commands
            if command.workspace_id == workspace.id
        ):
            raise AssertionError("coding workspace lacks passing tests")
        mount = f"type=volume,src={workspace.volume_name},dst=/workspace,readonly"
        for git_args, expected in (
            (("rev-list", "--count", "HEAD"), "2"),
            (("rev-parse", "HEAD"), workspace.final_git_head),
        ):
            observed = await command(
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--user",
                "10001:10001",
                "--mount",
                mount,
                "--workdir",
                "/workspace",
                "continuum-sandbox:phase6",
                "git",
                *git_args,
            )
            if observed != expected:
                raise AssertionError("workspace Git history differs from durable commit record")
    if any(approval.status.value == "PENDING" for approval in approvals):
        raise AssertionError("benchmark left an approval pending")
    if any(attempt.status.value in {"PENDING", "RUNNING"} for attempt in attempts):
        raise AssertionError("benchmark left an active attempt")
    if any(event.published_at is None for event in events):
        raise AssertionError("benchmark left unpublished outbox events")
    if dlq_delta != 0 or duplicate_transitions or duplicate_side_effects or lost_side_effects:
        raise AssertionError("benchmark correctness invariant failed")
    if any(workflow.status.value != "SUCCEEDED" for workflow in workflows):
        raise AssertionError("one or more benchmark workflows failed")
    if any(step.status.value != "SUCCEEDED" for step in steps):
        raise AssertionError("one or more benchmark steps failed")
    step_by_id = {step.id: step for step in steps}
    outbox_by_step = {event.step_id: event for event in events if event.step_id is not None}
    workflow_latencies = [
        elapsed_ms(workflow.started_at, workflow.completed_at) for workflow in workflows
    ]
    claim_latencies = [elapsed_ms(attempt.created_at, attempt.started_at) for attempt in attempts]
    queue_latencies = [
        elapsed_ms(outbox_by_step[attempt.step_id].created_at, attempt.created_at)
        for attempt in attempts
        if attempt.step_id in outbox_by_step and attempt.attempt_number == 1
    ]
    attempts_by_step_number = {
        (attempt.step_id, attempt.attempt_number): attempt for attempt in attempts
    }
    recovery_latencies = [
        elapsed_ms(
            attempts_by_step_number[(attempt.step_id, attempt.attempt_number - 1)].completed_at,
            attempt.started_at,
        )
        for attempt in attempts
        if attempt.attempt_number > 1
        and (attempt.step_id, attempt.attempt_number - 1) in attempts_by_step_number
    ]
    return {
        "workflow_count": len(items),
        "total_steps": len(steps),
        "successful_workflows": len(workflows),
        "failed_workflows": 0,
        "workflow_type_distribution": dict(Counter(item.kind for item in items)),
        "workflow_latency_ms": timings(workflow_latencies),
        "step_latency_ms": timings(
            [elapsed_ms(step.started_at, step.completed_at) for step in steps]
        ),
        "execution_claim_latency_ms": timings(claim_latencies),
        "queue_wait_time_ms": timings(queue_latencies),
        "outbox_publish_delay_ms": timings(
            [elapsed_ms(event.created_at, event.published_at) for event in events]
        ),
        "attempt_duration_ms": timings(
            [elapsed_ms(attempt.started_at, attempt.completed_at) for attempt in attempts]
        ),
        "recovery_expiry_to_claim_ms": timings(recovery_latencies),
        "recovery_count": sum(attempt.attempt_number > 1 for attempt in attempts),
        "expired_attempts": sum(attempt.status.value == "EXPIRED" for attempt in attempts),
        "attempts": len(attempts),
        "executor_ids": sorted(
            {attempt.executor_id for attempt in attempts if attempt.executor_id}
        ),
        "agent_runs": len(runs),
        "agent_turns": len(turns),
        "model_calls": len(model_calls),
        "tool_calls": len(tool_calls),
        "refund_operations": refund_count,
        "coding_workflows": len(workspaces),
        "sandbox_commands": len(commands),
        "test_executions": sum(command.command_type == "run_tests" for command in commands),
        "approval_decisions": len(approvals),
        "duplicate_side_effect_count": duplicate_side_effects,
        "lost_side_effect_count": lost_side_effects,
        "duplicate_transition_count": duplicate_transitions,
        "unpublished_outbox_events": 0,
        "pending_attempts": 0,
        "running_attempts": 0,
        "pending_approvals": 0,
        "unexpected_dlq_delta": dlq_delta,
        "workflow_ids": [str(item.workflow_id) for item in items],
        "attempt_history": [
            {
                "workflow_id": str(attempt.workflow_id),
                "attempt_number": attempt.attempt_number,
                "status": attempt.status.value,
                "executor_id": attempt.executor_id,
                "created_at": attempt.created_at.isoformat(),
                "completed_at": attempt.completed_at.isoformat() if attempt.completed_at else None,
            }
            for attempt in attempts
            if attempt.attempt_number > 1 or attempt.status.value == "EXPIRED"
        ],
        "step_types": dict(Counter(step.step_type for step in step_by_id.values())),
    }


async def owner_container(executor_id: str) -> str:
    for container in (await compose("ps", "-q", "executor")).splitlines():
        hostname = await command("docker", "inspect", "--format", "{{.Config.Hostname}}", container)
        if executor_id.startswith(hostname):
            return container
    raise AssertionError(f"executor owner {executor_id} has no running container")


async def inject_failure(
    sessions: async_sessionmaker[AsyncSession],
    items: list[WorkItem],
    fault: str,
    target_workflows: int,
) -> dict[str, Any]:
    deadline = monotonic() + 90
    while monotonic() < deadline:
        ids = [item.workflow_id for item in items]
        if not ids:
            await asyncio.sleep(0.1)
            continue
        async with sessions() as session:
            running = list(
                (
                    await session.scalars(
                        select(ExecutionAttempt).where(
                            ExecutionAttempt.workflow_id.in_(ids),
                            ExecutionAttempt.status == "RUNNING",
                        )
                    )
                ).all()
            )
            active = list(
                (
                    await session.scalars(
                        select(Workflow).where(Workflow.id.in_(ids), Workflow.status == "RUNNING")
                    )
                ).all()
            )
            slow = [
                attempt
                for attempt in running
                if attempt.step_id
                in {
                    item.id
                    for item in (
                        await session.scalars(
                            select(WorkflowStep).where(
                                WorkflowStep.workflow_id.in_(ids),
                                WorkflowStep.step_type == "slow_noop",
                            )
                        )
                    ).all()
                }
            ]
            published = len(
                list(
                    (
                        await session.scalars(
                            select(OutboxEvent.id).where(
                                OutboxEvent.workflow_id.in_(ids),
                                OutboxEvent.published_at.is_not(None),
                            )
                        )
                    ).all()
                )
            )
            consumed = len(
                list(
                    (
                        await session.scalars(
                            select(ConsumedEvent.id).where(ConsumedEvent.workflow_id.in_(ids))
                        )
                    ).all()
                )
            )
        minimum_active = min(20, target_workflows // 2)
        ready = (
            len(active) >= minimum_active and slow
            if fault == "executor"
            else len(items) >= minimum_active and published > 0 and active
        )
        if ready:
            break
        await asyncio.sleep(0.1)
    else:
        raise TimeoutError("no active workflow and published event under load; fault not injected")
    if fault == "executor":
        owner = cast(str, slow[0].executor_id)
        container = await owner_container(owner)
        owned = sum(attempt.executor_id == owner for attempt in running)
    elif fault == "worker":
        containers = (await compose("ps", "-q", "worker")).splitlines()
        if not containers:
            raise AssertionError("no event worker container")
        container = containers[0]
        owner = await command("docker", "inspect", "--format", "{{.Config.Hostname}}", container)
        owned = 0
    else:
        raise ValueError("fault must be executor or worker")
    await command("docker", "kill", "--signal=KILL", container)
    await command("docker", "start", container)
    return {
        "fault": f"SIGKILL {fault}",
        "container_id": container[:12],
        "owner": owner,
        "running_attempts_owned_at_kill": owned,
        "active_workflows_at_kill": len(active),
        "submitted_workflows_at_kill": len(items),
        "submissions_in_progress_at_kill": len(items) < target_workflows,
        "published_events_at_kill": published,
        "consumed_events_at_kill": consumed,
        "injected_at": now(),
    }


async def run_batch(
    settings: BenchSettings,
    benchmark_type: str,
    *,
    workflows: int,
    concurrency: int,
    experiment_id: str,
    fault: str | None = None,
) -> dict[str, Any]:
    if concurrency < 1:
        raise ValueError("concurrency must be positive")
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        _, dlq_before = await dlq_offsets(settings.kafka_bootstrap)
        began = monotonic()
        started_at = now()
        async with (
            httpx.AsyncClient(base_url=settings.api_url, timeout=30) as api,
            httpx.AsyncClient(base_url=settings.payments_url, timeout=15) as payments,
        ):
            semaphore = asyncio.Semaphore(concurrency)

            submitted: list[WorkItem] = []

            async def submit_record(kind: str, index: int) -> WorkItem:
                item = await submit(api, semaphore, kind, index, experiment_id)
                submitted.append(item)
                return item

            worker_fault = (
                asyncio.create_task(inject_failure(sessions, submitted, fault, workflows))
                if fault == "worker"
                else None
            )
            items = await asyncio.gather(
                *(
                    submit_record(kind, index)
                    for index, kind in enumerate(workload_kinds(benchmark_type, workflows))
                )
            )
            if worker_fault:
                fault_result = await worker_fault
            elif fault:
                fault_result = await inject_failure(sessions, items, fault, workflows)
            else:
                fault_result = None
            approvals = await wait_terminal(
                sessions,
                api,
                [item.workflow_id for item in items],
                timeout_seconds=settings.timeout_seconds,
                approvals=benchmark_type == "mixed",
            )
            wall = monotonic() - began
            completed_at = now()
            utc_wall = (
                datetime.fromisoformat(completed_at) - datetime.fromisoformat(started_at)
            ).total_seconds()
            if abs(utc_wall - wall) > max(5.0, wall * 0.05):
                raise RuntimeError(
                    "host clock/suspend gap invalidated benchmark timing: "
                    f"UTC={utc_wall:.1f}s monotonic={wall:.1f}s"
                )
            _, dlq_after = await dlq_offsets(settings.kafka_bootstrap)
            result = await durable_result(
                sessions, items, payments, dlq_delta=dlq_after - dlq_before
            )
        result.update(
            {
                "benchmark_type": benchmark_type,
                "experiment_id": experiment_id,
                "started_at": started_at,
                "completed_at": completed_at,
                "client_concurrency": concurrency,
                "wall_clock_duration_seconds": wall,
                "utc_wall_seconds": utc_wall,
                "workflow_throughput_per_second": throughput(result["successful_workflows"], wall),
                "step_throughput_per_second": throughput(result["total_steps"], wall),
                "approval_api_decisions": approvals,
                "fault_injection": fault_result,
            }
        )
        return result
    finally:
        await engine.dispose()


def environment() -> dict[str, Any]:
    return {
        "scope": "local Docker Compose development benchmark",
        "os": platform.system(),
        "architecture": platform.machine(),
        "python": platform.python_version(),
        "cpu_logical_count": os.cpu_count(),
        "docker": subprocess.check_output(["docker", "--version"], text=True).strip(),
        "postgres_image": "postgres:16-alpine",
        "kafka_image": "apache/kafka:3.9.1",
    }


def report_markdown(summary: dict[str, Any]) -> str:
    lines = [
        f"# {summary['benchmark_type']} — {summary['experiment_id']}",
        "",
        f"Revision: `{summary['git_commit']}`. Local Docker Compose only.",
        "",
        f"Runs: {len(summary['runs'])}. Telemetry: {summary['telemetry']}.",
        "",
        "| Run | Workflows | Success | Throughput/s | Median ms | p95 ms | Claim p95 ms |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for index, run in enumerate(summary["runs"], 1):
        lines.append(
            f"| {index} | {run['workflow_count']} | {run['successful_workflows']} | "
            f"{run['workflow_throughput_per_second']:.3f} | "
            f"{run['workflow_latency_ms']['median']:.1f} | "
            f"{run['workflow_latency_ms']['p95']:.1f} | "
            f"{run['execution_claim_latency_ms']['p95']:.1f} |"
        )
    lines += ["", "Results are local measurements, not production capacity or an SLO.", ""]
    return "\n".join(lines)


async def execute(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "prepare":
        await prepare(executors=args.executors, workers=args.workers)
        return {"prepared": True, "project": PROJECT}
    if args.command == "clean":
        await clean()
        return {"cleaned": True, "project": PROJECT}
    if args.command not in {"scaling", "mixed", "failure-load"}:
        raise ValueError("unknown command")
    settings = BenchSettings(timeout_seconds=args.timeout)
    if git_dirty() and not args.allow_dirty:
        raise RuntimeError("benchmark requires a clean Git tree; use --allow-dirty for smoke only")
    await scale(args.executors, args.workers)
    experiment_id = str(uuid4())
    directory = ROOT / "artifacts" / "benchmarks" / experiment_id
    directory.mkdir(parents=True)
    config = {
        "experiment_id": experiment_id,
        "git_commit": git_sha(),
        "git_dirty": git_dirty(),
        "benchmark_type": args.command,
        "executors": args.executors,
        "event_workers": args.workers,
        "workflows_per_run": args.workflows,
        "client_concurrency": args.concurrency,
        "repetitions": args.repetitions,
        "warmup_workflows": args.warmup,
        "telemetry": "off",
        "fault": args.fault if args.command == "failure-load" else None,
        "environment": environment(),
        "kafka_step_ready_partitions": await kafka_partitions(settings.kafka_bootstrap),
        "started_at": now(),
    }
    (directory / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    if args.warmup:
        await run_batch(
            settings,
            "scaling",
            workflows=args.warmup,
            concurrency=args.concurrency,
            experiment_id=f"{experiment_id}-warmup",
        )
    runs = []
    for repetition in range(args.repetitions):
        run = await run_batch(
            settings,
            args.command,
            workflows=args.workflows,
            concurrency=args.concurrency,
            experiment_id=f"{experiment_id}-{repetition}",
            fault=args.fault if args.command == "failure-load" else None,
        )
        run["executor_count"] = args.executors
        run["event_worker_count"] = args.workers
        runs.append(run)
        with (directory / "runs.jsonl").open("a") as stream:
            stream.write(json.dumps(run) + "\n")
        print(
            json.dumps(
                {
                    "run": repetition + 1,
                    "throughput": run["workflow_throughput_per_second"],
                    "latency_p95_ms": run["workflow_latency_ms"]["p95"],
                }
            ),
            flush=True,
        )
    summary = {**config, "completed_at": now(), "runs": runs, "aggregate": aggregate_runs(runs)}
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (directory / "summary.md").write_text(report_markdown(summary))
    return {"artifact_directory": str(directory), "aggregate": summary["aggregate"]}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("prepare", "scaling", "mixed", "failure-load", "clean"):
        sub = commands.add_parser(name)
        if name == "clean":
            continue
        sub.add_argument("--executors", type=int, default=3)
        sub.add_argument("--workers", type=int, default=3)
        if name == "prepare":
            continue
        sub.add_argument("--workflows", type=int, default=120)
        sub.add_argument("--concurrency", type=int, default=25)
        sub.add_argument("--repetitions", type=int, default=1)
        sub.add_argument("--warmup", type=int, default=20)
        sub.add_argument("--timeout", type=float, default=360)
        sub.add_argument(
            "--allow-dirty", action="store_true", help="smoke only; do not publish results"
        )
        if name == "failure-load":
            sub.add_argument("--fault", choices=("executor", "worker"), default="executor")
        else:
            sub.set_defaults(fault=None)
    return result


def main() -> None:
    args = parser().parse_args()
    print(json.dumps(asyncio.run(execute(args)), indent=2))


if __name__ == "__main__":
    main()
