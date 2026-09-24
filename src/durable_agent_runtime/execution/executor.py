"""Claim an attempt, execute with no DB transaction, then finalize with fencing."""

import asyncio
import logging
import os
import signal
from time import monotonic
from typing import Any
from uuid import uuid4

from opentelemetry import context as otel_context
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from durable_agent_runtime.core.config import Settings, get_settings
from durable_agent_runtime.core.logging import configure_logging
from durable_agent_runtime.db.models import ExecutionAttempt, WorkflowStep
from durable_agent_runtime.db.session import SessionFactory, engine
from durable_agent_runtime.execution.tools import (
    ExecutionContext,
    PermanentToolError,
    TransientToolError,
    execute_tool,
)
from durable_agent_runtime.observability.context import extract_traceparent
from durable_agent_runtime.observability.metrics import count, duration
from durable_agent_runtime.observability.runtime import configure, error, span
from durable_agent_runtime.services.execution import ExecutionService, LostLease

logger = logging.getLogger(__name__)


async def execute_attempt(
    attempt: ExecutionAttempt,
    *,
    executor_id: str,
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> str:
    async with sessions() as session:
        step = await session.scalar(select(WorkflowStep).where(WorkflowStep.id == attempt.step_id))
        if step is None:
            raise RuntimeError("Claimed step disappeared")
        step_type, step_input = step.step_type, step.input
    attributes = {
        "continuum.workflow.id": str(attempt.workflow_id),
        "continuum.step.id": str(attempt.step_id),
        "continuum.execution_attempt.id": str(attempt.id),
        "continuum.execution_attempt.number": attempt.attempt_number,
        "continuum.executor.id": executor_id,
        "continuum.recovered": attempt.attempt_number > 1,
        "continuum.step.type": step_type,
    }
    attached = otel_context.attach(extract_traceparent(attempt.traceparent))
    try:
        with span("execution.run", attributes):
            pass
        result = await _execute_attempt(
            attempt,
            step_type=step_type,
            step_input=step_input,
            executor_id=executor_id,
            sessions=sessions,
            settings=settings,
        )
        with span("execution.result", attributes) as active:
            active.set_attribute("continuum.attempt.status", result)
            active.set_attribute("continuum.lease_lost", result == "lost_lease")
            if result == "failed":
                error(active, "execution_failed")
        return result
    finally:
        otel_context.detach(attached)


async def _execute_attempt(
    attempt: ExecutionAttempt,
    *,
    step_type: str,
    step_input: dict[str, Any],
    executor_id: str,
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> str:
    """All tool I/O occurs after the claim session has committed and closed."""
    token = attempt.lease_token
    if token is None:
        raise RuntimeError("A claimed attempt must have a lease token")
    began = monotonic()
    if attempt.started_at is not None:
        duration(
            "continuum_execution_claim_latency_seconds",
            (attempt.started_at - attempt.created_at).total_seconds(),
            step_type=step_type,
        )
    context = ExecutionContext(
        workflow_id=attempt.workflow_id,
        step_id=attempt.step_id,
        attempt_id=attempt.id,
        attempt_number=attempt.attempt_number,
    )
    heartbeat_stop = asyncio.Event()
    lost_lease = asyncio.Event()

    async def beat() -> None:
        while not heartbeat_stop.is_set():
            try:
                await asyncio.wait_for(
                    heartbeat_stop.wait(), timeout=settings.executor_heartbeat_seconds
                )
            except TimeoutError:
                pass
            if heartbeat_stop.is_set():
                return
            try:
                async with sessions() as session:
                    owned = await ExecutionService(session).heartbeat(
                        attempt.id,
                        executor_id=executor_id,
                        lease_token=token,
                        lease_seconds=settings.executor_lease_seconds,
                    )
            except Exception:
                count("continuum_heartbeats", result="error")
                logger.exception(
                    "process_type=executor operation=heartbeat_error attempt_id=%s executor_id=%s",
                    attempt.id,
                    executor_id,
                )
                # A temporary database interruption is not proof ownership was lost.
                continue
            if not owned:
                count("continuum_heartbeats", result="lost_lease")
                lost_lease.set()
                return
            count("continuum_heartbeats", result="ok")

    heartbeat_task = asyncio.create_task(beat())
    if step_type == "coding_agent":
        from durable_agent_runtime.coding.runner import run_coding_agent

        tool_task = asyncio.create_task(
            run_coding_agent(
                step_input,
                context,
                executor_id=executor_id,
                lease_token=token,
                sessions=sessions,
                settings=settings,
            )
        )
    elif step_type == "support_agent":
        from durable_agent_runtime.agent.runner import run_support_agent

        tool_task = asyncio.create_task(
            run_support_agent(
                step_input,
                context,
                executor_id=executor_id,
                lease_token=token,
                sessions=sessions,
                settings=settings,
            )
        )
    else:
        tool_task = asyncio.create_task(
            execute_tool(step_type, step_input, context, payments_url=settings.mock_payments_url)
        )
    lease_task = asyncio.create_task(lost_lease.wait())
    outcome: dict[str, object] | None = None
    failure: PermanentToolError | None = None
    transient = False
    try:
        done, _pending = await asyncio.wait(
            {tool_task, lease_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if lease_task in done and lost_lease.is_set():
            tool_task.cancel()
            await asyncio.gather(tool_task, return_exceptions=True)
            return "lost_lease"
        try:
            outcome = await tool_task
        except PermanentToolError as exc:
            failure = exc
        except LostLease:
            return "lost_lease"
        except TransientToolError:
            transient = True
            logger.warning(
                "process_type=executor operation=transient_tool_failure attempt_id=%s "
                "workflow_id=%s step_id=%s",
                attempt.id,
                attempt.workflow_id,
                attempt.step_id,
            )
        except Exception:
            transient = True
            logger.exception(
                "process_type=executor operation=unexpected_tool_failure attempt_id=%s", attempt.id
            )
    finally:
        if not tool_task.done():
            tool_task.cancel()
            await asyncio.gather(tool_task, return_exceptions=True)
        lease_task.cancel()
        heartbeat_stop.set()
        await heartbeat_task
        await asyncio.gather(lease_task, return_exceptions=True)
    if transient:
        return "transient"
    if lost_lease.is_set():
        return "lost_lease"
    try:
        async with sessions() as session:
            service = ExecutionService(session)
            if failure:
                with span("execution.finalize", {"continuum.attempt.status": "failed"}):
                    await service.finalize_failure(
                        attempt.id,
                        executor_id=executor_id,
                        lease_token=token,
                        error_code=failure.code,
                        error_detail=str(failure),
                    )
                count("continuum_execution_attempts", status="failed", step_type=step_type)
                duration(
                    "continuum_execution_attempt_duration_seconds",
                    monotonic() - began,
                    step_type=step_type,
                    status="failed",
                )
                return "failed"
            with span("execution.finalize", {"continuum.attempt.status": "succeeded"}):
                await service.finalize_success(
                    attempt.id,
                    executor_id=executor_id,
                    lease_token=token,
                    output=outcome or {},
                )
            count("continuum_execution_attempts", status="succeeded", step_type=step_type)
            duration(
                "continuum_execution_attempt_duration_seconds",
                monotonic() - began,
                step_type=step_type,
                status="succeeded",
            )
            return "succeeded"
    except LostLease:
        logger.warning(
            "process_type=executor operation=lost_lease attempt_id=%s executor_id=%s",
            attempt.id,
            executor_id,
        )
        return "lost_lease"


async def executor_loop(
    stop: asyncio.Event,
    sessions: async_sessionmaker[AsyncSession],
    *,
    executor_id: str,
    settings: Settings,
) -> None:
    while not stop.is_set():
        try:
            async with sessions() as session:
                attempt = await ExecutionService(session).claim_next(
                    executor_id=executor_id, lease_seconds=settings.executor_lease_seconds
                )
            if attempt is not None:
                if (
                    settings.app_env == "faultlab"
                    and os.getenv("FAULTLAB_PAUSE_AFTER_CLAIM") == "1"
                ):
                    logger.warning(
                        "process_type=executor operation=faultlab_paused_after_claim attempt_id=%s",
                        attempt.id,
                    )
                    await asyncio.Event().wait()
                await execute_attempt(
                    attempt, executor_id=executor_id, sessions=sessions, settings=settings
                )
        except Exception:
            logger.exception(
                "process_type=executor operation=loop_error executor_id=%s", executor_id
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.executor_poll_interval_seconds)
        except TimeoutError:
            pass


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    configure("continuum-executor", settings)
    executor_id = f"{os.getenv('HOSTNAME', 'executor')}-{uuid4().hex[:8]}"
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    logger.info("process_type=executor operation=started executor_id=%s", executor_id)
    loop_task = asyncio.create_task(
        executor_loop(stop, SessionFactory, executor_id=executor_id, settings=settings)
    )
    stop_task = asyncio.create_task(stop.wait())
    try:
        done, _pending = await asyncio.wait(
            {loop_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if loop_task in done:
            loop_task.result()
        else:
            try:
                await asyncio.wait_for(loop_task, timeout=settings.executor_drain_seconds)
            except TimeoutError:
                logger.warning(
                    "process_type=executor operation=drain_timeout executor_id=%s", executor_id
                )
                loop_task.cancel()
                await asyncio.gather(loop_task, return_exceptions=True)
    finally:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run())
