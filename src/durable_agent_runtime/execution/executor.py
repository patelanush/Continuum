"""Claim an attempt, execute with no DB transaction, then finalize with fencing."""

import asyncio
import logging
import os
import signal
from uuid import uuid4

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
from durable_agent_runtime.services.execution import ExecutionService, LostLease

logger = logging.getLogger(__name__)


async def execute_attempt(
    attempt: ExecutionAttempt,
    *,
    executor_id: str,
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> str:
    """All tool I/O occurs after the claim session has committed and closed."""
    token = attempt.lease_token
    if token is None:
        raise RuntimeError("A claimed attempt must have a lease token")
    async with sessions() as session:
        step = await session.scalar(select(WorkflowStep).where(WorkflowStep.id == attempt.step_id))
        if step is None:
            raise RuntimeError("Claimed step disappeared")
        step_type, step_input = step.step_type, step.input
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
                logger.exception(
                    "process_type=executor operation=heartbeat_error attempt_id=%s executor_id=%s",
                    attempt.id,
                    executor_id,
                )
                # A temporary database interruption is not proof ownership was lost.
                continue
            if not owned:
                lost_lease.set()
                return

    heartbeat_task = asyncio.create_task(beat())
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
                await service.finalize_failure(
                    attempt.id,
                    executor_id=executor_id,
                    lease_token=token,
                    error_code=failure.code,
                    error_detail=str(failure),
                )
                return "failed"
            await service.finalize_success(
                attempt.id,
                executor_id=executor_id,
                lease_token=token,
                output=outcome or {},
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
