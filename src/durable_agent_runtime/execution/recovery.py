"""Database-time lease recovery, safe with multiple schedulers."""

import asyncio
import logging
import signal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from durable_agent_runtime.core.config import get_settings
from durable_agent_runtime.core.logging import configure_logging
from durable_agent_runtime.db.session import SessionFactory, engine
from durable_agent_runtime.observability.runtime import configure
from durable_agent_runtime.services.execution import ExecutionService

logger = logging.getLogger(__name__)


async def recovery_loop(
    stop: asyncio.Event,
    sessions: async_sessionmaker[AsyncSession],
    *,
    poll_interval: float,
) -> None:
    while not stop.is_set():
        try:
            async with sessions() as session:
                count = await ExecutionService(session).recover_expired()
        except Exception:
            logger.exception("process_type=recovery operation=scan_failed")
            count = 0
        if count < 20:
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)
            except TimeoutError:
                pass


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    configure("continuum-recovery", settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await recovery_loop(
            stop, SessionFactory, poll_interval=settings.recovery_scan_interval_seconds
        )
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run())
