import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from durable_agent_runtime.api.errors import install_exception_handlers
from durable_agent_runtime.api.routes.approvals import router as approvals_router
from durable_agent_runtime.api.routes.health import router as health_router
from durable_agent_runtime.api.routes.workflows import router as workflows_router
from durable_agent_runtime.core.config import get_settings
from durable_agent_runtime.core.logging import configure_logging
from durable_agent_runtime.db.session import SessionFactory, engine
from durable_agent_runtime.observability.gauges import sampling_loop
from durable_agent_runtime.observability.runtime import configure


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)
    configure("continuum-api", settings)
    stop = asyncio.Event()
    sampler = (
        asyncio.create_task(sampling_loop(stop, SessionFactory)) if settings.otel_enabled else None
    )
    try:
        yield
    finally:
        stop.set()
        if sampler is not None:
            await sampler
        await engine.dispose()


def create_app() -> FastAPI:
    application = FastAPI(title="Durable Agent Runtime", version="0.1.0", lifespan=lifespan)
    application.include_router(health_router)
    application.include_router(workflows_router)
    application.include_router(approvals_router)
    install_exception_handlers(application)
    FastAPIInstrumentor.instrument_app(application, exclude_spans=["receive", "send"])
    return application


app = create_app()
