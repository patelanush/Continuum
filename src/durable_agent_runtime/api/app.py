from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from durable_agent_runtime.api.errors import install_exception_handlers
from durable_agent_runtime.api.routes.approvals import router as approvals_router
from durable_agent_runtime.api.routes.health import router as health_router
from durable_agent_runtime.api.routes.workflows import router as workflows_router
from durable_agent_runtime.core.config import get_settings
from durable_agent_runtime.core.logging import configure_logging
from durable_agent_runtime.db.session import engine


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    configure_logging(get_settings().log_level)
    yield
    await engine.dispose()


def create_app() -> FastAPI:
    application = FastAPI(title="Durable Agent Runtime", version="0.1.0", lifespan=lifespan)
    application.include_router(health_router)
    application.include_router(workflows_router)
    application.include_router(approvals_router)
    install_exception_handlers(application)
    return application


app = create_app()
