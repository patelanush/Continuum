import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from durable_agent_runtime.domain.errors import (
    DomainError,
    InvalidStateTransition,
    InvariantViolation,
    StepNotFound,
    WorkflowConflict,
    WorkflowNotFound,
)

logger = logging.getLogger(__name__)


def install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(WorkflowNotFound)
    @app.exception_handler(StepNotFound)
    async def not_found_handler(_request: Request, exc: DomainError) -> JSONResponse:
        return _error(status.HTTP_404_NOT_FOUND, "not_found", str(exc))

    @app.exception_handler(WorkflowConflict)
    @app.exception_handler(InvalidStateTransition)
    @app.exception_handler(InvariantViolation)
    async def conflict_handler(_request: Request, exc: DomainError) -> JSONResponse:
        return _error(status.HTTP_409_CONFLICT, "conflict", str(exc))


def _error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code, content={"error": {"code": code, "message": message}}
    )
