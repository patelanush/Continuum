"""Small explicit tool registry with per-step retry-safety semantics."""

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from typing import Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, Field, ValidationError

from durable_agent_runtime.observability.context import current_traceparent
from durable_agent_runtime.observability.runtime import error, span


class RetrySafety(StrEnum):
    READ_ONLY = "READ_ONLY"
    IDEMPOTENT = "IDEMPOTENT"
    IDEMPOTENCY_KEY_SUPPORTED = "IDEMPOTENCY_KEY_SUPPORTED"
    RECONCILABLE = "RECONCILABLE"
    NON_IDEMPOTENT = "NON_IDEMPOTENT"


TOOL_SAFETY: dict[str, RetrySafety] = {
    "noop": RetrySafety.IDEMPOTENT,
    "slow_noop": RetrySafety.IDEMPOTENT,
    "mock_refund": RetrySafety.IDEMPOTENCY_KEY_SUPPORTED,
    "support_agent": RetrySafety.IDEMPOTENCY_KEY_SUPPORTED,
    "coding_agent": RetrySafety.RECONCILABLE,
}


class PermanentToolError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        super().__init__(detail)


class TransientToolError(Exception):
    """Leave the attempt to expire; recovery may retry a safe operation."""


class SlowNoopInput(BaseModel):
    duration_ms: int = Field(default=0, ge=0, le=120_000)


class MockRefundInput(BaseModel):
    customer_id: str = Field(min_length=1, max_length=200)
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    delay_after_commit_ms: int = Field(default=0, ge=0, le=30_000)
    delay_before_commit_ms: int = Field(default=0, ge=0, le=30_000)
    faultlab_request_timeout_ms: int = Field(default=0, ge=0, le=30_000)
    faultlab_mode: Literal[
        "normal", "fail_before_commit", "error_after_commit", "timeout_before_commit"
    ] = "normal"


@dataclass(frozen=True)
class ExecutionContext:
    workflow_id: UUID
    step_id: UUID
    attempt_id: UUID
    attempt_number: int

    @property
    def operation_id(self) -> str:
        return operation_id(self.step_id)


def operation_id(step_id: UUID) -> str:
    """One logical side effect per step, independent of attempt number or worker."""
    return f"continuum:{step_id}"


def retry_safety(step_type: str) -> RetrySafety:
    try:
        return TOOL_SAFETY[step_type]
    except KeyError as exc:
        raise PermanentToolError("UNSUPPORTED_STEP_TYPE", f"No executor for {step_type}") from exc


def can_retry_after_crash(step_type: str, attempt_number: int, max_attempts: int) -> bool:
    if attempt_number >= max_attempts:
        return False
    try:
        safety = retry_safety(step_type)
    except PermanentToolError:
        return False
    return safety in {
        RetrySafety.IDEMPOTENT,
        RetrySafety.IDEMPOTENCY_KEY_SUPPORTED,
        RetrySafety.RECONCILABLE,
    }


async def execute_tool(
    step_type: str,
    step_input: dict[str, Any],
    context: ExecutionContext,
    *,
    payments_url: str,
) -> dict[str, Any]:
    safety = retry_safety(step_type)
    if safety == RetrySafety.NON_IDEMPOTENT:
        raise PermanentToolError("UNSAFE_RETRY", "Non-idempotent steps cannot be auto-recovered")
    try:
        if step_type == "noop":
            return {}
        if step_type == "slow_noop":
            slow_command = SlowNoopInput.model_validate(step_input)
            await asyncio.sleep(slow_command.duration_ms / 1000)
            return {"duration_ms": slow_command.duration_ms}
        command = MockRefundInput.model_validate(step_input)
    except ValidationError as exc:
        raise PermanentToolError("INVALID_STEP_INPUT", str(exc)) from exc

    request = {
        "customer_id": command.customer_id,
        "amount": str(command.amount),
        "delay_after_commit_ms": command.delay_after_commit_ms,
        "delay_before_commit_ms": command.delay_before_commit_ms,
        "faultlab_mode": command.faultlab_mode,
    }
    try:
        timeout: httpx.Timeout | float = 45
        if command.faultlab_mode == "timeout_before_commit":
            if command.faultlab_request_timeout_ms < 1:
                raise PermanentToolError("INVALID_STEP_INPUT", "FaultLab timeout must be positive")
            timeout = httpx.Timeout(
                connect=5,
                read=command.faultlab_request_timeout_ms / 1000,
                write=5,
                pool=5,
            )
        with span(
            "external.http",
            {
                "http.request.method": "POST",
                "continuum.operation.sha256": sha256(context.operation_id.encode()).hexdigest(),
            },
        ) as active:
            headers = {"Idempotency-Key": context.operation_id}
            traceparent = current_traceparent()
            if traceparent is not None:
                headers["traceparent"] = traceparent
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(
                        f"{payments_url.rstrip('/')}/refunds", headers=headers, json=request
                    )
            except httpx.RequestError:
                error(active, "external_service_error")
                raise
            active.set_attribute("http.response.status_code", response.status_code)
            if response.status_code >= 400:
                error(active, "external_service_error")
        if response.status_code in {408, 429}:
            raise TransientToolError(f"Mock payments returned HTTP {response.status_code}")
        if 400 <= response.status_code < 500:
            raise PermanentToolError(
                "EXTERNAL_REJECTED", f"Mock payments returned HTTP {response.status_code}"
            )
        response.raise_for_status()
        result: dict[str, Any] = response.json()
        return result
    except (httpx.RequestError, httpx.HTTPStatusError) as exc:
        raise TransientToolError(type(exc).__name__) from exc
