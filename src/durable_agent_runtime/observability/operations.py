"""Safe agent/model/tool observations around actual external work."""

from collections.abc import Iterator
from contextlib import contextmanager
from time import monotonic
from uuid import UUID

from opentelemetry.trace import Span

from durable_agent_runtime.observability.metrics import count, duration
from durable_agent_runtime.observability.runtime import error, span


def error_class(exc: BaseException) -> str:
    name = type(exc).__name__
    if isinstance(exc, TimeoutError) or "Timeout" in name:
        return "timeout"
    if name in {"ValidationError", "ValueError", "KeyError", "JSONDecodeError"}:
        return "invalid_model_output"
    if name in {"ProviderUnavailable", "ProviderConfigurationError"}:
        return "provider_error"
    if name == "SandboxUnavailable":
        return "sandbox_unavailable"
    return "other"


@contextmanager
def model_call(call_id: UUID, provider: str, model: str, attempt_number: int) -> Iterator[Span]:
    began = monotonic()
    with span(
        "model.call",
        {
            "continuum.model_call.id": str(call_id),
            "continuum.execution_attempt.number": attempt_number,
            "gen_ai.system": provider,
            "gen_ai.provider.name": provider,
            "gen_ai.request.model": model,
        },
    ) as active:
        try:
            yield active
        except Exception as exc:
            error(active, error_class(exc))
            count("continuum_model_calls", provider=provider, model=model, status="failed")
            duration(
                "continuum_model_call_duration_seconds",
                monotonic() - began,
                provider=provider,
                model=model,
                status="failed",
            )
            raise
        else:
            count("continuum_model_calls", provider=provider, model=model, status="succeeded")
            duration(
                "continuum_model_call_duration_seconds",
                monotonic() - began,
                provider=provider,
                model=model,
                status="succeeded",
            )


def model_tokens(
    active: Span, provider: str, model: str, input_tokens: int | None, output_tokens: int | None
) -> None:
    if input_tokens is not None and input_tokens >= 0:
        active.set_attribute("gen_ai.usage.input_tokens", input_tokens)
        count(
            "continuum_model_tokens",
            input_tokens,
            provider=provider,
            model=model,
            direction="input",
        )
    if output_tokens is not None and output_tokens >= 0:
        active.set_attribute("gen_ai.usage.output_tokens", output_tokens)
        count(
            "continuum_model_tokens",
            output_tokens,
            provider=provider,
            model=model,
            direction="output",
        )


@contextmanager
def tool_call(tool_id: UUID, name: str, semantics: str) -> Iterator[Span]:
    began = monotonic()
    with span(
        "tool.call",
        {
            "continuum.agent_tool_call.id": str(tool_id),
            "continuum.tool.name": name,
            "continuum.tool.semantics": semantics,
        },
    ) as active:
        try:
            yield active
        except Exception as exc:
            error(active, error_class(exc))
            count("continuum_tool_calls", tool_name=name, status="failed")
            duration(
                "continuum_tool_call_duration_seconds",
                monotonic() - began,
                tool_name=name,
                status="failed",
            )
            raise
        else:
            count("continuum_tool_calls", tool_name=name, status="succeeded")
            duration(
                "continuum_tool_call_duration_seconds",
                monotonic() - began,
                tool_name=name,
                status="succeeded",
            )
