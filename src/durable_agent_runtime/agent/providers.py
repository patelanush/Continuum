"""Deterministic test provider and real local Ollama structured-output provider."""

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from durable_agent_runtime.agent.decisions import DECISION_ADAPTER


class ProviderUnavailable(Exception):
    pass


class ProviderConfigurationError(Exception):
    pass


@dataclass(frozen=True)
class ModelResult:
    raw: dict[str, Any]
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


class ModelProvider(Protocol):
    async def generate(
        self, request: dict[str, Any], *, turn_number: int, attempt_number: int
    ) -> ModelResult: ...


class FakeModelProvider:
    """Script index is durable model-call attempt number, not process memory."""

    def __init__(self, script: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self.script = script or {}
        self.invocations: list[tuple[int, int]] = []

    async def generate(
        self, request: dict[str, Any], *, turn_number: int, attempt_number: int
    ) -> ModelResult:
        self.invocations.append((turn_number, attempt_number))
        entries = self.script.get(str(turn_number))
        if entries:
            action = entries[min(attempt_number - 1, len(entries) - 1)]
        elif turn_number == 1:
            action = {"type": "tool_call", "tool_name": "read_refund_policy", "arguments": {}}
        elif turn_number == 2:
            action = {
                "type": "tool_call",
                "tool_name": "refund_customer",
                "arguments": {"customer_id": request["customer_id"], "amount": request["amount"]},
            }
        else:
            action = {"type": "final", "response": "The duplicate charge has been refunded."}
        if action.get("fake_error") == "timeout":
            raise TimeoutError("scripted model timeout")
        if action.get("fake_error") == "unavailable":
            raise ProviderUnavailable("scripted provider outage")
        if action.get("fake_error") == "malformed":
            return ModelResult(raw={"invalid": "model output"})
        if action.get("fake_delay_ms"):
            await asyncio.sleep(int(action["fake_delay_ms"]) / 1000)
        return ModelResult(
            raw={key: value for key, value in action.items() if key != "fake_delay_ms"}
        )


class OllamaModelProvider:
    def __init__(self, base_url: str, model: str, timeout_seconds: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds

    async def generate(
        self, request: dict[str, Any], *, turn_number: int, attempt_number: int
    ) -> ModelResult:
        del turn_number, attempt_number
        body = {
            "model": self.model,
            "messages": request["messages"],
            "format": DECISION_ADAPTER.json_schema(),
            "stream": False,
            "options": {"temperature": 0},
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(f"{self.base_url}/api/chat", json=body)
            if response.status_code in {400, 404}:
                raise ProviderConfigurationError(
                    f"Ollama rejected model/request with HTTP {response.status_code}"
                )
            response.raise_for_status()
            payload: dict[str, Any] = response.json()
            return parse_ollama_payload(payload)
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            raise ProviderUnavailable(type(exc).__name__) from exc


def parse_ollama_payload(payload: dict[str, Any]) -> ModelResult:
    content = payload["message"]["content"]
    raw: dict[str, Any] = json.loads(content)
    return ModelResult(
        raw=raw,
        prompt_tokens=payload.get("prompt_eval_count"),
        completion_tokens=payload.get("eval_count"),
    )
