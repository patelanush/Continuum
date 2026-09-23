from uuid import uuid4

import pytest
from pydantic import ValidationError

from durable_agent_runtime.agent.decisions import (
    DECISION_ADAPTER,
    agent_operation_id,
    canonical_hash,
    validate_tool_arguments,
)
from durable_agent_runtime.agent.providers import (
    FakeModelProvider,
    ProviderUnavailable,
    parse_ollama_payload,
)
from durable_agent_runtime.domain.enums import (
    AgentRunStatus,
    AgentToolCallStatus,
    AgentTurnStatus,
    ModelCallStatus,
)
from durable_agent_runtime.domain.errors import InvalidStateTransition
from durable_agent_runtime.domain.state_machine import (
    validate_agent_run_transition,
    validate_agent_tool_call_transition,
    validate_agent_turn_transition,
    validate_model_call_transition,
)


def test_agent_state_machines_reject_self_and_terminal_transitions() -> None:
    validate_agent_run_transition(AgentRunStatus.PENDING, AgentRunStatus.RUNNING)
    validate_agent_run_transition(AgentRunStatus.RUNNING, AgentRunStatus.SUCCEEDED)
    validate_agent_turn_transition(AgentTurnStatus.PENDING_MODEL, AgentTurnStatus.TOOL_PENDING)
    validate_agent_turn_transition(AgentTurnStatus.TOOL_PENDING, AgentTurnStatus.COMPLETED)
    validate_model_call_transition(ModelCallStatus.RUNNING, ModelCallStatus.FAILED)
    validate_agent_tool_call_transition(AgentToolCallStatus.PENDING, AgentToolCallStatus.SUCCEEDED)
    with pytest.raises(InvalidStateTransition):
        validate_agent_run_transition(AgentRunStatus.SUCCEEDED, AgentRunStatus.SUCCEEDED)
    with pytest.raises(InvalidStateTransition):
        validate_agent_turn_transition(AgentTurnStatus.COMPLETED, AgentTurnStatus.COMPLETED)
    with pytest.raises(InvalidStateTransition):
        validate_model_call_transition(ModelCallStatus.FAILED, ModelCallStatus.FAILED)
    with pytest.raises(InvalidStateTransition):
        validate_agent_tool_call_transition(
            AgentToolCallStatus.SUCCEEDED, AgentToolCallStatus.SUCCEEDED
        )


def test_decision_schema_and_allowlisted_tool_arguments() -> None:
    decision = DECISION_ADAPTER.validate_python(
        {
            "type": "tool_call",
            "tool_name": "refund_customer",
            "arguments": {"customer_id": "c", "amount": "49.99"},
        }
    )
    assert decision.type == "tool_call"
    assert validate_tool_arguments("refund_customer", decision.arguments) == {
        "customer_id": "c",
        "amount": "49.99",
    }
    with pytest.raises(ValueError, match="Unauthorized"):
        validate_tool_arguments("shell", {})
    with pytest.raises(ValidationError):
        validate_tool_arguments("refund_customer", {"customer_id": "c", "amount": "-1"})
    with pytest.raises(ValidationError):
        DECISION_ADAPTER.validate_python({"type": "unknown", "response": "x"})


def test_hash_and_operation_identity_are_stable() -> None:
    assert canonical_hash({"a": 1, "b": 2}) == canonical_hash({"b": 2, "a": 1})
    assert canonical_hash({"a": 1}) != canonical_hash({"a": 2})
    tool_id = uuid4()
    assert agent_operation_id(tool_id) == agent_operation_id(tool_id)
    assert agent_operation_id(tool_id) != agent_operation_id(uuid4())


async def test_fake_provider_scripts_attempts_without_randomness() -> None:
    provider = FakeModelProvider(
        {
            "1": [
                {"fake_error": "timeout"},
                {"fake_error": "malformed"},
                {"type": "final", "response": "done", "fake_delay_ms": 1},
            ]
        }
    )
    request = {"customer_id": "c", "amount": "1.00"}
    with pytest.raises(TimeoutError):
        await provider.generate(request, turn_number=1, attempt_number=1)
    assert (await provider.generate(request, turn_number=1, attempt_number=2)).raw == {
        "invalid": "model output"
    }
    assert (await provider.generate(request, turn_number=1, attempt_number=3)).raw == {
        "type": "final",
        "response": "done",
    }
    assert provider.invocations == [(1, 1), (1, 2), (1, 3)]


async def test_fake_provider_unavailability() -> None:
    provider = FakeModelProvider({"1": [{"fake_error": "unavailable"}]})
    with pytest.raises(ProviderUnavailable):
        await provider.generate({}, turn_number=1, attempt_number=1)


def test_ollama_fixture_parses_tokens_and_decision() -> None:
    payload = {
        "message": {"content": '{"type":"final","response":"done"}'},
        "prompt_eval_count": 40,
        "eval_count": 8,
    }
    result = parse_ollama_payload(payload)
    assert result.raw == {"type": "final", "response": "done"}
    assert result.prompt_tokens == 40 and result.completion_tokens == 8
    assert DECISION_ADAPTER.validate_python(result.raw).type == "final"
