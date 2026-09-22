from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from durable_agent_runtime.events import STEP_READY_TOPIC, StepReadyEvent, workflow_message_key


def event() -> StepReadyEvent:
    workflow_id = uuid4()
    return StepReadyEvent(
        event_id=uuid4(),
        event_type="step.ready",
        schema_version=1,
        occurred_at=datetime.now(UTC),
        workflow_id=workflow_id,
        step_id=uuid4(),
        correlation_id=workflow_id,
        causation_id=None,
        payload={},
    )


def test_event_json_round_trip_and_key() -> None:
    original = event()
    assert StepReadyEvent.from_bytes(original.to_bytes()) == original
    assert workflow_message_key(original.workflow_id) == str(original.workflow_id).encode("ascii")
    assert STEP_READY_TOPIC == "continuum.step.ready.v1"


@pytest.mark.parametrize("field,value", [("schema_version", 2), ("event_type", "wrong")])
def test_unsupported_envelope_is_rejected(field: str, value: object) -> None:
    payload = event().model_dump(mode="json")
    payload[field] = value
    with pytest.raises(ValidationError):
        StepReadyEvent.model_validate(payload)


def test_naive_timestamp_and_extra_fields_are_rejected() -> None:
    payload = event().model_dump(mode="json")
    payload["occurred_at"] = "2026-09-22T10:00:00"
    payload["untrusted"] = "value"
    with pytest.raises(ValidationError):
        StepReadyEvent.model_validate(payload)


def test_malformed_json_is_rejected() -> None:
    with pytest.raises(ValidationError):
        StepReadyEvent.from_bytes(b"not-json")
