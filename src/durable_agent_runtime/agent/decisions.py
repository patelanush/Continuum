"""Strict provider-independent decisions and canonical identity helpers."""

import hashlib
import json
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


class ToolDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["tool_call"]
    tool_name: str = Field(min_length=1)
    arguments: dict[str, Any]


class FinalDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["final"]
    response: str = Field(min_length=1)


AgentDecision = Annotated[ToolDecision | FinalDecision, Field(discriminator="type")]
DECISION_ADAPTER: TypeAdapter[AgentDecision] = TypeAdapter(AgentDecision)


def canonical_hash(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def agent_operation_id(tool_call_id: UUID) -> str:
    return f"continuum:agent-tool:{tool_call_id}"


class RefundArguments(BaseModel):
    model_config = ConfigDict(extra="forbid")
    customer_id: str = Field(min_length=1, max_length=200)
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)


class PolicyArguments(BaseModel):
    model_config = ConfigDict(extra="forbid")


TOOL_ARGUMENTS: dict[str, type[BaseModel]] = {
    "read_refund_policy": PolicyArguments,
    "refund_customer": RefundArguments,
}


def validate_tool_arguments(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    try:
        schema = TOOL_ARGUMENTS[name]
    except KeyError as exc:
        raise ValueError(f"Unauthorized tool: {name}") from exc
    return schema.model_validate(arguments).model_dump(mode="json")
