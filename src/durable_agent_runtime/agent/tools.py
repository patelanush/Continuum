"""Allowlisted agent tools; external HTTP is never called in a DB transaction."""

from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Any

import httpx

from durable_agent_runtime.execution.tools import PermanentToolError, TransientToolError
from durable_agent_runtime.observability.context import current_traceparent
from durable_agent_runtime.observability.runtime import span

POLICY = {
    "policy_version": "v1",
    "policy": "Duplicate charges may be refunded when customer and amount are provided.",
}


async def execute_agent_tool(
    name: str,
    arguments: dict[str, Any],
    operation_id: str,
    *,
    payments_url: str,
    faultlab_delay_ms: int = 0,
) -> dict[str, Any]:
    if name == "read_refund_policy":
        return POLICY.copy()
    if name != "refund_customer":
        raise PermanentToolError("UNKNOWN_AGENT_TOOL", f"Tool {name} is not allowlisted")
    body = {"customer_id": arguments["customer_id"], "amount": arguments["amount"]}
    if faultlab_delay_ms:
        body["delay_after_commit_ms"] = faultlab_delay_ms
    try:
        with span(
            "external.http",
            {
                "http.request.method": "POST",
                "continuum.operation.sha256": sha256(operation_id.encode()).hexdigest(),
            },
        ) as active:
            headers = {"Idempotency-Key": operation_id}
            traceparent = current_traceparent()
            if traceparent is not None:
                headers["traceparent"] = traceparent
            async with httpx.AsyncClient(timeout=45) as client:
                response = await client.post(
                    f"{payments_url.rstrip('/')}/refunds", headers=headers, json=body
                )
            active.set_attribute("http.response.status_code", response.status_code)
        if response.status_code in {408, 429} or response.status_code >= 500:
            raise TransientToolError(f"Payments HTTP {response.status_code}")
        if response.status_code >= 400:
            raise PermanentToolError("REFUND_REJECTED", f"Payments HTTP {response.status_code}")
        payload: dict[str, Any] = response.json()
        try:
            matches_amount = Decimal(str(payload.get("amount"))) == Decimal(
                str(arguments["amount"])
            )
        except InvalidOperation:
            matches_amount = False
        if (
            payload.get("idempotency_key") != operation_id
            or payload.get("customer_id") != arguments["customer_id"]
            or not matches_amount
        ):
            raise PermanentToolError(
                "EXTERNAL_RESULT_MISMATCH", "Refund response does not match the durable tool call"
            )
        return payload
    except httpx.RequestError as exc:
        raise TransientToolError(type(exc).__name__) from exc
