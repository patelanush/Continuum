"""Verify a keyed refund crosses executor and mock-payments service spans."""

import asyncio
import json
from pathlib import Path
from time import monotonic
from typing import Any

from phase5_agent_demo import run
from trace_workflow import fetch_trace, search_workflow


async def main() -> None:
    workflow = await run("fake", "http://127.0.0.1:8000", "http://127.0.0.1:8001", 90)
    workflow_id: str = workflow["workflow_id"]
    required = {"model.call", "tool.call", "external.http", "payment.refund"}
    deadline = monotonic() + 40
    trace_id = ""
    spans: list[dict[str, Any]] = []
    while monotonic() < deadline:
        for candidate in search_workflow(workflow_id, "http://127.0.0.1:3200"):
            candidate_spans = fetch_trace(candidate, "http://127.0.0.1:3200")
            names = {item["name"] for item in candidate_spans}
            services = {item["service"] for item in candidate_spans}
            if required <= names and "continuum-mock-payments" in services:
                trace_id, spans = candidate, candidate_spans
                break
        if trace_id:
            break
        await asyncio.sleep(1)
    if not trace_id:
        raise AssertionError("Tempo lacks the correlated external service call")
    summary = {
        "workflow_id": workflow_id,
        "trace_id": trace_id,
        "span_count": len(spans),
        "services_present": sorted({item["service"] for item in spans}),
        "required_span_categories_present": sorted(required),
        "workflow_status": workflow["workflow_status"],
        "model_calls": workflow["model_calls"],
        "tool_calls": workflow["tool_calls"],
        "refund_count": workflow["refund_count"],
    }
    path = Path("benchmarks/results/phase7-external-trace.json")
    await asyncio.to_thread(path.write_text, json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
