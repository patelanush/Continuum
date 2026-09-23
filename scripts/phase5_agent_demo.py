"""Run and inspect a support-agent workflow through the public read-only API."""

import argparse
import asyncio
import json
from pathlib import Path
from time import monotonic
from typing import Any
from uuid import uuid4

import httpx


async def run(
    provider: str, api_url: str, payments_url: str, timeout_seconds: float
) -> dict[str, Any]:
    customer_id = f"phase5-demo-{uuid4()}"
    began = monotonic()
    async with httpx.AsyncClient(timeout=10) as api, httpx.AsyncClient(timeout=10) as payments:
        created = await api.post(
            f"{api_url}/api/v1/workflows",
            json={
                "workflow_type": "support-demo",
                "steps": [
                    {
                        "name": "resolve-refund",
                        "step_type": "support_agent",
                        "input": {
                            "customer_id": customer_id,
                            "amount": "49.99",
                            "request": "I was charged twice. Please refund the duplicate charge.",
                            "provider": provider,
                        },
                    }
                ],
            },
        )
        created.raise_for_status()
        workflow_id = created.json()["id"]
        started = await api.post(f"{api_url}/api/v1/workflows/{workflow_id}/start")
        started.raise_for_status()
        deadline = monotonic() + timeout_seconds
        while monotonic() < deadline:
            response = await api.get(f"{api_url}/api/v1/workflows/{workflow_id}")
            response.raise_for_status()
            workflow = response.json()
            if workflow["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            await asyncio.sleep(0.25)
        else:
            raise TimeoutError(f"Workflow {workflow_id} did not reach a terminal state")
        trace_response = await api.get(f"{api_url}/api/v1/workflows/{workflow_id}/agent")
        trace_response.raise_for_status()
        traces: list[dict[str, Any]] = trace_response.json()
        if len(traces) != 1:
            raise AssertionError(f"Expected one agent run, found {len(traces)}")
        trace = traces[0]
        turns: list[dict[str, Any]] = trace["turns"]
        refund_tools = [
            turn["tool_call"]
            for turn in turns
            if turn["tool_call"] and turn["tool_call"]["tool_name"] == "refund_customer"
        ]
        count_response = await payments.get(
            f"{payments_url}/refunds/count", params={"customer_id": customer_id}
        )
        count_response.raise_for_status()
        refund_count = count_response.json()["count"]
        if workflow["status"] != "SUCCEEDED" or refund_count != 1 or len(refund_tools) != 1:
            raise AssertionError(
                f"Demo invariant failed: workflow={workflow['status']}, "
                f"refund_count={refund_count}, refund_tools={len(refund_tools)}"
            )
        refund = refund_tools[0]
        by_key = await payments.get(
            f"{payments_url}/refunds/by-idempotency-key/{refund['operation_id']}"
        )
        by_key.raise_for_status()
        if by_key.json()["refund_id"] != refund["result"]["refund_id"]:
            raise AssertionError("External refund differs from durable agent tool result")
        model_calls = [call for turn in turns for call in turn["model_calls"]]
        for turn in turns:
            tool = turn["tool_call"]
            print(f"Turn {turn['turn_number']}: {turn['decision']}")
            if tool:
                print(
                    f"  {tool['tool_name']} operation={tool['operation_id']} "
                    f"result={tool['result']}"
                )
        print(f"Final: {trace['final_response']}")
        process = await asyncio.create_subprocess_exec(
            "git", "rev-parse", "HEAD", stdout=asyncio.subprocess.PIPE
        )
        output, _ = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError("Unable to read Git commit")
        commit = output.decode().strip()
        prompt_counts = [
            call["prompt_tokens"] for call in model_calls if call["prompt_tokens"] is not None
        ]
        completion_counts = [
            call["completion_tokens"]
            for call in model_calls
            if call["completion_tokens"] is not None
        ]
        return {
            "git_commit": commit,
            "provider": provider,
            "model": trace["model"],
            "workflow_id": workflow_id,
            "workflow_status": workflow["status"],
            "agent_turns": len(turns),
            "model_calls": len(model_calls),
            "tool_calls": sum(turn["tool_call"] is not None for turn in turns),
            "refund_count": refund_count,
            "final_response_length": len(trace["final_response"] or ""),
            "prompt_tokens": sum(prompt_counts) if prompt_counts else None,
            "completion_tokens": sum(completion_counts) if completion_counts else None,
            "elapsed_ms": round((monotonic() - began) * 1000),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=["fake", "ollama"], default="fake")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--payments-url", default="http://localhost:8001")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--summary-file", type=Path)
    args = parser.parse_args()
    result = asyncio.run(run(args.provider, args.api_url, args.payments_url, args.timeout))
    print(json.dumps(result, indent=2))
    if args.summary_file:
        args.summary_file.parent.mkdir(parents=True, exist_ok=True)
        args.summary_file.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
