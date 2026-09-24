"""Replay a valid Kafka event and a malformed trace header without duplicate execution."""

import asyncio
import json
from pathlib import Path
from time import monotonic
from typing import Any
from uuid import UUID

import httpx
from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from trace_workflow import fetch_trace, search_workflow

from durable_agent_runtime.db.models import OutboxEvent
from durable_agent_runtime.events import StepReadyEvent, workflow_message_key
from durable_agent_runtime.observability.context import kafka_headers


async def prom(expression: str) -> float:
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(
            "http://127.0.0.1:9090/api/v1/query", params={"query": expression}
        )
        response.raise_for_status()
        values = response.json()["data"]["result"]
        return float(values[0]["value"][1]) if values else 0.0


async def main() -> None:
    before = await prom("sum(continuum_kafka_duplicates_total)")
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=15) as client:
        created = await client.post(
            "/api/v1/workflows",
            json={
                "workflow_type": "coding-demo",
                "steps": [{"name": "dedupe", "step_type": "noop", "input": {}}],
            },
        )
        created.raise_for_status()
        workflow_id = created.json()["id"]
        started = await client.post(f"/api/v1/workflows/{workflow_id}/start")
        started.raise_for_status()
        deadline = monotonic() + 60
        while monotonic() < deadline:
            state = await client.get(f"/api/v1/workflows/{workflow_id}")
            state.raise_for_status()
            if state.json()["status"] == "SUCCEEDED":
                break
            await asyncio.sleep(0.2)
        else:
            raise TimeoutError("Original workflow did not complete")
        engine = create_async_engine("postgresql+asyncpg://durable:durable@127.0.0.1:55433/durable")
        try:
            sessions = async_sessionmaker(engine)
            async with sessions() as session:
                row = await session.scalar(
                    select(OutboxEvent).where(OutboxEvent.workflow_id == UUID(workflow_id))
                )
                if row is None:
                    raise AssertionError("Original outbox event absent")
                event = StepReadyEvent.from_outbox(row)
                topic = row.topic
                parent = row.traceparent
        finally:
            await engine.dispose()
        if not parent:
            raise AssertionError("Outbox trace context absent")
        producer = AIOKafkaProducer(bootstrap_servers="127.0.0.1:19092", acks="all")
        await producer.start()
        try:
            for headers in (
                kafka_headers(parent),
                [("traceparent", b"malformed-trace-context")],
            ):
                await producer.send_and_wait(
                    topic,
                    key=workflow_message_key(event.workflow_id),
                    value=event.to_bytes(),
                    headers=headers,
                )
        finally:
            await producer.stop()
        deadline = monotonic() + 30
        while monotonic() < deadline:
            after = await prom("sum(continuum_kafka_duplicates_total)")
            if after - before >= 2:
                break
            await asyncio.sleep(1)
        else:
            raise AssertionError("Duplicate metric did not rise twice")
        attempts = await client.get(f"/api/v1/workflows/{workflow_id}/attempts")
        attempts.raise_for_status()
        records: list[dict[str, Any]] = attempts.json()
        if len(records) != 1 or records[0]["status"] != "SUCCEEDED":
            raise AssertionError(f"Replay changed execution history: {records}")
    deadline = monotonic() + 30
    trace_id = ""
    original_count = 0
    duplicate_count = 0
    while monotonic() < deadline:
        for candidate in search_workflow(workflow_id, "http://127.0.0.1:3200"):
            spans = fetch_trace(candidate, "http://127.0.0.1:3200")
            original_count = sum(
                item["name"] == "kafka.consume"
                and item["attributes"].get("continuum.event.duplicate", {}).get("boolValue")
                is False
                for item in spans
            )
            duplicate_count = sum(
                item["name"] == "kafka.consume"
                and item["attributes"].get("continuum.event.duplicate", {}).get("boolValue") is True
                for item in spans
            )
            if original_count >= 1 and duplicate_count >= 1:
                trace_id = candidate
                break
        if trace_id:
            break
        await asyncio.sleep(1)
    if not trace_id:
        raise AssertionError("Original and replay consume spans were not correlated in Tempo")
    summary = {
        "workflow_id": workflow_id,
        "trace_id": trace_id,
        "original_consume_spans": original_count,
        "linked_duplicate_consume_spans": duplicate_count,
        "duplicate_metric_delta": round(after - before),
        "malformed_trace_header_accepted_as_valid_business_event": True,
        "execution_attempts": len(records),
        "workflow_status": "SUCCEEDED",
    }
    path = Path("benchmarks/results/phase7-duplicate-trace.json")
    await asyncio.to_thread(path.write_text, json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
