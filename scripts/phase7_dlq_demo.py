"""Send one invalid envelope and verify DLQ publication and Prometheus count."""

import asyncio
import hashlib
import json
from pathlib import Path
from time import monotonic
from uuid import uuid4

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from durable_agent_runtime.events import DEAD_LETTER_TOPIC, STEP_READY_TOPIC


async def dlq_count() -> float:
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(
            "http://127.0.0.1:9090/api/v1/query",
            params={"query": "sum(continuum_dlq_messages_total)"},
        )
        response.raise_for_status()
        results = response.json()["data"]["result"]
        return float(results[0]["value"][1]) if results else 0.0


async def main() -> None:
    before = await dlq_count()
    raw = b"{invalid-observability-demo-envelope"
    digest = hashlib.sha256(raw).hexdigest()
    consumer = AIOKafkaConsumer(
        DEAD_LETTER_TOPIC,
        bootstrap_servers="127.0.0.1:19092",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers="127.0.0.1:19092", acks="all")
    await consumer.start()
    await producer.start()
    try:
        for _ in range(20):
            await consumer.getmany(timeout_ms=500)
            if consumer.assignment():
                break
        if not consumer.assignment():
            raise TimeoutError("DLQ consumer did not receive partitions")
        await producer.send_and_wait(
            STEP_READY_TOPIC,
            key=str(uuid4()).encode(),
            value=raw,
        )
        deadline = monotonic() + 30
        matched = False
        while monotonic() < deadline:
            try:
                record = await asyncio.wait_for(consumer.getone(), timeout=5)
            except TimeoutError:
                continue
            message = json.loads(record.value)
            if message.get("payload_sha256") == digest:
                matched = True
                break
        if not matched:
            raise AssertionError("No matching DLQ record was observed")
    finally:
        await producer.stop()
        await consumer.stop()
    deadline = monotonic() + 30
    while monotonic() < deadline:
        after = await dlq_count()
        if after - before >= 1:
            break
        await asyncio.sleep(1)
    else:
        raise AssertionError("DLQ metric did not increase")
    summary = {
        "scenario": "invalid-event-to-dlq",
        "dlq_record_matched_payload_hash": True,
        "prometheus_dlq_metric_delta": round(after - before),
        "reason": "invalid_event",
    }
    path = Path("benchmarks/results/phase7-dlq-demo.json")
    await asyncio.to_thread(path.write_text, json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
