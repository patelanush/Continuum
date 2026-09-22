"""Real HTTP calls to independently persisted mock-payments, not ASGI mocks."""

import asyncio
import os
from uuid import uuid4

import httpx
import pytest

pytestmark = pytest.mark.integration

PAYMENTS_URL = os.getenv("MOCK_PAYMENTS_URL", "http://localhost:8001")


async def test_refund_idempotency_and_conflicting_key() -> None:
    key = f"test:{uuid4()}"
    request = {"customer_id": "customer-a", "amount": "49.99"}
    async with httpx.AsyncClient(base_url=PAYMENTS_URL, timeout=5) as client:
        before = (await client.get("/refunds/count")).json()["count"]
        first = await client.post("/refunds", json=request, headers={"Idempotency-Key": key})
        assert first.status_code == 200
        repeated = await client.post("/refunds", json=request, headers={"Idempotency-Key": key})
        assert repeated.status_code == 200
        assert repeated.json() == first.json()
        fetched = await client.get(f"/refunds/{first.json()['refund_id']}")
        assert fetched.json() == first.json()
        by_key = await client.get(f"/refunds/by-idempotency-key/{key}")
        assert by_key.json() == first.json()
        assert (await client.get("/refunds/count")).json()["count"] == before + 1
        conflict = await client.post(
            "/refunds",
            json={"customer_id": "customer-b", "amount": "49.99"},
            headers={"Idempotency-Key": key},
        )
        assert conflict.status_code == 409
        assert (await client.get("/refunds/count")).json()["count"] == before + 1


async def test_idempotency_header_and_request_validation() -> None:
    async with httpx.AsyncClient(base_url=PAYMENTS_URL, timeout=5) as client:
        assert (
            await client.post("/refunds", json={"customer_id": "a", "amount": "1.00"})
        ).status_code == 422
        assert (
            await client.post(
                "/refunds",
                json={"customer_id": "a", "amount": "-1"},
                headers={"Idempotency-Key": f"test:{uuid4()}"},
            )
        ).status_code == 422


async def test_post_commit_delay_exposes_refund_before_response() -> None:
    key = f"test:{uuid4()}"
    async with httpx.AsyncClient(base_url=PAYMENTS_URL, timeout=5) as client:
        task = asyncio.create_task(
            client.post(
                "/refunds",
                json={
                    "customer_id": "delayed-customer",
                    "amount": "2.50",
                    "delay_after_commit_ms": 1200,
                },
                headers={"Idempotency-Key": key},
            )
        )
        async with asyncio.timeout(3):
            while True:
                existing = await client.get(f"/refunds/by-idempotency-key/{key}")
                if existing.status_code == 200:
                    break
                assert existing.status_code == 404
                await asyncio.sleep(0.02)
        assert not task.done(), "refund should commit before delayed HTTP response"
        assert (await task).json() == existing.json()
