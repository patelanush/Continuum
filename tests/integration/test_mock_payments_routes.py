"""Route-level coverage on the real payments PostgreSQL; crash tests use real HTTP separately."""

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from durable_agent_runtime.mock_payments.app import app

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def payments_client() -> AsyncIterator[AsyncClient]:
    pool = await asyncpg.create_pool(
        os.getenv(
            "PAYMENTS_DATABASE_URL", "postgresql://payments:payments@localhost:55434/payments"
        )
    )
    app.state.pool = pool
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://payments"
        ) as client:
            yield client
    finally:
        await pool.close()


async def test_route_idempotency_conflict_lookup_and_missing(payments_client: AsyncClient) -> None:
    assert (await payments_client.get("/health/ready")).status_code == 200
    key = f"route:{uuid4()}"
    request = {"customer_id": "route-customer", "amount": "12.34"}
    first = await payments_client.post("/refunds", json=request, headers={"Idempotency-Key": key})
    assert first.status_code == 200
    assert (
        await payments_client.post("/refunds", json=request, headers={"Idempotency-Key": key})
    ).json() == first.json()
    fetched = await payments_client.get(f"/refunds/{first.json()['refund_id']}")
    assert fetched.json() == first.json()
    assert (await payments_client.get(f"/refunds/by-idempotency-key/{key}")).json() == first.json()
    assert (await payments_client.get("/refunds/count")).json()["count"] >= 1
    assert (
        await payments_client.post(
            "/refunds",
            json={"customer_id": "other", "amount": "12.34"},
            headers={"Idempotency-Key": key},
        )
    ).status_code == 409
    assert (await payments_client.get(f"/refunds/{uuid4()}")).status_code == 404
    assert (
        await payments_client.get(f"/refunds/by-idempotency-key/route:{uuid4()}")
    ).status_code == 404


async def test_post_commit_delay_path_and_decimal_validation(payments_client: AsyncClient) -> None:
    key = f"route:{uuid4()}"
    response = await payments_client.post(
        "/refunds",
        json={"customer_id": "delay", "amount": "2.50", "delay_after_commit_ms": 20},
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 200
    assert (
        await payments_client.post(
            "/refunds",
            json={"customer_id": "delay", "amount": "2.501"},
            headers={"Idempotency-Key": f"route:{uuid4()}"},
        )
    ).status_code == 422
