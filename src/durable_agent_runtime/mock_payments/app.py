"""Idempotent refund API; post-commit delay exposes the lost-response boundary."""

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

import asyncpg
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field


class RefundRequest(BaseModel):
    customer_id: str = Field(min_length=1, max_length=200)
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    delay_after_commit_ms: int = Field(default=0, ge=0, le=30_000)
    delay_before_commit_ms: int = Field(default=0, ge=0, le=30_000)
    faultlab_mode: Literal[
        "normal", "fail_before_commit", "error_after_commit", "timeout_before_commit"
    ] = "normal"


class RefundResponse(BaseModel):
    refund_id: UUID
    idempotency_key: str
    customer_id: str
    amount: Decimal


def response_from_row(row: asyncpg.Record) -> RefundResponse:
    return RefundResponse(
        refund_id=row["refund_id"],
        idempotency_key=row["idempotency_key"],
        customer_id=row["customer_id"],
        amount=row["amount"],
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.pool = await asyncpg.create_pool(
        os.environ.get(
            "PAYMENTS_DATABASE_URL",
            "postgresql://payments:payments@127.0.0.1:55434/payments",
        )
    )
    try:
        yield
    finally:
        await app.state.pool.close()


app = FastAPI(title="Continuum Mock Payments", lifespan=lifespan)


@app.get("/health/ready")
async def ready() -> dict[str, str]:
    async with app.state.pool.acquire() as connection:
        await connection.fetchval("SELECT 1")
    return {"status": "ready"}


@app.post("/refunds", response_model=RefundResponse)
async def create_refund(
    command: RefundRequest, idempotency_key: str = Header(min_length=1, max_length=200)
) -> RefundResponse:
    if command.faultlab_mode != "normal":
        if os.getenv("APP_ENV") != "faultlab":
            raise HTTPException(400, "Fault injection is disabled")
        if command.faultlab_mode == "fail_before_commit":
            raise HTTPException(503, "FaultLab failure before refund commit")
        if command.faultlab_mode == "timeout_before_commit":
            if command.delay_before_commit_ms < 1:
                raise HTTPException(400, "FaultLab pre-commit delay must be positive")
            # The client's read deadline elapses before this independent service
            # enters its own refund transaction. No side effect is committed.
            await asyncio.sleep(command.delay_before_commit_ms / 1000)
            raise HTTPException(503, "FaultLab failure after pre-commit delay")
    async with app.state.pool.acquire() as connection:
        async with connection.transaction():
            inserted = await connection.fetchrow(
                "INSERT INTO refunds(refund_id, idempotency_key, customer_id, amount) "
                "VALUES ($1, $2, $3, $4) ON CONFLICT (idempotency_key) DO NOTHING "
                "RETURNING refund_id, idempotency_key, customer_id, amount",
                uuid4(),
                idempotency_key,
                command.customer_id,
                command.amount,
            )
            row = inserted or await connection.fetchrow(
                "SELECT refund_id, idempotency_key, customer_id, amount "
                "FROM refunds WHERE idempotency_key = $1",
                idempotency_key,
            )
            if row is None:
                raise RuntimeError("Idempotency row disappeared")
            if row["customer_id"] != command.customer_id or row["amount"] != command.amount:
                raise HTTPException(409, "Idempotency key reused with different refund parameters")
    # This delay is deliberately AFTER the independent payments DB transaction commits.
    if inserted is not None and command.delay_after_commit_ms:
        await asyncio.sleep(command.delay_after_commit_ms / 1000)
    if inserted is not None and command.faultlab_mode == "error_after_commit":
        raise HTTPException(503, "FaultLab response failure after refund commit")
    return response_from_row(row)


@app.get("/refunds/by-idempotency-key/{key}", response_model=RefundResponse)
async def refund_by_key(key: str) -> RefundResponse:
    async with app.state.pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT refund_id, idempotency_key, customer_id, amount "
            "FROM refunds WHERE idempotency_key = $1",
            key,
        )
    if row is None:
        raise HTTPException(404, "Refund not found")
    return response_from_row(row)


@app.get("/refunds/count")
async def refund_count(customer_id: str | None = Query(default=None)) -> dict[str, int]:
    async with app.state.pool.acquire() as connection:
        if customer_id is None:
            count = await connection.fetchval("SELECT count(*) FROM refunds")
        else:
            count = await connection.fetchval(
                "SELECT count(*) FROM refunds WHERE customer_id = $1", customer_id
            )
    return {"count": int(count)}


@app.get("/refunds/{refund_id}", response_model=RefundResponse)
async def refund_by_id(refund_id: UUID) -> RefundResponse:
    async with app.state.pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT refund_id, idempotency_key, customer_id, amount "
            "FROM refunds WHERE refund_id = $1",
            refund_id,
        )
    if row is None:
        raise HTTPException(404, "Refund not found")
    return response_from_row(row)
