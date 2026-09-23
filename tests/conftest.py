import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from durable_agent_runtime.api.app import create_app
from durable_agent_runtime.db.session import get_session

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL", "postgresql+asyncpg://durable:durable@127.0.0.1:55433/durable_test"
)

test_engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
TestSession = async_sessionmaker(test_engine, expire_on_commit=False, class_=AsyncSession)


@pytest_asyncio.fixture(autouse=True)
async def clean_database(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    if request.node.get_closest_marker("integration") is None:
        yield
        return
    async with test_engine.begin() as connection:
        await connection.execute(
            text("TRUNCATE state_transitions, workflow_steps, workflows RESTART IDENTITY CASCADE")
        )
    yield


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    async with TestSession() as database_session:
        yield database_session


@pytest_asyncio.fixture
async def client() -> AsyncIterator[AsyncClient]:
    app = create_app()

    async def override_session() -> AsyncIterator[AsyncSession]:
        async with TestSession() as database_session:
            yield database_session

    app.dependency_overrides[get_session] = override_session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http_client:
        yield http_client


@pytest.fixture
def workflow_payload() -> dict[str, object]:
    return {
        "workflow_type": "demo",
        "input": {"task": "example"},
        "steps": [
            {"name": "analyze", "step_type": "noop", "input": {}},
            {"name": "execute", "step_type": "noop", "input": {}, "max_attempts": 2},
        ],
    }
