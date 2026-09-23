"""Optional real-model smoke; excluded from ordinary CI and make check."""

import os

import pytest

from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.domain.enums import AgentRunStatus
from durable_agent_runtime.execution.executor import execute_attempt
from tests.conftest import TestSession
from tests.integration.test_agent import records, support_input
from tests.integration.test_execution import claim, create_scheduled

pytestmark = [pytest.mark.integration, pytest.mark.ollama]


async def test_real_local_ollama_support_agent() -> None:
    _, step_id = await create_scheduled(
        step_count=1, step_type="support_agent", step_input=support_input("ollama-demo-customer")
    )
    attempt = await claim("ollama-test", lease_seconds=180)
    config = Settings(
        app_env="test",
        agent_provider="ollama",
        ollama_base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        ollama_model=os.getenv("OLLAMA_MODEL", "qwen2.5:3b"),
        executor_lease_seconds=180,
        model_timeout_seconds=120,
    )
    assert (
        await execute_attempt(
            attempt, executor_id="ollama-test", sessions=TestSession, settings=config
        )
        == "succeeded"
    )
    run, turns, calls, tools = await records(step_id)
    assert run.status == AgentRunStatus.SUCCEEDED
    assert len(turns) >= 2 and len(calls) >= 2 and len(tools) >= 1
