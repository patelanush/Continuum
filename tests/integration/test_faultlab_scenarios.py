"""Representative real-container fault paths; opt in after starting isolated Compose."""

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from durable_agent_runtime.faultlab.models import TrialResult
from durable_agent_runtime.faultlab.runner import run_experiment
from durable_agent_runtime.faultlab.runtime import DockerController, FaultLabRuntime
from durable_agent_runtime.faultlab.scenarios import SCENARIOS

pytestmark = [pytest.mark.integration, pytest.mark.faultlab]


@pytest.mark.parametrize(
    "scenario_name",
    list(SCENARIOS),
)
async def test_real_faultlab_scenario(scenario_name: str) -> None:
    if os.getenv("RUN_FAULTLAB_DOCKER") != "1":
        pytest.skip("set RUN_FAULTLAB_DOCKER=1 after starting the isolated FaultLab stack")
    trial = TrialResult(
        experiment_id=uuid4(),
        trial_id=uuid4(),
        scenario_name=scenario_name,
        scenario_version=SCENARIOS[scenario_name].version,
        seed=42,
        started_at=datetime.now(UTC),
    )
    async with FaultLabRuntime(DockerController()) as runtime:
        await SCENARIOS[scenario_name].run(runtime, trial)
    assert trial.correct, trial.failure_reason


async def test_runner_persists_real_baseline_trials() -> None:
    if os.getenv("RUN_FAULTLAB_DOCKER") != "1":
        pytest.skip("set RUN_FAULTLAB_DOCKER=1 after starting the isolated FaultLab stack")
    store, summary = await run_experiment(
        {"baseline": 2}, seed=7, concurrency=2, keep_stack=True, start_stack=False
    )
    assert summary["total_trials"] == 2
    assert summary["correct_trials"] == 2
    assert len(store.read_trials()) == 2
