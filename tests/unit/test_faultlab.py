"""Pure FaultLab evidence, aggregation, reporting, and registry contracts."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest

from durable_agent_runtime.db.models import (
    ExecutionAttempt,
    OutboxEvent,
    StateTransition,
    Workflow,
    WorkflowStep,
)
from durable_agent_runtime.domain.enums import (
    EntityType,
    ExecutionAttemptStatus,
    StepStatus,
    WorkflowStatus,
)
from durable_agent_runtime.faultlab.assertions import classify_workflow
from durable_agent_runtime.faultlab.cli import invoke, parser
from durable_agent_runtime.faultlab.metrics import aggregate, duration_stats, percentile, ratio
from durable_agent_runtime.faultlab.models import ExperimentConfig, TrialResult
from durable_agent_runtime.faultlab.reporting import render_markdown
from durable_agent_runtime.faultlab.runner import publish_summary, run_trial, summarize
from durable_agent_runtime.faultlab.runtime import DockerController, FaultLabRuntime
from durable_agent_runtime.faultlab.scenarios import CAMPAIGNS, SCENARIOS, Scenario
from durable_agent_runtime.faultlab.storage import ExperimentStore


def config() -> ExperimentConfig:
    return ExperimentConfig(
        experiment_id=uuid4(),
        scenario_names=["baseline"],
        runs_per_scenario={"baseline": 1},
        seed=42,
        concurrency=2,
        git_commit="a" * 40,
        git_dirty=False,
        compose_project="continuum-faultlab",
        executor_count=3,
        started_at=datetime.now(UTC),
        environment={"system": "test"},
    )


def trial(configuration: ExperimentConfig, *, scenario: str = "baseline") -> TrialResult:
    now = datetime.now(UTC)
    return TrialResult(
        experiment_id=configuration.experiment_id,
        trial_id=uuid4(),
        scenario_name=scenario,
        scenario_version=1,
        seed=123,
        started_at=now,
        completed_at=now + timedelta(seconds=1),
        correct=True,
        actual_terminal_status="SUCCEEDED",
        workflow_id=uuid4(),
        attempt_count=1,
        workflow_elapsed_ms=1000,
        notes={"steps_succeeded": 1},
    )


def test_registry_and_campaigns_have_stable_names() -> None:
    required = {
        "baseline",
        "executor-crash-before-execution",
        "executor-crash-during-pure-work",
        "executor-crash-after-side-effect",
        "missed-heartbeats",
        "stale-owner-finalize",
        "duplicate-kafka-delivery",
        "lost-kafka-offset-ack",
        "kafka-outage",
        "dispatcher-crash-after-kafka-ack",
        "malformed-kafka-event",
        "external-service-timeout-before-side-effect",
        "external-response-lost-after-side-effect",
        "concurrent-recovery-race",
        "executor-restart",
        "database-interruption",
    }
    assert required <= SCENARIOS.keys()
    assert all(scenario.version >= 1 for scenario in SCENARIOS.values())
    assert set(CAMPAIGNS) == {"smoke", "ai-smoke", "side-effects", "reliability"}
    assert all(set(counts) <= SCENARIOS.keys() for counts in CAMPAIGNS.values())
    assert SCENARIOS["kafka-outage"].exclusive
    assert not SCENARIOS["baseline"].exclusive


def test_percentile_and_binary_rates_use_raw_samples() -> None:
    assert percentile(list(range(1, 21)), 95) == 19
    assert percentile([10], 95) == 10
    assert percentile([], 95) is None
    with pytest.raises(ValueError):
        percentile([1], 0)
    assert duration_stats([1, 2, 3, 4]) == {
        "sample_size": 4,
        "median_ms": 2.5,
        "p95_ms": 4,
        "max_ms": 4,
    }
    assert ratio(1, 4) == 0.25
    assert ratio(0, 0) is None


def test_aggregate_separates_intentionally_unsafe_baseline() -> None:
    configuration = config()
    clean = trial(configuration)
    clean.recovery_required = True
    clean.recovered = True
    clean.recovery_time_ms = 55
    clean.fault_detection_latency_ms = 30
    clean.injected_failure_count = 1
    failed = trial(configuration, scenario="kafka-outage")
    failed.correct = False
    failed.actual_terminal_status = "FAILED"
    failed.duplicate_side_effect_count = 1
    failed.failure_reason = "duplicate refund"
    unsafe = trial(configuration, scenario="unsafe-refund-retry-baseline")
    unsafe.duplicate_side_effect_count = 1
    result = aggregate([clean, failed, unsafe], 10)
    assert result["total_trials"] == 3
    assert result["continuum_trials"] == 2
    assert result["correctness_rate"] == 0.5
    assert result["recovery_success_rate"] == 1
    assert result["duplicate_side_effects"] == 1
    assert result["unsafe_baseline_duplicate_side_effects"] == 1
    assert result["recovery_time"]["p95_ms"] == 55
    assert result["workflow_throughput_per_second"] == 0.1
    assert result["workflows_processed"] == 2
    assert result["workflows_succeeded"] == 1
    assert result["incorrect_trial_ids"] == [str(failed.trial_id)]


def test_storage_roundtrip_and_report_derive_from_jsonl(tmp_path: Path) -> None:
    configuration = config()
    store = ExperimentStore(configuration.experiment_id, root=tmp_path)
    store.initialize(configuration)
    first = trial(configuration)
    store.append(first)
    assert store.read_trials() == [first]
    assert store.read_config() == configuration
    summary = aggregate(store.read_trials(), 1)
    markdown = render_markdown(configuration, store.read_trials(), summary)
    store.write_summary(summary, markdown)
    assert json.loads((store.directory / "summary.json").read_text())["correct_trials"] == 1
    assert str(configuration.experiment_id) in (store.directory / "summary.md").read_text()
    assert len((store.directory / "trials.jsonl").read_text().splitlines()) == 1
    with pytest.raises(FileExistsError):
        store.initialize(configuration)


async def test_runner_preserves_failed_trial_and_cli_lists_scenarios(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    configuration = config()
    store = ExperimentStore(configuration.experiment_id, root=tmp_path)
    store.initialize(configuration)

    async def fail(_runtime: FaultLabRuntime, _trial: TrialResult) -> None:
        raise AssertionError("injected failure")

    import asyncio

    await run_trial(
        cast(FaultLabRuntime, object()),
        store,
        configuration,
        Scenario("test-failure", "", fail),
        99,
        asyncio.Semaphore(1),
    )
    recorded = store.read_trials()[0]
    assert not recorded.correct
    assert recorded.seed == 99
    assert recorded.failure_reason == "AssertionError: injected failure"
    result = summarize(store)
    assert result["incorrect_trials"] == 1
    assert await invoke(argparse.Namespace(command="list")) == 0
    assert "baseline" in capsys.readouterr().out
    assert parser().parse_args(["run", "baseline", "--runs", "3"]).runs == 3


def test_official_publish_rejects_dirty_or_mismatched_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configuration = config()
    store = ExperimentStore(configuration.experiment_id, root=tmp_path)
    store.initialize(configuration)
    store.append(trial(configuration))
    monkeypatch.setattr(
        "durable_agent_runtime.faultlab.runner.git_state", lambda: ("b" * 40, False)
    )
    with pytest.raises(ValueError, match="same clean Git revision"):
        publish_summary(store)


def test_official_publish_derives_compact_summary_from_raw_trials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configuration = config()
    store = ExperimentStore(configuration.experiment_id, root=tmp_path / "raw")
    store.initialize(configuration)
    store.append(trial(configuration))
    monkeypatch.setattr(
        "durable_agent_runtime.faultlab.runner.git_state",
        lambda: (configuration.git_commit, False),
    )
    monkeypatch.setattr("durable_agent_runtime.faultlab.runner.REPO_ROOT", tmp_path)
    publish_summary(store)
    published = json.loads((tmp_path / "benchmarks/results/latest.json").read_text())
    assert published["experiment_id"] == str(configuration.experiment_id)
    assert published["total_trials"] == 1
    assert published["git_commit"] == configuration.git_commit


async def test_classification_detects_duplicate_external_effect_and_transition() -> None:
    now = datetime.now(UTC)
    workflow_id, step_id = uuid4(), uuid4()
    workflow = Workflow(
        id=workflow_id,
        workflow_type="test",
        status=WorkflowStatus.SUCCEEDED,
        started_at=now,
        completed_at=now + timedelta(seconds=1),
    )
    step = WorkflowStep(
        id=step_id,
        workflow_id=workflow_id,
        position=0,
        name="refund",
        step_type="mock_refund",
        status=StepStatus.SUCCEEDED,
    )
    attempt = ExecutionAttempt(
        id=uuid4(),
        workflow_id=workflow_id,
        step_id=step_id,
        attempt_number=1,
        status=ExecutionAttemptStatus.SUCCEEDED,
    )
    event = OutboxEvent(
        id=uuid4(),
        workflow_id=workflow_id,
        step_id=step_id,
        event_type="step.ready",
        schema_version=1,
        correlation_id=workflow_id,
        payload={},
        topic="test",
        message_key=str(workflow_id),
        published_at=now,
    )
    transition = StateTransition(
        id=uuid4(),
        workflow_id=workflow_id,
        entity_type=EntityType.STEP,
        entity_id=step_id,
        from_status="RUNNING",
        to_status="SUCCEEDED",
    )

    class FakeRuntime:
        async def snapshot(self, _id: object) -> dict[str, Any]:
            return {
                "workflow": workflow,
                "steps": [step],
                "attempts": [attempt],
                "outbox": [event],
                "transitions": [transition, transition],
            }

        async def refund_count(self, _customer: str) -> int:
            return 2

        async def refund(self, _key: str) -> dict[str, str]:
            return {
                "refund_id": str(uuid4()),
                "idempotency_key": f"continuum:{step_id}",
                "customer_id": "customer",
            }

    configuration = config()
    result = trial(configuration)
    result.workflow_id = workflow_id
    result.step_id = step_id
    await classify_workflow(
        cast(FaultLabRuntime, FakeRuntime()),
        result,
        expected_attempts=1,
        expected_refunds=1,
        customer_id="customer",
        expected_outbox=1,
    )
    assert not result.correct
    assert result.duplicate_transition_count == 1
    assert result.duplicate_side_effect_count == 1
    assert "duplicate transitions" in (result.failure_reason or "")


def test_docker_controller_refuses_other_compose_project() -> None:
    with pytest.raises(ValueError):
        DockerController(project="continuum")
