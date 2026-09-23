"""Execute isolated experiments; retain failed trials rather than hiding exceptions."""

import asyncio
import json
import os
import platform
import traceback
from datetime import UTC, datetime
from random import Random
from typing import Any
from uuid import uuid4

from durable_agent_runtime.faultlab.metrics import aggregate
from durable_agent_runtime.faultlab.models import ExperimentConfig, TrialResult
from durable_agent_runtime.faultlab.reporting import render_markdown
from durable_agent_runtime.faultlab.runtime import (
    PROJECT,
    REPO_ROOT,
    DockerController,
    FaultLabRuntime,
    git_state,
)
from durable_agent_runtime.faultlab.scenarios import SCENARIOS, Scenario
from durable_agent_runtime.faultlab.storage import ExperimentStore


def environment_info(docker: DockerController) -> dict[str, str | int | float]:
    return {
        "system": platform.system(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count() or 0,
        "executor_lease_seconds": float(docker.environment["EXECUTOR_LEASE_SECONDS"]),
        "executor_heartbeat_seconds": float(docker.environment["EXECUTOR_HEARTBEAT_SECONDS"]),
        "executor_poll_interval_seconds": float(
            docker.environment["EXECUTOR_POLL_INTERVAL_SECONDS"]
        ),
        "recovery_scan_interval_seconds": float(
            docker.environment["RECOVERY_SCAN_INTERVAL_SECONDS"]
        ),
        "outbox_publish_lease_seconds": float(docker.environment["OUTBOX_PUBLISH_LEASE_SECONDS"]),
    }


async def run_trial(
    runtime: FaultLabRuntime,
    store: ExperimentStore,
    config: ExperimentConfig,
    scenario: Scenario,
    seed: int,
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore:
        trial = TrialResult(
            experiment_id=config.experiment_id,
            trial_id=uuid4(),
            scenario_name=scenario.name,
            scenario_version=scenario.version,
            seed=seed,
            started_at=datetime.now(UTC),
        )
        try:
            await scenario.run(runtime, trial)
        except Exception as exc:
            trial.notes["exception_traceback"] = traceback.format_exc(limit=8)
            trial.finish(error=f"{type(exc).__name__}: {exc}")
        else:
            trial.finish()
        store.append(trial)
        if not trial.correct:
            print(
                f"INCORRECT {scenario.name} trial={trial.trial_id}: {trial.failure_reason}",
                flush=True,
            )


async def run_experiment(
    counts: dict[str, int],
    *,
    seed: int,
    concurrency: int,
    keep_stack: bool = False,
    executors: int = 3,
    start_stack: bool = True,
) -> tuple[ExperimentStore, dict[str, Any]]:
    unknown = set(counts) - set(SCENARIOS)
    if unknown:
        raise ValueError(f"Unknown scenarios: {sorted(unknown)}")
    if not counts or any(count < 1 for count in counts.values()):
        raise ValueError("Every selected scenario needs at least one trial")
    if not 1 <= concurrency <= 32:
        raise ValueError("Concurrency must be between 1 and 32")
    docker = DockerController()
    commit, dirty = git_state()
    config = ExperimentConfig(
        experiment_id=uuid4(),
        scenario_names=list(counts),
        runs_per_scenario=counts,
        seed=seed,
        concurrency=concurrency,
        git_commit=commit,
        git_dirty=dirty,
        compose_project=PROJECT,
        executor_count=executors,
        started_at=datetime.now(UTC),
        environment=environment_info(docker),
    )
    store = ExperimentStore(config.experiment_id)
    store.initialize(config)
    random = Random(seed)
    try:
        if start_stack:
            await docker.up(executors=executors)
        async with FaultLabRuntime(docker) as runtime:
            for name, count in counts.items():
                scenario = SCENARIOS[name]
                limit = 1 if scenario.exclusive else concurrency
                semaphore = asyncio.Semaphore(limit)
                trial_seeds = [random.getrandbits(64) for _ in range(count)]
                pending = [
                    run_trial(runtime, store, config, scenario, trial_seed, semaphore)
                    for trial_seed in trial_seeds
                ]
                for completed_count, completed_trial in enumerate(
                    asyncio.as_completed(pending), start=1
                ):
                    await completed_trial
                    if completed_count % 50 == 0:
                        print(f"{name}: {completed_count}/{count} trials recorded", flush=True)
                completed = [trial for trial in store.read_trials() if trial.scenario_name == name]
                print(
                    f"{name}: {sum(trial.correct for trial in completed)}/{len(completed)} correct",
                    flush=True,
                )
    finally:
        if not keep_stack:
            await docker.clean()
    summary = summarize(store)
    return store, summary


def summarize(store: ExperimentStore) -> dict[str, Any]:
    config = store.read_config()
    trials = store.read_trials()
    start = min((trial.started_at for trial in trials), default=config.started_at)
    end = max((trial.completed_at or trial.started_at for trial in trials), default=start)
    elapsed = round((end - start).total_seconds(), 3)
    summary = {
        "experiment_id": str(config.experiment_id),
        "started_at": start.isoformat(),
        "completed_at": end.isoformat(),
        "git_commit": config.git_commit,
        "git_dirty": config.git_dirty,
        "seed": config.seed,
        "executor_count": config.executor_count,
        "concurrency": config.concurrency,
        **aggregate(trials, elapsed),
    }
    store.write_summary(summary, render_markdown(config, trials, summary))
    return summary


def publish_summary(store: ExperimentStore) -> None:
    """Only publish derived results from a clean current revision's raw data."""
    config = store.read_config()
    commit, dirty = git_state()
    if config.git_dirty or dirty or commit != config.git_commit:
        raise ValueError("Official results require the same clean Git revision as the experiment")
    summary = summarize(store)
    destination = REPO_ROOT / "benchmarks" / "results"
    destination.mkdir(parents=True, exist_ok=True)
    latest = {
        "experiment_id": summary["experiment_id"],
        "git_commit": summary["git_commit"],
        "total_trials": summary["total_trials"],
        "fault_injected_trials": summary["fault_injected_trials"],
        "recovered_trials": summary["recovered_trials"],
        "workflow_completion_rate": summary["workflow_completion_rate"],
        "correct_trials": summary["correct_trials"],
        "incorrect_trials": summary["incorrect_trials"],
        "duplicate_side_effects": summary["duplicate_side_effects"],
        "lost_side_effects": summary["lost_side_effects"],
        "p95_recovery_ms": summary["recovery_time"]["p95_ms"],
        "workflows_processed": summary["workflows_processed"],
        "steps_succeeded": summary["steps_succeeded"],
        "executor_count": summary["executor_count"],
        "scenario_count": len(summary["scenario_breakdown"]),
    }
    if all(name.startswith("coding-") for name in config.scenario_names):
        report_stem = "phase6-coding-summary"
    elif all(name.startswith("agent-") for name in config.scenario_names):
        report_stem = "phase5-ai-summary"
    else:
        report_stem = "phase4-summary"
    # Keep the historical Phase 4 latest.json stable when publishing later cohorts.
    json_name = "latest.json" if report_stem == "phase4-summary" else f"{report_stem}.json"
    (destination / json_name).write_text(
        json.dumps(latest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (destination / f"{report_stem}.md").write_text(
        (store.directory / "summary.md").read_text(encoding="utf-8"), encoding="utf-8"
    )
