"""Human-readable report generated from raw trial records and configuration."""

from datetime import UTC, datetime
from typing import Any

from durable_agent_runtime.faultlab.models import ExperimentConfig, TrialResult


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.3f}%"


def render_markdown(
    config: ExperimentConfig, trials: list[TrialResult], summary: dict[str, Any]
) -> str:
    lines = [
        "# FaultLab Experiment",
        "",
        f"Experiment ID: `{config.experiment_id}`  ",
        f"Generated: {datetime.now(UTC).isoformat()}  ",
        f"Git commit: `{config.git_commit}` (dirty: {str(config.git_dirty).lower()})  ",
        f"Seed: {config.seed}  ",
        f"Concurrency: {config.concurrency}  ",
        f"Executors: {config.executor_count}  ",
        f"Compose project: `{config.compose_project}`",
        "",
        "## Configuration",
        "",
        "| Setting | Value |",
        "| --- | --- |",
        *[f"| {name} | {value} |" for name, value in sorted(config.environment.items())],
        "",
        "## Reliability",
        "",
        f"Continuum trials: {summary['continuum_trials']}  ",
        f"Correct: {summary['correct_trials']}  ",
        f"Incorrect: {summary['incorrect_trials']}  ",
        f"Correctness rate: {_percent(summary['correctness_rate'])}  ",
        f"Workflow completion rate: {_percent(summary['workflow_completion_rate'])}  ",
        f"Recovery success rate: {_percent(summary['recovery_success_rate'])}",
        "",
        "| Scenario | Trials | Correct | Incorrect | Injected failures |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for name, row in summary["scenario_breakdown"].items():
        lines.append(
            f"| {name} | {row['trials']} | {row['correct']} | {row['incorrect']} | "
            f"{row['injected_failures']} |"
        )
    lines.extend(
        [
            "",
            "## Side effects and state integrity",
            "",
            f"Duplicate external side effects: {summary['duplicate_side_effects']}  ",
            "Duplicate external side-effect trial rate: "
            f"{_percent(summary['duplicate_side_effect_rate'])}  ",
            f"Lost external side effects: {summary['lost_side_effects']}  ",
            f"Lost external side-effect trial rate: {_percent(summary['lost_side_effect_rate'])}  ",
            f"Duplicate state transitions: {summary['duplicate_transitions']}  ",
            f"Duplicate-transition trial rate: {_percent(summary['duplicate_transition_rate'])}  ",
            f"Duplicate logical outbox events: {summary['duplicate_outbox_events']}  ",
            f"Intentionally unsafe baseline: {summary['unsafe_baseline_trials']} trials, "
            f"{summary['unsafe_baseline_duplicate_side_effects']} duplicate effects.",
            "",
            "## Recovery and performance",
            "",
            f"Recovery time (n={summary['recovery_time']['sample_size']}): "
            f"median {summary['recovery_time']['median_ms']} ms, "
            f"p95 {summary['recovery_time']['p95_ms']} ms, "
            f"max {summary['recovery_time']['max_ms']} ms.  ",
            f"Workflow elapsed (n={summary['workflow_elapsed_time']['sample_size']}): "
            f"median {summary['workflow_elapsed_time']['median_ms']} ms, "
            f"p95 {summary['workflow_elapsed_time']['p95_ms']} ms.  ",
            f"Fault detection (n={summary['fault_detection_latency']['sample_size']}): "
            f"median {summary['fault_detection_latency']['median_ms']} ms, "
            f"p95 {summary['fault_detection_latency']['p95_ms']} ms.  ",
            f"Event to durable attempt (n={summary['event_to_attempt_time']['sample_size']}): "
            f"median {summary['event_to_attempt_time']['median_ms']} ms, "
            f"p95 {summary['event_to_attempt_time']['p95_ms']} ms.  ",
            f"Pending attempt to claim (n={summary['pending_to_claim_time']['sample_size']}): "
            f"median {summary['pending_to_claim_time']['median_ms']} ms, "
            f"p95 {summary['pending_to_claim_time']['p95_ms']} ms.  ",
            f"Outbox publish delay (n={summary['outbox_publish_delay']['sample_size']}): "
            f"median {summary['outbox_publish_delay']['median_ms']} ms, "
            f"p95 {summary['outbox_publish_delay']['p95_ms']} ms.  ",
            f"Measured campaign runtime: {summary['campaign_runtime_seconds']} seconds.  ",
            f"Workflow throughput: {summary['workflow_throughput_per_second']} workflows/s.  ",
            f"Step throughput: {summary['step_throughput_per_second']} successful steps/s.",
            "",
            "## Incorrect trials",
            "",
        ]
    )
    incorrect = [trial for trial in trials if not trial.correct]
    lines.extend(
        [
            f"- `{trial.trial_id}` {trial.scenario_name}: {trial.failure_reason}"
            for trial in incorrect
        ]
        or ["None."]
    )
    lines.extend(
        [
            "",
            "## Environment and scope",
            "",
            "Local Docker Compose, one Kafka KRaft broker, local PostgreSQL and independent "
            "mock-payments PostgreSQL. Container/process faults are deliberately injected. "
            "These measurements are local failure evidence, not production-scale reliability "
            "or multi-broker HA guarantees.",
            "",
            "Per-trial JSON evidence is in `trials.jsonl`; `config.json` records the seed, "
            "revision and runtime settings. All rates and percentiles are recalculated "
            "from raw trials.",
            "",
        ]
    )
    return "\n".join(lines)
