"""Aggregate only persisted raw trials; unsafe controls remain a separate cohort."""

import math
from collections import defaultdict
from statistics import median
from typing import Any

from durable_agent_runtime.faultlab.models import TrialResult


def percentile(values: list[float], percent: float) -> float | None:
    if not values:
        return None
    if not 0 < percent <= 100:
        raise ValueError("percent must be in (0, 100]")
    ordered = sorted(values)
    return ordered[math.ceil(percent / 100 * len(ordered)) - 1]


def duration_stats(values: list[float]) -> dict[str, float | int | None]:
    return {
        "sample_size": len(values),
        "median_ms": round(median(values), 3) if values else None,
        "p95_ms": percentile(values, 95),
        "max_ms": max(values) if values else None,
    }


def ratio(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def aggregate(trials: list[TrialResult], elapsed_seconds: float | None = None) -> dict[str, Any]:
    continuum = [item for item in trials if item.scenario_name != "unsafe-refund-retry-baseline"]
    unsafe = [item for item in trials if item.scenario_name == "unsafe-refund-retry-baseline"]
    by_scenario: dict[str, list[TrialResult]] = defaultdict(list)
    for trial in trials:
        by_scenario[trial.scenario_name].append(trial)
    recovering = [item for item in continuum if item.recovery_required]
    expected_success = [item for item in continuum if item.expected_terminal_status == "SUCCEEDED"]
    external_operations = [item for item in continuum if item.expected_side_effect_count > 0]
    workflow_times = [
        item.workflow_elapsed_ms for item in continuum if item.workflow_elapsed_ms is not None
    ]
    recovery_times = [
        item.recovery_time_ms for item in recovering if item.recovery_time_ms is not None
    ]
    detection_times = [
        item.fault_detection_latency_ms
        for item in continuum
        if item.fault_detection_latency_ms is not None
    ]
    event_scheduling_times = [
        item.event_to_attempt_ms for item in continuum if item.event_to_attempt_ms is not None
    ]
    claim_times = [
        item.attempt_pending_to_claim_ms
        for item in continuum
        if item.attempt_pending_to_claim_ms is not None
    ]
    outbox_times = [
        item.outbox_publish_delay_ms
        for item in continuum
        if item.outbox_publish_delay_ms is not None
    ]
    workflows = sum(item.actual_terminal_status is not None for item in continuum)
    succeeded_workflows = sum(item.actual_terminal_status == "SUCCEEDED" for item in continuum)
    steps = sum(int(item.notes.get("steps_succeeded", 0)) for item in continuum)
    scenario_summary = {
        name: {
            "trials": len(items),
            "correct": sum(item.correct for item in items),
            "incorrect": sum(not item.correct for item in items),
            "injected_failures": sum(item.injected_failure_count for item in items),
            "duplicate_side_effects": sum(item.duplicate_side_effect_count for item in items),
            "lost_side_effects": sum(item.lost_side_effect_count for item in items),
        }
        for name, items in sorted(by_scenario.items())
    }
    return {
        "total_trials": len(trials),
        "continuum_trials": len(continuum),
        "unsafe_baseline_trials": len(unsafe),
        "unsafe_baseline_incorrect_trials": sum(not item.correct for item in unsafe),
        "all_incorrect_trials": sum(not item.correct for item in trials),
        "correct_trials": sum(item.correct for item in continuum),
        "incorrect_trials": sum(not item.correct for item in continuum),
        "correctness_rate": ratio(sum(item.correct for item in continuum), len(continuum)),
        "fault_injected_trials": sum(item.injected_failure_count > 0 for item in continuum),
        "injected_failures": sum(item.injected_failure_count for item in continuum),
        "workflow_completion_rate": ratio(
            sum(item.actual_terminal_status == "SUCCEEDED" for item in expected_success),
            len(expected_success),
        ),
        "recovery_success_rate": ratio(sum(item.recovered for item in recovering), len(recovering)),
        "recovered_trials": sum(item.recovered for item in recovering),
        "duplicate_side_effects": sum(item.duplicate_side_effect_count for item in continuum),
        "lost_side_effects": sum(item.lost_side_effect_count for item in continuum),
        "duplicate_side_effect_rate": ratio(
            sum(item.duplicate_side_effect_count > 0 for item in external_operations),
            len(external_operations),
        ),
        "lost_side_effect_rate": ratio(
            sum(item.lost_side_effect_count > 0 for item in external_operations),
            len(external_operations),
        ),
        "duplicate_transitions": sum(item.duplicate_transition_count for item in continuum),
        "duplicate_transition_rate": ratio(
            sum(item.duplicate_transition_count > 0 for item in continuum), len(continuum)
        ),
        "duplicate_outbox_events": sum(item.outbox_duplicate_count for item in continuum),
        "mean_attempt_count": (
            round(sum(item.attempt_count for item in continuum) / workflows, 3)
            if workflows
            else None
        ),
        "workflows_processed": workflows,
        "workflows_succeeded": succeeded_workflows,
        "steps_succeeded": steps,
        "workflow_throughput_per_second": (
            round(succeeded_workflows / elapsed_seconds, 6)
            if elapsed_seconds and elapsed_seconds > 0
            else None
        ),
        "step_throughput_per_second": (
            round(steps / elapsed_seconds, 6) if elapsed_seconds and elapsed_seconds > 0 else None
        ),
        "campaign_runtime_seconds": elapsed_seconds,
        "recovery_time": duration_stats(recovery_times),
        "workflow_elapsed_time": duration_stats(workflow_times),
        "fault_detection_latency": duration_stats(detection_times),
        "event_to_attempt_time": duration_stats(event_scheduling_times),
        "pending_to_claim_time": duration_stats(claim_times),
        "outbox_publish_delay": duration_stats(outbox_times),
        "lease_expirations": sum(item.lease_expiration_count for item in continuum),
        "kafka_redeliveries_observed": sum(item.kafka_redelivery_count or 0 for item in continuum),
        "unsafe_baseline_duplicate_side_effects": sum(
            item.duplicate_side_effect_count for item in unsafe
        ),
        "scenario_breakdown": scenario_summary,
        "incorrect_trial_ids": [str(item.trial_id) for item in continuum if not item.correct],
    }
