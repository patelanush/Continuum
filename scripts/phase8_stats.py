"""Small, deterministic statistics and reporting helpers for local load runs."""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from itertools import pairwise
from typing import Any


def percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    if not 0 <= fraction <= 1:
        raise ValueError("fraction must be between zero and one")
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def distribution(values: Sequence[float]) -> dict[str, float | int | None]:
    return {
        "samples": len(values),
        "median": percentile(values, 0.5),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
    }


def throughput(successes: int, wall_seconds: float) -> float:
    if wall_seconds <= 0 or successes < 0:
        raise ValueError("wall_seconds must be positive and successes nonnegative")
    return successes / wall_seconds


def scaling(baseline: float, candidate: float, replicas: int) -> dict[str, float]:
    if baseline <= 0 or candidate < 0 or replicas < 1:
        raise ValueError("invalid scaling inputs")
    speedup = candidate / baseline
    return {"speedup": speedup, "efficiency": speedup / replicas}


def saturation_point(throughputs: dict[int, float], *, threshold: float = 0.10) -> int | None:
    """First measured replica count with less than threshold marginal gain."""
    if not 0 <= threshold < 1:
        raise ValueError("invalid threshold")
    counts = sorted(throughputs)
    for previous, current in pairwise(counts):
        if throughputs[previous] <= 0:
            raise ValueError("throughputs must be positive")
        gain = throughputs[current] / throughputs[previous] - 1
        if gain < threshold:
            return current
    return None


def aggregate_runs(runs: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not runs:
        raise ValueError("at least one run is required")
    numeric = (
        "workflow_throughput_per_second",
        "step_throughput_per_second",
        "wall_clock_duration_seconds",
    )
    means: dict[str, float | None] = {
        key: statistics.mean(float(run[key]) for run in runs) for key in numeric
    }
    for timing in (
        "workflow_latency_ms",
        "execution_claim_latency_ms",
        "queue_wait_time_ms",
        "outbox_publish_delay_ms",
        "attempt_duration_ms",
    ):
        for percentile_name in ("median", "p95"):
            values = [
                float(run[timing][percentile_name])
                for run in runs
                if run[timing][percentile_name] is not None
            ]
            means[f"{timing}_{percentile_name}"] = statistics.mean(values) if values else None
    return {
        "run_count": len(runs),
        "means": means,
        "total_workflows": sum(int(run["workflow_count"]) for run in runs),
        "successful_workflows": sum(int(run["successful_workflows"]) for run in runs),
        "failed_workflows": sum(int(run["failed_workflows"]) for run in runs),
        "recoveries": sum(int(run["recovery_count"]) for run in runs),
        "duplicate_side_effects": sum(int(run["duplicate_side_effect_count"]) for run in runs),
        "lost_side_effects": sum(int(run["lost_side_effect_count"]) for run in runs),
        "duplicate_transitions": sum(int(run["duplicate_transition_count"]) for run in runs),
    }
