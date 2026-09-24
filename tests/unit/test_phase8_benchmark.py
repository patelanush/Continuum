"""Benchmark arithmetic, configuration, and report contract."""

import json

import pytest
from phase8_benchmark import (
    BenchSettings,
    report_markdown,
    specification,
    workload_kinds,
)
from phase8_stats import aggregate_runs, distribution, percentile, saturation_point, scaling


def test_configuration_and_deterministic_mix() -> None:
    with pytest.raises(ValueError):
        BenchSettings(project="some-other-project")
    with pytest.raises(ValueError):
        BenchSettings(timeout_seconds=0)
    kinds = workload_kinds("mixed", 20)
    assert {kind: kinds.count(kind) for kind in set(kinds)} == {
        "normal": 8,
        "support": 5,
        "refund": 4,
        "coding": 3,
    }
    assert workload_kinds("scaling", 10) == ["scaling"] * 10
    request, customer = specification("support", 2, "experiment")
    assert customer == "bench-experiment-2"
    assert request["input"]["experiment_id"] == "experiment"
    assert request["steps"][0]["input"]["provider"] == "fake"


def test_percentiles_and_scaling_math() -> None:
    assert percentile([], 0.95) is None
    assert percentile([1, 2, 3, 4, 5], 0.95) == pytest.approx(4.8)
    assert distribution([1, 2, 3])["median"] == 2
    assert scaling(10, 24, 3)["speedup"] == pytest.approx(2.4)
    assert scaling(10, 24, 3)["efficiency"] == pytest.approx(0.8)
    assert saturation_point({1: 10, 3: 24, 5: 25}) == 5
    assert saturation_point({1: 10, 3: 24, 5: 32}) is None


def test_aggregate_and_report_are_serializable() -> None:
    run = {
        "workflow_count": 10,
        "successful_workflows": 10,
        "failed_workflows": 0,
        "recovery_count": 0,
        "duplicate_side_effect_count": 0,
        "lost_side_effect_count": 0,
        "duplicate_transition_count": 0,
        "workflow_throughput_per_second": 2.5,
        "step_throughput_per_second": 2.5,
        "wall_clock_duration_seconds": 4.0,
        "workflow_latency_ms": {"median": 500.0, "p95": 750.0},
        "execution_claim_latency_ms": {"median": 100.0, "p95": 200.0},
        "queue_wait_time_ms": {"median": 50.0, "p95": 100.0},
        "outbox_publish_delay_ms": {"median": 20.0, "p95": 40.0},
        "attempt_duration_ms": {"median": 250.0, "p95": 300.0},
    }
    aggregate = aggregate_runs([run, run])
    assert aggregate["successful_workflows"] == 20
    assert aggregate["means"]["workflow_throughput_per_second"] == 2.5
    summary = {
        "benchmark_type": "scaling",
        "experiment_id": "test",
        "git_commit": "abc123",
        "telemetry": "off",
        "runs": [run],
        "aggregate": aggregate,
    }
    assert "Claim p95" in report_markdown(summary)
    assert json.loads(json.dumps(summary))["aggregate"]["total_workflows"] == 20
