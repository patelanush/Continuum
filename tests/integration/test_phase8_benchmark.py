"""A tiny benchmark through the real isolated FastAPI/Kafka/executor stack."""

import json
import os
from pathlib import Path

import pytest
from phase8_benchmark import BenchSettings, report_markdown, run_batch
from phase8_stats import aggregate_runs


@pytest.mark.faultlab
async def test_tiny_api_driven_benchmark_generates_a_durable_summary(tmp_path: Path) -> None:
    if os.getenv("RUN_FAULTLAB_DOCKER") != "1":
        pytest.skip("requires the isolated FaultLab stack")
    directory = tmp_path
    settings = BenchSettings(
        api_url="http://127.0.0.1:28000",
        payments_url="http://127.0.0.1:18001",
        database_url="postgresql+asyncpg://durable:durable@127.0.0.1:55435/durable",
        kafka_bootstrap="127.0.0.1:19093",
        timeout_seconds=60,
    )
    run = await run_batch(
        settings, "scaling", workflows=10, concurrency=5, experiment_id="pytest-phase8-tiny"
    )
    assert run["successful_workflows"] == 10
    assert run["total_steps"] == 10
    assert run["unexpected_dlq_delta"] == 0
    assert run["duplicate_transition_count"] == 0
    summary = {
        "benchmark_type": "scaling",
        "experiment_id": "pytest-phase8-tiny",
        "git_commit": "test",
        "telemetry": "off",
        "runs": [run],
        "aggregate": aggregate_runs([run]),
    }
    (directory / "summary.json").write_text(json.dumps(summary))
    (directory / "summary.md").write_text(report_markdown(summary))
    assert (
        json.loads((directory / "summary.json").read_text())["aggregate"]["successful_workflows"]
        == 10
    )
