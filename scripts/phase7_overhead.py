"""Measure end-to-end workflow latency and throughput with telemetry OFF and ON."""

import asyncio
import json
import os
import statistics
from pathlib import Path
from time import monotonic
from typing import Any

import httpx

RUN_COUNT = 150
CONCURRENCY = 20
MODES = ("off", "on", "off", "on")
SERVICES = ("api", "dispatcher", "worker", "executor", "recovery-scheduler", "mock-payments")


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


async def compose_mode(mode: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "up",
        "-d",
        "--wait",
        "--no-build",
        "--scale",
        "worker=3",
        "--scale",
        "executor=3",
        *SERVICES,
        env={**os.environ, "OTEL_ENABLED": str(mode == "on").lower()},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode:
        raise RuntimeError(f"Compose toggle failed: {(stdout + stderr).decode()[-3000:]}")
    await asyncio.sleep(3)


async def workflow(client: httpx.AsyncClient, semaphore: asyncio.Semaphore) -> float:
    async with semaphore:
        created = await client.post(
            "/api/v1/workflows",
            json={
                "workflow_type": "coding-demo",
                "steps": [{"name": "benchmark", "step_type": "noop", "input": {}}],
            },
        )
        created.raise_for_status()
        workflow_id = created.json()["id"]
        began = monotonic()
        started = await client.post(f"/api/v1/workflows/{workflow_id}/start")
        started.raise_for_status()
        deadline = began + 120
        while monotonic() < deadline:
            response = await client.get(f"/api/v1/workflows/{workflow_id}")
            response.raise_for_status()
            status = response.json()["status"]
            if status == "SUCCEEDED":
                return monotonic() - began
            if status in {"FAILED", "CANCELLED"}:
                raise AssertionError(f"Benchmark workflow ended {status}")
            await asyncio.sleep(0.1)
        raise TimeoutError(f"Benchmark workflow {workflow_id} timed out")


async def run_once(mode: str, ordinal: int) -> dict[str, Any]:
    await compose_mode(mode)
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=20) as client:
        semaphore = asyncio.Semaphore(CONCURRENCY)
        began = monotonic()
        latencies = await asyncio.gather(*(workflow(client, semaphore) for _ in range(RUN_COUNT)))
        elapsed = monotonic() - began
    return {
        "mode": mode,
        "ordinal": ordinal,
        "workflows": RUN_COUNT,
        "concurrency": CONCURRENCY,
        "median_seconds": round(statistics.median(latencies), 4),
        "p95_seconds": round(percentile(latencies, 0.95), 4),
        "throughput_per_second": round(RUN_COUNT / elapsed, 4),
        "wall_seconds": round(elapsed, 4),
    }


async def main() -> None:
    runs = []
    try:
        for ordinal, mode in enumerate(MODES, 1):
            row = await run_once(mode, ordinal)
            runs.append(row)
            print(json.dumps(row), flush=True)
    finally:
        await compose_mode("on")
    aggregate = {}
    for mode in ("off", "on"):
        matching = [row for row in runs if row["mode"] == mode]
        aggregate[mode] = {
            key: round(statistics.mean(row[key] for row in matching), 4)
            for key in ("median_seconds", "p95_seconds", "throughput_per_second")
        }
    impact = {
        "median_latency_percent": round(
            (aggregate["on"]["median_seconds"] / aggregate["off"]["median_seconds"] - 1) * 100,
            2,
        ),
        "p95_latency_percent": round(
            (aggregate["on"]["p95_seconds"] / aggregate["off"]["p95_seconds"] - 1) * 100,
            2,
        ),
        "throughput_percent": round(
            (
                aggregate["on"]["throughput_per_second"] / aggregate["off"]["throughput_per_second"]
                - 1
            )
            * 100,
            2,
        ),
    }
    summary = {
        "workload": "single-step noop workflow through API, outbox, Kafka, worker, executor",
        "total_workflows": RUN_COUNT * len(MODES),
        "concurrency": CONCURRENCY,
        "order": list(MODES),
        "runs": runs,
        "mean_of_run_summaries": aggregate,
        "relative_impact": impact,
        "environment": "local Docker Compose, 3 workers, 3 executors, same images and database",
    }
    json_path = Path("benchmarks/results/phase7-observability-overhead.json")
    markdown_path = Path("benchmarks/results/phase7-observability-overhead.md")
    await asyncio.to_thread(json_path.write_text, json.dumps(summary, indent=2) + "\n")
    markdown = (
        "# Phase 7 observability overhead\n\n"
        "Local Docker Compose, three event workers, three executors, 20 concurrent clients. "
        "Each of four runs completed 150 identical one-step noop workflows. "
        "Modes were OFF, ON, OFF, ON; all runs used the same built images and database.\n\n"
        "| Mode | Run | Workflows | Median (s) | p95 (s) | Throughput (workflows/s) |\n"
        "| --- | ---: | ---: | ---: | ---: | ---: |\n"
        + "".join(
            f"| {row['mode']} | {row['ordinal']} | {row['workflows']} | "
            f"{row['median_seconds']} | {row['p95_seconds']} | "
            f"{row['throughput_per_second']} |\n"
            for row in runs
        )
        + "\nMean of run summaries: "
        f"OFF median {aggregate['off']['median_seconds']} s, "
        f"ON median {aggregate['on']['median_seconds']} s; "
        f"OFF p95 {aggregate['off']['p95_seconds']} s, "
        f"ON p95 {aggregate['on']['p95_seconds']} s; "
        f"OFF throughput {aggregate['off']['throughput_per_second']} workflows/s, "
        f"ON throughput {aggregate['on']['throughput_per_second']} workflows/s.\n\n"
        f"Relative impact: median {impact['median_latency_percent']:+.2f}%, "
        f"p95 {impact['p95_latency_percent']:+.2f}%, "
        f"throughput {impact['throughput_percent']:+.2f}%. "
        "These local measurements include scheduling and polling noise "
        "and are not a production SLO.\n"
    )
    await asyncio.to_thread(markdown_path.write_text, markdown)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
