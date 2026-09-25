# Phase 8 mixed durable-agent workload

Benchmark revision: `dde3460f1a5cbb49b92593b1c1dcf25aead91695`; experiment `9c47c658-3fc7-4c44-867b-dac0b8f66455`. Fresh isolated Compose volumes, five executors, three event workers, FakeModelProvider, telemetry off, 20 unmeasured warmup workflows, and 20 client requests in flight.

The 100 measured workflows comprised 40 normal multi-step, 25 support-agent, 20 direct-refund, and 15 coding-agent workflows. They produced 140 steps, 180 agent turns, 180 model calls, 140 tool calls, 105 sandbox commands, 15 passing test runs, and 15 approvals. The independent payment service contained exactly 45 expected refunds.

All 100 workflows succeeded in 79.43 s (1.259/s). Workflow median latency was 50.72 s and p95 was 72.40 s. There were zero recoveries, duplicate/lost tested effects, duplicate transitions, or unexpected DLQ messages. The queue drained. Each coding workspace had one patch checkpoint, a passing test record, and one actual local Git commit matching durable state.

This workload is much heavier than one-step `slow_noop`; its throughput should not be compared directly to infrastructure scaling throughput. One earlier mixed diagnostic was excluded from timing because the host suspended and UTC elapsed time diverged from the process timer. [Machine-readable summary](phase8-mixed.json).
