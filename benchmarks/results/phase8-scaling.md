# Phase 8 scaling benchmark

Benchmark revision: `dde3460f1a5cbb49b92593b1c1dcf25aead91695`. Local 8-core arm64 Docker Desktop; PostgreSQL 16, Kafka 3.9.1, three step-ready partitions, telemetry off. Each configuration used fresh benchmark volumes and 20 unmeasured warmup workflows. All measured workflows used the real FastAPI → PostgreSQL outbox → Kafka → event worker → durable attempt → executor path with a deterministic 250 ms `slow_noop` step.

## Core comparison: 180 workflows per run, three repetitions

| Executors | Mean throughput (workflows/s) | Mean median latency | Mean p95 latency | Mean claim p95 | Speedup | Efficiency |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.516 | 35.05 s | 65.42 s | 62.97 s | 1.00× | 1.00 |
| 3 | 7.321 | 11.25 s | 20.85 s | 18.92 s | 2.91× | 0.97 |
| 5 | 11.805 | 6.41 s | 11.51 s | 9.67 s | 4.69× | 0.94 |

The table reports the mean of three run summaries; per-run throughput and all three load levels are in [JSON](phase8-scaling.json). All 1,620 core repeated workflows succeeded with zero recoveries, duplicate transitions, or unexpected DLQ messages. The 60- and 500-workflow levels also completed successfully at 1, 3, and 5 executors.

## Sustained 500-workflow diagnostic

| Executors | Workers | Repetitions | Mean throughput (workflows/s) | Mean p95 latency | Mean claim p95 | Mean outbox p95 | Recoveries |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3 | 1 | 2.511 | 183.38 s | 179.53 s | 3.63 s | 0 |
| 3 | 3 | 1 | 7.379 | 57.42 s | 53.41 s | 3.94 s | 0 |
| 5 | 3 | 1 | 12.285 | 31.86 s | 27.48 s | 4.22 s | 0 |
| 8 | 3 | 3 | 18.056 | 16.20 s | 11.07 s | 5.81 s | 0 |
| 12 | 3 | 3 | 22.174 | 9.41 s | 2.53 s | 7.58 s | 0 |
| 16 | 3 | 3 | 8.641 | 32.43 s | 0.98 s | 29.44 s | 10 |

A separate 12-executor, **one-worker** experiment averaged 22.963 workflows/s across three 500-workflow runs, versus 22.174/s with three workers. The 3.6% difference is within observed local run variation; the Kafka partition count was not changed.

## Interpretation

Scaling remained useful through 12 executors. At 16, throughput fell sharply and varied from 4.11 to 14.10 workflows/s; the first two runs had 8 and 2 lease expirations, all recovered. Mean p95 outbox delay rose to 29.44 seconds while claim p95 fell below 1 second. The local useful range therefore ends before 16 executors; 12 is the best measured configuration on this machine. This is a local saturation observation, not a general capacity limit.

A separate 8-executor PostgreSQL probe sampled zero lock waiters, while pending attempts peaked at 286 and unpublished outbox events at 223. One Docker snapshot showed dispatcher and PostgreSQL using more CPU than an individual executor. These samples and the timing shift toward outbox/queue delay point to upstream submission/dispatch pressure and local scheduling at high replica counts; they do not isolate one SQL query as the sole bottleneck.

Raw per-workflow records remain in ignored `artifacts/benchmarks/<experiment-id>/` on the measurement machine. The committed JSON keeps per-run values, configuration, revision, and correctness totals without workflow IDs or host paths.
