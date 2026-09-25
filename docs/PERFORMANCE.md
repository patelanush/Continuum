# Local performance and scaling

Continuum's final benchmark asks how the **durable API → outbox → Kafka → event worker → execution attempt → executor** path scales, and whether its correctness checks survive a worker crash during sustained load. These are local Docker Desktop measurements, not production capacity or availability estimates. The machine had eight logical CPU cores and about 8 GiB of memory, with PostgreSQL 16 and one Kafka 3.9.1 broker. The `continuum.step.ready.v1` topic had three partitions throughout.

## Method

The [benchmark runner](../scripts/phase8_benchmark.py) submits every workflow through FastAPI. It does not invoke workflow services directly. After completion it reads durable PostgreSQL state and independently queries mock-payments. For coding workflows it also checks patch checkpoints, passing test records, and actual Git history in the persistent workspace volume. It rejects nonterminal workflows, pending/running attempts, unpublished outbox events, pending approvals, unexpected DLQ messages, duplicate transitions, and duplicate/lost tested refund effects. A UTC-versus-process-clock check rejects runs interrupted by host suspension.

Each executor/worker configuration had fresh `continuum-bench` Compose volumes, a unique experiment ID, the clean benchmark revision `dde3460f`, 20 unmeasured warmup workflows, 25 client requests in flight for scaling, and telemetry disabled. The fast scaling workload was one 250 ms `slow_noop` step per workflow. API submission, queue drain, and terminal completion are included in throughput wall time. Three repetitions were used for the central 180-workflow comparisons and the 500-workflow 8/12/16-executor diagnostics. Raw per-workflow records are in ignored `artifacts/benchmarks/<experiment-id>/`; the [committed JSON](../benchmarks/results/phase8-scaling.json) preserves each run's throughput and summary statistics without workflow IDs.

| Executors | Workers | Workflows/run | Runs | Mean workflows/s | Mean median latency | Mean p95 latency | Mean claim p95 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3 | 180 | 3 | 2.516 | 35.05 s | 65.42 s | 62.97 s |
| 3 | 3 | 180 | 3 | 7.321 | 11.25 s | 20.85 s | 18.92 s |
| 5 | 3 | 180 | 3 | 11.805 | 6.41 s | 11.51 s | 9.67 s |

Three executors gave 2.91× the one-executor throughput at this load (97.0% scaling efficiency); five gave 4.69× (93.8%). All 1,620 repeated core workflows succeeded. The 60- and 500-workflow levels also ran at every core replica count. At 500 workflows, the one/three/five-executor throughputs were 2.511, 7.379, and 12.285 workflows/s; each run drained with 500/500 successes. The 500-workflow one-executor p95 latency was 183.38 s, of which claim wait accounted for 179.53 s; p95 attempt execution was only 0.288 s. Thus more executors directly relieved pending-attempt wait in the core range.

## Where scaling stopped helping locally

Five executors had not reached the knee, so the same 500-workflow workload was repeated at 8, 12, and 16 executors with three workers:

| Executors | Runs | Mean workflows/s | Per-run workflows/s | Mean p95 latency | Mean claim p95 | Mean outbox p95 | Incidental recoveries |
|---:|---:|---:|---|---:|---:|---:|---:|
| 8 | 3 | 18.056 | 17.872, 17.913, 18.384 | 16.20 s | 11.07 s | 5.81 s | 0 |
| 12 | 3 | 22.174 | 23.241, 23.787, 19.495 | 9.41 s | 2.53 s | 7.58 s | 0 |
| 16 | 3 | 8.641 | 4.108, 7.714, 14.102 | 32.43 s | 0.98 s | 29.44 s | 10 |

The operational knee in this local test was **between 12 and 16 executors**: adding four replicas lowered mean throughput by 61.0%, raised p95 latency, and caused 8 and 2 lease expirations in the first two 16-executor runs. All ten replacement attempts completed and all 1,500 workflows succeeded, but 16 executors are not a sensible local setting. Twelve executors gave the highest measured mean throughput. Its 500-workflow throughput was 8.83× the one-executor 500-workflow run; that ratio is specific to this machine, short step, and API submission pattern. A production lease should be sized against expected scheduler delays, and saturation monitoring should precede replica increases.

At 12 executors, claim p95 fell to 2.53 s while outbox p95 grew to 7.58 s. At 16, claim p95 was below 1 s but outbox p95 reached 29.44 s. This shift means executors were no longer the main source of wait. A separate eight-executor PostgreSQL probe sampled zero lock waiters, while pending attempts peaked at 286 and unpublished outbox events at 223. One contemporaneous Docker snapshot showed dispatcher CPU at 58.47%, PostgreSQL at 44.32%, and each executor at 6.5–9.4%; these are point samples, not utilization averages. The 16-executor snapshots showed a busy API/PostgreSQL/Kafka and slower Docker interactions. Together, these observations support an **upstream submission/outbox/host-scheduling limit** at high replica counts. They do not prove one SQL lock or one Kafka component is the sole bottleneck. The dispatcher still uses short outbox claims; this benchmark did not find evidence to reverse the Phase 4 lock-boundary design.

One versus three event workers was tested at 12 executors with three Kafka partitions and three 500-workflow repetitions. One worker averaged 22.963 workflows/s; three workers averaged 22.174/s. The 3.6% difference is within run variability and provides no evidence that the three partitions limited this workload. There was no justified reason to raise the partition count. Consumer lag was not claimed because the benchmark did not establish a reliable per-group lag measurement.

## Mixed durable-agent workload

The [mixed result](../benchmarks/results/phase8-mixed.md) used five executors, three workers, FakeModelProvider, and 100 measured workflows: 40 normal multi-step, 25 support-agent, 20 direct-refund, and 15 coding-agent. They produced 140 steps, 180 agent turns/model calls, 140 tool calls, 105 sandbox commands, 15 passing test records, and 15 approved local commits. All 100 succeeded in 79.43 s (1.259 workflows/s), with a 50.72 s median and 72.40 s p95 workflow latency. The independent payment ledger held exactly 45 expected refunds; no duplicate/lost effects or duplicate transitions were found. This workload cannot be compared directly to one-step `slow_noop` throughput. An earlier mixed diagnostic was excluded from timing after a host suspend made UTC time and process time disagree.

## Failure during sustained load

The [failure report](../benchmarks/results/phase8-load-failure.md) used three executors and three workers with 50% five-second `slow_noop`, 25% direct refund, and 25% support-agent refund. In the 300-workflow executor scenario, the SIGKILL targeted a worker owning a running attempt while 297 workflows were active. Its attempt expired, one replacement succeeded, and all 300 workflows finished. The mock-payments service contained all 150 expected refunds exactly once; no duplicate transitions, unexpected DLQ messages, orphan attempts, pending approvals, or outbox backlog remained. The replacement was claimed **258.95 s after expiry** because it joined the normal pending queue behind the sustained workload. Recovery correctness held, but recovery latency under backlog is a real limitation.

A separate event-worker SIGKILL occurred with 13 active workflows during submissions. All 120 measured workflows finished, and all 60 expected refunds occurred once. The sampled published and consumed counts were equal at the kill instant, so this scenario proves continued processing after a worker death but does not establish an actual redelivery. Dedicated Kafka duplicate/replay FaultLab scenarios cover redelivery safety.

## Observability and post-load health

With the Phase 7 stack enabled, a separate 100-workflow API/Kafka/executor load completed 100/100; Prometheus's workflow-start counter increased from 1 to 101, and Tempo returned a 10-span load trace across API, dispatcher, event worker, and executor. After load, Collector scrape, two Grafana datasources, four provisioned dashboards, and 44 dashboard queries still passed `make observability-check`. The API and PostgreSQL health checks passed after the fault runs; the benchmark's next warmup workflows also confirmed the normal path still worked. These observations establish local post-load function, not high availability.

## Reproduce

```bash
uv sync
make benchmark-prepare
make benchmark-scaling EXECUTORS=1 WORKFLOWS=180 REPETITIONS=3
make benchmark-clean
make benchmark-prepare EXECUTORS=3 WORKERS=3
make benchmark-scaling EXECUTORS=3 WORKFLOWS=180 REPETITIONS=3
make benchmark-clean
```

Repeat the same clean/prepare/run cycle for five executors. `make benchmark-mixed` and `make benchmark-load-failure` run the other classes after preparing their matching replica counts. The runner writes `config.json`, `runs.jsonl`, `summary.json`, and `summary.md` under ignored `artifacts/benchmarks/<experiment-id>/`. It records Git SHA, clean/dirty state, timestamps, environment, partitions, and measured workload. `make benchmark-clean` deletes only the isolated benchmark project's containers and volumes. Core `make up` volumes and historical FaultLab artifacts are unaffected.

## Limits of these findings

The broker is single-node; these runs do not test Kafka HA, remote deployments, hostile code isolation, authentication, or arbitrary non-idempotent external APIs. Docker Desktop scheduling and API client submission contribute to observed throughput. CPU values were sparse snapshots, not a sampled capacity model. The five-second benchmark lease intentionally makes missed heartbeats visible; the 16-executor result should be read as oversubscription of this local environment, not a universal replica limit. No Phase 4 historical result was overwritten.
