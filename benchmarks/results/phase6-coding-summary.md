# FaultLab Experiment

Experiment ID: `7edc7c61-b36f-4219-9f7b-42fd4ea22d21`  
Generated: 2026-09-23T16:40:38.396962+00:00  
Git commit: `fffc5a5ba57fa1caa1dc6d4a9297e724dfb2e70d` (dirty: false)  
Seed: 42  
Concurrency: 4  
Executors: 3  
Compose project: `continuum-faultlab`

## Configuration

| Setting | Value |
| --- | --- |
| cpu_count | 8 |
| executor_heartbeat_seconds | 1.0 |
| executor_lease_seconds | 5.0 |
| executor_poll_interval_seconds | 0.1 |
| machine | arm64 |
| outbox_publish_lease_seconds | 11.0 |
| recovery_scan_interval_seconds | 0.2 |
| system | Darwin |

## Reliability

Continuum trials: 11  
Correct: 11  
Incorrect: 0  
Correctness rate: 100.000%  
Workflow completion rate: 100.000%  
Recovery success rate: 100.000%

| Scenario | Trials | Correct | Incorrect | Injected failures |
| --- | ---: | ---: | ---: | ---: |
| coding-baseline | 1 | 1 | 0 | 0 |
| coding-command-timeout | 1 | 1 | 0 | 0 |
| coding-crash-after-commit | 1 | 1 | 0 | 1 |
| coding-crash-after-decision | 1 | 1 | 0 | 1 |
| coding-crash-after-patch | 1 | 1 | 0 | 1 |
| coding-crash-after-tests | 1 | 1 | 0 | 1 |
| coding-crash-before-commit | 1 | 1 | 0 | 1 |
| coding-executor-killed | 1 | 1 | 0 | 1 |
| coding-path-traversal | 1 | 1 | 0 | 0 |
| coding-sandbox-killed | 1 | 1 | 0 | 1 |
| coding-workspace-divergence | 1 | 1 | 0 | 1 |

## Side effects and state integrity

Duplicate external side effects: 0  
Duplicate external side-effect trial rate: n/a  
Lost external side effects: 0  
Lost external side-effect trial rate: n/a  
Duplicate state transitions: 0  
Duplicate-transition trial rate: 0.000%  
Duplicate logical outbox events: 0  
Intentionally unsafe baseline: 0 trials, 0 duplicate effects.

## Recovery and performance

Recovery time (n=7): median 6032.449 ms, p95 16218.396 ms, max 16218.396 ms.  
Workflow elapsed (n=10): median 25252.505 ms, p95 54185.511 ms.  
Fault detection (n=8): median 4948.768 ms, p95 5221.863 ms.  
Event to durable attempt (n=10): median 333.387 ms, p95 7637.154 ms.  
Pending attempt to claim (n=10): median 2362.055 ms, p95 5762.848 ms.  
Outbox publish delay (n=10): median 215.128 ms, p95 796.161 ms.  
Measured campaign runtime: 318.578 seconds.  
Workflow throughput: 0.025112 workflows/s.  
Step throughput: 0.025112 successful steps/s.

## Incorrect trials

None.

## Environment and scope

Local Docker Compose, one Kafka KRaft broker, local PostgreSQL and independent mock-payments PostgreSQL. Container/process faults are deliberately injected. These measurements are local failure evidence, not production-scale reliability or multi-broker HA guarantees.

Per-trial JSON evidence is in `trials.jsonl`; `config.json` records the seed, revision and runtime settings. All rates and percentiles are recalculated from raw trials.
