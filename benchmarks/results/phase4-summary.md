# FaultLab Experiment

Experiment ID: `b191853c-c1d8-4e37-91a2-a469f719567c`  
Generated: 2026-09-23T00:14:50.108128+00:00  
Git commit: `e0aecfdb8e18c041d487b7f6d25923c90f6f552b` (dirty: false)  
Seed: 42  
Concurrency: 8  
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

Continuum trials: 5075  
Correct: 5075  
Incorrect: 0  
Correctness rate: 100.000%  
Workflow completion rate: 100.000%  
Recovery success rate: 100.000%

| Scenario | Trials | Correct | Incorrect | Injected failures |
| --- | ---: | ---: | ---: | ---: |
| baseline | 2200 | 2200 | 0 | 0 |
| concurrent-recovery-race | 10 | 10 | 0 | 10 |
| database-interruption | 2 | 2 | 0 | 2 |
| dispatcher-crash-after-kafka-ack | 5 | 5 | 0 | 5 |
| duplicate-kafka-delivery | 2200 | 2200 | 0 | 2200 |
| executor-crash-after-side-effect | 10 | 10 | 0 | 10 |
| executor-crash-before-execution | 5 | 5 | 0 | 5 |
| executor-crash-during-pure-work | 5 | 5 | 0 | 5 |
| executor-restart | 5 | 5 | 0 | 5 |
| external-response-lost-after-side-effect | 500 | 500 | 0 | 500 |
| external-service-timeout-before-side-effect | 100 | 100 | 0 | 100 |
| kafka-outage | 3 | 3 | 0 | 3 |
| lost-kafka-offset-ack | 10 | 10 | 0 | 10 |
| malformed-kafka-event | 5 | 5 | 0 | 5 |
| missed-heartbeats | 5 | 5 | 0 | 5 |
| stale-owner-finalize | 10 | 10 | 0 | 10 |
| unsafe-refund-retry-baseline | 100 | 100 | 0 | 100 |

## Side effects and state integrity

Duplicate external side effects: 0  
Duplicate external side-effect trial rate: 0.000%  
Lost external side effects: 0  
Lost external side-effect trial rate: 0.000%  
Duplicate state transitions: 0  
Duplicate-transition trial rate: 0.000%  
Duplicate logical outbox events: 0  
Intentionally unsafe baseline: 100 trials, 100 duplicate effects.

## Recovery and performance

Recovery time (n=547): median 5114.089 ms, p95 5330.51 ms, max 11762.448 ms.  
Workflow elapsed (n=5075): median 282.577 ms, p95 5563.013 ms.  
Fault detection (n=647): median 5033.83 ms, p95 5156.663 ms.  
Event to durable attempt (n=5075): median 85.002 ms, p95 231.58 ms.  
Pending attempt to claim (n=5075): median 147.425 ms, p95 268.98 ms.  
Outbox publish delay (n=5075): median 81.255 ms, p95 221.082 ms.  
Measured campaign runtime: 3037.452 seconds.  
Workflow throughput: 1.637886 workflows/s.  
Step throughput: 1.637886 successful steps/s.

## Incorrect trials

None.

## Environment and scope

Local Docker Compose, one Kafka KRaft broker, local PostgreSQL and independent mock-payments PostgreSQL. Container/process faults are deliberately injected. These measurements are local failure evidence, not production-scale reliability or multi-broker HA guarantees.

Per-trial JSON evidence is in `trials.jsonl`; `config.json` records the seed, revision and runtime settings. All rates and percentiles are recalculated from raw trials.
