# FaultLab methodology and findings

FaultLab asks whether Continuum's supported workflow and external-effect invariants survive *repeated, controlled* distributed failures. It is a reproducible local experiment harness, not a new workflow engine or production monitoring system. PostgreSQL remains authoritative; FaultLab stores evidence outside the runtime in `artifacts/faultlab/<experiment-id>/`.

## What a correct trial means

For a one-step successful workflow, the harness requires the workflow and step to be `SUCCEEDED`, the expected number and terminal states of attempts, exactly one logical outbox event, all outbox work published, no active attempt, sequential positions, and the expected seven audit transitions with no repeated transition edge. For a refund, it also queries the **independent** mock-payments HTTP service by the trial's unique customer and stable `continuum:<step_id>` key. Two refunds are a failure even when Continuum says `SUCCEEDED`; zero refunds are a failure when one was expected. A deliberately permanent pre-commit service failure is correct only when the workflow ends `FAILED` after bounded attempts with zero refunds; it is not counted as a successful recovery.

`correct` means all specified invariants held. `recovered` additionally requires a recovery-relevant trial to finish `SUCCEEDED`. The intentionally unsafe baseline is a separate cohort: its two-refund outcome is expected *for that control*, but is never included in Continuum's correctness rate. Exceptions and incorrect outcomes are appended to JSONL, not hidden or silently retried by the harness.

## Fault inventory

| Scenario | Injected boundary | Mechanism | Expected outcome |
| --- | --- | --- | --- |
| `baseline` | none | API → Kafka → executor | One attempt, success |
| `executor-crash-before-execution` | after claim, before tool | FaultLab-gated pause, real SIGKILL | Expiry, replacement, success |
| `executor-crash-during-pure-work` | slow pure tool | Real owner-container SIGKILL | Expiry, replacement, success |
| `executor-crash-after-side-effect` | refund committed, before finalization | Poll independent refund, real SIGKILL | Same key, one refund, success |
| `missed-heartbeats` | live owner stops making progress | Real `docker pause`/unpause | Lease expiry, replacement, success |
| `stale-owner-finalize` | old token after replacement | Force database-time expiry, service call | Old finalization rejected |
| `duplicate-kafka-delivery` | same application event ID replayed | Real Kafka republish and group-offset observation | One inbox/attempt/outbox transition |
| `lost-kafka-offset-ack` | DB committed, offset uncommitted | Real Kafka consume without first commit, redelivery | Inbox dedupe, one attempt |
| `kafka-outage` | broker unavailable after state transaction | Stop/start exact isolated broker container | Durable unpublished outbox, later success |
| `dispatcher-crash-after-kafka-ack` | acknowledged publish before `published_at` | FaultLab-gated post-ack pause, real SIGKILL | Same event ID republished, deduped |
| `malformed-kafka-event` | invalid JSON on step topic | Real Kafka send | DLQ record; healthy workflow continues |
| `external-service-timeout-before-side-effect` | real HTTP read timeouts before refund commit | FaultLab-gated pre-commit delay and short client deadline | Bounded `FAILED`, zero refunds |
| `external-response-lost-after-side-effect` | first response 503 after durable commit | FaultLab-gated mock mode | Retry same key, one refund, success |
| `concurrent-recovery-race` | two schedulers see expired lease | Force DB expiry; two concurrent sessions | One replacement generation |
| `executor-restart` | SIGTERM while pure tool runs | Real container restart | Graceful drain, one attempt |
| `database-interruption` | PostgreSQL unavailable mid-execution | Stop/start exact isolated DB container | No false commit, eventual success |
| `unsafe-refund-retry-baseline` | client response lost after first refund commit, then retry with a **different** key | Harness-only HTTP control; cancel first request after independent lookup | Two refunds; intentionally unsafe |

The service-mode controls are accepted only with `APP_ENV=faultlab`. The dispatcher/after-claim hooks are likewise gated and are not ordinary public API controls. The stale-token and recovery-race scenarios use genuine PostgreSQL locking/concurrent transactions but do **not** claim to be physical process crashes. `executor-restart` deliberately tests graceful SIGTERM, unlike the SIGKILL cases.

## Isolation and reproduction

`continuum-faultlab` is the only Compose project the controller will operate. It uses its own named volumes and host ports API `18000`, payments `18001`, Continuum PostgreSQL `55435`, payments PostgreSQL `55436`, Kafka `19093`. FaultLab cleanup removes only this project; the normal development volumes are never reset by its CLI. Destructive infrastructure scenarios are exclusive, while unique-workflow scenarios can run at bounded `--concurrency`. Unique customer IDs isolate refund counts.

```bash
uv sync
uv run continuum-faultlab list
uv run continuum-faultlab campaign smoke --seed 42 --concurrency 2
uv run continuum-faultlab campaign reliability --seed 42 --concurrency 8
uv run continuum-faultlab report EXPERIMENT_ID
```

Each run records an experiment UUID, trial UUIDs, seed, scenario version, configuration, Git SHA and dirty state, timestamps, fault point, workflow/attempt/executor IDs, expected/actual terminal status, side-effect counts, audit/outbox duplicates, and timings. `config.json` plus `trials.jsonl` are the raw audit source. `summary.json` and `summary.md` are regenerated from those records. A clean-revision `report --publish` writes compact curated files to `benchmarks/results/`; raw large artifacts remain ignored. Rerunning a seed reproduces the scenario selection and control logic, but Docker scheduling and timings are not bit-for-bit deterministic.

The smoke campaign runs one trial per Continuum scenario. The full reliability campaign is on-demand, not part of routine GitHub Actions. CI runs the standard Phase 1–3 suite, FaultLab unit tests, and representative real-container scenario tests. `make check` performs the complete local quality gate and cleans the isolated stack afterward.

## Metrics and denominators

- Correctness rate: correct Continuum trials / all Continuum trials. The unsafe control is excluded.
- Completion rate: `SUCCEEDED` / trials expected to succeed. The deliberately persistent pre-commit timeout case expects `FAILED` and is not counted as a missing completion.
- Recovery success rate: correctly `SUCCEEDED` / trials requiring recovery. Correct bounded failures are not mislabeled recovered.
- Duplicate/lost effect trial rates: trials with extra/missing refunds / Continuum trials expecting a refund. Counts are also shown separately.
- Recovery time: fault injection timestamp to replacement attempt `started_at`, only where a replacement exists. Fault detection: injection to old attempt `completed_at` on expiry.
- Workflow elapsed: durable workflow `started_at` to `completed_at`. Event-to-attempt, pending-to-claim, and outbox-publish delays come from durable timestamps. Median and nearest-rank p95 are computed from raw samples, with each sample size reported.
- Throughput: successful workflows/steps divided by first trial start to last trial completion for that experiment. This includes serial destructive scenarios and must not be interpreted as steady-state service capacity.

No local single-broker measurement is a production-scale reliability probability. Fault injection timing, Docker startup, and laptop resource contention affect latencies. The broker has one KRaft node, and the test payments service is not a real processor.

## Phase 4 findings before the official campaign

The pre-change dispatcher held PostgreSQL outbox row locks while awaiting Kafka. In a controlled 0.5-second post-ack hold, another transaction could not lock the row and the transaction lasted 565 ms. With the expiring claim design, the claim transaction measured 17.69 ms and another transaction locked the row during the same 0.5-second broker hold. This is a lock-boundary finding, **not** a comparable end-to-end throughput improvement claim.

Executor idle polling was measured before changes against PostgreSQL at 0.5-second intervals: one executor issued 10 claim queries over 5.155 seconds (1.94/s), three issued 30 over 5.174 seconds (5.80/s). This did not justify a polling redesign. It will be revisited if larger campaigns reveal database pressure.

An initial dirty-revision smoke run exposed three harness-control mistakes, not duplicate-side-effect runtime bugs: the missed-heartbeat injector reclaimed its own replacement, a background recovery scheduler raced an intended two-scan test, and graceful restart was incorrectly expected to create a second attempt. The scenarios were corrected, each affected case was rerun successfully, and the original failed JSONL records remain separate from official clean-revision percentages. A clean-volume startup also exposed a Kafka init race after broker recreation; topic creation now has bounded readiness retries.

## Official results

The final clean-revision campaign summary is generated into [the committed benchmark report](../benchmarks/results/phase4-summary.md) and machine-readable [latest.json](../benchmarks/results/latest.json). Only numbers derived from that final experiment belong in the README or engineering report; exploratory dirty-revision trials above are not mixed into its rates.
