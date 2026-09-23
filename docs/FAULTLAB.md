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

`continuum-faultlab` is the only Compose project the controller will operate. It uses its own named volumes and host ports API `28000`, payments `18001`, Continuum PostgreSQL `55435`, payments PostgreSQL `55436`, Kafka `19093`. FaultLab cleanup removes only this project; the normal development volumes are never reset by its CLI. Destructive infrastructure scenarios are exclusive, while unique-workflow scenarios can run at bounded `--concurrency`. Unique customer IDs isolate refund counts.

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

The clean-revision experiment [`b191853c-c1d8-4e37-91a2-a469f719567c`](../benchmarks/results/phase4-summary.md) ran against code commit `e0aecfdb8e18c041d487b7f6d25923c90f6f552b` with seed 42, concurrency 8, three executors, a five-second executor lease, and one local Kafka broker. It lasted 3,037.452 seconds. Its [machine-readable summary](../benchmarks/results/latest.json) was generated from 5,175 raw JSONL records: 5,075 Continuum trials and 100 separate intentionally unsafe controls.

- Continuum correctness: **5,075/5,075**; incorrect trial IDs: none. There were 2,875 fault-injected Continuum trials. The 100 persistent pre-commit timeout trials correctly ended `FAILED` after bounded attempts and are not counted as workflow-completion failures.
- Expected-to-succeed workflow completion: **4,975/4,975**. Recovery-to-success where required: **2,770/2,770**. These are observed local sample rates, not estimates of production failure probability.
- External effects: **0 duplicate and 0 lost refunds** among 510 Continuum trials expecting a refund (500 lost-response and 10 post-commit SIGKILL). There were **0 duplicate state transitions** and **0 duplicate logical outbox events** across Continuum trials. The unsafe baseline created **100 duplicate refunds in 100 trials** after cancelling the first post-commit response and retrying with a new key.
- Replacement-attempt recovery latency (n=547): median **5,114.089 ms**, p95 **5,330.51 ms**, maximum **11,762.448 ms**. This sample excludes recovery modes without a replacement attempt, even though they contribute to the recovery-success count. The five-second lease is a major determinant of this latency.
- Durable workflow elapsed time (n=5,075): median **282.577 ms**, p95 **5,563.013 ms**. Event-to-attempt p95 was **231.58 ms**, pending-to-claim p95 **268.98 ms**, and outbox-publish p95 **221.082 ms**. All are from durable timestamps in the raw records.
- Whole mixed-campaign throughput was **1.637886 succeeded workflows/second** over 3,037.452 seconds. This includes serialized container stops/restarts and deliberately timed lease expiries; it is **not** steady-state capacity. The high-volume noop and replay segments completed much faster than the physical-fault segments.

Scenario counts and exact outcomes appear in the generated report; notably real post-refund SIGKILL was 10/10 correct, Kafka outage 3/3, PostgreSQL interruption 2/2, dispatcher post-ack SIGKILL 5/5, and concurrent recovery 10/10. No Continuum correctness defect appeared in the final clean-revision run. Earlier dirty-revision harness-isolation mistakes and the Kafka topic-init race are documented above; none of their failed trials was folded into these percentages. Raw evidence remains at `artifacts/faultlab/b191853c-c1d8-4e37-91a2-a469f719567c/` in this workspace and is ignored by Git; only derived summaries are committed.

## Phase 5 AI-agent scenarios (separate cohort)

Phase 5 adds an `ai-smoke` campaign of eight scenarios. It does **not** change or merge the official Phase 4 5,075-trial Continuum denominator. The FakeModelProvider is scripted to make model-call faults and choices reproducible; the runtime still uses real PostgreSQL, Kafka, executors, and the independent payments HTTP/PostgreSQL service. Four crash scenarios use real container SIGKILL with FaultLab-gated pauses or a payment post-commit delay. Their correctness assertions additionally inspect AgentRun, turn/model-call/tool-call identities and the external refund by the durable *agent-tool* operation key.

| Scenario | Boundary | Required evidence |
| --- | --- | --- |
| `agent-model-timeout` | first fake invocation times out | Failed ModelCall retained; later valid decision; one refund |
| `agent-malformed-output` | first response is invalid JSON shape | No tool from invalid response; later valid decision |
| `agent-crash-after-decision` | refund decision and tool identity committed, no HTTP yet | Real SIGKILL; same turn/model call/tool ID; one refund |
| `agent-crash-after-side-effect` | mock refund committed, tool result not committed | Poll external refund then real SIGKILL; replacement uses same operation ID; one refund |
| `agent-crash-after-tool-result` | tool result and next turn committed | Real SIGKILL; tool is not executed again |
| `agent-crash-after-final` | final answer committed, outer attempt still RUNNING | Real SIGKILL; no new inference; outer step succeeds |
| `agent-max-turn-limit` | model keeps selecting policy lookup | Bounded `FAILED`, `MAX_AGENT_TURNS_EXCEEDED` |
| `agent-unknown-tool` | model selects `shell` | No tool call materialized; bounded failure |

Run `make faultlab-ai-smoke` or `uv run continuum-faultlab campaign ai-smoke --seed 42`. These are small boundary demonstrations, not another 5,000-trial reliability estimate. The real Ollama smoke is a separate optional test, excluded from CI; fake-provider correctness does not prove general model quality. Phase 5 fault hooks are environment-gated (`APP_ENV=faultlab`) and unavailable through public APIs.

Clean-code-commit `afef98d` local run `bd9c0771-28c7-48cc-98c6-ed4c4f58c155` recorded 8/8 correct AI trials, four injected SIGKILLs and four recovered workflows, zero duplicate/lost refunds, and zero duplicate workflow transitions. Six workflows succeeded; the max-turn and unknown-tool scenarios correctly failed closed. Raw per-trial JSONL and generated summary remain under ignored `artifacts/faultlab/<experiment-id>/` on the test machine. These eight trials do not alter the Phase 4 campaign's denominator or guarantee future model quality.

## Phase 6 coding scenarios (separate cohort)

The `coding-smoke` campaign contains eleven distinct scenarios against the bundled Python fixture and real Docker sandbox. Eight inject a real executor or sandbox SIGKILL or an unexpected workspace mutation; the other three check baseline behavior, traversal rejection, and test timeout. It is not part of the Phase 4 official 5,075-trial denominator. Trial records additionally inspect one logical workspace, one patch checkpoint, a successful bounded test record, approval/commit SHA agreement, and (for patch recovery) `already_applied` on the replacement operation.

| Scenario | Boundary / assertion |
| --- | --- |
| `coding-baseline` | inspection, patch, tests, approval, one local commit |
| `coding-crash-after-decision` | persisted patch decision is not re-inferred after executor SIGKILL |
| `coding-crash-after-patch` | file write exists but result missing; replacement recognizes exact after-state |
| `coding-crash-after-tests` | successful test response lost; bounded test reruns |
| `coding-sandbox-killed` | container SIGKILL; same volume/workspace, new sandbox |
| `coding-executor-killed` | executor SIGKILL mid-run; replacement attempt resumes |
| `coding-crash-before-commit` | approval committed, executor killed before Git effect |
| `coding-crash-after-commit` | commit exists, DB SHA missing; operation trailer prevents second commit |
| `coding-workspace-divergence` | external file mutation causes expected fail-closed workflow |
| `coding-path-traversal` | `../../etc/passwd` read tool fails without approval/commit |
| `coding-command-timeout` | real sandbox pytest timeout returns `timed_out` |

Use `make faultlab-coding-smoke` or `uv run continuum-faultlab campaign coding-smoke --seed 42`. A development run on a dirty tree is diagnostic only. Final Phase 6 measurements, if published, must come from a clean revision and remain separate from Phase 4/5 results. The sandbox is local process isolation, not a hostile-code security certification; see [SANDBOX_SECURITY.md](SANDBOX_SECURITY.md).
