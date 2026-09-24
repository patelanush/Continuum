# Failure Model through Phase 6

Continuum uses PostgreSQL-authoritative state, Kafka at-least-once transport, a transactional outbox, an idempotent inbox, and durable leased execution attempts. It does **not** promise exactly-once distributed or arbitrary external execution.

## Addressed

- **Database commits while Kafka is unavailable:** State/audit/outbox commit together. An unpublished row survives the outage; the dispatcher retries the same stable event ID. A local broker stop/restart was exercised in Phase 2.
- **Dispatcher publishes before marking the row:** Publication can repeat. The same `event_id` and inbox uniqueness make the replay harmless for scheduling.
- **Consumer commits DB but loses Kafka offset acknowledgement:** Replayed event does not create another attempt; offset can be committed after dedupe. Real Kafka integration coverage deliberately omits a first offset commit.
- **Stale or malformed Kafka event:** PostgreSQL truth wins. Stale events become no-ops; invalid envelopes or unmatched durable outbox events go to the DLQ before source-offset commit.
- **Executor dies before or during work:** Heartbeats stop, the database-time lease expires, the scheduler marks the attempt EXPIRED, and a safe replacement can be claimed. A real container SIGKILL demo covers pure work.
- **Executor dies after a keyed external side effect:** The mock payment is committed independently, the executor is SIGKILLed before Continuum finalizes, and a replacement retries with the same per-step idempotency key. The mock returns the original refund. A real container-crash demo verifies one refund and a successful replacement.
- **External response lost after a durable side effect:** The delayed-response mock makes this window deterministic. A PostgreSQL/HTTP integration test separately simulates a lost finalization result without physically killing a process.
- **Stale executor wakes after lease replacement:** UUID token and expiry validation reject finalization/heartbeat; the old owner cannot overwrite a newer result.
- **Concurrent executors/recovery schedulers:** PostgreSQL locks, `SKIP LOCKED`, and unique attempt indexes prevent duplicate claims or replacement generations. Tests use independent concurrent sessions.
- **Healthy long work:** A heartbeat extends an unexpired lease; tests run longer than the initial lease and observe one attempt.
- **Transaction rollback:** Inbox + initial scheduling and attempt finalization + step/audit/outbox roll back as units. No Kafka offset is committed before initial scheduling commits.
- **Repeated, injected fault boundaries:** FaultLab exercises container SIGKILL before execution, during pure work, and after an independently committed refund; process pause without heartbeats; broker/database outages; ack-before-publish-finalization dispatcher death; Kafka replay/missed offset acknowledgement; malformed messages; stale fencing; and concurrent recovery. Every run preserves raw per-trial evidence and independently checks refund count.
- **Outbox broker-I/O lock duration:** Phase 4 moved Kafka publication outside PostgreSQL row-lock transactions. Expiring, fenced outbox claims keep crashes recoverable; consumer dedupe still handles re-publication.
- **Model timeout and malformed structured output:** Every model attempt is recorded. Invalid output cannot materialize a tool; bounded retries may produce a later valid decision. Unsupported model/request configuration fails permanently.
- **Crash after a committed model decision:** The accepted AgentTurn decision and AgentToolCall identity are durable together. A replacement does not re-infer that turn, even if the model would choose differently.
- **Crash before decision persistence:** The interrupted ModelCall is retained as failed. Inference may run again and return a different answer; no action was authorized from the uncommitted response.
- **Crash after agent refund side effect:** The tool call and operation ID precede HTTP I/O. Retry uses the same key; the independent payments service returns the original refund. A real FaultLab executor SIGKILL after payment commit verifies one refund and no reinference of the persisted refund turn.
- **Crash after persisted tool result or final answer:** The next turn or final AgentRun result is resumed, not recomputed. Real FaultLab container crashes exercise both windows.
- **Unknown tool and unbounded loop:** The tool allowlist rejects unknown names and arguments; model attempts and agent turns have configured finite limits.
- **Patch response lost after filesystem write:** The persisted coding tool call has a stable operation ID and expected-before SHA-256/file map. Replacement inspects the same persistent workspace, recognizes the exact after-state, and records the patch result without writing twice. A real executor SIGKILL after patch application was exercised.
- **Sandbox container dies:** The logical workspace volume survives; a new container mounts it and checks the durable fingerprint. FaultLab kills the actual sandbox container.
- **Tests finish but result is lost:** The fixed pytest command can run again; a replacement records the new bounded result. A SIGKILL after test execution exercises this boundary.
- **Approved Git commit response is lost:** Git HEAD carries an exact `Continuum-Operation-ID` trailer. Recovery reuses its SHA and does not make another commit. FaultLab kills an executor after Git commit but before DB result.
- **Unexpected workspace divergence or path escape:** Fingerprint/expected-hash mismatch fails closed; a central resolver rejects traversal, absolute host paths, and symlink escapes. Tests and FaultLab cover these controls.
- **Command timeout:** The sandbox terminates the command process group and returns a timed-out result; no unbounded test command is accepted.
- **Malformed model-generated Python replacement:** Local Ollama produced an unterminated docstring in a real coding attempt. The first run's sandbox tests caught it and refused approval. Phase 6 now parses Python replacements before decision materialization and again before sandbox write, so syntax-invalid content consumes a bounded failed ModelCall rather than changing the workspace. The invalid-replacement regression test preserves this boundary.
- **Premature coding final answer:** A local code model returned `final` after only reading the task. Continuum now rejects that decision as a failed ModelCall until a durable patch checkpoint and a later passing sandbox-test record exist. Bounded exhaustion fails both AgentRun and workflow without approval or commit; a scripted retry test proves the model can continue instead.

## Boundary table

| Failure boundary | Durable outcome | Next action |
| --- | --- | --- |
| Consumer DB transaction fails | No inbox/attempt commit | Kafka redelivery |
| Consumer DB commits, offset commit fails | Inbox and pending attempt exist | Redelivery dedupes |
| Executor dies after claim, before external call | RUNNING attempt until lease expiry | Scheduler creates safe replacement |
| External keyed refund commits, response is lost | Refund exists; attempt remains RUNNING | Expiry, retry same key, receive original refund |
| Heartbeat loses lease | Old attempt can no longer renew/finalize | Stop old tool if possible; scheduler/new owner proceeds |
| Finalization transaction fails | Attempt/step/audit/outbox remain pre-finalization | Lease expiry and safe retry |
| Two schedulers see one expired lease | One wins workflow/attempt locks | One replacement only |
| Cancellation races with in-flight HTTP | Finalization is fenced, but external action may still occur | Tool-specific reconciliation is future work |

## Deferred and limitations

- Truly non-idempotent APIs without a reliable key or reconciliation mechanism are **not** automatically retried. An unknown tool type cannot acquire a safe crash-retry guarantee merely by being configured as a workflow step.
- `mock_refund` is an independent local demonstrator, not a real payment integration. Its idempotency-key storage has no production retention/expiry policy. Test-only delay controls must remain local.
- A transient payment/DB outage can lead to lease expiry and repeated **safe** calls. Max attempts are bounded, but there is no general retry/backoff policy, circuit breaker, or tool-specific reconciliation yet.
- Cancellation fences durable finalization but cannot guarantee that an already-sent external HTTP request or sandbox operation was cancelled before its effect. Compensating actions remain future work. Phase 6 provides local commit approval, not a production authorization system.
- Kafka is one local KRaft broker with a named volume, not broker HA or disaster recovery. Multi-region recovery is not implemented.
- Broader model-provider outages beyond bounded local Ollama retries, arbitrary tool reconciliation, production-grade approval/authorization, Redis coordination, OpenTelemetry, and Kubernetes remain planned. FaultLab validates supported operations, not these absent capabilities.
- A database interruption around a commit can leave a caller uncertain whether that commit succeeded. Durable rereads and idempotent commands/keys limit harm; arbitrary external reconciliation remains future work.
- A continuously failing external service can exhaust `max_attempts` and correctly fail a workflow with no refund. FaultLab treats that as a **correct bounded failure**, not a recovered workflow.
- Graceful executor restart may finish one in-flight attempt while heartbeating; SIGKILL/pause scenarios are separate. FaultLab originally expected two attempts from a graceful restart and corrected that harness assumption rather than altering runtime behavior.
- FaultLab's first missed-heartbeat experiment allowed the faulted executor to reclaim its own replacement, producing three safe attempts. The injector now pauses the live container to prevent that test artifact. The original failed trial remains in ignored raw artifacts and is not mixed with final clean-campaign percentages.
- FaultLab's first concurrent-recovery experiment raced a background scheduler in addition to its intended two scans. It now stops that scheduler for the controlled two-session race. This was an experiment-isolation defect, not a duplicate-attempt runtime bug.

The final clean-revision [FaultLab campaign](../benchmarks/results/phase4-summary.md) classified 5,075/5,075 Continuum trials correctly, including real process and infrastructure faults, with zero duplicate or lost refunds among 510 trials expecting one. Its 100 separate unsafe retry controls each duplicated a refund. This is evidence for the supported local operations and one-broker topology, not a guarantee for arbitrary APIs, production traffic, or untested failure combinations.

## Phase 5 unresolved boundaries

- Provider-side inference may have completed but its response can disappear before the PostgreSQL decision commit. A replacement may call the model again and get a different response. Continuum guarantees replay of **persisted** decisions, not exactly-once or deterministic inference.
- An arbitrary non-idempotent tool cannot be made safe by the agent tables. The allowlist currently contains only a deterministic read and the mock payment API's idempotency-key contract. Idempotency-key retention and real payment reconciliation are outside this local demonstration.
- Cancellation fences future checkpoints and prevents a new model call after the workflow is cancelled, but cannot undo an already-sent HTTP refund. Human approval, compensation, and tool-specific cancellation are deferred.
- Prompt injection defense, API authentication/authorization, sensitive-data retention, sandboxed code execution, coding-agent filesystem effects, multi-agent coordination, context compression, a provider outage across all local model capacity, and broker HA are not solved.
- Phase 4's 5,075-trial Continuum result predates Phase 5. Phase 5 AI smoke results are separate and must not be silently added to that reliability denominator.

## Phase 6 unresolved boundaries

- The fixture-backed local Docker sandbox is not a hardened hostile-code or multi-tenant security boundary. The trusted executor controls the host Docker daemon; the sandbox never receives its socket. Kernel escape, malicious package supply chain, and cross-tenant isolation require stronger controls.
- Only one bundled Python fixture is accepted as a repository source. Remote clone, networked dependencies, multiple language images, remote push/PR side effects, merge conflicts, and concurrent coding agents on one repository are deferred.
- Approval is durably required before a **local** commit, but its development API has no authentication or human identity. An already in-flight patch/commit cannot be undone by cancellation. The executor currently remains leased while waiting for a decision.
- Git/file reconciliation fails closed on unexpected workspace changes. It is not a general distributed filesystem transaction. A stale in-flight Docker operation around lease turnover warrants stronger per-workspace effect fencing before untrusted concurrent workloads.
- Phase 4 official benchmark numbers predate Phases 5–6. Coding FaultLab smoke is a separate boundary validation, not an addition to the 5,075-trial reliability percentage.

## Phase 7 observational failures

- **Collector unavailable:** The batch exporter times out and may discard spans/metrics; workflow transactions, Kafka progress, and execution continue. A local test stopped the Collector during five workflows and observed all five succeed with one attempt each. A new trace appeared after restart.
- **Tempo unavailable:** The Collector may fail to forward or drop traces. Workflow execution does not call Tempo. Durable state remains queryable.
- **Prometheus or Grafana unavailable:** Dashboards and metric queries disappear temporarily; neither is on the execution path.
- **Export queue saturation:** The bounded batch queue may discard spans. It does not hold database locks or block business operations waiting for remote export.
- **Malformed trace headers or persisted context:** Validation drops the relation and starts fresh context. The event is still parsed, deduped, and acknowledged according to its business envelope. A local malformed-header replay left one completed attempt.
- **Exporter loss near a durable commit:** Counters and traces are best-effort observations and may not exactly equal the durable ledger after a process crash. Investigate with PostgreSQL attempts/transitions and external effect IDs.
- **Privacy regression:** The central exporter allowlist strips unknown attributes, events, and status text. A sentinel unit test and a real Tempo coding trace check for prompt/source/API-key sentinels.

The Phase 7 trace and overhead artifacts are separate from the official Phase 4 FaultLab campaign. The telemetry stack is local and optional; it adds no Kubernetes or broker availability guarantee.
