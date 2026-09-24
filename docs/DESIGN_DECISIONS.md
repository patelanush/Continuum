# Design Decisions through Phase 4

## PostgreSQL is the source of truth

**Decision:** Persist workflow state, execution position, and history in PostgreSQL.

**Reason:** Multi-row transactions, constraints, and row locks provide the correctness guarantees
needed before work distribution exists.

**Tradeoff:** PostgreSQL availability is required for readiness and mutations. This is preferable
to reconciling multiple authoritative stores.

## No Kafka in Phase 1

**Decision:** Do not add a broker until durable commands and invariants are proven.

**Reason:** Kafka will transport work/events; it must not become an accidental state authority.

**Tradeoff:** Phase 1 cannot distribute work. Phase 2 can build on precise transactional state.

## Explicit state machines

**Decision:** Central domain maps enumerate all legal edges; route handlers cannot mutate state.

**Reason:** Illegal, skipped, self, and terminal transitions fail consistently and are unit-testable.

**Tradeoff:** Adding a state requires an intentional domain and migration review.

## Database row locking

**Decision:** Mutation commands lock the workflow then its ordered steps with `FOR UPDATE`.

**Reason:** Database locks coordinate all processes and serialize decisions made from shared state.

**Tradeoff:** Mutations for one workflow are serialized. That is the desired Phase 1 consistency
boundary and does not prevent parallelism across workflows.

## PostgreSQL integration tests

**Decision:** All persistence, API, and concurrency tests use PostgreSQL; SQLite is unsupported.

**Reason:** JSONB, locking, constraints, asyncpg behavior, and transaction semantics are precisely
the behavior under test.

**Tradeoff:** Full tests require Docker/PostgreSQL, while pure state-machine tests remain fast and
database-free.

## Audited transitions

**Decision:** Write immutable-through-application transition rows inside the mutation transaction.

**Reason:** Operators and future workers need a durable explanation of how current state arose.

**Tradeoff:** Mutations perform extra inserts. This audit table is intentionally not an outbox.

## No exactly-once claim

**Decision:** Document commands as transactional and selected duplicates as idempotent, not
exactly-once execution.

**Reason:** Future brokers and external systems can deliver or apply effects more than once, and
the ambiguous-side-effect window cannot be removed by a database transaction alone.

**Tradeoff:** Future tool operations need idempotency keys, reconciliation, and at-least-once
semantics.

## Final-position pointer

**Decision:** Retain `current_step_position` at the last step after success/failure.

**Reason:** The pointer remains useful during postmortems; terminal workflow status prevents use as
an execution instruction.

**Tradeoff:** Consumers must interpret the pointer together with status, not `NULL` as completion.

## Offset pagination in Phase 1

**Decision:** List newest-first with bounded `limit` and `offset`.

**Reason:** It is simple and adequate for a local correctness phase.

**Tradeoff:** Inserts can shift later pages and large offsets degrade. Introduce a `(created_at,id)`
cursor before production-scale listing.

## Compose host port 55433

**Decision:** Publish PostgreSQL on host port 55433 while retaining container port 5432.

**Reason:** It avoids common conflicts with a local PostgreSQL installation and remains overridable
with `POSTGRES_PORT`.

**Tradeoff:** Host-side commands must use 55433 or an explicitly coordinated override.

## Kafka transports work; PostgreSQL remains authoritative

**Decision:** Add Apache Kafka in Phase 2 for partitioned worker delivery while keeping
PostgreSQL as the only workflow state authority.

**Reason:** Consumer groups distribute work across processes and Kafka retains events for
redelivery/replay. PostgreSQL provides the transactional state, constraints, and locking that a
Kafka record alone cannot prove.

**Tradeoff:** The runtime now operates two durable systems and must explicitly handle their
non-atomic boundary. A PostgreSQL-only queue would be simpler at small scale, but would couple
worker polling and retained event transport to the state store rather than exercising the
event-driven distributed architecture this phase is designed to prove.

## KRaft and local broker topology

**Decision:** Run one Kafka 3.9.1 broker in KRaft mode with a named volume and no ZooKeeper.

**Reason:** KRaft is Kafka's controller architecture and avoids a second coordination service.

**Tradeoff:** This local topology survives a container restart but has no broker high availability
or disaster recovery guarantee.

The Apache image's default log directory is `/tmp/kafka-logs`; merely mounting a volume at
`/var/lib/kafka/data` did not persist broker logs across container replacement. Phase 3 explicitly
sets `KAFKA_LOG_DIRS=/var/lib/kafka/data`. Existing local broker data was copied into the volume
and a broker container replacement was validated. This is local persistence, not HA.

## Workflow ID as message key

**Decision:** Key `step.ready` messages by `workflow_id` and use three topic partitions.

**Reason:** All messages for one workflow route to one partition while separate workflows can be
assigned to different workers. Kafka ordering is per partition only.

**Tradeoff:** A single high-volume workflow cannot spread across partitions. Sequential execution
is intentional in Phase 2; no global event order is claimed.

## At-least-once publication and transactional outbox

**Decision:** Insert an outbox row in the same PostgreSQL transaction that makes a step READY.
The dispatcher sends acknowledged Kafka messages and only then marks rows published.

**Reason:** A direct DB-then-Kafka dual write can lose work if the API crashes between operations.
An unpublished outbox row survives a broker outage and can be retried.

**Tradeoff:** The dispatcher can publish a message twice if it crashes after Kafka ack but before
committing `published_at`. The outbox ID remains the stable event ID across retries.

## Producer idempotence does not imply exactly once

**Decision:** Enable Kafka producer idempotence while explicitly retaining the outbox and inbox.

**Reason:** Kafka's producer feature addresses some duplicate sends inside Kafka. It cannot make
PostgreSQL commits, Kafka acknowledgements, and future external API effects one atomic operation.

**Tradeoff:** Consumers must still tolerate duplicates and stale records. No exactly-once
distributed execution claim is made.

## Durable inbox and offset order

**Decision:** Claim `(consumer_group,event_id)` with one atomic insert and a unique index. Commit
the Kafka offset only after the inbox and workflow transaction commits, or after DLQ publication
is acknowledged for a permanently invalid message.

**Reason:** A query-then-insert check races across workers. Committing an offset first could lose
work after a crash. A database commit followed by a missing offset commit causes safe redelivery.

**Tradeoff:** Kafka may replay messages and the inbox grows with processed events. Retention and
archival need a later operational policy.

## Caller-owned worker transaction

**Decision:** Preserve Phase 1 public service methods and add narrow service methods that apply
step transitions inside a caller-owned transaction.

**Reason:** Worker processing must atomically insert the inbox record, mutate step/workflow state,
write audit history, and create downstream outbox work. Nested independent commits would break
that unit.

**Tradeoff:** The service has two entry paths for some commands. The transactional methods are
explicitly named, never commit, and share the same transition helpers and locks.

## Phase 2 sequential noop execution (superseded by Phase 3 attempts)

**Decision:** Phase 2 workers originally executed deterministic `noop` directly. Phase 3 replaced
that path with durable scheduling and a separate executor; unsupported types now fail at tool
execution and are not crash-retried.

**Reason:** This proves delivery and state correctness without introducing external side-effect
ambiguity or long-running execution semantics.

**Tradeoff:** Phase 3 adds leases and bounded crash recovery; parallel DAGs and arbitrary tools
remain deferred.

## Durable attempts and separation from Kafka ingestion

**Decision:** A Kafka consumer commits a PENDING attempt with its inbox record, then commits the
offset. A separate executor claims the attempt from PostgreSQL.

**Reason:** Long external calls must not keep a Kafka message in-flight or a PostgreSQL transaction
open. PostgreSQL attempts survive consumer/executor death independently of Kafka offsets.

**Tradeoff:** Another polling process and durable table are required. Kafka is still useful for
ordered, replayable workflow-readiness transport; PostgreSQL is the work-ownership authority.

## PostgreSQL claiming with `SKIP LOCKED`

**Decision:** Executors lock workflow/steps/attempt in a consistent order and use `FOR UPDATE SKIP
LOCKED` while claiming eligible attempts. The scheduler uses the same pattern for expiry.

**Reason:** Multiple executors/schedulers can compete without process-local locks or duplicate
ownership. Database uniqueness constraints provide a second line of defense.

**Tradeoff:** Same-workflow mutations serialize, deliberately preserving sequential invariants.
Polling has bounded intervals and may add small scheduling delay.

## Leases, heartbeats, and fencing tokens

**Decision:** A claim generates a random UUID `lease_token` alongside `executor_id`; periodic
heartbeats and finalization compare both with durable RUNNING status and an unexpired lease.

**Reason:** A human-readable ID may be reused after a process restart. The token identifies one
ownership generation and rejects a stale process after expiry/replacement.

**Tradeoff:** Executors must keep heartbeating during long operations. Temporary database outages
can cause expiry and duplicate safe execution; they cannot grant two valid finalizations.

## Database time is lease authority

**Decision:** Claim, heartbeat eligibility, finalization validity, and scheduler expiry use
PostgreSQL `clock_timestamp()` rather than executor/scheduler host clocks.

**Reason:** Clock skew between distributed hosts must not decide ownership.

**Tradeoff:** PostgreSQL availability is necessary for lease renewal and recovery.

## Reserve/execute/finalize boundaries

**Decision:** Commit the claim before external I/O; commit finalization in a second transaction.
No Continuum database row lock is held during an HTTP request or slow tool.

**Reason:** A transaction cannot span an independent payment API, and a long lock would block
progress/cancellation. Crash recovery depends on a durable RUNNING attempt between boundaries.

**Tradeoff:** The result can be ambiguous after an external success and worker death. Safe retry
therefore depends on the external operation contract, not on a fictitious cross-system transaction.

## Stable per-step operation ID and explicit retry safety

**Decision:** Derive `continuum:<step_id>` once per logical step, not per attempt. Register each
tool's retry semantics explicitly. `mock_refund` uses the key in the external HTTP header; unknown
or non-idempotent operations are not automatically recovered.

**Reason:** A new key on attempt 2 could repeat a refund. An API without reliable idempotency or
reconciliation cannot safely recover an ambiguous side effect.

**Tradeoff:** The current tool set is intentionally small. Production integrations must document
key scope/retention, parameter matching, and reconciliation before enabling retries.

## Independent mock-payments storage

**Decision:** Use a separate PostgreSQL instance and HTTP service for deterministic refunds, with
a unique idempotency key and a test-only post-commit response delay.

**Reason:** Sharing Continuum's transaction would hide the cross-system ambiguity Phase 3 tests.
The delayed response exposes a real side-effect-committed/worker-not-finalized boundary.

**Tradeoff:** The mock is local and deliberately simple, not a payment processor or production
service. Its testing controls and development credentials must not be exposed publicly.

## No Redis for leases

**Decision:** Store correctness-critical leases in PostgreSQL with attempts and workflow state.

**Reason:** Expiry, replacement, and finalization need one durable transactional authority.

**Tradeoff:** Redis could later help caching or rate limiting, but would add another consistency
boundary for ownership without a Phase 3 need.

## FaultLab is external to runtime truth

**Decision:** Keep experiment configuration and trial results in local JSONL/artifacts rather than workflow PostgreSQL tables.

**Reason:** Fault-injection evidence must not change production workflow invariants or become a second authority. Raw records can be audited and reaggregated independently.

**Tradeoff:** The harness needs filesystem artifact management; ignored raw files are not automatically replicated off the laptop. Small curated summaries can be committed.

## Deterministic, boundary-specific faults

**Decision:** Record a seed and exact injection point for every trial; prefer real SIGKILL, pause, container stop/restart, and Kafka delivery. Use test-only hooks only for boundaries not otherwise observable, such as the dispatcher pause after broker acknowledgement.

**Reason:** Random chaos without recorded seeds and boundary confirmation can produce irreproducible successes. A workflow finishing is insufficient evidence of fault recovery.

**Tradeoff:** Docker-control scenarios are slower and infrastructure-sensitive. Database-level stale-owner/race tests supplement, but are not misrepresented as process crashes.

## Correctness includes independent external effects

**Decision:** For refunds, query mock-payments by unique customer and stable key; count duplicate and lost effects independently of Continuum's status.

**Reason:** `SUCCEEDED` with two refunds is an incorrect trial. The cross-service ambiguity is the reason for Phase 3's stable operation key.

**Tradeoff:** The test mock's idempotency behavior is not a guarantee that a future real API implements the same contract or retention.

## Fenced, expiring outbox claims

**Decision:** Add a short `FOR UPDATE SKIP LOCKED` claim transaction with a UUID token and PostgreSQL-time expiry. Await Kafka outside the database transaction, then mark published only through a token-fenced update.

**Reason:** A controlled pre-change probe held an outbox row lock throughout a 0.5-second broker hold (565 ms transaction). Post-change, a separate transaction could lock that row during the same controlled hold; its claim transaction measured 17.69 ms. The old design could hold a batch of locks across multiple 10-second broker timeouts.

**Tradeoff:** Outbox claims add two nullable columns, an index, and a migration. A crashed or over-time publisher may republish after claim expiry, so the same stable event ID, inbox dedupe, and at-least-once semantics remain mandatory. This is a lock-duration improvement, not an exactly-once claim or a general throughput percentage.

## Bounded executor polling retained

**Decision:** Keep the existing 0.5-second idle polling design for now.

**Reason:** A pre-change focused probe against PostgreSQL observed 10 idle claim queries over 5.155 seconds with one executor (1.94/s) and 30 over 5.174 seconds with three executors (5.80/s). There was no hot loop to justify adding LISTEN/NOTIFY or Redis.

**Tradeoff:** Polling adds scheduling delay and query volume proportional to executor count. Revisit after larger, comparable measurements rather than asserting scalability from this local probe.

## Raw results, clean revisions, and CI scope

**Decision:** Persist every trial before aggregation; publish curated benchmark summaries only when the experiment and current working tree refer to the same clean Git revision. Keep full reliability campaigns manual; CI runs real-container representative scenarios and the normal suite.

**Reason:** Aggregate percentages must be reproducible from raw failures, not hand-written. Thousands of container faults on every push would waste free CI minutes and introduce environmental noise.

**Tradeoff:** Timing will vary by machine and local single-broker topology. A clean local campaign is evidence for this configuration, not a production-scale reliability bound.

## Phase 5: agent inside an existing workflow step

**Decision:** Run `support_agent` under an ordinary leased execution attempt. Persist one AgentRun per step, with numbered AgentTurns, ModelCalls, and AgentToolCalls.

**Reason:** Kafka scheduling, attempt recovery, heartbeat, cancellation fencing, and workflow progression already have durable semantics. A second agent workflow engine would duplicate those correctness boundaries. The four agent tables retain nondeterministic decisions and sub-operations across outer-attempt replacement.

**Tradeoff:** The current agent is sequential and one tool action per turn. There are no parallel tool calls, DAGs, long-term memory, or arbitrary user-defined agents.

## Persist the decision before performing its tool

**Decision:** Commit a validated structured model decision and Continuum-generated tool-call identity in the same PostgreSQL transaction. Never replace a committed turn decision. Generate `continuum:agent-tool:<tool_call_id>` once and reuse it across outer attempts.

**Reason:** A restarted model could choose a different action for the same context. Re-running a committed decision or creating a new refund key could duplicate external effects. Provider-generated tool IDs are not a reliable application identity.

**Tradeoff:** Inference that completed but was not persisted may be lost and repeated, possibly with a different answer. No exactly-once model-inference claim is made. A tool without a trustworthy idempotency/reconciliation contract is still unsafe to retry automatically.

## Short fenced agent checkpoints and reconstructed context

**Decision:** Record model-call intent, invoke Ollama outside a DB transaction, commit the accepted decision, execute a durable tool outside a DB transaction, then commit its result and next turn. Every mutation verifies the outer database-time lease token. Reconstruct each model request from persisted input/turns/results and the run's source-controlled prompt version; save the exact normalized request and SHA-256 hash.

**Reason:** Long model/tool I/O must not hold row locks. Fencing prevents a stale executor from writing a late result. Persisted context and prompt version make a replacement independent of in-memory conversation history or a changed default prompt.

**Tradeoff:** More short transactions and durable rows are required. Context is not compressed in Phase 5; long-run context growth is deferred.

## Strict one-action decisions and a closed tool allowlist

**Decision:** Accept only schema-validated `tool_call` or `final` JSON. Validate selected tools with typed arguments; unknown names and extra arguments fail closed. Allow only read-only policy lookup and keyed mock refund for the support agent.

**Reason:** Parsing prose or allowing arbitrary URLs/commands would turn a malformed or hostile model response into an unsafe side effect. One action per turn keeps checkpoint and replay identity unambiguous.

**Tradeoff:** A compact local model may need a prompt correction or bounded retry to produce valid tool arguments. This is visible in failed ModelCalls rather than silently coerced.

## Bounded local providers, no paid API dependency

**Decision:** Use a scripted FakeModelProvider in CI/FaultLab and an Ollama `/api/chat` JSON-schema provider for real local inference. Record timeout/malformed failures, retry only within `MODEL_MAX_ATTEMPTS`, and stop nonfinal loops at `AGENT_MAX_TURNS`. Ollama 400/404 configuration errors fail permanently.

**Reason:** Deterministic failure-boundary tests cannot depend on model sampling or multi-gigabyte downloads. A local provider proves the adapter path without paid credentials, while structured validation keeps the durable contract provider-independent.

**Tradeoff:** A fake-provider test does not prove model quality, and a single compact local-model smoke does not prove general tool-selection reliability. Local inference has hardware/electricity costs even without paid API charges.

## Phase 6: one logical workspace, disposable Docker containers

**Decision:** A coding step owns one durable workspace/volume; each execution attempt has a new sandbox record/container. Only the trusted executor talks to Docker. The sandbox is non-root, non-privileged, networkless, resource-limited, and mounted with only its workspace volume, never the Docker socket.

**Reason:** The agent must not execute code on the host or lose edits when an executor/container dies. Docker gives a practical local process/filesystem boundary while PostgreSQL and the volume retain logical state.

**Tradeoff:** The trusted executor's Docker control access is powerful; this is not hardened hostile-code or multi-tenant isolation. The current source is a bundled Python fixture. Public URL cloning and networked dependency installation are deferred pending a trust/network policy.

## Structured tools instead of a shell

**Decision:** Allowlist typed list/read/search, full-file replacement, fixed pytest, and Git status/diff. File paths share one secure resolver. Do not provide arbitrary `run_command`, Docker controls, URLs, or `shell=True` to the model.

**Reason:** Bounded, explicit operations have auditable identities and individual recovery policies. A model-generated shell string cannot be safely inferred to be read-only or idempotent.

**Tradeoff:** The agent is limited to Python fixture tasks and one approved test command. Broader repositories need explicit tool, image, and dependency policies.

## Reconcile Git/file effects, never blindly replay them

**Decision:** Persist the AgentToolCall and SandboxCommand intent before applying a patch. Compare durable checkpoint file hashes with actual workspace; apply one atomic file replacement or recognize its exact after-state. Save a Git diff/tree fingerprint checkpoint. Treat patch and local Git commit as `RECONCILABLE`, not idempotent. Tag commits with an exact operation-ID trailer.

**Reason:** A process can die after the filesystem/Git effect but before its DB result. Repeating an unchecked patch or commit could duplicate or corrupt work. Git plus file hashes make the ambiguous result inspectable; unexpected divergence fails closed.

**Tradeoff:** Full-file replacement is more constrained than arbitrary unified diffs. The current reconciliation does not guarantee hostile concurrent writers are safe; only one leased coding execution is supported per step. Read-only-ish tests may rerun after response loss.

Python replacement text is parsed before the model decision is accepted and again inside the sandbox before a write. A real `qwen2.5:3b` attempt exposed an unterminated docstring that tests caught after application; the validation gate now records that response as a failed ModelCall and asks for a corrected decision instead of mutating the workspace.

## Explicit durable approval before local commit

**Decision:** Require a passing sandbox test result and a persisted `APPROVED` request before invoking local Git commit. Keep remote push/PR creation out of Phase 6.

**Reason:** Model completion is not human authorization to publish a code change. Approval is a durable transaction boundary; the commit operation has its own stable identity for response-loss recovery.

**Tradeoff:** Approval waiting currently holds an executor lease and the development API is unauthenticated. Production use needs identity/authorization and a suspended-attempt state to release capacity. The code sandbox is not a substitute for human review.

## Phase 7: OpenTelemetry with local Tempo, Prometheus, and Grafana

**Decision:** Use the OpenTelemetry SDK/OTLP for trace and metric generation, a local Collector for routing, Tempo for trace storage, Prometheus for operational time series, and source-controlled Grafana provisioning. Keep the stack behind an optional Compose profile.

**Reason:** OTel gives a shared trace context and service identity across Python processes. Tempo is a lightweight local trace backend, Prometheus supports bounded rate/backlog queries and alerts, and Grafana provides one inspectable UI. The optional profile leaves normal development and CI lightweight. No paid service is needed.

**Tradeoff:** This is one local trace backend and one Prometheus process, not a highly available telemetry deployment. Exact Kafka consumer lag is not exported. Grafana's local password and localhost-only ports are development settings.

## Durable W3C context and asynchronous links

**Decision:** Persist only a validated W3C `traceparent` on outbox events, execution attempts, and approval requests. Inject it into Kafka headers without changing the event JSON. Use short spans and span links for recovery and later approval actions.

**Reason:** A dispatcher, worker, replacement executor, or approver may run after the originating call is gone. In-memory context alone cannot cross these durable boundaries. A replacement is causally related to the dead attempt, but it is a new execution and should remain distinguishable.

**Tradeoff:** A sampled-out or lost exporter batch can leave gaps. A trace is diagnostic context, while workflow/step/attempt IDs remain the durable identities. Malformed context starts a fresh trace and never changes event processing.

## Bounded telemetry and central redaction

**Decision:** Strip all unapproved span attributes, events, and status text before OTLP export. Exclude prompts, model responses, customer/refund data, source/patch text, command output, credentials, and arbitrary exception strings. Put durable IDs in traces, never metric labels. Bound configurable metric dimensions to `other`.

**Reason:** Trace search needs IDs, while Prometheus series cardinality and privacy require small bounded labels. Raw agent/coding content can carry secrets. A central exporter policy protects against accidental attributes from automatic instrumentation.

**Tradeoff:** Some debugging requires consulting the durable record or sandbox artifact under its separate access controls. SQLAlchemy auto-instrumentation is opt-in because a real coding trace generated about 600 SQL spans; high-level transaction spans are the local default.

## Telemetry is outside correctness

**Decision:** Batch span export with a bounded queue and timeout. Export metrics periodically. Never make a workflow transaction, Kafka acknowledgement, model/tool result, or lease depend on Collector/Tempo/Prometheus/Grafana availability. Count recurring heartbeats instead of emitting a span for each.

**Reason:** Telemetry outages and backpressure must not become workflow outages or lock regressions. A real Collector stop test completed five workflows and a post-restart workflow produced a new Tempo trace.

**Tradeoff:** Telemetry can be lost during an outage, and counters can miss an event if a process dies just after its durable commit. PostgreSQL and external effect state remain the audit authority. Local 100% sampling is a development default only.
