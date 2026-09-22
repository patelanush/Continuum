# Design Decisions through Phase 3

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
