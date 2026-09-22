# Architecture through Phase 4

## Authority and layers

PostgreSQL owns workflow, step, attempt, lease, transition audit, outbox, and consumed-event state. Kafka transports `step.ready` events; a Kafka record is not proof a step remains READY. FastAPI invokes workflow services, never sets statuses directly. Executors invoke a small tool layer with no database session. Mock-payments is a separate HTTP process backed by an independent PostgreSQL database, so Continuum cannot atomically transact with a refund.

## Producer and event-ingestion paths

Starting a workflow commits `PENDING → RUNNING`, step 0 `PENDING → READY`, audit rows, and a `step.ready` outbox row atomically. Successful nonfinal step completion similarly commits the current step, next readiness, pointer, audit, and downstream outbox. The dispatcher briefly claims unpublished rows with `FOR UPDATE SKIP LOCKED` and a database-time, UUID-fenced publish lease, then **commits before Kafka I/O**. It waits for broker acknowledgement before a separate compare-and-set transaction writes `published_at`. Failed sends release the claim and record an error; a crashed publisher's claim expires. A publish that succeeds just before the dispatcher crashes can be repeated with the same outbox ID as `event_id`; inbox dedupe remains required. A slow send can outlive a claim and cause duplicate publication, but a stale publisher cannot finalize another claim.

The event consumer validates the versioned JSON envelope and key against the durable outbox row. In **one** PostgreSQL transaction, it inserts `(consumer_group,event_id)` using `ON CONFLICT DO NOTHING`, checks workflow/step truth, and creates attempt 1 `PENDING` if current and not already scheduled. A duplicate or stale event does not create work. Only after this transaction commits does it commit the Kafka offset. Invalid messages go to the acknowledged DLQ before source-offset commit. The consumer does not execute tools or hold a Kafka message during a long operation.

## Reserve → execute → finalize

1. **Reserve transaction:** An executor finds eligible PENDING attempts, locks workflow/steps/attempt in a consistent order with `FOR UPDATE SKIP LOCKED`, rechecks state, assigns an executor ID and random UUID lease token, sets an expiration using PostgreSQL time, and moves the attempt `PENDING → RUNNING`. The initial claim also moves step `READY → RUNNING` and audits that transition. A replacement claim leaves the step RUNNING and increments its attempt count without duplicating that audit edge. Commit.
2. **Execute with no Continuum transaction open:** The executor reads immutable step input, runs `noop`, `slow_noop`, or makes an HTTP request to mock-payments. A separate heartbeat task periodically compares attempt ID, status, executor ID, lease token, and unexpired lease in PostgreSQL before extending the lease. A failed compare-and-set means ownership is lost.
3. **Finalize transaction:** The service locks workflow/steps/attempt, validates current status and matching unexpired ownership, then commits attempt success/failure, output/error, step/workflow progression, audit, and any next outbox event together. A stale executor cannot finalize. A failed transaction rolls all components back.

All lease-expiry decisions use `clock_timestamp()` from PostgreSQL. Host clocks are not an ownership authority. The token fences an old process even if a later process reuses an executor ID. The transition state machine separately validates every attempt edge; no attempt can restart from a terminal status.

## Recovery

The scheduler polls RUNNING attempts whose lease expired according to database time. It locks workflow/steps/attempt, rechecks expiration, marks the attempt EXPIRED, and—only for explicitly safe retry semantics and while `attempt_number < max_attempts`—creates a single PENDING replacement in the same transaction. A partial unique index permits at most one PENDING/RUNNING attempt per step, while `UNIQUE(step_id,attempt_number)` prevents duplicate generations. Two scheduler processes can race safely via database locks and `SKIP LOCKED`. The step remains RUNNING between attempts. Exhaustion fails the step/workflow with `MAX_EXECUTION_ATTEMPTS_EXCEEDED`; an unknown/unsafe tool type is not automatically retried after a crash.

Cancellation moves nonterminal steps and active attempts to CANCELLED under the workflow lock. It fences finalization, but cannot revoke an HTTP side effect already in flight. Production-facing cancellation of external operations would need tool-specific reconciliation.

## External side effects and the ambiguous-result window

The operation key is derived from the stable step ID: `continuum:<step_id>`. It is unchanged across attempt 1, 2, or 3. Mock-payments requires `Idempotency-Key`, stores it uniquely in its own database, returns the existing refund for a matching repeat, and returns 409 if logical parameters differ. Its test-only post-commit delay creates the exact window: refund committed, response still pending, executor SIGKILLed. After lease expiry, a replacement sends the same key and receives the original refund. This proves the local mock-refund operation is safely retryable under that API's contract, **not** that arbitrary external operations are exactly once.

The runtime only registers `IDEMPOTENT` (`noop`, `slow_noop`) and `IDEMPOTENCY_KEY_SUPPORTED` (`mock_refund`) tools. Unknown or truly non-idempotent effects are not crash-retried automatically. Full reconciliation for APIs without idempotency keys remains deferred.

## Concurrency and topology

The service locks workflow then ordered steps for state changes. Claim and recovery lock in the same order before the attempt, preventing process-local correctness dependencies and limiting same-workflow contention. Existing PostgreSQL constraints still enforce unique step positions and one READY/RUNNING sequential step. Three Kafka partitions let different workflows flow through separate event consumers; `workflow_id` is the Kafka message key and ordering is only per partition. Executors compete independently for durable attempts using PostgreSQL `SKIP LOCKED`, so Kafka partition ownership does not pin a long tool call to one worker.

Compose uses one Kafka KRaft broker and named volumes, not broker high availability. The primary and mock-payments PostgreSQL instances are separate. Graceful SIGTERM stops new claims and drains an in-flight call for a bounded period while heartbeating. SIGKILL skips cleanup, which is why lease recovery exists.

## FaultLab boundary and evidence

FaultLab is an external harness, not a new workflow-state authority. It uses an exact, isolated Compose project with its own PostgreSQL/Kafka/payments volumes and different host ports. Docker controls are centralized and reject another project name. Each scenario records experiment/trial IDs, seed, Git revision and dirty state, injection boundary, expected/observed terminal status, attempts, transition/outbox duplicates, independently counted refunds, timing, and failure details in JSONL. Reports are regenerated from that raw file; no experiment results are written into Continuum's runtime schema.

Scenario cleanup and campaign exclusivity matter: container-kill, Kafka-stop, PostgreSQL-stop, and scheduler-race trials run serially; workflow-isolated baseline, duplicate-event, and external-response trials can run concurrently at a configured cap. The deliberately unsafe refund comparison uses two different idempotency keys only in the harness, never as a runtime option. FaultLab-only executor/dispatcher hooks are gated by `APP_ENV=faultlab`; mock-payment failure modes are rejected otherwise.

The harness checks PostgreSQL state and the *independent* payments service. A succeeded workflow with a second refund, missing refund, duplicate audit edge, duplicate logical outbox event, unexpected active attempt, or incorrect sequential step position is a failed trial. Some faults (SIGKILL, `docker pause`, Kafka/PostgreSQL outage) are real process/container failures. Stale-token and concurrent-scheduler tests intentionally force expiry with database time and run genuine concurrent PostgreSQL transactions. The distinction is retained in each scenario's name and documentation.
