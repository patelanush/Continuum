# Architecture through Phase 2

## Ownership and layers

PostgreSQL owns workflow status, ordered steps, the current position, transition audit, outbox,
and consumed-event inbox. Kafka transports ready-step commands. A message is a request to inspect
database state, not proof that a step remains executable.

FastAPI validates requests and invokes the workflow service. The service owns state-machine rules,
row locks, audit rows, and outbox creation. The dispatcher reads unpublished outbox rows and
publishes them to Kafka. Workers consume, validate, deduplicate, execute `noop`, and persist the
result through the same service.

## Producer path and the DB/Kafka boundary

`start_workflow` commits workflow `PENDING -> RUNNING`, step 0 `PENDING -> READY`, the pointer,
audit rows, and a `step.ready` outbox row in one transaction. Intermediate completion commits the
current step's success, the next step's readiness, pointer advance, audit rows, and the next
outbox row in one transaction. A rollback removes every component.

The dispatcher polls up to 20 unpublished rows using `FOR UPDATE SKIP LOCKED`. It publishes JSON
with `acks=all` and producer idempotence, waits for acknowledgement, then sets `published_at`.
Failed sends increment `publish_attempts` and set `last_error` while leaving `published_at` NULL.
Polling is bounded and retries after temporary broker failure. Multiple dispatchers can safely
poll without selecting the same locked row concurrently.

There is no atomic transaction spanning PostgreSQL and Kafka. If the dispatcher crashes after a
Kafka acknowledgement but before committing `published_at`, the row is republished with the same
outbox ID as `event_id`. This is expected at-least-once publication. Producer idempotence reduces
some producer-level duplicates but cannot close the PostgreSQL/Kafka gap.

## Consumer path

Workers in `continuum-workers-v1` use `enable_auto_commit=False`. They validate the versioned JSON
envelope and Kafka key, then check that the event matches its durable outbox row. Inside one
PostgreSQL transaction, `INSERT ... ON CONFLICT DO NOTHING ... RETURNING` claims
`(consumer_group,event_id)`, and the workflow service locks the workflow and steps. A duplicate
returns without a second state change. A distinct stale event is recorded in the inbox and makes
no workflow mutation. A current `noop` step moves READY -> RUNNING -> SUCCEEDED; this creates the
next outbox row or completes the workflow. The database commit precedes the explicit Kafka offset
commit.

If database processing fails, inbox, state, audit, and downstream outbox changes roll back, and
the worker retries without committing the offset. If the offset commit fails after the database
commit, Kafka may redeliver; the inbox uniqueness constraint turns the replay into a no-op.
Malformed or unsupported envelopes and valid envelopes that do not match durable outbox state
go to `continuum.dead-letter.v1`. The worker commits the source offset only after an acknowledged
DLQ write. DLQ records contain a payload hash and length, not raw message content.

## Kafka topology and ordering

The local Compose topology is one Kafka 3.9.1 broker in KRaft mode with a named data volume. It
has no ZooKeeper and no broker high availability. `kafka-init` deterministically creates
`continuum.step.ready.v1` and `continuum.dead-letter.v1`, each with three partitions. Ready
events use `workflow_id` as the Kafka key. Kafka preserves order within a partition, so events
for one workflow route together; no global ordering is claimed. Three workers can each own a
partition and process different workflows concurrently. Steps within a workflow remain strictly
sequential, with no DAG or parallel-step execution.

## Database constraints and transaction ownership

Workflow and step transitions remain explicit domain maps. Every mutating command takes a
`SELECT ... FOR UPDATE` lock on the workflow, then its steps in position order. PostgreSQL also
enforces unique `(workflow_id,position)` and a partial unique index allowing at most one READY or
RUNNING step. The success handoff flushes the old step's terminal status before activating the
next step, within the same transaction.

Phase 1 public service methods still open and commit their own transactions. Phase 2 adds narrow
`*_in_transaction` service methods so the worker can own one transaction containing inbox claim,
state changes, audit writes, and downstream outbox creation. These methods never commit. No
process-local lock participates in correctness.

## Audit and outbox distinction

`state_transitions` is the immutable-through-application history of status changes.
`outbox_events` is pending transport work. `consumed_events` is the durable inbox for one
consumer group. They are separate concepts and may have different retention needs in later
phases. A completed workflow keeps `current_step_position` at its final position for inspection;
terminal status prevents further execution.
