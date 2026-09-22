# Failure Model through Phase 2

The runtime uses at-least-once Kafka delivery, durable PostgreSQL state, a transactional outbox,
and an idempotent consumer inbox. It does not provide exactly-once distributed execution.

## Addressed now

- **Database commit before broker availability:** The workflow state, audit rows, and outbox row
  commit together. If Kafka is unavailable, the row remains unpublished and the dispatcher
  retries. A physical single-broker stop/restart was exercised locally.
- **Dispatcher ack then crash:** The event may be published again because `published_at` was not
  committed. It keeps the same `event_id`; the inbox deduplicates it. This boundary is modeled by
  republishing the same envelope, not by physically crashing a dispatcher at that instant.
- **Duplicate delivery or missing Kafka offset commit:** A worker may commit database state then
  fail before offset commit. Redelivery uses `INSERT ... ON CONFLICT DO NOTHING` on
  `(consumer_group,event_id)`; the business operation is not repeated. Integration tests omit the
  first offset commit and redeliver the same event ID through real Kafka.
- **Stale event:** A cancelled or already-advanced workflow wins over the Kafka record. The event
  is recorded as consumed and causes no new state transition or downstream work.
- **Malformed or unsupported envelope:** Validation failures and messages inconsistent with their
  durable outbox row go to the dead-letter topic. The source offset is committed only after an
  acknowledged DLQ publish. If DLQ publication fails, the source remains eligible for retry.
- **Distributed duplicate race:** A unique inbox index and workflow row locks coordinate workers
  across processes. Tests race independent PostgreSQL sessions.
- **Database failure before commit:** Inbox, workflow state, audit, and downstream outbox changes
  all roll back. The Kafka offset remains uncommitted.
- **Broker container restart:** The local named Kafka volume retains topic data; application
  state remains in PostgreSQL. This is a single-broker restart test, not high availability.

## Failure sequences

| Boundary | Durable result | Recovery |
| --- | --- | --- |
| DB transaction fails | No inbox, state, audit, or new outbox commit | Kafka redelivery retries |
| DB commits; offset commit fails | State and inbox exist | Redelivery is deduplicated, then offset can commit |
| Outbox send fails | Outbox stays unpublished with attempt/error | Dispatcher retries |
| Kafka ack succeeds; dispatcher dies before DB mark | Same outbox row remains unpublished | Republish same event ID; inbox deduplicates |
| Event is stale | No workflow mutation | Inbox records handled stale event; offset commits |
| DLQ send fails | No source offset commit | Worker retries the message/DLQ send |

## Still deferred

Phase 2 `noop` steps finish inside one short database transaction. This does not solve a worker
dying during a long-running tool call, uncertain external side effects, model/tool timeouts,
retry scheduling, tool reconciliation, or sandbox crashes. Worker leases and heartbeats, human
approval, Redis coordination, full fault injection, and multi-broker disaster recovery are not
implemented. A future tool executor must define idempotency keys and reconciliation for effects
that succeed externally while their acknowledgement is lost.

PostgreSQL interruption during commit can still leave a caller uncertain whether the transaction
succeeded. Durable state can be reread; an automated reconciliation policy remains future work.
The local dead-letter topic has no replay UI or retention policy yet.
