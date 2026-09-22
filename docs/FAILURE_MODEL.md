# Failure Model through Phase 3

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
- Cancellation fences durable finalization but cannot guarantee that an already-sent external HTTP request was cancelled before its effect. Human approval and compensating actions remain future work.
- Kafka is one local KRaft broker with a named volume, not broker HA or disaster recovery. Multi-region recovery is not implemented.
- LLM/model failures, arbitrary tool timeouts, code sandbox crashes, human approval, Redis coordination, OpenTelemetry, Kubernetes, and a full FaultLab campaign remain planned.
- A database interruption around a commit can leave a caller uncertain whether that commit succeeded. Durable rereads and idempotent commands/keys limit harm; comprehensive reconciliation and large-scale fault testing are Phase 4+ work.
