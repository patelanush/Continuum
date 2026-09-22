# Durable Agent Runtime (Continuum)

A fault-tolerant execution runtime for long-running AI workflows.

**What happens when an AI agent successfully performs an external action, but its worker crashes before recording the result?** Continuum's Phase 3 foundation records durable execution attempts, fences them with short leases, and retries only operations with explicit idempotency semantics. It does **not** claim exactly-once execution across PostgreSQL, Kafka, and an external service.

## Implemented through Phase 3

- Sequential workflow and step state machines, PostgreSQL row locks and constraints, transactional transition audit
- FastAPI create/get/list/start/cancel/history API and read-only attempt diagnostics
- Transactional outbox, Kafka KRaft transport, versioned JSON events, inbox dedupe, manual offset commits, DLQ
- Event consumers that transactionally turn a valid `step.ready` message into one `PENDING` execution attempt
- Independent executors that claim attempts using PostgreSQL `FOR UPDATE SKIP LOCKED`, with UUID lease tokens and heartbeats
- Recovery scheduler that uses PostgreSQL time to expire abandoned attempts and schedule bounded replacements
- Deterministic `noop`, `slow_noop`, and an idempotency-key-backed `mock_refund` tool
- Separate mock-payments HTTP service and PostgreSQL datastore for the external-side-effect crash demonstration

There are no LLMs, real payments, arbitrary tools, Redis, Kubernetes, or HA Kafka cluster.

## Architecture

```mermaid
flowchart TD
    Client --> API[FastAPI]
    API --> WS[Workflow Service]
    WS --> PG[(Continuum PostgreSQL)]
    PG -->|outbox rows| D[Outbox Dispatcher]
    D --> K[(Kafka KRaft)]
    K --> C[Event Consumer Group]
    C -->|inbox + pending attempt; then offset commit| PG
    PG -->|SKIP LOCKED claim| E[Executor Pool]
    E -->|stable Idempotency-Key| M[Mock Payments HTTP]
    M --> MP[(Independent Payments PostgreSQL)]
    E -->|fenced finalization| PG
    R[Recovery Scheduler] -->|expired leases, DB time| PG
```

The execution boundary is **reserve → commit → external execution without an open Continuum DB transaction → fenced finalize → commit**. Kafka ingestion ends after durable attempt creation; a Kafka record is never held in-flight during a long tool call. A killed executor leaves an attempt whose lease expires. Another executor retries the same logical step with the same operation key (`continuum:<step_id>`). The mock payment API returns the original refund for a repeated key and matching parameters. This makes the demonstrated refund retry safe; it does not make arbitrary external effects safe.

## Quick start

Requires Docker Compose. All services use local, no-cost images and safe development credentials. PostgreSQL, Kafka, and mock-payments state use named volumes.

```bash
make up
curl http://localhost:8000/health/ready
curl http://localhost:8001/health/ready
```

For distributed execution:

```bash
docker compose up --build -d --scale worker=3 --scale executor=3
docker compose ps
```

`make down` retains volumes. `make reset` deletes this project's PostgreSQL, payments, and Kafka volumes irreversibly. Host ports default to API `8000`, mock-payments `8001`, Continuum PostgreSQL `55433`, payments PostgreSQL `55434`, and Kafka `19092`; the database/broker ports are overrideable in Compose.

## API example

```bash
curl -sS -X POST http://localhost:8000/api/v1/workflows \
  -H 'content-type: application/json' \
  -d '{"workflow_type":"demo","steps":[{"name":"first","step_type":"noop"},{"name":"second","step_type":"noop"}]}'
curl -sS -X POST http://localhost:8000/api/v1/workflows/WORKFLOW_ID/start
curl -sS http://localhost:8000/api/v1/workflows/WORKFLOW_ID
curl -sS http://localhost:8000/api/v1/workflows/WORKFLOW_ID/attempts
curl -sS http://localhost:8000/api/v1/workflows/WORKFLOW_ID/history
```

`/start` returns after its state/outbox transaction, not after execution. Poll GET until `SUCCEEDED` or `FAILED`. Attempts are read-only diagnostics. `mock_refund` input is `{"customer_id":"customer-123","amount":"49.99"}`. `slow_noop` accepts `{"duration_ms":1000}` for lease testing. The mock payment `delay_after_commit_ms` input is **test-only** and deliberately delays its HTTP response after the independent refund commit.

Workflow listing uses newest-first bounded offset pagination (`status`, `limit`, `offset`). It should move to cursor pagination before large-scale deployment.

## Tests and demonstrations

```bash
uv sync
docker compose up -d --wait postgres kafka mock-payments
docker compose up kafka-init
DATABASE_URL=postgresql+asyncpg://durable:durable@localhost:55433/durable_test uv run alembic upgrade head
make check
```

`make check` runs Ruff format/lint, strict mypy, and the complete unit, PostgreSQL, real-Kafka, API, and real-HTTP suite with an 85% coverage floor. `make test-unit`, `make test-kafka`, `make test-execution`, `make test-recovery`, and `make test-payments` run focused subsets. The application never uses `metadata.create_all()` as startup migration.

With the full scaled stack running, execute the repeatable local crash and distribution demo:

```bash
uv run python scripts/phase3_demo.py all
```

It SIGKILLs the precise executor owning a test attempt, polls durable state, and checks a committed refund is not duplicated. It also tests pure-work crash recovery, a heartbeat beyond the original lease, and 20 five-step workflows across multiple executors. The demo changes local state and kills test executor containers; run it only against this local Compose stack.

## Planned

- Phase 4: dedicated FaultLab and large-scale controlled failure injection
- Later: arbitrary tool reconciliation, approval gates, LLM integration, sandboxed coding agents, observability, deployment topology

See [Architecture](docs/ARCHITECTURE.md), [Design Decisions](docs/DESIGN_DECISIONS.md), and [Failure Model](docs/FAILURE_MODEL.md) for exact boundaries and limitations.
