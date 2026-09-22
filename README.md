# Durable Agent Runtime

A fault-tolerant execution runtime for long-running AI workflows.

Continuum currently provides durable, sequential workflows executed asynchronously by Kafka
workers. PostgreSQL is the source of truth for workflow state. Apache Kafka transports ready-step
events with at-least-once delivery; a transactional outbox and durable consumer inbox make
publication and duplicate consumption safe. Phase 2 executes deterministic `noop` steps only.
There are no external tools or LLM calls yet, and the runtime does not claim exactly-once execution.

## Implemented

- Explicit workflow and step state machines with PostgreSQL row locks and transactional audits
- FastAPI create, retrieve, list, start, cancel, history, liveness, and readiness endpoints
- Transactional `step.ready` outbox events on start and sequential progression
- Apache Kafka 3.9.1 in single-broker KRaft mode, with three ready-topic partitions and a DLQ
- A standalone outbox dispatcher using acknowledged, idempotent Kafka producer sends
- Scalable workers in one consumer group, with manual offsets committed after database work
- Atomic inbox deduplication by `(consumer_group, event_id)` and durable stale-event handling
- JSON event schema version 1; malformed or unsupported messages go to the DLQ
- Deterministic `noop` execution from READY through SUCCEEDED to the next step or final workflow
- PostgreSQL and real Kafka integration tests, CI, migrations, and Docker Compose

Worker leases, long-running execution, retries, external side effects, Redis, observability,
sandboxing, and Kubernetes remain planned.

## Architecture

```mermaid
flowchart TD
    Client --> API[FastAPI]
    API --> Service[Workflow Service]
    Service --> PG[(PostgreSQL: workflows, steps, audit, outbox, inbox)]
    PG --> Dispatcher[Outbox Dispatcher]
    Dispatcher --> Kafka[(Kafka: step.ready.v1)]
    Kafka --> Workers[Worker Consumer Group]
    Workers --> Service
    Workers --> DLQ[(Kafka: dead-letter.v1)]
```

The service writes step state, audit rows, and outbox work in one PostgreSQL transaction. The
dispatcher publishes the stable `event_id` and marks its outbox row published only after a Kafka
acknowledgement. A worker inserts its inbox record and applies the workflow transition in one
transaction, then commits the Kafka offset. Replays are expected and harmless for database state.
See [Architecture](docs/ARCHITECTURE.md) for the failure boundaries.

## Quick start

Requirements: Docker Compose. PostgreSQL uses host port `55433`; Kafka uses `19092`. Both are
configurable with `POSTGRES_PORT` and `KAFKA_PORT`. Inside Compose, services use ports 5432 and 9092.

```bash
docker compose up --build -d --scale worker=3
curl http://localhost:8000/health/ready
```

Compose waits for PostgreSQL and Kafka health, creates both topics, runs the Alembic migration,
and starts the API, dispatcher, and workers. Data uses named PostgreSQL and Kafka volumes.

Common commands:

```bash
make up             # full stack with one worker
make scale-workers  # full stack with three workers
make logs           # follow API, dispatcher, worker, and Kafka logs
make down           # stop, retain volumes
make reset          # delete this project's PostgreSQL and Kafka volumes
```

`make reset` irreversibly removes local workflow and broker data. No database server installed on
the host is required.

## API example

Create a five-step workflow:

```bash
curl -sS -X POST http://localhost:8000/api/v1/workflows \
  -H 'content-type: application/json' \
  -d '{"workflow_type":"demo","input":{"task":"example"},"steps":[
    {"name":"one","step_type":"noop"},
    {"name":"two","step_type":"noop"},
    {"name":"three","step_type":"noop"},
    {"name":"four","step_type":"noop"},
    {"name":"five","step_type":"noop"}]}'
```

Use the returned UUID:

```bash
curl -sS -X POST http://localhost:8000/api/v1/workflows/WORKFLOW_ID/start
curl -sS http://localhost:8000/api/v1/workflows/WORKFLOW_ID
curl -sS http://localhost:8000/api/v1/workflows/WORKFLOW_ID/history
```

`/start` returns after the PostgreSQL transaction, usually with status `RUNNING`; workers finish
the workflow asynchronously. Poll `GET` for `SUCCEEDED` or `FAILED`. Start is idempotent while
already running. Cancelling a nonterminal workflow makes any later ready event stale.

Listing supports `status`, `limit` (1–100), and `offset`, newest first. Offset pagination remains
a Phase 1 simplicity choice and should become cursor pagination for large deployments.

Only `noop` is executable in Phase 2. A ready step with another opaque `step_type` is marked
FAILED with `UNSUPPORTED_STEP_TYPE` so it does not stall indefinitely.

## Development and tests

Install Python 3.12 and `uv`, then:

```bash
uv sync
docker compose up -d --wait postgres kafka
docker compose up kafka-init
DATABASE_URL=postgresql+asyncpg://durable:durable@localhost:55433/durable_test \
  uv run alembic upgrade head
make check
```

`make check` verifies Ruff format, Ruff lint, strict mypy, and all unit, PostgreSQL, API, and real
Kafka tests with an 85% coverage floor. `make test-unit` does not require Docker. `make test-kafka`
runs the Kafka-marked tests; they use isolated topics and the test PostgreSQL database.
`make migrate` upgrades the development database. The application never uses `create_all()` at
startup.

## Roadmap

- **Phase 3 (planned):** worker leases, heartbeats, retry/backoff scheduling, and external-effect
  idempotency/reconciliation
- **Phase 4 (planned):** tool execution
- **Phase 5 (planned):** FaultLab and controlled fault injection
- **Phase 6 (planned):** local and hosted AI provider integration
- **Phase 7 (planned):** sandboxed coding agent
- **Phase 8 (planned):** GitHub workflow integration
- **Phase 9 (planned):** distributed tracing and metrics
- **Phase 10 (planned):** benchmarking and Kubernetes

See [Design Decisions](docs/DESIGN_DECISIONS.md) and [Failure Model](docs/FAILURE_MODEL.md) for
the exact reliability claims and deferred failures.
