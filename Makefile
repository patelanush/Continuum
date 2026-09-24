.PHONY: up down reset migrate migration test test-unit test-integration test-kafka test-execution test-recovery test-payments test-agent test-coding lint format format-check typecheck check logs scale-workers scale-executors faultlab-up faultlab-smoke faultlab-ai-smoke faultlab-coding-smoke faultlab-reliability faultlab-side-effects faultlab-clean agent-demo-fake agent-demo-ollama coding-demo-fake coding-demo-ollama observability-up observability-down observability-logs observability-check trace-demo metrics-demo recovery-trace-demo

DATABASE_URL ?= postgresql+asyncpg://durable:durable@127.0.0.1:55433/durable
TEST_DATABASE_URL ?= postgresql+asyncpg://durable:durable@127.0.0.1:55433/durable_test
KAFKA_BOOTSTRAP_SERVERS ?= 127.0.0.1:19092
FAULTLAB_TEST_DATABASE_URL = postgresql+asyncpg://durable:durable@127.0.0.1:55435/durable_test

up:
	docker compose up --build -d

observability-up:
	OTEL_ENABLED=true docker compose --profile observability up --build -d

observability-down:
	docker compose --profile observability stop grafana prometheus otel-collector tempo

observability-logs:
	docker compose --profile observability logs -f otel-collector tempo prometheus grafana

observability-check:
	uv run python scripts/phase7_observability_check.py

trace-demo:
	uv run python scripts/phase7_trace_demo.py

metrics-demo:
	uv run python scripts/phase7_metrics_demo.py

recovery-trace-demo:
	uv run python scripts/phase7_recovery_demo.py

scale-workers:
	docker compose up --build -d --scale worker=3

scale-executors:
	docker compose up --build -d --scale executor=3

down:
	docker compose down

reset:
	docker compose down --volumes --remove-orphans

migrate:
	DATABASE_URL=$(DATABASE_URL) uv run alembic upgrade head

migration:
	DATABASE_URL=$(DATABASE_URL) uv run alembic revision --autogenerate -m "$(message)"

faultlab-up:
	uv run continuum-faultlab up

faultlab-smoke:
	uv run continuum-faultlab campaign smoke

faultlab-ai-smoke:
	uv run continuum-faultlab campaign ai-smoke

faultlab-coding-smoke:
	uv run continuum-faultlab campaign coding-smoke

faultlab-reliability:
	uv run continuum-faultlab campaign reliability --concurrency 8

faultlab-side-effects:
	uv run continuum-faultlab campaign side-effects

faultlab-clean:
	uv run continuum-faultlab clean

test: faultlab-up
	@trap 'uv run continuum-faultlab clean' EXIT; \
	DATABASE_URL=$(FAULTLAB_TEST_DATABASE_URL) uv run alembic upgrade head && \
	RUN_FAULTLAB_DOCKER=1 TEST_DATABASE_URL=$(FAULTLAB_TEST_DATABASE_URL) \
	KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19093 \
	MOCK_PAYMENTS_URL=http://127.0.0.1:18001 \
	PAYMENTS_DATABASE_URL=postgresql://payments:payments@127.0.0.1:55436/payments \
	uv run pytest

test-unit:
	uv run pytest tests/unit --no-cov

test-integration:
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) uv run pytest tests/integration tests/api --no-cov

test-kafka:
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) uv run pytest -m kafka --no-cov

test-execution:
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) uv run pytest tests/integration/test_execution.py --no-cov

test-recovery:
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) uv run pytest tests/integration/test_recovery.py --no-cov

test-payments:
	uv run pytest tests/integration/test_mock_payments.py tests/integration/test_mock_payments_routes.py --no-cov

test-agent:
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) uv run pytest tests/unit/test_agent.py tests/integration/test_agent.py --no-cov

test-coding:
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) uv run pytest tests/unit/test_coding_security.py tests/integration/test_coding_agent.py tests/integration/test_coding_sandbox.py --no-cov

agent-demo-fake:
	uv run python scripts/phase5_agent_demo.py --provider fake

agent-demo-ollama:
	uv run python scripts/phase5_agent_demo.py --provider ollama

coding-demo-fake:
	uv run python scripts/phase6_coding_demo.py --provider fake

coding-demo-ollama:
	uv run python scripts/phase6_coding_demo.py --provider ollama --timeout 600

logs:
	docker compose logs -f api dispatcher worker executor recovery-scheduler mock-payments kafka

lint:
	uv run ruff check .

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

typecheck:
	uv run mypy

check: format-check lint typecheck test
