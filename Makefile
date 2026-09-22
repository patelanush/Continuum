.PHONY: up down reset migrate migration test test-unit test-integration test-kafka test-execution test-recovery test-payments lint format format-check typecheck check logs scale-workers scale-executors

DATABASE_URL ?= postgresql+asyncpg://durable:durable@localhost:55433/durable
TEST_DATABASE_URL ?= postgresql+asyncpg://durable:durable@localhost:55433/durable_test
KAFKA_BOOTSTRAP_SERVERS ?= localhost:19092

up:
	docker compose up --build -d

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

test:
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) uv run pytest

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
