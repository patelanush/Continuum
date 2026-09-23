FROM docker:28-cli AS docker-cli

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
RUN pip install --no-cache-dir uv==0.9.7
COPY pyproject.toml uv.lock* README.md ./
COPY src ./src
COPY fixtures/coding ./fixtures/coding
COPY alembic ./alembic
COPY alembic.ini ./
RUN uv sync --frozen --no-dev

EXPOSE 8000
CMD ["sh", "-c", "alembic upgrade head && exec uvicorn durable_agent_runtime.api.app:app --host 0.0.0.0 --port 8000"]
