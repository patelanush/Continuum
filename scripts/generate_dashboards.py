"""Generate reviewable local Grafana dashboards from bounded metric names."""

# PromQL query literals are kept intact for dashboard review.
# ruff: noqa: E501

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "observability/grafana/dashboards"
Query = str | tuple[tuple[str, str], ...]


def panel(number: int, title: str, query: Query, *, unit: str = "short") -> dict[str, object]:
    expressions = (
        ((query, "{{status}} {{step_type}} {{provider}} {{model}} {{tool_name}} {{reason}}"),)
        if isinstance(query, str)
        else query
    )
    return {
        "id": number,
        "title": title,
        "type": "timeseries",
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "gridPos": {"h": 8, "w": 12, "x": 0 if number % 2 else 12, "y": ((number - 1) // 2) * 8},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom"}},
        "targets": [
            {
                "expr": expression,
                "refId": chr(65 + index),
                "legendFormat": legend,
            }
            for index, (expression, legend) in enumerate(expressions)
        ],
    }


DASHBOARDS: dict[str, tuple[str, list[tuple[str, Query, str]]]] = {
    "system-overview": (
        "Continuum — System Overview",
        [
            ("Workflow start rate", "sum(rate(continuum_workflows_started_total[5m]))", "ops"),
            (
                "Workflow completions",
                "sum by (status) (rate(continuum_workflows_completed_total[5m]))",
                "ops",
            ),
            ("Active workflows", "continuum_active_workflows", "short"),
            (
                "Pending and running attempts",
                (
                    ("continuum_pending_execution_attempts", "pending"),
                    ("continuum_running_execution_attempts", "running"),
                ),
                "short",
            ),
            (
                "Recoveries and lease expirations",
                (
                    (
                        "sum by (reason) (rate(continuum_recoveries_total[5m]))",
                        "recoveries {{reason}}",
                    ),
                    ("rate(continuum_lease_expirations_total[5m])", "lease expirations"),
                ),
                "ops",
            ),
            (
                "Kafka publish and consume",
                (
                    ("sum(rate(continuum_kafka_events_published_total[5m]))", "published"),
                    ("sum(rate(continuum_kafka_events_consumed_total[5m]))", "consumed"),
                ),
                "ops",
            ),
            ("Unpublished outbox", "continuum_unpublished_outbox_events", "short"),
            ("DLQ messages", "sum(increase(continuum_dlq_messages_total[1h]))", "short"),
            ("Model calls", "sum by (status) (rate(continuum_model_calls_total[5m]))", "ops"),
            ("Tool calls", "sum by (status) (rate(continuum_tool_calls_total[5m]))", "ops"),
            (
                "Sandbox commands",
                "sum by (status) (rate(continuum_sandbox_commands_total[5m]))",
                "ops",
            ),
        ],
    ),
    "reliability": (
        "Continuum — Reliability",
        [
            (
                "Attempt outcomes",
                "sum by (status, step_type) (increase(continuum_execution_attempts_total[1h]))",
                "short",
            ),
            (
                "Recoveries by reason",
                "sum by (reason) (increase(continuum_recoveries_total[1h]))",
                "short",
            ),
            ("Lease expirations", "increase(continuum_lease_expirations_total[1h])", "short"),
            ("Kafka duplicates", "increase(continuum_kafka_duplicates_total[1h])", "short"),
            (
                "DLQ by reason",
                "sum by (reason) (increase(continuum_dlq_messages_total[1h]))",
                "short",
            ),
            (
                "Failed tools",
                'sum by (tool_name) (increase(continuum_tool_calls_total{status="failed"}[1h]))',
                "short",
            ),
            (
                "Failed steps",
                'sum by (step_type) (increase(continuum_steps_completed_total{status="failed"}[1h]))',
                "short",
            ),
            (
                "Recovery latency p50 / p95",
                (
                    (
                        "histogram_quantile(0.50, sum by (le) (rate(continuum_recovery_duration_seconds_bucket[5m])))",
                        "p50",
                    ),
                    (
                        "histogram_quantile(0.95, sum by (le) (rate(continuum_recovery_duration_seconds_bucket[5m])))",
                        "p95",
                    ),
                ),
                "s",
            ),
            (
                "Attempt duration p50 / p95",
                (
                    (
                        "histogram_quantile(0.50, sum by (le) (rate(continuum_execution_attempt_duration_seconds_bucket[5m])))",
                        "p50",
                    ),
                    (
                        "histogram_quantile(0.95, sum by (le) (rate(continuum_execution_attempt_duration_seconds_bucket[5m])))",
                        "p95",
                    ),
                ),
                "s",
            ),
            ("Pending attempts", "continuum_pending_execution_attempts", "short"),
            ("Unpublished outbox", "continuum_unpublished_outbox_events", "short"),
        ],
    ),
    "ai-agent": (
        "Continuum — AI Agent",
        [
            (
                "Agent run outcomes",
                "sum by (agent_type, status) (increase(continuum_agent_runs_total[1h]))",
                "short",
            ),
            (
                "Model calls",
                "sum by (provider, model, status) (rate(continuum_model_calls_total[5m]))",
                "ops",
            ),
            (
                "Model latency p50 / p95",
                (
                    (
                        "histogram_quantile(0.50, sum by (le) (rate(continuum_model_call_duration_seconds_bucket[5m])))",
                        "p50",
                    ),
                    (
                        "histogram_quantile(0.95, sum by (le) (rate(continuum_model_call_duration_seconds_bucket[5m])))",
                        "p95",
                    ),
                ),
                "s",
            ),
            (
                "Input and output tokens",
                "sum by (direction) (rate(continuum_model_tokens_total[5m]))",
                "ops",
            ),
            (
                "Tool calls",
                "sum by (tool_name, status) (rate(continuum_tool_calls_total[5m]))",
                "ops",
            ),
            (
                "Tool latency p95",
                "histogram_quantile(0.95, sum by (tool_name, le) (rate(continuum_tool_call_duration_seconds_bucket[5m])))",
                "s",
            ),
            (
                "Coding patch and tests",
                'sum by (command_type, status) (increase(continuum_sandbox_commands_total{command_type=~"apply_patch|run_tests"}[1h]))',
                "short",
            ),
            (
                "Approval decisions",
                "sum by (decision) (increase(continuum_approvals_total[1h]))",
                "short",
            ),
        ],
    ),
    "coding-agent": (
        "Continuum — Coding Agent",
        [
            (
                "Sandbox startup p95",
                "histogram_quantile(0.95, sum by (le) (rate(continuum_sandbox_startup_duration_seconds_bucket[5m])))",
                "s",
            ),
            (
                "Sandbox command outcomes",
                "sum by (command_type, status) (increase(continuum_sandbox_commands_total[1h]))",
                "short",
            ),
            (
                "Test duration p95",
                "histogram_quantile(0.95, sum by (le) (rate(continuum_test_duration_seconds_bucket[5m])))",
                "s",
            ),
            (
                "Test success and failure",
                'sum by (status) (increase(continuum_sandbox_commands_total{command_type="run_tests"}[1h]))',
                "short",
            ),
            (
                "Workspace reconciliation",
                "sum by (result) (increase(continuum_workspace_reconciliations_total[1h]))",
                "short",
            ),
            (
                "Commit outcomes",
                "sum by (status) (increase(continuum_git_commits_total[1h]))",
                "short",
            ),
            ("Pending approvals", "continuum_pending_approvals", "short"),
            (
                "Coding workflow completion",
                'sum by (status) (increase(continuum_steps_completed_total{step_type="coding_agent"}[1h]))',
                "short",
            ),
        ],
    ),
}


def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    for uid, (title, definitions) in DASHBOARDS.items():
        dashboard = {
            "uid": f"continuum-{uid}",
            "title": title,
            "schemaVersion": 39,
            "version": 1,
            "refresh": "10s",
            "time": {"from": "now-1h", "to": "now"},
            "tags": ["continuum", "local"],
            "panels": [
                panel(index, name, query, unit=unit)
                for index, (name, query, unit) in enumerate(definitions, 1)
            ],
        }
        (ROOT / f"{uid}.json").write_text(json.dumps(dashboard, indent=2) + "\n")


if __name__ == "__main__":
    main()
