"""Provisioned panels must query instruments the application actually defines."""

import json
import re
from pathlib import Path

from durable_agent_runtime.observability.metrics import COUNTER_LABELS, GAUGES, HISTOGRAM_LABELS


def test_dashboard_queries_reference_declared_metrics() -> None:
    root = Path(__file__).resolve().parents[2] / "observability/grafana/dashboards"
    files = sorted(root.glob("*.json"))
    assert len(files) == 4
    declared = {f"{name}_total" for name in COUNTER_LABELS} | set(GAUGES)
    declared |= {
        f"{name}_{suffix}" for name in HISTOGRAM_LABELS for suffix in ("bucket", "sum", "count")
    }
    titles = set()
    for path in files:
        dashboard = json.loads(path.read_text())
        titles.add(dashboard["title"])
        assert dashboard["panels"]
        for panel in dashboard["panels"]:
            for target in panel["targets"]:
                expression = target["expr"]
                referenced = set(re.findall(r"\bcontinuum_[a-z0-9_]+\b", expression))
                assert referenced, (path.name, panel["title"])
                assert referenced <= declared, (path.name, panel["title"], referenced - declared)
    assert titles == {
        "Continuum — System Overview",
        "Continuum — Reliability",
        "Continuum — AI Agent",
        "Continuum — Coding Agent",
    }
