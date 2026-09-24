"""Locate a workflow in local Tempo and print safe span names and Grafana route."""

import argparse
import json
from collections import Counter
from typing import Any

import httpx


def _value(attributes: list[dict[str, Any]], key: str) -> str | None:
    for attribute in attributes:
        if attribute.get("key") == key:
            raw: dict[str, Any] = attribute.get("value", {})
            return raw.get("stringValue")
    return None


def search_workflow(workflow_id: str, tempo_url: str) -> list[str]:
    response = httpx.get(
        f"{tempo_url.rstrip('/')}/api/search",
        params={"tags": f"continuum.workflow.id={workflow_id}", "limit": 50},
        timeout=20,
    )
    response.raise_for_status()
    return [item["traceID"] for item in response.json().get("traces", [])]


def fetch_trace(trace_id: str, tempo_url: str) -> list[dict[str, Any]]:
    response = httpx.get(f"{tempo_url.rstrip('/')}/api/v2/traces/{trace_id}", timeout=20)
    response.raise_for_status()
    groups: list[dict[str, Any]] = response.json()["trace"]["resourceSpans"]
    spans: list[dict[str, Any]] = []
    for group in groups:
        service = _value(group.get("resource", {}).get("attributes", []), "service.name")
        for scope in group.get("scopeSpans", []):
            for item in scope.get("spans", []):
                spans.append(
                    {
                        "service": service or "unknown",
                        "name": item["name"],
                        "span_id": item.get("spanId"),
                        "parent_span_id": item.get("parentSpanId"),
                        "attributes": {
                            attr["key"]: attr.get("value", {})
                            for attr in item.get("attributes", [])
                        },
                    }
                )
    return spans


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workflow_id")
    parser.add_argument("--tempo-url", default="http://127.0.0.1:3200")
    args = parser.parse_args()
    results = []
    for trace_id in search_workflow(args.workflow_id, args.tempo_url):
        spans = fetch_trace(trace_id, args.tempo_url)
        results.append(
            {
                "trace_id": trace_id,
                "grafana_url": f'http://127.0.0.1:3000/explore?left={{"datasource":"tempo","queries":[{{"query":"{trace_id}","queryType":"traceql"}}]}}',
                "span_count": len(spans),
                "services": sorted({item["service"] for item in spans}),
                "spans": dict(Counter(item["name"] for item in spans)),
            }
        )
    print(json.dumps({"workflow_id": args.workflow_id, "traces": results}, indent=2))


if __name__ == "__main__":
    main()
