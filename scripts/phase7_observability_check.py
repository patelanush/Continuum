"""Probe real local backends, provisioned Grafana objects and stored spans."""

import asyncio
import json
from pathlib import Path
from time import monotonic

import httpx
from trace_workflow import fetch_trace, search_workflow


async def smoke_workflow() -> str:
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=10) as client:
        created = await client.post(
            "/api/v1/workflows",
            json={
                "workflow_type": "coding-demo",
                "steps": [{"name": "observe", "step_type": "noop", "input": {}}],
            },
        )
        created.raise_for_status()
        workflow_id: str = created.json()["id"]
        started = await client.post(f"/api/v1/workflows/{workflow_id}/start")
        started.raise_for_status()
        for _ in range(80):
            response = await client.get(f"/api/v1/workflows/{workflow_id}")
            response.raise_for_status()
            if response.json()["status"] == "SUCCEEDED":
                return workflow_id
            await asyncio.sleep(0.25)
    raise AssertionError("Observability smoke workflow did not complete")


async def main() -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        for url in (
            "http://127.0.0.1:3200/ready",
            "http://127.0.0.1:9090/-/ready",
            "http://127.0.0.1:3000/api/health",
        ):
            deadline = monotonic() + 45
            while True:
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    break
                except httpx.HTTPError:
                    if monotonic() >= deadline:
                        raise
                    await asyncio.sleep(1)
        targets = (await client.get("http://127.0.0.1:9090/api/v1/targets")).json()["data"][
            "activeTargets"
        ]
        if not any(
            item["labels"].get("job") == "continuum" and item["health"] == "up" for item in targets
        ):
            raise AssertionError("Prometheus is not scraping the Collector")
        auth = ("admin", "continuum-local")
        datasources = (await client.get("http://127.0.0.1:3000/api/datasources", auth=auth)).json()
        if {item["uid"] for item in datasources} != {"prometheus", "tempo"}:
            raise AssertionError("Grafana datasources were not provisioned")
        for uid in ("prometheus", "tempo"):
            health = await client.get(
                f"http://127.0.0.1:3000/api/datasources/uid/{uid}/health", auth=auth
            )
            health.raise_for_status()
            if health.json()["status"] != "OK":
                raise AssertionError(f"Grafana datasource {uid} is unhealthy")
        dashboards = (
            await client.get("http://127.0.0.1:3000/api/search?type=dash-db", auth=auth)
        ).json()
        if len([item for item in dashboards if item["uid"].startswith("continuum-")]) < 4:
            raise AssertionError("Continuum dashboards were not provisioned")
        query_count = 0
        paths = await asyncio.to_thread(
            lambda: list(Path("observability/grafana/dashboards").glob("*.json"))
        )
        for path in paths:
            dashboard = json.loads(await asyncio.to_thread(path.read_text))
            for panel in dashboard["panels"]:
                for target in panel["targets"]:
                    query = await client.get(
                        "http://127.0.0.1:9090/api/v1/query",
                        params={"query": target["expr"]},
                    )
                    query.raise_for_status()
                    if query.json()["status"] != "success":
                        raise AssertionError(f"Invalid dashboard query in {path.name}")
                    query_count += 1
    workflow_id = await smoke_workflow()
    deadline = monotonic() + 30
    trace_id = None
    while monotonic() < deadline:
        for candidate in search_workflow(workflow_id, "http://127.0.0.1:3200"):
            names = {item["name"] for item in fetch_trace(candidate, "http://127.0.0.1:3200")}
            if {"api.workflow.start", "kafka.publish", "kafka.consume", "execution.run"} <= names:
                trace_id = candidate
                break
        if trace_id:
            break
        await asyncio.sleep(1)
    if not trace_id:
        raise AssertionError("Tempo did not store a cross-service workflow trace")
    async with httpx.AsyncClient(timeout=10) as client:
        query = await client.get(
            "http://127.0.0.1:9090/api/v1/query",
            params={"query": "continuum_workflows_started_total"},
        )
        query.raise_for_status()
        if not query.json()["data"]["result"]:
            raise AssertionError("Prometheus has no Continuum workflow metrics")
    print(
        json.dumps(
            {
                "collector_scrape": "up",
                "tempo_trace_id": trace_id,
                "workflow_id": workflow_id,
                "grafana_datasources": ["prometheus", "tempo"],
                "grafana_dashboards": len(dashboards),
                "dashboard_queries_validated": query_count,
                "continuum_metric_present": True,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
