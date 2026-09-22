from typing import Any
from uuid import uuid4

import pytest
from httpx import AsyncClient


@pytest.mark.integration
async def test_liveness_and_readiness(client: AsyncClient) -> None:
    assert (await client.get("/health/live")).json() == {"status": "ok"}
    ready = await client.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready"}


@pytest.mark.integration
async def test_create_get_list_filter_and_history(
    client: AsyncClient, workflow_payload: dict[str, object]
) -> None:
    created_response = await client.post("/api/v1/workflows", json=workflow_payload)
    assert created_response.status_code == 201
    created = created_response.json()
    assert created["status"] == "PENDING"
    assert [step["position"] for step in created["steps"]] == [0, 1]

    retrieved = await client.get(f"/api/v1/workflows/{created['id']}")
    assert retrieved.status_code == 200
    assert retrieved.json()["id"] == created["id"]

    listed = await client.get("/api/v1/workflows", params={"status": "PENDING", "limit": 1})
    assert listed.status_code == 200
    assert listed.json()["total"] == 1
    assert listed.json()["items"][0]["id"] == created["id"]

    empty = await client.get("/api/v1/workflows", params={"status": "RUNNING"})
    assert empty.json()["items"] == []

    history = await client.get(f"/api/v1/workflows/{created['id']}/history")
    assert history.status_code == 200
    assert [item["to_status"] for item in history.json()] == ["PENDING", "PENDING", "PENDING"]


@pytest.mark.integration
async def test_start_is_idempotent_then_cancel_and_conflict(
    client: AsyncClient, workflow_payload: dict[str, object]
) -> None:
    workflow_id = (await client.post("/api/v1/workflows", json=workflow_payload)).json()["id"]
    first = await client.post(f"/api/v1/workflows/{workflow_id}/start")
    second = await client.post(f"/api/v1/workflows/{workflow_id}/start")
    assert first.status_code == second.status_code == 200
    assert second.json()["status"] == "RUNNING"
    assert second.json()["steps"][0]["status"] == "READY"

    cancelled = await client.post(
        f"/api/v1/workflows/{workflow_id}/cancel", json={"reason": "operator request"}
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "CANCELLED"

    conflict = await client.post(f"/api/v1/workflows/{workflow_id}/start")
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "conflict"


@pytest.mark.integration
async def test_unknown_workflow_returns_consistent_404(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/workflows/{uuid4()}")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


@pytest.mark.integration
@pytest.mark.parametrize(
    "payload",
    [
        {"workflow_type": "demo", "steps": []},
        {"workflow_type": " ", "steps": [{"name": "x", "step_type": "noop"}]},
        {"workflow_type": "demo", "steps": [{"name": "", "step_type": "noop"}]},
        {
            "workflow_type": "demo",
            "steps": [{"name": "x", "step_type": "noop", "max_attempts": 0}],
        },
    ],
)
async def test_request_validation_returns_422(client: AsyncClient, payload: dict[str, Any]) -> None:
    response = await client.post("/api/v1/workflows", json=payload)
    assert response.status_code == 422


@pytest.mark.integration
async def test_list_pagination_is_newest_first(
    client: AsyncClient, workflow_payload: dict[str, object]
) -> None:
    first = (await client.post("/api/v1/workflows", json=workflow_payload)).json()["id"]
    second = (await client.post("/api/v1/workflows", json=workflow_payload)).json()["id"]
    response = await client.get("/api/v1/workflows", params={"limit": 1, "offset": 0})
    assert response.json()["total"] == 2
    assert response.json()["items"][0]["id"] == second
    page_two = await client.get("/api/v1/workflows", params={"limit": 1, "offset": 1})
    assert page_two.json()["items"][0]["id"] == first
