"""Validated, append-only evidence formats for FaultLab runs."""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class ExperimentConfig(BaseModel):
    experiment_id: UUID
    scenario_names: list[str]
    runs_per_scenario: dict[str, int]
    seed: int
    concurrency: int = Field(ge=1, le=32)
    git_commit: str
    git_dirty: bool
    compose_project: str
    executor_count: int = Field(ge=1)
    started_at: datetime
    environment: dict[str, str | int | float]


class TrialResult(BaseModel):
    experiment_id: UUID
    trial_id: UUID
    scenario_name: str
    scenario_version: int = Field(ge=1)
    seed: int
    started_at: datetime
    completed_at: datetime | None = None
    workflow_id: UUID | None = None
    step_id: UUID | None = None
    attempt_ids: list[UUID] = Field(default_factory=list)
    executor_ids: list[str] = Field(default_factory=list)
    fault_type: str | None = None
    fault_injection_point: str | None = None
    fault_injected_at: datetime | None = None
    injected_failure_count: int = 0
    expected_terminal_status: str = "SUCCEEDED"
    actual_terminal_status: str | None = None
    recovered: bool = False
    recovery_required: bool = False
    correct: bool = False
    failure_reason: str | None = None
    attempt_count: int = 0
    duplicate_side_effect_count: int = 0
    lost_side_effect_count: int = 0
    expected_side_effect_count: int = 0
    duplicate_transition_count: int = 0
    outbox_duplicate_count: int = 0
    kafka_redelivery_count: int | None = None
    lease_expiration_count: int = 0
    recovery_time_ms: float | None = None
    workflow_elapsed_ms: float | None = None
    fault_detection_latency_ms: float | None = None
    event_to_attempt_ms: float | None = None
    attempt_pending_to_claim_ms: float | None = None
    outbox_publish_delay_ms: float | None = None
    external_side_effect_id: UUID | None = None
    notes: dict[str, Any] = Field(default_factory=dict)

    def finish(self, *, error: str | None = None) -> None:
        self.completed_at = datetime.now(UTC)
        if error is not None:
            self.correct = False
            self.failure_reason = error
