from uuid import UUID


class DomainError(Exception):
    """Base class for expected domain failures."""


class WorkflowNotFound(DomainError):
    def __init__(self, workflow_id: UUID) -> None:
        super().__init__(f"Workflow {workflow_id} was not found")


class StepNotFound(DomainError):
    def __init__(self, step_id: UUID) -> None:
        super().__init__(f"Step {step_id} was not found")


class ApprovalNotFound(DomainError):
    def __init__(self, approval_id: UUID) -> None:
        super().__init__(f"Approval {approval_id} was not found")


class InvalidStateTransition(DomainError):
    def __init__(self, entity: str, from_status: str, to_status: str) -> None:
        super().__init__(f"Invalid {entity} transition: {from_status} -> {to_status}")


class WorkflowConflict(DomainError):
    """The requested command conflicts with current durable state."""


class InvariantViolation(DomainError):
    """Durable state does not satisfy a workflow invariant."""
