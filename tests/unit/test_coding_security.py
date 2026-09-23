from pathlib import Path
from uuid import uuid4

import pytest

from durable_agent_runtime.coding.decisions import TOOL_SEMANTICS, validate_coding_arguments
from durable_agent_runtime.coding.runner import CodingInput
from durable_agent_runtime.coding.sandbox import DockerSandbox
from durable_agent_runtime.coding.sandbox_tool import SandboxToolError, resolve_workspace_path
from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.domain.enums import ApprovalStatus, CommandStatus, WorkspaceStatus
from durable_agent_runtime.domain.errors import InvalidStateTransition
from durable_agent_runtime.domain.state_machine import (
    validate_approval_transition,
    validate_command_transition,
    validate_workspace_transition,
)
from durable_agent_runtime.execution.tools import RetrySafety


def test_path_resolver_rejects_traversal_absolute_and_internal_paths(tmp_path: Path) -> None:
    (tmp_path / "file.txt").write_text("safe")
    assert resolve_workspace_path(tmp_path, "file.txt") == tmp_path / "file.txt"
    for path in ("../secret", "../../etc/passwd", "/etc/passwd", ".git/config"):
        with pytest.raises(SandboxToolError):
            resolve_workspace_path(tmp_path, path)


def test_path_resolver_rejects_symlink_escape(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret")
    (workspace / "escape").symlink_to(outside)
    with pytest.raises(SandboxToolError, match="escapes"):
        resolve_workspace_path(workspace, "escape")


def test_path_resolver_rejects_symlink_into_git_metadata(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("private")
    (tmp_path / "indirect").symlink_to(".git/config")
    with pytest.raises(SandboxToolError, match="Internal repository"):
        resolve_workspace_path(tmp_path, "indirect")


def test_sandbox_uses_persisted_volume_project_across_config_change() -> None:
    workspace_id = uuid4()
    sandbox = DockerSandbox(
        workspace_id,
        uuid4(),
        f"original-ws-{workspace_id.hex}",
        Settings(sandbox_volume_prefix="changed"),
    )
    assert sandbox.project == "original"
    with pytest.raises(ValueError, match="durable workspace identity"):
        DockerSandbox(workspace_id, uuid4(), "wrong-ws-0000", Settings())


def test_path_resolver_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(SandboxToolError, match="does not exist"):
        resolve_workspace_path(tmp_path, "missing")


def test_coding_tool_registry_is_closed_and_mutations_are_reconcilable() -> None:
    assert validate_coding_arguments("list_files", {"path": ".", "depth": 2}) == {
        "path": ".",
        "depth": 2,
    }
    assert validate_coding_arguments("run_tests", {}) == {}
    assert validate_coding_arguments("run_tests", {"path": "."}) == {"path": "."}
    assert TOOL_SEMANTICS["apply_patch"] == RetrySafety.RECONCILABLE
    with pytest.raises(ValueError, match="Unauthorized"):
        validate_coding_arguments("run_command", {"command": "rm -rf /"})
    with pytest.raises(ValueError):
        validate_coding_arguments("run_tests", {"command": "sh"})
    with pytest.raises(ValueError):
        validate_coding_arguments("run_tests", {"path": "../outside"})
    with pytest.raises(ValueError):
        validate_coding_arguments(
            "apply_patch", {"path": "x", "expected_sha256": "placeholder", "replacement_text": "x"}
        )
    with pytest.raises(ValueError, match="Invalid Python replacement"):
        validate_coding_arguments(
            "apply_patch",
            {
                "path": "module.py",
                "expected_sha256": "a" * 64,
                "replacement_text": '"""unterminated',
            },
        )


def test_coding_input_accepts_only_bundled_fixture_and_fixed_test_command() -> None:
    valid = {
        "repository": "fixture:discount_service",
        "task": "Fix the bug",
        "test_command": "pytest -q",
    }
    assert CodingInput.model_validate(valid).repository == "fixture:discount_service"
    with pytest.raises(ValueError):
        CodingInput.model_validate({**valid, "repository": "https://example.com/repo.git"})
    with pytest.raises(ValueError):
        CodingInput.model_validate({**valid, "test_command": "sh -c anything"})


def test_workspace_command_and_approval_states_are_terminal() -> None:
    validate_workspace_transition(WorkspaceStatus.ACTIVE, WorkspaceStatus.WAITING_APPROVAL)
    validate_command_transition(CommandStatus.RUNNING, CommandStatus.TIMED_OUT)
    validate_approval_transition(ApprovalStatus.PENDING, ApprovalStatus.APPROVED)
    with pytest.raises(InvalidStateTransition):
        validate_workspace_transition(WorkspaceStatus.COMPLETED, WorkspaceStatus.ACTIVE)
    with pytest.raises(InvalidStateTransition):
        validate_command_transition(CommandStatus.SUCCEEDED, CommandStatus.RUNNING)
    with pytest.raises(InvalidStateTransition):
        validate_approval_transition(ApprovalStatus.REJECTED, ApprovalStatus.APPROVED)
