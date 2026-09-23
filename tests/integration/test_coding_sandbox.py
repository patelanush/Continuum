"""Real Docker sandbox: confined volume, fixed tools, patch reconciliation, local commit."""

import json
from uuid import uuid4

import pytest

from durable_agent_runtime.coding.fake import fixture_script
from durable_agent_runtime.coding.sandbox import (
    DockerSandbox,
    SandboxOperationRejected,
    docker,
)
from durable_agent_runtime.core.config import Settings

pytestmark = pytest.mark.integration


async def test_real_sandbox_fix_and_reconcile() -> None:
    workspace_id = uuid4()
    sandbox_id = uuid4()
    volume_name = f"continuum-test-ws-{workspace_id.hex}"
    sandbox = DockerSandbox(
        workspace_id,
        sandbox_id,
        volume_name,
        Settings(sandbox_volume_prefix="continuum-test"),
    )
    await sandbox.start()
    try:
        host = json.loads(await docker("inspect", "--format", "{{json .HostConfig}}", sandbox.name))
        mounts = json.loads(await docker("inspect", "--format", "{{json .Mounts}}", sandbox.name))
        assert host["Privileged"] is False
        assert host["NetworkMode"] == "none"
        assert host["ReadonlyRootfs"] is True
        assert host["Memory"] == 512 * 1024 * 1024
        assert host["PidsLimit"] == 128
        assert len(mounts) == 1 and mounts[0]["Type"] == "volume"
        assert mounts[0]["Destination"] == "/workspace"
        prepared = await sandbox.call(
            "prepare",
            {"repository": "fixture:discount_service", "initialize_if_missing": True},
        )
        assert prepared["git_head"]
        failing = await sandbox.call("run_tests", {"timeout_seconds": 15}, timeout_seconds=20)
        assert failing["exit_code"] != 0 and not failing["timed_out"]
        assert "FAILED" in failing["stdout"], failing
        patch = fixture_script()["5"][0]["arguments"]
        assert isinstance(patch, dict)
        args = {**patch, "before_file_hashes": prepared["file_hashes"]}
        applied = await sandbox.call("apply_patch", args)
        assert applied["already_applied"] is False
        replay = await sandbox.call("apply_patch", args)
        assert replay["already_applied"] is True
        assert replay["fingerprint"]["tree_hash"] == applied["fingerprint"]["tree_hash"]
        passing = await sandbox.call("run_tests", {"timeout_seconds": 15}, timeout_seconds=20)
        assert passing["exit_code"] == 0 and "6 passed" in passing["stdout"]
        with pytest.raises(SandboxOperationRejected):
            await sandbox.call("read_file", {"path": "../../etc/passwd"})
        operation_id = f"continuum:test-commit:{uuid4()}"
        commit_args = {
            "operation_id": operation_id,
            "expected_tree_hash": applied["fingerprint"]["tree_hash"],
            "expected_git_head": prepared["git_head"],
            "message": "Fix checkout discounts",
        }
        with pytest.raises(SandboxOperationRejected, match="commit precondition"):
            await sandbox.call("git_commit", {**commit_args, "expected_git_head": "0" * 40})
        committed = await sandbox.call("git_commit", commit_args)
        replayed = await sandbox.call("git_commit", commit_args)
        assert committed["commit_sha"] == replayed["commit_sha"]
        assert committed["already_committed"] is False
        assert replayed["already_committed"] is True
        observed = await sandbox.call(
            "prepare", {"repository": "fixture:discount_service", "initialize_if_missing": False}
        )
        assert observed["head_operation_id"] == operation_id
    finally:
        await sandbox.stop()
        await docker("volume", "rm", volume_name)


async def test_interrupted_preparation_recovers_and_git_metadata_is_private() -> None:
    workspace_id = uuid4()
    volume_name = f"continuum-test-ws-{workspace_id.hex}"
    sandbox = DockerSandbox(
        workspace_id,
        uuid4(),
        volume_name,
        Settings(sandbox_volume_prefix="continuum-test"),
    )
    await sandbox.start()
    try:
        # A crash after git init but before its first commit must not strand the volume.
        await docker("exec", sandbox.name, "git", "init", "-q", "/workspace")
        prepared = await sandbox.call(
            "prepare",
            {"repository": "fixture:discount_service", "initialize_if_missing": True},
        )
        assert prepared["git_head"]
        # A symlink that resolves within the volume must not expose .git metadata.
        await docker("exec", sandbox.name, "ln", "-s", ".git/config", "/workspace/config-link")
        with pytest.raises(SandboxOperationRejected):
            await sandbox.call("read_file", {"path": "config-link"})
    finally:
        await sandbox.stop()
        await docker("volume", "rm", volume_name)


async def test_prepared_workspace_never_reinitializes_missing_git_metadata() -> None:
    workspace_id = uuid4()
    volume_name = f"continuum-test-ws-{workspace_id.hex}"
    sandbox = DockerSandbox(
        workspace_id,
        uuid4(),
        volume_name,
        Settings(sandbox_volume_prefix="continuum-test"),
    )
    await sandbox.start()
    try:
        await sandbox.call(
            "prepare",
            {"repository": "fixture:discount_service", "initialize_if_missing": True},
        )
        await docker("exec", sandbox.name, "rm", "-rf", "/workspace/.git")
        with pytest.raises(SandboxOperationRejected, match="Git baseline missing"):
            await sandbox.call(
                "prepare",
                {"repository": "fixture:discount_service", "initialize_if_missing": False},
            )
        source = await sandbox.call("read_file", {"path": "checkout/pricing.py"})
        assert "discount" in source["content"]
    finally:
        await sandbox.stop()
        await docker("volume", "rm", volume_name)
