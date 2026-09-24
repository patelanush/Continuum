"""Trusted Docker control plane; model actions execute only in sandbox containers."""

import asyncio
import hashlib
import json
import logging
from time import monotonic
from typing import Any
from uuid import UUID

from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.observability.metrics import count, duration
from durable_agent_runtime.observability.runtime import error, span

logger = logging.getLogger(__name__)


class SandboxUnavailable(RuntimeError):
    pass


class SandboxOperationRejected(ValueError):
    pass


async def docker(*argv: str, stdin: bytes | None = None, timeout_seconds: float = 30) -> str:
    process = await asyncio.create_subprocess_exec(
        "docker",
        *argv,
        stdin=asyncio.subprocess.PIPE if stdin is not None else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, errors = await asyncio.wait_for(process.communicate(stdin), timeout=timeout_seconds)
    except (TimeoutError, asyncio.CancelledError):
        process.kill()
        await process.communicate()
        raise
    if process.returncode != 0:
        raise SandboxUnavailable(
            f"docker {argv[0]} failed ({process.returncode}): "
            + errors.decode(errors="replace")[-1000:]
        )
    if len(output) > 1_000_000:
        raise SandboxUnavailable("Docker response exceeded control-plane limit")
    return output.decode(errors="replace").strip()


class DockerSandbox:
    """One disposable container for one leased execution attempt."""

    def __init__(
        self,
        workspace_id: UUID,
        sandbox_id: UUID,
        volume_name: str,
        settings: Settings,
    ) -> None:
        self.workspace_id = workspace_id
        self.sandbox_id = sandbox_id
        self.volume_name = volume_name
        self.settings = settings
        self.name = f"continuum-sb-{sandbox_id.hex}"
        # The workspace's persisted volume prefix, not the current process setting,
        # determines which older containers must be retired after configuration drift.
        self.project = volume_name.rpartition("-ws-")[0]
        if not self.project or not volume_name.endswith(workspace_id.hex):
            raise ValueError("Sandbox volume does not match durable workspace identity")

    async def start(self) -> None:
        began = monotonic()
        with span(
            "sandbox.create",
            {
                "continuum.workspace.id": str(self.workspace_id),
                "continuum.sandbox.id": str(self.sandbox_id),
            },
        ) as active:
            try:
                await self._start()
            except Exception:
                error(active, "sandbox_unavailable")
                raise
            else:
                duration("continuum_sandbox_startup_duration_seconds", monotonic() - began)

    async def _start(self) -> None:
        await docker(
            "volume",
            "create",
            "--label",
            f"continuum.project={self.project}",
            "--label",
            f"continuum.workspace={self.workspace_id}",
            self.volume_name,
        )
        stale = await docker(
            "ps",
            "-aq",
            "--filter",
            f"label=continuum.project={self.project}",
            "--filter",
            f"label=continuum.workspace={self.workspace_id}",
        )
        for container_id in stale.splitlines():
            await docker("rm", "-f", container_id)
        await docker(
            "run",
            "--detach",
            "--rm",
            "--name",
            self.name,
            "--label",
            f"continuum.project={self.project}",
            "--label",
            f"continuum.workspace={self.workspace_id}",
            "--network",
            "none",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,nosuid,size=64m",
            "--cpus",
            "1",
            "--memory",
            "512m",
            "--pids-limit",
            "128",
            "--security-opt",
            "no-new-privileges",
            "--cap-drop",
            "ALL",
            "--user",
            "10001:10001",
            "--env",
            f"CONTINUUM_MAX_FILE_READ_BYTES={self.settings.coding_max_file_read_bytes}",
            "--env",
            f"CONTINUUM_MAX_PATCH_BYTES={self.settings.coding_max_patch_bytes}",
            "--env",
            f"CONTINUUM_MAX_COMMAND_OUTPUT_BYTES={self.settings.coding_max_command_output_bytes}",
            "--env",
            f"CONTINUUM_MAX_SEARCH_RESULTS={self.settings.coding_max_search_results}",
            "--mount",
            f"type=volume,src={self.volume_name},dst=/workspace",
            "--workdir",
            "/workspace",
            self.settings.sandbox_image,
            "sleep",
            "infinity",
            timeout_seconds=60,
        )
        logger.info(
            "process_type=executor operation=sandbox_started workspace_id=%s "
            "sandbox_id=%s container_ref=%s",
            self.workspace_id,
            self.sandbox_id,
            self.name,
        )

    async def stop(self) -> None:
        with span("sandbox.destroy", {"continuum.sandbox.id": str(self.sandbox_id)}):
            try:
                await docker("rm", "-f", self.name, timeout_seconds=15)
            except SandboxUnavailable:
                pass

    async def call(
        self, operation: str, request: dict[str, Any], *, timeout_seconds: float = 35
    ) -> dict[str, Any]:
        began = monotonic()
        safe_operation = (
            operation
            if operation
            in {
                "prepare",
                "fingerprint",
                "list_files",
                "read_file",
                "search_files",
                "apply_patch",
                "run_tests",
                "git_status",
                "git_diff",
                "git_commit",
            }
            else "other"
        )
        attributes: dict[str, str | int | bool] = {
            "continuum.workspace.id": str(self.workspace_id),
            "continuum.sandbox.id": str(self.sandbox_id),
            "continuum.command.type": safe_operation,
        }
        if operation == "apply_patch" and isinstance(request.get("replacement_text"), str):
            attributes["continuum.patch.sha256"] = hashlib.sha256(
                request["replacement_text"].encode()
            ).hexdigest()
            attributes["continuum.patch.file_count"] = 1
        with span(f"coding.{safe_operation}", attributes):
            with span("sandbox.command", attributes) as active:
                status = "failed"
                try:
                    result = await self._call(operation, request, timeout_seconds=timeout_seconds)
                    exit_code = result.get("exit_code")
                    if isinstance(exit_code, int):
                        active.set_attribute("continuum.command.exit_code", exit_code)
                    status = (
                        "failed" if isinstance(exit_code, int) and exit_code != 0 else "succeeded"
                    )
                    if status == "failed":
                        error(active, "command_failed")
                    return result
                except TimeoutError:
                    status = "timeout"
                    active.set_attribute("continuum.command.timed_out", True)
                    error(active, "sandbox_timeout")
                    raise
                except Exception:
                    status = "failed"
                    error(active, "sandbox_error")
                    raise
                finally:
                    elapsed = monotonic() - began
                    count("continuum_sandbox_commands", command_type=operation, status=status)
                    duration(
                        "continuum_sandbox_command_duration_seconds",
                        elapsed,
                        command_type=operation,
                        status=status,
                    )
                    if operation == "run_tests":
                        duration("continuum_test_duration_seconds", elapsed, status=status)

    async def _call(
        self, operation: str, request: dict[str, Any], *, timeout_seconds: float = 35
    ) -> dict[str, Any]:
        allowed = {
            "prepare",
            "fingerprint",
            "list_files",
            "read_file",
            "search_files",
            "apply_patch",
            "run_tests",
            "git_status",
            "git_diff",
            "git_commit",
        }
        if operation not in allowed:
            raise SandboxOperationRejected("Unknown structured sandbox operation")
        payload = json.dumps(request, separators=(",", ":")).encode()
        if len(payload) > 100_000:
            raise SandboxOperationRejected("Sandbox request exceeds size limit")
        process = await asyncio.create_subprocess_exec(
            "docker",
            "exec",
            "-i",
            self.name,
            "python",
            "/opt/sandbox_tool.py",
            operation,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            output, errors = await asyncio.wait_for(
                process.communicate(payload), timeout=timeout_seconds
            )
        except (TimeoutError, asyncio.CancelledError):
            process.kill()
            await process.communicate()
            await self.stop()
            raise
        if len(output) > 1_000_000:
            raise SandboxUnavailable("Sandbox response exceeded control-plane limit")
        try:
            envelope: dict[str, Any] = json.loads(output)
        except (ValueError, TypeError) as exc:
            raise SandboxUnavailable(
                "Sandbox did not return structured JSON: " + errors.decode(errors="replace")[-500:]
            ) from exc
        if envelope.get("ok") is not True:
            raise SandboxOperationRejected(str(envelope.get("error", "Sandbox operation failed")))
        if process.returncode != 0:
            raise SandboxUnavailable("Sandbox command exited unsuccessfully")
        result: dict[str, Any] = envelope["result"]
        return result
