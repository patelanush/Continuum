"""Fixed, structured operations executed *inside* the unprivileged sandbox.

The trusted executor supplies one JSON request on stdin. No model-controlled shell
command is interpreted here. This module uses only the Python standard library.
"""

import ast
import hashlib
import json
import os
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from time import monotonic
from typing import Any

ROOT = Path("/workspace")
FIXTURES = Path("/opt/fixtures")
MAX_FILE_BYTES = int(os.getenv("CONTINUUM_MAX_FILE_READ_BYTES", "32768"))
MAX_PATCH_BYTES = int(os.getenv("CONTINUUM_MAX_PATCH_BYTES", "32768"))
MAX_OUTPUT_BYTES = int(os.getenv("CONTINUUM_MAX_COMMAND_OUTPUT_BYTES", "8192"))
MAX_SEARCH_RESULTS = int(os.getenv("CONTINUUM_MAX_SEARCH_RESULTS", "50"))
MAX_FILES = 500


class SandboxToolError(ValueError):
    pass


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_hash(value: object) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def resolve_workspace_path(root: Path, raw: str, *, must_exist: bool = True) -> Path:
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts or not raw:
        raise SandboxToolError("Path must be relative to the workspace without traversal")
    if any(part in {".git", ".continuum"} for part in relative.parts):
        raise SandboxToolError("Internal repository paths are not accessible")
    resolved_root = root.resolve()
    resolved = (resolved_root / relative).resolve(strict=False)
    if not resolved.is_relative_to(resolved_root):
        raise SandboxToolError("Path escapes the workspace")
    if any(part in {".git", ".continuum"} for part in resolved.relative_to(resolved_root).parts):
        raise SandboxToolError("Internal repository paths are not accessible")
    if must_exist and not resolved.exists():
        raise SandboxToolError("Path does not exist")
    return resolved


def git(root: Path, *argv: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", *argv],
        cwd=root,
        capture_output=True,
        check=False,
        env={
            **os.environ,
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
        },
        timeout=30,
    )
    if check and result.returncode != 0:
        raise SandboxToolError(result.stderr.decode(errors="replace")[:1000])
    return result


def tracked_files(root: Path) -> list[str]:
    result = git(root, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
    names = sorted({item.decode("utf-8") for item in result.stdout.split(b"\0") if item})
    if len(names) > MAX_FILES:
        raise SandboxToolError("Workspace has too many files for Phase 6 fingerprinting")
    return names


def fingerprint(root: Path) -> dict[str, Any]:
    files: dict[str, str] = {}
    for name in tracked_files(root):
        path = resolve_workspace_path(root, name)
        original = root / name
        if original.is_symlink():
            files[name] = sha256(os.readlink(original).encode())
        elif path.is_file():
            files[name] = sha256(path.read_bytes())
        else:
            raise SandboxToolError("Unsupported repository entry")
    diff = git(root, "diff", "--binary", "--no-ext-diff", "--", ".").stdout
    if len(diff) > 1_000_000:
        raise SandboxToolError("Workspace diff exceeds Phase 6 limit")
    message = git(root, "log", "-1", "--format=%B").stdout.decode(errors="replace")
    operation_trailers = [
        line.removeprefix("Continuum-Operation-ID: ")
        for line in message.splitlines()
        if line.startswith("Continuum-Operation-ID: ")
    ]
    return {
        "file_hashes": files,
        "tree_hash": canonical_hash(files),
        "diff_hash": sha256(diff),
        "git_head": git(root, "rev-parse", "HEAD").stdout.decode().strip(),
        "head_operation_id": operation_trailers[0] if len(operation_trailers) == 1 else None,
        "dirty": bool(git(root, "status", "--porcelain", "--untracked-files=normal").stdout),
    }


def prepare(root: Path, source: str, initialize_if_missing: bool) -> dict[str, Any]:
    if source != "fixture:discount_service":
        raise SandboxToolError("Only the bundled local fixture is supported")
    baseline_exists = (root / ".git").is_dir() and git(
        root, "rev-parse", "--verify", "HEAD", check=False
    ).returncode == 0
    if not baseline_exists:
        if not initialize_if_missing:
            raise SandboxToolError("WORKSPACE_RECONCILIATION_FAILED: Git baseline missing")
        # This volume has no accepted Git baseline yet. Recreate a partial copy
        # left by a crash during initial preparation; no agent edits can exist.
        for child in root.iterdir():
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
        shutil.copytree(FIXTURES / "discount_service", root, dirs_exist_ok=True)
        git(root, "init", "-q")
        git(root, "add", "--all")
        git(
            root,
            "-c",
            "user.name=Continuum Fixture",
            "-c",
            "user.email=fixture@example.local",
            "commit",
            "-qm",
            "Fixture baseline",
        )
    return fingerprint(root)


def list_files(root: Path, path: str, depth: int) -> dict[str, Any]:
    target = resolve_workspace_path(root, path)
    if not target.is_dir() or depth < 0 or depth > 4:
        raise SandboxToolError("Expected directory and depth from 0 to 4")
    names = [
        name
        for name in tracked_files(root)
        if (root / name).is_relative_to(target)
        and len((root / name).relative_to(target).parts) <= depth + 1
    ]
    return {"files": names[:100], "truncated": len(names) > 100}


def read_file(root: Path, path: str) -> dict[str, Any]:
    target = resolve_workspace_path(root, path)
    if not target.is_file() or target.stat().st_size > MAX_FILE_BYTES:
        raise SandboxToolError("File is not readable text within size limit")
    data = target.read_bytes()
    try:
        content = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SandboxToolError("File is not UTF-8 text") from exc
    return {"path": path, "content": content, "sha256": sha256(data)}


def search_files(root: Path, query: str, path: str, max_results: int) -> dict[str, Any]:
    if not query or len(query) > 200 or max_results < 1 or max_results > MAX_SEARCH_RESULTS:
        raise SandboxToolError("Invalid bounded search request")
    target = resolve_workspace_path(root, path)
    names = tracked_files(root)
    hits: list[dict[str, Any]] = []
    for name in names:
        file = root / name
        if not file.is_relative_to(target) and file != target:
            continue
        if file.is_symlink() or not file.is_file() or file.stat().st_size > MAX_FILE_BYTES:
            continue
        try:
            lines = file.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            continue
        for number, line in enumerate(lines, 1):
            if query in line:
                hits.append({"path": name, "line": number, "text": line[:300]})
                if len(hits) >= max_results:
                    return {"matches": hits, "truncated": True}
    return {"matches": hits, "truncated": False}


def apply_file(root: Path, request: dict[str, Any]) -> dict[str, Any]:
    path = str(request["path"])
    target = resolve_workspace_path(root, path)
    if not target.is_file() or (root / path).is_symlink() or path not in tracked_files(root):
        raise SandboxToolError("Only existing tracked regular files can be patched")
    replacement = str(request["replacement_text"]).encode("utf-8")
    if len(replacement) > MAX_PATCH_BYTES:
        raise SandboxToolError("Patch replacement exceeds size limit")
    if path.endswith(".py"):
        try:
            ast.parse(replacement.decode("utf-8"))
        except SyntaxError as exc:
            raise SandboxToolError(f"Invalid Python replacement at line {exc.lineno}") from exc
    expected_before = str(request["expected_sha256"])
    desired_after = sha256(replacement)
    actual = fingerprint(root)
    before_files = request["before_file_hashes"]
    after_files = {**before_files, path: desired_after}
    before_tree = canonical_hash(before_files)
    after_tree = canonical_hash(after_files)
    if actual["tree_hash"] == after_tree and sha256(target.read_bytes()) == desired_after:
        return {
            "already_applied": True,
            "fingerprint": actual,
            "path": path,
            "before_sha256": expected_before,
            "after_sha256": desired_after,
        }
    if actual["tree_hash"] != before_tree or sha256(target.read_bytes()) != expected_before:
        raise SandboxToolError("WORKSPACE_RECONCILIATION_FAILED: unexpected file state")
    fd, temp_name = tempfile.mkstemp(prefix=".continuum-edit-", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(replacement)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temp_name, stat.S_IMODE(target.stat().st_mode))
        os.replace(temp_name, target)
        dir_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
    after = fingerprint(root)
    if after["tree_hash"] != after_tree:
        raise SandboxToolError("WORKSPACE_RECONCILIATION_FAILED: patch postcondition")
    return {
        "already_applied": False,
        "fingerprint": after,
        "path": path,
        "before_sha256": expected_before,
        "after_sha256": desired_after,
    }


def run_tests(root: Path, timeout_seconds: int) -> dict[str, Any]:
    argv = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"]
    began = monotonic()
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        process = subprocess.Popen(
            argv,
            cwd=root,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
            env={
                **os.environ,
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
                "PYTHONPATH": str(root),
            },
        )
        timed_out = False
        try:
            process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        stdout.seek(0)
        stderr.seek(0)
        out = stdout.read(MAX_OUTPUT_BYTES + 1)
        err = stderr.read(MAX_OUTPUT_BYTES + 1)
    return {
        "argv": argv,
        "exit_code": None if timed_out else process.returncode,
        "timed_out": timed_out,
        "stdout": out[:MAX_OUTPUT_BYTES].decode(errors="replace"),
        "stderr": err[:MAX_OUTPUT_BYTES].decode(errors="replace"),
        "output_truncated": len(out) > MAX_OUTPUT_BYTES or len(err) > MAX_OUTPUT_BYTES,
        "duration_ms": round((monotonic() - began) * 1000),
    }


def git_commit(root: Path, request: dict[str, Any]) -> dict[str, Any]:
    operation_id = str(request["operation_id"])
    head_message = git(root, "log", "-1", "--format=%B").stdout.decode()
    trailer = f"Continuum-Operation-ID: {operation_id}"
    if trailer in head_message.splitlines():
        return {
            "commit_sha": git(root, "rev-parse", "HEAD").stdout.decode().strip(),
            "already_committed": True,
            "fingerprint": fingerprint(root),
        }
    current = fingerprint(root)
    if (
        current["tree_hash"] != request["expected_tree_hash"]
        or current["git_head"] != request["expected_git_head"]
        or not current["dirty"]
    ):
        raise SandboxToolError("WORKSPACE_RECONCILIATION_FAILED: commit precondition")
    git(root, "add", "--all")
    git(
        root,
        "-c",
        "user.name=Continuum Agent",
        "-c",
        "user.email=continuum@example.local",
        "commit",
        "-qm",
        str(request["message"])[:120],
        "-m",
        trailer,
    )
    return {
        "commit_sha": git(root, "rev-parse", "HEAD").stdout.decode().strip(),
        "already_committed": False,
        "fingerprint": fingerprint(root),
    }


def execute(root: Path, operation: str, request: dict[str, Any]) -> dict[str, Any]:
    if operation == "prepare":
        return prepare(
            root,
            str(request["repository"]),
            bool(request.get("initialize_if_missing", False)),
        )
    if operation == "fingerprint" or operation == "git_status":
        return fingerprint(root)
    if operation == "list_files":
        return list_files(root, str(request.get("path", ".")), int(request.get("depth", 2)))
    if operation == "read_file":
        return read_file(root, str(request["path"]))
    if operation == "search_files":
        return search_files(
            root,
            str(request["query"]),
            str(request.get("path", ".")),
            int(request.get("max_results", 20)),
        )
    if operation == "apply_patch":
        return apply_file(root, request)
    if operation == "run_tests":
        return run_tests(root, int(request["timeout_seconds"]))
    if operation == "git_diff":
        diff = git(root, "diff", "--no-ext-diff", "--", ".").stdout
        return {
            "diff": diff[:MAX_OUTPUT_BYTES].decode(errors="replace"),
            "truncated": len(diff) > MAX_OUTPUT_BYTES,
            "diff_hash": sha256(diff),
        }
    if operation == "git_commit":
        return git_commit(root, request)
    raise SandboxToolError("Unknown sandbox operation")


def main() -> None:
    operation = sys.argv[1]
    try:
        request: dict[str, Any] = json.load(sys.stdin)
        result = execute(ROOT, operation, request)
        print(json.dumps({"ok": True, "result": result}, separators=(",", ":")))
    except (SandboxToolError, KeyError, TypeError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)[:1000]}, separators=(",", ":")))
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
