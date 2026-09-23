"""Allowlisted, typed coding tool arguments."""

import ast
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from durable_agent_runtime.execution.tools import RetrySafety


class ListFilesArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str = "."
    depth: int = Field(default=2, ge=0, le=4)


class ReadFileArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str = Field(min_length=1, max_length=300)


class SearchFilesArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str = Field(min_length=1, max_length=200)
    path: str = "."
    max_results: int = Field(default=20, ge=1, le=50)


class ApplyPatchArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str = Field(min_length=1, max_length=300)
    expected_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    replacement_text: str = Field(max_length=32_768)


class EmptyArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    # Small local models sometimes attach the workspace root to a no-path tool.
    # Only the exact root sentinel is accepted; the fixed operation still has no path choice.
    path: Literal["."] | None = None


TOOL_ARGUMENTS: dict[str, type[BaseModel]] = {
    "list_files": ListFilesArgs,
    "read_file": ReadFileArgs,
    "search_files": SearchFilesArgs,
    "apply_patch": ApplyPatchArgs,
    "run_tests": EmptyArgs,
    "git_status": EmptyArgs,
    "git_diff": EmptyArgs,
}

TOOL_SEMANTICS: dict[str, RetrySafety] = {
    "list_files": RetrySafety.READ_ONLY,
    "read_file": RetrySafety.READ_ONLY,
    "search_files": RetrySafety.READ_ONLY,
    "apply_patch": RetrySafety.RECONCILABLE,
    "run_tests": RetrySafety.IDEMPOTENT,
    "git_status": RetrySafety.READ_ONLY,
    "git_diff": RetrySafety.READ_ONLY,
}


def validate_coding_arguments(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    try:
        schema = TOOL_ARGUMENTS[name]
    except KeyError as exc:
        raise ValueError(f"Unauthorized coding tool: {name}") from exc
    valid = schema.model_validate(arguments).model_dump(mode="json", exclude_none=True)
    if name == "apply_patch" and valid["path"].endswith(".py"):
        try:
            ast.parse(valid["replacement_text"])
        except SyntaxError as exc:
            raise ValueError(f"Invalid Python replacement: {exc.msg} at line {exc.lineno}") from exc
    return valid
