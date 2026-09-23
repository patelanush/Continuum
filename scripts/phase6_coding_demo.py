"""Run the isolated coding fixture through the public API and approve its local commit."""

import argparse
import asyncio
import json
import re
from pathlib import Path
from time import monotonic
from typing import Any

import httpx


async def docker_output(*arguments: str) -> str:
    process = await asyncio.create_subprocess_exec(
        "docker", *arguments, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    output, error = await asyncio.wait_for(process.communicate(), timeout=30)
    if process.returncode != 0:
        raise RuntimeError(error.decode(errors="replace")[-500:])
    return output.decode().strip()


async def git_output(*arguments: str) -> str:
    process = await asyncio.create_subprocess_exec(
        "git", *arguments, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    output, error = await asyncio.wait_for(process.communicate(), timeout=10)
    if process.returncode != 0:
        raise RuntimeError(error.decode(errors="replace")[-500:])
    return output.decode().strip()


async def run(
    provider: str, api_url: str, timeout_seconds: float, existing_workflow_id: str | None = None
) -> dict[str, Any]:
    began = monotonic()
    async with httpx.AsyncClient(timeout=30) as api:
        if existing_workflow_id is None:
            created = await api.post(
                f"{api_url}/api/v1/workflows",
                json={
                    "workflow_type": "coding-demo",
                    "steps": [
                        {
                            "name": "fix-discount",
                            "step_type": "coding_agent",
                            "input": {
                                "repository": "fixture:discount_service",
                                "task": "Fix the discount bug in TASK.md and run tests.",
                                "test_command": "pytest -q",
                                "provider": provider,
                            },
                        }
                    ],
                },
            )
            created.raise_for_status()
            workflow_id = created.json()["id"]
            print(f"Coding workflow: {workflow_id}")
            started = await api.post(f"{api_url}/api/v1/workflows/{workflow_id}/start")
            started.raise_for_status()
        else:
            workflow_id = existing_workflow_id
        deadline = monotonic() + timeout_seconds
        approval_id: str | None = None
        while monotonic() < deadline:
            try:
                response = await api.get(f"{api_url}/api/v1/workflows/{workflow_id}")
                response.raise_for_status()
                workflow = response.json()
                if workflow["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                    break
                workspace_response = await api.get(
                    f"{api_url}/api/v1/workflows/{workflow_id}/coding"
                )
                workspace_response.raise_for_status()
                workspaces = workspace_response.json()
                if workspaces:
                    approvals_response = await api.get(
                        f"{api_url}/api/v1/approvals", params={"limit": 100}
                    )
                    approvals_response.raise_for_status()
                    matches = [
                        item
                        for item in approvals_response.json()
                        if item["workspace_id"] == workspaces[0]["id"]
                    ]
                    if matches:
                        approval = matches[0]
                        if approval_id is None:
                            approval_id = approval["id"]
                            print(f"Approval required: {approval['action_type']} {approval_id}")
                            print(f"Summary: {approval['summary']}")
                        if approval["status"] == "PENDING":
                            decision = await api.post(
                                f"{api_url}/api/v1/approvals/{approval_id}/approve"
                            )
                            decision.raise_for_status()
            except httpx.ReadTimeout:
                pass
            await asyncio.sleep(1.0)
        else:
            raise TimeoutError(f"Coding workflow {workflow_id} did not finish")
        traces_response = await api.get(f"{api_url}/api/v1/workflows/{workflow_id}/agent")
        traces_response.raise_for_status()
        traces: list[dict[str, Any]] = traces_response.json()
        workspace_response = await api.get(f"{api_url}/api/v1/workflows/{workflow_id}/coding")
        workspace_response.raise_for_status()
        workspaces = workspace_response.json()
        if len(traces) != 1 or len(workspaces) != 1:
            raise AssertionError("Expected one agent run and one coding workspace")
        if approval_id is None:
            approvals_response = await api.get(f"{api_url}/api/v1/approvals", params={"limit": 100})
            approvals_response.raise_for_status()
            approval_id = next(
                (
                    item["id"]
                    for item in approvals_response.json()
                    if item["workspace_id"] == workspaces[0]["id"]
                ),
                None,
            )
        trace, workspace = traces[0], workspaces[0]
        turns: list[dict[str, Any]] = trace["turns"]
        tools = [turn["tool_call"] for turn in turns if turn["tool_call"]]
        model_calls = [call for turn in turns for call in turn["model_calls"]]
        for turn in turns:
            tool = turn["tool_call"]
            print(f"Turn {turn['turn_number']}: {tool['tool_name'] if tool else 'final'}")
            if tool:
                print(f"  status={tool['status']} result={str(tool['result'])[:300]}")
        if workflow["status"] != "SUCCEEDED":
            raise AssertionError(f"Coding workflow failed: {workflow}")
        if approval_id is None or not workspace["final_git_head"]:
            raise AssertionError("Missing approval or final local commit")
        test_tools = [tool for tool in tools if tool["tool_name"] == "run_tests"]
        if len(test_tools) != 1 or test_tools[0]["result"]["exit_code"] != 0:
            raise AssertionError("A successful test execution was not persisted")
        passed_match = re.search(r"(\d+) passed", test_tools[0]["result"]["stdout"])
        if passed_match is None:
            raise AssertionError("Passing test count not present in recorded pytest output")
        volumes = await docker_output(
            "volume", "ls", "-q", "--filter", f"label=continuum.workspace={workspace['id']}"
        )
        volume_names = volumes.splitlines()
        if len(volume_names) != 1:
            raise AssertionError("Expected one labeled coding workspace volume")
        mount = f"type=volume,src={volume_names[0]},dst=/workspace,readonly"

        async def git_readonly(*git_args: str) -> str:
            return await docker_output(
                "run",
                "--rm",
                "--network",
                "none",
                "--user",
                "10001:10001",
                "--mount",
                mount,
                "--workdir",
                "/workspace",
                "continuum-sandbox:phase6",
                "git",
                *git_args,
            )

        history_count = int(await git_readonly("rev-list", "--count", "HEAD"))
        files_changed = (await git_readonly("diff", "--name-only", "HEAD^", "HEAD")).splitlines()
        if history_count != 2 or files_changed != ["checkout/pricing.py"]:
            raise AssertionError(
                f"Expected one minimal local commit, history={history_count}, files={files_changed}"
            )
        result = {
            "git_commit": await git_output("rev-parse", "HEAD"),
            "git_dirty": bool(await git_output("status", "--porcelain")),
            "provider": provider,
            "model": trace["model"],
            "workflow_id": workflow_id,
            "workflow_status": workflow["status"],
            "agent_turns": len(turns),
            "model_calls": len(model_calls),
            "tool_calls": len(tools),
            "prompt_tokens": sum(call["prompt_tokens"] or 0 for call in model_calls)
            if any(call["prompt_tokens"] is not None for call in model_calls)
            else None,
            "completion_tokens": sum(call["completion_tokens"] or 0 for call in model_calls)
            if any(call["completion_tokens"] is not None for call in model_calls)
            else None,
            "approval_id": approval_id,
            "approval_required": True,
            "baseline_git_sha": workspace["baseline_git_head"],
            "final_git_sha": workspace["final_git_head"],
            "test_exit_code": test_tools[0]["result"]["exit_code"],
            "test_count": int(passed_match.group(1)),
            "files_changed": files_changed,
            "agent_commit_count": history_count - 1,
            "elapsed_ms": round((monotonic() - began) * 1000),
        }
        print(json.dumps(result, indent=2))
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=["fake", "ollama"], default="fake")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--workflow-id", help="Resume observing an already-created demo workflow")
    parser.add_argument("--summary-file", type=Path)
    args = parser.parse_args()
    result = asyncio.run(run(args.provider, args.api_url, args.timeout, args.workflow_id))
    if args.summary_file:
        args.summary_file.parent.mkdir(parents=True, exist_ok=True)
        args.summary_file.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
