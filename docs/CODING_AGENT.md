# Sandboxed coding agent

Phase 6 adds a `coding_agent` workflow step to the existing Kafka → durable attempt → leased executor path. The initial supported source is the bundled `fixture:discount_service` Python repository. It is copied into a dedicated Docker volume and initialized as a Git repository. Public Git URL cloning is deliberately not implemented: arbitrary remote source, credentials, and network-enabled preparation need a separate trust policy.

The logical `CodingWorkspace` is unique per workflow step and survives executor attempts. Each attempt gets a separate `SandboxExecution` container record. The container is disposable; the labeled workspace volume and PostgreSQL AgentRun, turns, tools, commands, approvals, and checkpoints are durable. A replacement executor creates a new container over the same volume, reads the workspace fingerprint, and resumes the same AgentRun. The coding agent never receives a Docker command or host path.

The versioned `coding-v2` prompt exposes only `list_files`, `read_file`, `search_files`, `apply_patch`, `run_tests`, `git_status`, and `git_diff`. Arguments are Pydantic-validated. The model makes at most one tool decision per turn; the existing AgentService persists the decision and `AgentToolCall` before any sandbox effect. Context is rebuilt from durable prior turns. A `final` decision is rejected as a failed ModelCall until a durable patch checkpoint and a later passing test record exist; after bounded invalid attempts the AgentRun fails rather than being marked successful. The deterministic fake provider inspects the fixture task, source and tests, applies a full-file replacement, runs six fixture tests, then answers. Ollama uses the same structured-decision contract. This is not a general-purpose shell agent.

## Effect boundaries

Each operation has a durable `SandboxCommand` intent. Docker/Git/file/test I/O runs outside Continuum database transactions. Results are then recorded in a short fenced transaction. `run_tests` is safe to repeat; the fixed command is `python -m pytest -q -p no:cacheprovider`, bounded by timeout and output limits. A successful test record later than the latest patch checkpoint is required before approval can be requested.

`apply_patch` is intentionally **reconcilable**, not blindly idempotent. It is a typed full-file replacement (`path`, expected SHA-256 of the original file, replacement text), not a shell script or unchecked textual patch. Python replacements must parse successfully before the model decision is accepted and again inside the sandbox before any write. Before writing, the sandbox compares the actual tracked/untracked file map with the durable expected-before map. It atomically replaces the file. If the executor crashes before recording the result, a replacement presents the same persisted tool call, arguments, and operation ID. The sandbox recognizes the exact expected-after state and returns `already_applied`; it does not write again. An unexplained tree/hash or Git HEAD mismatch fails closed as `WORKSPACE_RECONCILIATION_FAILED`, except that a post-approval commit bearing the exact durable operation trailer is recognized during recovery. Once a workspace has been prepared, missing Git metadata is never silently reinitialized over edits. Each accepted mutation has a durable checkpoint with Git HEAD, Git diff hash, file hashes and tree hash. Git metadata itself is not blindly hashed.

The test command is predefined by the supported workflow configuration (`pytest -q`) and translated to fixed argv inside the sandbox. The model cannot submit a shell command. Tests can generate incidental files, so the workspace fingerprint is checked before further effects and before Git commit. Command outputs are bounded both in transport and in PostgreSQL excerpts.

## Approval and local commit

After a final agent response and a passing test record, Continuum creates one `COMMIT_PATCH` approval request. No Git commit occurs while it is `PENDING`. The local API offers read-only approval diagnostics plus explicit idempotent approve/reject actions. Approval is committed to PostgreSQL before any Git commit begins. Rejection fails the workflow without a commit. This local development API currently has **no authentication**; it must not be exposed to untrusted networks. A production approval plane needs identity, authorization, audit and CSRF/session design.

The approved operation has a stable `continuum:git-commit:<approval-id>` identity. A Git commit is made **inside the sandbox**, with a fixed local author and a `Continuum-Operation-ID` trailer. If the executor crashes after Git commits but before PostgreSQL records its SHA, recovery inspects HEAD and the exact operation trailer, reuses that SHA, and creates no second commit. Continuum does not push, create a branch on a remote, or open a PR.

An executor currently waits for approval while maintaining its outer lease. This keeps recovery simple but occupies one executor slot; a future approval-suspension state should release compute capacity. Cancellation before a new action prevents further work, but an already in-flight filesystem write or approved commit is not automatically undone. A sandbox is process/container isolation, not hardened hostile-code multi-tenant isolation; see [Sandbox security](SANDBOX_SECURITY.md).

## Run the demo

```bash
make up
make coding-demo-fake
# Optional local Ollama qwen2.5:3b, if running:
make coding-demo-ollama
```

The demo creates an isolated fixture workspace, prints the durable tool trajectory, observes a pending approval, approves through the API, and checks the final local commit/test record. `make faultlab-coding-smoke` runs isolated crash scenarios and stores raw JSONL evidence under ignored `artifacts/faultlab/`.

The scripted fake-provider demo completed the fixture with seven turns, six coding tools, six passing sandbox tests, explicit approval, and one local commit. A real local `qwen2.5:3b` attempt produced invalid patches and did not pass tests. A later `qwen2.5-coder:3b` attempt listed files and read `TASK.md`, then returned `final` without editing or testing. After the premature-final guard was added, another real `qwen2.5-coder:3b` run recorded three rejected final ModelCalls, ended `MODEL_ATTEMPTS_EXHAUSTED`, and created no approval or commit. That is a safe failure, **not** a successful real-model coding demo. This small fixture does not establish general coding capability; better model/prompt evaluations remain necessary. The second local run also filled the host disk, destabilizing the normal development Docker stack; isolated-suite and clean-revision FaultLab results were collected before that environmental failure.

The host-process coverage percentage excludes `coding/sandbox_tool.py`: it runs in a separate disposable Docker process, so the host pytest collector cannot see its executed lines. The tool is instead exercised by real-container integration tests for preparation, path confinement, patch replay, test execution, timeouts, Git commit replay, and corrupted Git metadata. This is a coverage instrumentation boundary, not a claim that sandbox code has the same measured line coverage as the host runtime.
