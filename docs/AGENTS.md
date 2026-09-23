# Durable support agent (Phase 5)

`support_agent` is one ordinary sequential Continuum workflow step. Kafka still only materializes a durable outer execution attempt; a leased executor runs the agent and the existing recovery scheduler replaces an expired outer attempt. The agent does not create a second workflow engine.

## Durable records

- `agent_runs` has `UNIQUE(step_id)`. Replacement attempts load the same run, provider/model selection, prompt version, max-turn limit, and final answer.
- `agent_turns` has `UNIQUE(agent_run_id, turn_number)`. A turn holds one normalized model request and at most one immutable accepted decision. Its request hash is SHA-256 of canonical JSON.
- `model_calls` has `UNIQUE(agent_turn_id, attempt_number)`. Each invocation is recorded before network I/O. Timeouts, malformed output, and interrupted calls remain visible. Success, validated decision, and tool-call materialization commit together. A call's exact normalized request and hash are stored.
- `agent_tool_calls` has `UNIQUE(agent_turn_id)` and a unique Continuum-assigned `operation_id`. Validated tool name, the accepted decision's arguments, their canonical SHA-256 hash, and the operation ID are persisted before a tool runs. No provider-generated call ID is trusted for correctness.

The simplified turn states are `PENDING_MODEL → TOOL_PENDING → COMPLETED` for a tool, or `PENDING_MODEL → COMPLETED` for a final answer. A failed turn is terminal. The run, model-call, and tool-call status transitions are explicitly validated as well. One action per turn is intentional; there are no parallel model-selected tools in Phase 5.

## Recovery boundaries

1. An executor records a `RUNNING` ModelCall, commits, invokes the provider **without an open Continuum transaction**, then validates the JSON decision. If it dies before the response is persisted, the response can be lost; recovery marks the interrupted call failed and may invoke the model again. The new response may differ. This is not exactly-once inference.
2. A valid response is committed with the turn decision and, for a tool, the durable tool-call identity. From that commit onward, recovery loads the accepted decision. It does **not** ask the model to decide that turn again.
3. The tool runs outside a Continuum transaction. `read_refund_policy` is read-only. `refund_customer` calls the independent mock-payments HTTP service with `Idempotency-Key: continuum:agent-tool:<tool_call_id>`. If the refund commits but the executor dies before saving its result, replacement execution retries the *same* durable tool call and key; the payments service returns the original refund. A persisted result is reused without another HTTP call.
4. Tool result, completed turn, and next turn commit together. The next model request is reconstructed from persisted initial step input, prior decisions, and prior tool results—not process memory. The versioned prompt text is selected by the run's stored prompt version. The exact request for each call is persisted.
5. A final answer completes AgentRun before outer attempt finalization. If the executor dies in between, replacement returns the persisted answer and finalizes the outer step without another inference.

Every mutating agent checkpoint verifies the current outer attempt's executor ID, UUID lease token, unexpired database-time lease, and active workflow/step. Heartbeats continue during model and tool I/O. Cancellation fences new checkpoints and model invocations, but an external HTTP request already in flight cannot be undone.

## Providers and decisions

`FakeModelProvider` is a deterministic scripted provider for CI and FaultLab. `OllamaModelProvider` calls local Ollama `/api/chat` with a JSON schema in `format`, `stream: false`, and temperature zero. The internal decision contract is provider-independent:

```json
{"type":"tool_call","tool_name":"refund_customer","arguments":{"customer_id":"customer-123","amount":"49.99"}}
```

or `{"type":"final","response":"..."}`. Unknown decision types, unknown tools, malformed arguments, and extra tool arguments fail closed. Only `read_refund_policy` and `refund_customer` are allowlisted. Malformed/timeout calls are recorded and retried up to `MODEL_MAX_ATTEMPTS`; unknown model/request configuration fails permanently. `AGENT_MAX_TURNS` bounds the loop. Neither paid model services nor arbitrary HTTP/shell/code tools are present.

The source-controlled support prompt is currently `support-v2`; `support-v1` is retained for a run that was started before the prompt revision. A provider response generated but lost before PostgreSQL commit cannot be replayed. Safe recovery begins at the **persisted decision** boundary, and safe external retry additionally requires the tool's own idempotency contract.

## Inspect and reproduce

`GET /api/v1/workflows/{workflow_id}/agent` exposes a read-only trajectory: run, turns, decision, model-call summaries and hashes, tool-call summaries, results, and final response. It omits raw model requests/responses. The existing API has no authentication; do not expose it publicly with sensitive support inputs.

```bash
make up
make agent-demo-fake
# With local Ollama running and qwen2.5:3b available:
make agent-demo-ollama
make faultlab-ai-smoke
```

See [Architecture](ARCHITECTURE.md) for the transaction sequence and [Failure Model](FAILURE_MODEL.md) for unresolved boundaries.
