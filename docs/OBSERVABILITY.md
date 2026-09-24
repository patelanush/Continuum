# Phase 7 observability

## Questions and signals

Continuum keeps workflow truth in PostgreSQL. OpenTelemetry traces answer which request, event, consumer, attempt, model/tool call, approval, sandbox command, and recovery action participated in a workflow. Prometheus metrics answer whether failures, backlogs, retries, or latency are changing across workflows. Neither backend participates in workflow correctness.

The optional local profile runs **OpenTelemetry Collector → Tempo** for traces and **Collector Prometheus exporter → Prometheus → Grafana** for metrics. Applications send OTLP/gRPC to the Collector. Grafana provisions Tempo, Prometheus, and four dashboards from source control. Ports are bound to 127.0.0.1 by default: Grafana 3000, Prometheus 9090, Tempo 3200, Collector OTLP 4317. Grafana login is `admin` / `continuum-local`, development only. Tempo and Prometheus retain local data for 24 hours.

## Start and inspect

```bash
make observability-up
make observability-check
make trace-demo
make recovery-trace-demo
make metrics-demo
uv run python scripts/phase7_duplicate_demo.py
uv run python scripts/phase7_telemetry_outage.py
uv run python scripts/trace_workflow.py WORKFLOW_ID
```

Open Grafana at http://127.0.0.1:3000. The Tempo datasource supports search for `continuum.workflow.id`; the trace helper queries Tempo and prints trace IDs, services, span counts, and an Explore route. `make observability-down` stops only telemetry backends. `make up` leaves telemetry disabled and the optional profile off. The trace/metrics demo commands require the profile running.

The four dashboards are System Overview, Reliability, AI Agent, and Coding Agent. Their source is `observability/grafana/dashboards/`; `scripts/generate_dashboards.py` regenerates the JSON. Prometheus alerts in `observability/alerts.yaml` are **local example thresholds**, not production SLOs. Kafka's exact consumer lag is not exported: the dashboards show observed publish/consume rates, duplicates, DLQ count, and durable unpublished outbox depth instead. These are not a fabricated lag estimate.

## Trace boundaries and causality

FastAPI creates server spans. A workflow start transaction persists a validated W3C `traceparent` beside each outbox event. The business event JSON is unchanged. The dispatcher may publish much later: its `kafka.publish` span uses the persisted context, puts a new `traceparent` in Kafka headers, and finalizes the outbox claim after broker acknowledgement. A dispatcher claim batch can contain events from several workflows, so its measured `outbox.claim` span is a separate operational trace rather than a child of one workflow. Kafka processing extracts the message header for `kafka.consume`, inbox dedupe, and attempt materialization. Malformed metadata is ignored; a valid business event still proceeds. `tracestate` and arbitrary baggage are deliberately not persisted.

An attempt stores a trace context when materialized. Short `execution.run` and `execution.result` markers, model/turn/tool spans, sandbox spans, and approval/commit spans attach to that context. An attempt does **not** hold an open span across hours or human approval. Recovery emits an expiration span and a replacement span with a link to the prior attempt's durable context. A replacement attempt stores the replacement context and keeps the same workflow and step IDs. Approval stores a trace context so the later approval API action can be correlated with the waiting coding workflow. A replayed Kafka event has its own consume span; safe dedupe is marked `continuum.event.duplicate=true`, not ERROR.

Trace attributes use durable IDs (workflow, step, attempt, agent run/turn/call, workspace, approval) for search. Metrics never use these IDs as labels. HTTP instrumentation retains approved standard HTTP fields. SQLAlchemy instrumentation is opt-in via `OTEL_SQLALCHEMY_INSTRUMENTATION=true`: a real coding trace with it enabled contained about 600 SQL spans among 760 total, so the local default uses higher-level transaction spans and 70 spans for the validated coding demo. SQL parameter values are never exported.

Service identities are `continuum-api`, `continuum-dispatcher`, `continuum-event-worker`, `continuum-executor`, `continuum-recovery`, and `continuum-mock-payments`. Local tracing defaults to `parentbased_always_on` (100% for new roots); configure `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` for another environment. This local setting is not a production sampling recommendation.

## Metrics and failures

Counters cover workflow, step, attempt, recovery, lease, Kafka, duplicate, DLQ, agent, model, tool, sandbox, approval, and commit outcomes. Histograms cover workflow/attempt duration, lease-expiry-to-recovery detection delay, Kafka publish delay, claim latency, model/tool calls, sandbox startup/commands, and tests. The recovery histogram measures the time from the expired lease timestamp to the scheduler's expiration transaction; it does not include resumed execution. Gauges sample durable active workflows, pending/running attempts, active leases, unpublished outbox, and pending approvals every 15 seconds. Heartbeats use a counter; there is no heartbeat span per interval. Metric dimensions are bounded in `src/durable_agent_runtime/observability/metrics.py`; unknown configurable names map to `other`.

The exporter is asynchronous: spans use a bounded 2,048 item batch queue, 256 item batches, a one-second schedule, and a three-second export timeout. Metrics export every five seconds. Collector/Tempo/Prometheus/Grafana failure cannot fail a workflow; telemetry may be lost during an outage. Once the Collector returns, new telemetry resumes. Exported traces and metric counters are diagnostic observations, not a durable ledger; compare them to PostgreSQL outcomes when investigating a crash at an instrumentation boundary. A valid Kafka event with malformed trace metadata is still consumed normally.

The application span exporter applies a central attribute allowlist before sending OTLP to the Collector. See [privacy policy](OBSERVABILITY_PRIVACY.md). The actual coding, external-service, recovery, duplicate, DLQ, 20-workflow metrics, and Collector outage results live under `benchmarks/results/phase7-*.json`. The overhead report is generated by `scripts/phase7_overhead.py`.
