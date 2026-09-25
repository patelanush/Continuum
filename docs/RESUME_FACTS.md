# Auditable Continuum facts

This is a fact sheet for later resume editing, not a resume entry. Each number below names its source. Historical experiments used their recorded revisions; the Phase 8 workload revision is `dde3460f` (benchmark implementation and runtime). These are local test results, not population-level reliability or production availability claims.

| Fact | Measured value | Source |
|---|---:|---|
| Core stack | Python, FastAPI, PostgreSQL, Kafka KRaft, async SQLAlchemy, Alembic | [architecture](ARCHITECTURE.md), [`docker-compose.yml`](../docker-compose.yml), [`pyproject.toml`](../pyproject.toml) |
| Durable correctness mechanisms | Transactional outbox/inbox, manual offsets, leased/fenced attempts, heartbeats, reconciliation | [architecture](ARCHITECTURE.md), [failure model](FAILURE_MODEL.md) |
| Official Continuum FaultLab trials | 5,075 | [Phase 4 summary](../benchmarks/results/latest.json) |
| Fault-injected Continuum trials | 2,875 | [Phase 4 summary](../benchmarks/results/latest.json) |
| Required recoveries completed | 2,770 / 2,770 | [Phase 4 summary](../benchmarks/results/latest.json), [denominators](FAULTLAB.md) |
| Duplicate/lost tested refunds in official Continuum cohort | 0 / 0 | [Phase 4 summary](../benchmarks/results/latest.json) |
| Intentionally unsafe retry controls | 100 duplicate refunds in 100 separate controls | [Phase 4 report](../benchmarks/results/phase4-summary.md) |
| One real local-model support-agent demonstration | Ollama `qwen2.5:3b`, 3 turns, 3 model calls, 2 tool calls, 1 refund | [Phase 5 demo](../benchmarks/results/phase5-agent-demo.json) |
| Coding FaultLab campaign | 11 / 11 correct trials; 8 injected faults; 7 recovered | [Phase 6 summary](../benchmarks/results/phase6-coding-summary.json) |
| Duplicate tested coding effects in that campaign | 0 | [Phase 6 summary](../benchmarks/results/phase6-coding-summary.json) |
| One coding workflow trace | 70 spans; API, dispatcher, event worker, executor; model/tool, sandbox, approval, commit stages | [Phase 7 trace](../benchmarks/results/phase7-trace-demo.json) |
| Local observability median-latency impact | +0.38% in the measured OFF/ON experiment | [Phase 7 overhead](../benchmarks/results/phase7-observability-overhead.json) |
| Three-run 180-workflow throughput, 1/3/5 executors | 2.516 / 7.321 / 11.805 workflows/s | [Phase 8 scaling](../benchmarks/results/phase8-scaling.json) |
| Five-versus-one scaling speedup at that load | 4.69×, about 93.8% replica efficiency | [Phase 8 scaling](../benchmarks/results/phase8-scaling.json) |
| Best measured sustained local setting | 12 executors, 22.174 workflows/s mean over three 500-workflow runs | [Phase 8 scaling](../benchmarks/results/phase8-scaling.json) |
| Observed local saturation | 16 executors: 8.641 workflows/s mean and 10 incidental recovered lease expirations | [Phase 8 scaling](../benchmarks/results/phase8-scaling.json) |
| Mixed durable-agent workload | 100 / 100 workflows; 45 verified refunds; 15 coding commits | [Phase 8 mixed](../benchmarks/results/phase8-mixed.json) |
| Executor failure under concurrent load | 300 / 300 workflows; 1 replacement; 150 verified refunds; 0 duplicate/lost tested refunds | [Phase 8 failure](../benchmarks/results/phase8-load-failure.json) |
| Replacement claim latency under that backlog | 258.95 s after expiry, one sample | [Phase 8 failure](../benchmarks/results/phase8-load-failure.json) |
| Event-worker failure under load | 120 / 120 workflows; 60 verified refunds; no unexpected DLQ messages | [Phase 8 failure](../benchmarks/results/phase8-load-failure.json) |
| Primary coding crash demo | `EXPIRED → SUCCEEDED`; patch reconciled, 6 tests passed, 1 approved local commit, 0 duplicate patches/commits | [Phase 8 demo record](../benchmarks/results/phase8-demo.json), [demo runner](../scripts/phase8_demo.py) |
| Final local quality gate | 273 passed, 1 optional deselected, 87.78% coverage; Ruff and strict mypy passed | [Phase 8 quality record](../benchmarks/results/phase8-quality.json) |

The Phase 4 official cohort and Phase 8 load trials have different workloads and denominators; do not add or merge their counts. A successful keyed mock refund or reconciled coding commit does not prove exactly-once behavior for arbitrary external services. The primary demo and Phase 8 results do not establish hardened sandbox security or cloud-scale capacity.
