# Candidate operational indicators

These are **candidates**, not achieved production SLOs. The local Prometheus alert thresholds are demonstration defaults. Phase 4 FaultLab validates correctness for its tested faults and workloads; it does not prove arbitrary availability, broker fault tolerance, or latency at production scale.

| Indicator | Suggested measurement | Why it matters |
| --- | --- | --- |
| Workflow completion rate | Succeeded and failed completions divided by starts, accounting for still active workflows | Detect stuck or failing work |
| Workflow latency | p50/p95 duration by bounded workflow type | Detect delays in the full path |
| Recovery success | Recovered workflows that eventually succeed, verified from durable attempts | Check crash handling without hiding terminal failures |
| Recovery latency | Lease expiry to scheduler expiration (exported histogram); separately inspect replacement completion from durable attempts | Separate detection delay from resumed work |
| Duplicate side-effect correctness | Durable and external effect counts for keyed operations in FaultLab | Preserve exactly-once *effect* for tested keys |
| Outbox backlog | Unpublished outbox depth and oldest event age | Detect publication stalls |
| DLQ rate | Messages dead-lettered by bounded reason | Detect invalid or incompatible events |
| Model error rate | Failed calls divided by total calls by bounded provider/model | Detect provider or output failures |

An availability target would require a defined traffic population, observation window, exclusions, alert routing, error budget, and sustained load evidence. No such target is claimed here. Kafka consumer lag is not exported in Phase 7; publication and consumption rates plus outbox depth cannot substitute for exact lag.
