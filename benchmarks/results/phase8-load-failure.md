# Phase 8 failure under concurrent load

Benchmark revision: `dde3460f1a5cbb49b92593b1c1dcf25aead91695`. Each scenario used fresh isolated Compose volumes, three executors, three event workers, 20 unmeasured warmups, and telemetry off. The measured mix was 50% five-second `slow_noop`, 25% direct refund, and 25% support-agent refund.

## Active executor SIGKILL

At injection, the executor owned 1 running attempt and 297 of 300 measured workflows were active. The killed attempt expired; one replacement attempt succeeded. All 300 workflows finished, with 150 expected refunds verified individually against mock-payments, zero duplicate/lost refunds, zero duplicate transitions, and no unexpected DLQ messages. The replacement claim occurred 258.95 s after expiry because it queued behind the large pending workload. The queue and outbox drained; no approval remained pending.

## Event-worker SIGKILL

The killed worker stopped during submissions with 13 workflows active. All 120 measured workflows finished; 60 expected refunds occurred once each, with no duplicate transitions, unexpected DLQ messages, or orphan attempts. At the sampled kill instant published and consumed event counts were equal (7 each), so this run demonstrates continuation after a worker death but does not establish that an event was redelivered. Dedicated Kafka replay/FaultLab tests cover duplicate delivery separately.

These are controlled local observations, not a production availability estimate. [Machine-readable summary](phase8-load-failure.json).
