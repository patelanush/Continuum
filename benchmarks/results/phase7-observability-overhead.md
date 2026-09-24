# Phase 7 observability overhead

Local Docker Compose, three event workers, three executors, 20 concurrent clients. Each of four runs completed 150 identical one-step noop workflows. Modes were OFF, ON, OFF, ON; all runs used the same built images and database.

| Mode | Run | Workflows | Median (s) | p95 (s) | Throughput (workflows/s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| off | 1 | 150 | 3.6044 | 4.9896 | 4.922 |
| on | 2 | 150 | 3.5363 | 4.8097 | 5.0649 |
| off | 3 | 150 | 3.5206 | 4.5298 | 5.1743 |
| on | 4 | 150 | 3.6155 | 4.6378 | 5.1209 |

Mean of run summaries: OFF median 3.5625 s, ON median 3.5759 s; OFF p95 4.7597 s, ON p95 4.7238 s; OFF throughput 5.0481 workflows/s, ON throughput 5.0929 workflows/s.

Relative impact: median +0.38%, p95 -0.75%, throughput +0.89%. These local measurements include scheduling and polling noise and are not a production SLO.
