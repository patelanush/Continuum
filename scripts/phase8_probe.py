"""Sample PostgreSQL queue depth and lock waits during a separate load run.

Run alongside the benchmark; the sampler is intentionally excluded from primary
throughput runs because even read-only probes can perturb a small local server.
"""

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any

import asyncpg


async def sample(seconds: float, interval: float) -> list[dict[str, Any]]:
    connection = await asyncpg.connect("postgresql://durable:durable@127.0.0.1:55437/durable")
    rows: list[dict[str, Any]] = []
    deadline = monotonic() + seconds
    try:
        while monotonic() < deadline:
            activity = await connection.fetchrow(
                "SELECT count(*) FILTER (WHERE wait_event_type = 'Lock') AS lock_waiters, "
                "count(*) FILTER (WHERE state = 'active') AS active_connections "
                "FROM pg_stat_activity WHERE datname = current_database()"
            )
            depth = await connection.fetchrow(
                "SELECT "
                "(SELECT count(*) FROM execution_attempts WHERE status = 'PENDING') AS pending, "
                "(SELECT count(*) FROM execution_attempts WHERE status = 'RUNNING') AS running, "
                "(SELECT count(*) FROM outbox_events WHERE published_at IS NULL) AS unpublished"
            )
            assert activity is not None and depth is not None
            rows.append(
                {
                    "at": datetime.now(UTC).isoformat(),
                    "lock_waiters": activity["lock_waiters"],
                    "active_connections": activity["active_connections"],
                    "pending_attempts": depth["pending"],
                    "running_attempts": depth["running"],
                    "unpublished_outbox": depth["unpublished"],
                }
            )
            await asyncio.sleep(interval)
    finally:
        await connection.close()
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=90)
    parser.add_argument("--interval", type=float, default=0.5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.seconds <= 0 or args.interval <= 0:
        parser.error("seconds and interval must be positive")
    rows = asyncio.run(sample(args.seconds, args.interval))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(rows, indent=2) + "\n")
    print(
        json.dumps(
            {
                "samples": len(rows),
                "peak_lock_waiters": max((row["lock_waiters"] for row in rows), default=0),
                "peak_pending_attempts": max((row["pending_attempts"] for row in rows), default=0),
                "peak_unpublished_outbox": max(
                    (row["unpublished_outbox"] for row in rows), default=0
                ),
            }
        )
    )


if __name__ == "__main__":
    main()
