"""The dispatcher must not spin on an immediately rejected outbox row."""

import asyncio
from itertools import pairwise
from typing import Any

import pytest

from durable_agent_runtime.dispatcher import main as dispatcher


async def test_dispatch_loop_paces_nonempty_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    stop = asyncio.Event()
    clock = asyncio.get_running_loop()
    calls: list[float] = []

    async def rejected_batch(*_args: Any, **_kwargs: Any) -> int:
        calls.append(clock.time())
        if len(calls) == 3:
            stop.set()
        return 1

    monkeypatch.setattr(dispatcher, "dispatch_once", rejected_batch)
    await dispatcher.dispatch_loop(stop, None, None, poll_interval=0.03)  # type: ignore[arg-type]
    assert len(calls) == 3
    assert all(later - earlier >= 0.02 for earlier, later in pairwise(calls))
