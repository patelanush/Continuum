"""Deterministic fixture trajectory for CI; not a production coding policy."""

import hashlib
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "coding"


def fixture_script() -> dict[str, list[dict[str, Any]]]:
    before = (FIXTURES / "discount_service" / "checkout" / "pricing.py").read_bytes()
    after = (FIXTURES / "solutions" / "pricing.py").read_text()
    actions: list[dict[str, Any]] = [
        {"type": "tool_call", "tool_name": "list_files", "arguments": {"path": ".", "depth": 3}},
        {"type": "tool_call", "tool_name": "read_file", "arguments": {"path": "TASK.md"}},
        {
            "type": "tool_call",
            "tool_name": "read_file",
            "arguments": {"path": "checkout/pricing.py"},
        },
        {
            "type": "tool_call",
            "tool_name": "read_file",
            "arguments": {"path": "tests/test_pricing.py"},
        },
        {
            "type": "tool_call",
            "tool_name": "apply_patch",
            "arguments": {
                "path": "checkout/pricing.py",
                "expected_sha256": hashlib.sha256(before).hexdigest(),
                "replacement_text": after,
            },
        },
        {"type": "tool_call", "tool_name": "run_tests", "arguments": {}},
        {
            "type": "final",
            "response": "I corrected sequential discount calculation and the fixture tests pass.",
        },
    ]
    return {str(number): [action] for number, action in enumerate(actions, 1)}
