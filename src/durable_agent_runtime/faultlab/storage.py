"""Append raw evidence first, then derive regenerable summaries from it."""

import json
from pathlib import Path
from typing import Any
from uuid import UUID

from durable_agent_runtime.faultlab.models import ExperimentConfig, TrialResult
from durable_agent_runtime.faultlab.runtime import REPO_ROOT

ARTIFACT_ROOT = REPO_ROOT / "artifacts" / "faultlab"


class ExperimentStore:
    def __init__(self, experiment_id: UUID, *, root: Path = ARTIFACT_ROOT) -> None:
        self.directory = root / str(experiment_id)
        self.trials_path = self.directory / "trials.jsonl"

    def initialize(self, config: ExperimentConfig) -> None:
        self.directory.mkdir(parents=True, exist_ok=False)
        (self.directory / "config.json").write_text(
            config.model_dump_json(indent=2) + "\n", encoding="utf-8"
        )

    def append(self, result: TrialResult) -> None:
        with self.trials_path.open("a", encoding="utf-8") as output:
            output.write(result.model_dump_json() + "\n")
            output.flush()

    def read_config(self) -> ExperimentConfig:
        return ExperimentConfig.model_validate_json(
            (self.directory / "config.json").read_text(encoding="utf-8")
        )

    def read_trials(self) -> list[TrialResult]:
        if not self.trials_path.exists():
            return []
        return [
            TrialResult.model_validate_json(line)
            for line in self.trials_path.read_text(encoding="utf-8").splitlines()
            if line
        ]

    def write_summary(self, summary: dict[str, Any], markdown: str) -> None:
        summary_path = self.directory / "summary.json"
        temporary = summary_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(summary_path)
        (self.directory / "summary.md").write_text(markdown, encoding="utf-8")
