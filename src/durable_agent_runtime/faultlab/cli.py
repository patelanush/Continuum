"""CLI for isolated FaultLab scenarios, campaigns, reports and cleanup."""

import argparse
import asyncio
import json
from uuid import UUID

from durable_agent_runtime.faultlab.runner import publish_summary, run_experiment, summarize
from durable_agent_runtime.faultlab.runtime import DockerController
from durable_agent_runtime.faultlab.scenarios import CAMPAIGNS, SCENARIOS
from durable_agent_runtime.faultlab.storage import ExperimentStore


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="continuum-faultlab")
    subcommands = command.add_subparsers(dest="command", required=True)
    subcommands.add_parser("list", help="List versioned fault scenarios and campaigns")
    run = subcommands.add_parser("run", help="Run one scenario")
    run.add_argument("scenario", choices=sorted(SCENARIOS))
    run.add_argument("--runs", type=int, default=1)
    campaign = subcommands.add_parser("campaign", help="Run a predefined campaign")
    campaign.add_argument("name", choices=sorted(CAMPAIGNS))
    for selected in (run, campaign):
        selected.add_argument("--seed", type=int, default=42)
        selected.add_argument("--concurrency", type=int, default=4)
        selected.add_argument("--keep-stack", action="store_true")
        selected.add_argument(
            "--reuse-stack", action="store_true", help="Use an already running isolated stack"
        )
        selected.add_argument("--executors", type=int, default=3)
    report = subcommands.add_parser("report", help="Regenerate summaries from raw JSONL")
    report.add_argument("experiment_id", type=UUID)
    report.add_argument("--publish", action="store_true")
    subcommands.add_parser("up", help="Build and start the isolated three-executor test stack")
    subcommands.add_parser("clean", help="Remove only isolated FaultLab Compose volumes/containers")
    return command


async def invoke(arguments: argparse.Namespace) -> int:
    if arguments.command == "list":
        for name, scenario in SCENARIOS.items():
            mode = "exclusive" if scenario.exclusive else "parallel-safe"
            print(f"{name} v{scenario.version} [{mode}] — {scenario.description}")
        print("Campaigns:")
        for name, counts in CAMPAIGNS.items():
            print(f"  {name}: {sum(counts.values())} trials by default")
        return 0
    if arguments.command == "clean":
        await DockerController().clean()
        return 0
    if arguments.command == "up":
        await DockerController().up()
        return 0
    if arguments.command == "report":
        store = ExperimentStore(arguments.experiment_id)
        summary = summarize(store)
        if arguments.publish:
            publish_summary(store)
        print(json.dumps(summary, indent=2))
        return 0 if summary["all_incorrect_trials"] == 0 else 1
    counts = (
        {arguments.scenario: arguments.runs}
        if arguments.command == "run"
        else CAMPAIGNS[arguments.name]
    )
    store, summary = await run_experiment(
        counts,
        seed=arguments.seed,
        concurrency=arguments.concurrency,
        keep_stack=arguments.keep_stack,
        executors=arguments.executors,
        start_stack=not arguments.reuse_stack,
    )
    print(f"Experiment artifacts: {store.directory}")
    print(json.dumps(summary, indent=2))
    return 0 if summary["all_incorrect_trials"] == 0 else 1


def main() -> None:
    raise SystemExit(asyncio.run(invoke(parser().parse_args())))


if __name__ == "__main__":
    main()
