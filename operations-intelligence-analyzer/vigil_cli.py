"""CLI for Vigil data parser + tool-enabled agent."""

from __future__ import annotations

import argparse
import json

from vigil_agent import VigilToolAgent


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Vigil parser + agent CLI")
    p.add_argument("--data-dir", required=True, help="Directory containing mixed-format data files")
    p.add_argument(
        "--command",
        required=True,
        choices=["scan", "summary", "parse", "query", "agent"],
        help="Tool/agent action to execute",
    )
    p.add_argument("--file", help="Used with --command parse")
    p.add_argument("--question", help="Used with --command query or --command agent")
    return p


def main() -> None:
    args = _build_parser().parse_args()
    agent = VigilToolAgent()

    if args.command == "scan":
        result = agent.tool_scan_directory(args.data_dir)
    elif args.command == "summary":
        result = agent.tool_summarize_dataset(args.data_dir)
    elif args.command == "parse":
        if not args.file:
            raise SystemExit("--file is required for --command parse")
        result = agent.tool_parse_file(args.file)
    elif args.command == "query":
        if not args.question:
            raise SystemExit("--question is required for --command query")
        result = agent.tool_query(args.question, args.data_dir)
    else:
        instruction = args.question or "summarize this dataset"
        result = agent.run(instruction, args.data_dir)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
