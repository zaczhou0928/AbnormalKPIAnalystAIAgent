"""CLI entry point for the KPI analyst system."""

from __future__ import annotations

import argparse
import sys

from agentic_kpi_analyst.config import get_settings
from agentic_kpi_analyst.logging_utils import setup_logging, get_logger


def cmd_seed(args: argparse.Namespace) -> None:
    """Generate synthetic data and initialize the warehouse."""
    settings = get_settings()
    logger = get_logger("cli.seed")

    # Import here to avoid heavy imports at CLI parse time
    sys.path.insert(0, str(settings.warehouse_abs_path.parent.parent / "warehouse"))
    from warehouse.seed_data import generate_all
    from agentic_kpi_analyst.warehouse.connection import get_warehouse

    logger.info("generating_data")
    generate_all(settings.data_abs_dir)

    logger.info("initializing_warehouse")
    wh = get_warehouse(settings)
    wh.initialize()
    wh.close()

    logger.info("seed_complete")
    print(f"Data generated in {settings.data_abs_dir}")
    print(f"Warehouse initialized at {settings.warehouse_abs_path}")


def cmd_run_case(args: argparse.Namespace) -> None:
    """Run investigation for a specific anomaly case."""
    settings = get_settings()
    logger = get_logger("cli.run_case")

    from agentic_kpi_analyst.graph.graph import run_case
    result = run_case(args.case_id, settings)

    if result.report:
        print(f"Report: {result.report.markdown_path}")
    else:
        print("Investigation complete (no report generated)")


def cmd_eval(args: argparse.Namespace) -> None:
    """Run evaluation pipeline over all labeled cases."""
    settings = get_settings()
    logger = get_logger("cli.eval")

    from agentic_kpi_analyst.evals.runner import run_evaluation
    summary = run_evaluation(settings)

    print(f"\nEvaluation Summary:")
    print(f"  Cases: {summary.total_cases}")
    print(f"  Primary hit rate: {summary.primary_hit_rate:.1%}")
    print(f"  SQL success rate: {summary.avg_sql_success_rate:.1%}")
    print(f"  Evidence sufficiency: {summary.evidence_sufficiency_rate:.1%}")
    print(f"  Avg runtime: {summary.avg_runtime_seconds:.1f}s")


def cmd_demo(args: argparse.Namespace) -> None:
    """Generate reproducible demo artifacts for representative cases."""
    from agentic_kpi_analyst.demo import run_demo

    demo_dir = run_demo()
    print(f"\nDemo artifacts saved to {demo_dir}/")
    print("  Contents:")
    for child in sorted(demo_dir.iterdir()):
        if child.is_dir():
            n_files = sum(1 for _ in child.rglob("*") if _.is_file())
            print(f"    {child.name}/ ({n_files} files)")
        else:
            print(f"    {child.name}")


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="kpi-analyst",
        description="Agentic KPI Root-Cause Analyst",
    )
    parser.add_argument("--log-level", default="INFO", help="Log level")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Common args for all subcommands
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--log-level", default="INFO", help="Log level")

    # seed
    subparsers.add_parser("seed", help="Generate data and initialize warehouse", parents=[common])

    # run-case
    run_parser = subparsers.add_parser("run-case", help="Run investigation for a case", parents=[common])
    run_parser.add_argument("--case-id", required=True, help="Anomaly case ID")

    # eval
    subparsers.add_parser("eval", help="Run evaluation pipeline", parents=[common])

    # demo
    subparsers.add_parser("demo", help="Generate demo artifacts for representative cases", parents=[common])

    args = parser.parse_args()
    setup_logging(args.log_level)

    if args.command == "seed":
        cmd_seed(args)
    elif args.command == "run-case":
        cmd_run_case(args)
    elif args.command == "eval":
        cmd_eval(args)
    elif args.command == "demo":
        cmd_demo(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
