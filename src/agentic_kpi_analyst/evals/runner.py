"""Evaluation runner: executes all labeled cases and produces summary metrics."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from agentic_kpi_analyst.config import Settings, get_settings
from agentic_kpi_analyst.evals.case_loader import load_cases
from agentic_kpi_analyst.evals.graders import grade_case
from agentic_kpi_analyst.graph.graph import run_case
from agentic_kpi_analyst.logging_utils import get_logger
from agentic_kpi_analyst.models import CaseEvalResult, EvalSummary

logger = get_logger(__name__)


def run_evaluation(
    settings: Settings | None = None,
    case_ids: list[str] | None = None,
) -> EvalSummary:
    """Run the full evaluation pipeline.

    Args:
        settings: Application settings.
        case_ids: Optional list of specific case IDs to run. If None, runs all.

    Returns:
        EvalSummary with aggregate metrics and per-case results.
    """
    settings = settings or get_settings()
    cases_path = settings.data_abs_dir / "anomaly_cases.json"
    cases = load_cases(cases_path)

    if case_ids:
        cases = [c for c in cases if c.case_id in case_ids]

    logger.info("evaluation_started", n_cases=len(cases))
    eval_start = time.monotonic()

    per_case: list[CaseEvalResult] = []
    output_dir = settings.output_abs_dir / "evals"
    output_dir.mkdir(parents=True, exist_ok=True)

    for case in cases:
        logger.info("evaluating_case", case_id=case.case_id, kpi=case.kpi_name.value)

        try:
            state = run_case(case.case_id, settings)
            result = grade_case(case, state)
        except Exception as e:
            logger.error("case_failed", case_id=case.case_id, error=str(e))
            result = CaseEvalResult(
                case_id=case.case_id,
                primary_cause_hit=False,
                sql_success_rate=0.0,
                evidence_sufficient=False,
                runtime_seconds=0.0,
                predicted_primary_cause=f"ERROR: {str(e)[:200]}",
            )

        per_case.append(result)

        # Save per-case result
        case_out = output_dir / f"{case.case_id}.json"
        case_out.write_text(
            json.dumps(result.model_dump(), indent=2, default=str),
            encoding="utf-8",
        )

    # Compute aggregate metrics
    n = len(per_case)
    summary = EvalSummary(
        total_cases=n,
        primary_hit_rate=sum(r.primary_cause_hit for r in per_case) / n if n > 0 else 0,
        top_k_hit_rate=sum(r.top_k_hit for r in per_case) / n if n > 0 else 0,
        avg_sql_success_rate=sum(r.sql_success_rate for r in per_case) / n if n > 0 else 0,
        evidence_sufficiency_rate=sum(r.evidence_sufficient for r in per_case) / n if n > 0 else 0,
        avg_unsupported_claims=sum(r.unsupported_claim_count for r in per_case) / n if n > 0 else 0,
        human_review_trigger_rate=sum(r.human_review_triggered for r in per_case) / n if n > 0 else 0,
        avg_runtime_seconds=sum(r.runtime_seconds for r in per_case) / n if n > 0 else 0,
        per_case=per_case,
    )

    # Save summary
    summary_path = output_dir / "eval_summary.json"
    summary_path.write_text(
        json.dumps(summary.model_dump(), indent=2, default=str),
        encoding="utf-8",
    )

    # Save markdown summary
    md_lines = [
        "# Evaluation Summary\n",
        f"**Total Cases:** {summary.total_cases}",
        f"**Primary Hit Rate:** {summary.primary_hit_rate:.1%}",
        f"**Top-K Hit Rate:** {summary.top_k_hit_rate:.1%}",
        f"**SQL Success Rate:** {summary.avg_sql_success_rate:.1%}",
        f"**Evidence Sufficiency:** {summary.evidence_sufficiency_rate:.1%}",
        f"**Avg Unsupported Claims:** {summary.avg_unsupported_claims:.1f}",
        f"**Human Review Trigger Rate:** {summary.human_review_trigger_rate:.1%}",
        f"**Avg Runtime:** {summary.avg_runtime_seconds:.2f}s\n",
        "## Per-Case Results\n",
        "| Case | KPI | Primary Hit | SQL Success | Evidence | Confidence | Runtime |",
        "|------|-----|-------------|-------------|----------|------------|---------|",
    ]
    for r in per_case:
        case_obj = next((c for c in cases if c.case_id == r.case_id), None)
        kpi = case_obj.kpi_name.value if case_obj else "?"
        md_lines.append(
            f"| {r.case_id} | {kpi} | {'Y' if r.primary_cause_hit else 'N'} | "
            f"{r.sql_success_rate:.0%} | {'Y' if r.evidence_sufficient else 'N'} | "
            f"{r.confidence.value} | {r.runtime_seconds:.2f}s |"
        )

    md_path = output_dir / "eval_summary.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    elapsed = time.monotonic() - eval_start
    logger.info(
        "evaluation_complete",
        primary_hit_rate=f"{summary.primary_hit_rate:.1%}",
        sql_success=f"{summary.avg_sql_success_rate:.1%}",
        total_time=f"{elapsed:.1f}s",
    )

    return summary
