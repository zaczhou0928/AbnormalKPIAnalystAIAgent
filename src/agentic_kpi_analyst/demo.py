"""Generate reproducible demo artifacts for representative anomaly cases.

Runs 3 representative cases end-to-end in mock mode (no API keys needed),
saves all reports, charts, and a compact summary file.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path

from agentic_kpi_analyst.config import Settings, get_settings
from agentic_kpi_analyst.graph.graph import run_case
from agentic_kpi_analyst.logging_utils import get_logger

logger = get_logger(__name__)

# Representative cases covering diverse anomaly types:
#   ANO-001: conversion_rate drop (channel-driven)
#   ANO-002: refund_rate spike (category-driven)
#   ANO-005: order_count drop (payment-type-driven)
DEMO_CASE_IDS = ["ANO-001", "ANO-002", "ANO-005"]


def run_demo(settings: Settings | None = None) -> Path:
    """Run demo cases and save all artifacts to outputs/demo/.

    Returns:
        Path to the demo output directory.
    """
    settings = settings or get_settings()

    # Force mock mode for reproducibility
    settings.llm_mode = "mock"
    settings.human_review_auto_approve = True

    demo_dir = settings.output_abs_dir / "demo"
    if demo_dir.exists():
        shutil.rmtree(demo_dir)
    demo_dir.mkdir(parents=True, exist_ok=True)

    summary_entries: list[dict] = []

    for case_id in DEMO_CASE_IDS:
        logger.info("demo_running_case", case_id=case_id)

        result = run_case(case_id, settings)

        # Collect artifact paths produced by the pipeline
        case_dir = demo_dir / case_id
        case_dir.mkdir(parents=True, exist_ok=True)
        artifact_paths: dict[str, str] = {}

        # Copy report files into the demo case directory
        if result.report:
            for label, src_path_str in [
                ("markdown_report", result.report.markdown_path),
                ("html_report", result.report.html_path),
                ("json_report", result.report.json_path),
            ]:
                src = Path(src_path_str)
                if src.exists():
                    dst = case_dir / src.name
                    shutil.copy2(src, dst)
                    artifact_paths[label] = str(dst.relative_to(settings.output_abs_dir))

        # Copy chart files
        charts_src = settings.output_abs_dir / "charts" / result.run_id
        if charts_src.exists():
            charts_dst = case_dir / "charts"
            shutil.copytree(charts_src, charts_dst, dirs_exist_ok=True)
            chart_files = sorted(str(p.relative_to(settings.output_abs_dir)) for p in charts_dst.iterdir())
            artifact_paths["charts"] = chart_files  # type: ignore[assignment]

        # Extract confidence
        confidence = "inconclusive"
        if result.verification:
            c = result.verification.suggested_confidence
            confidence = c.value if hasattr(c, "value") else str(c)

        # Extract review status
        review = result.review_status
        review_str = review.value if hasattr(review, "value") else str(review)

        # Extract predicted primary cause
        predicted_cause = result.draft_conclusion or ""
        for f in result.findings:
            if (f.is_primary if not isinstance(f, dict) else f.get("is_primary", False)):
                predicted_cause = f.description if not isinstance(f, dict) else f.get("description", "")
                break

        summary_entries.append({
            "case_id": case_id,
            "predicted_primary_cause": predicted_cause[:300],
            "confidence": confidence,
            "review_status": review_str,
            "run_id": result.run_id,
            "artifacts": artifact_paths,
        })

        logger.info("demo_case_complete", case_id=case_id, confidence=confidence)

    # Write compact summary
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "mock",
        "cases": summary_entries,
    }
    summary_path = demo_dir / "demo_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    logger.info("demo_complete", output_dir=str(demo_dir), n_cases=len(summary_entries))
    return demo_dir
