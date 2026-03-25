"""Report renderer: generates markdown, HTML, and JSON outputs from workflow state."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import markdown as md
from jinja2 import Environment, BaseLoader

from agentic_kpi_analyst.logging_utils import get_logger
from agentic_kpi_analyst.models import ReportMetadata
from agentic_kpi_analyst.reports.templates import HTML_WRAPPER, MARKDOWN_TEMPLATE

logger = get_logger(__name__)


def _basename_filter(path: str) -> str:
    """Jinja2 filter to extract basename from a path."""
    return os.path.basename(path)


def _build_template_context(state: dict[str, Any]) -> dict[str, Any]:
    """Extract template variables from workflow state."""
    request = state.get("request")
    if hasattr(request, "model_dump"):
        request = request.model_dump()
    elif hasattr(request, "__dict__"):
        request = request.__dict__

    plan = state.get("plan", {})
    if hasattr(plan, "model_dump"):
        plan = plan.model_dump()

    verification = state.get("verification", {})
    if hasattr(verification, "model_dump"):
        verification = verification.model_dump()

    findings = state.get("findings", [])
    if findings and hasattr(findings[0], "model_dump"):
        findings = [f.model_dump() for f in findings]

    analysis_results = state.get("analysis_results", [])
    if analysis_results and hasattr(analysis_results[0], "model_dump"):
        analysis_results = [a.model_dump() for a in analysis_results]

    sql_results = state.get("sql_results", [])
    if sql_results and hasattr(sql_results[0], "model_dump"):
        sql_results = [s.model_dump() for s in sql_results]

    retrieved_docs = state.get("retrieved_docs", [])
    if retrieved_docs and hasattr(retrieved_docs[0], "model_dump"):
        retrieved_docs = [d.model_dump() for d in retrieved_docs]

    # Build executive summary
    draft_conclusion = state.get("draft_conclusion", "No conclusion reached.")
    confidence = verification.get("suggested_confidence", "inconclusive")

    kpi_name = request.get("kpi_name", "unknown") if isinstance(request, dict) else "unknown"
    # Handle enum values (KPIName.ORDER_COUNT -> order_count)
    if hasattr(kpi_name, "value"):
        kpi_name = kpi_name.value
    elif isinstance(kpi_name, str) and "." in kpi_name:
        kpi_name = kpi_name.split(".")[-1]

    # Follow-up suggestions
    follow_ups = [
        f"Validate the identified driver with domain experts.",
        f"Check if the {kpi_name} movement persists after the anomaly window.",
        f"Investigate potential secondary effects on related KPIs.",
        f"Review any operational changes made during the anomaly period.",
    ]

    return {
        "run_id": state.get("run_id", "unknown"),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "kpi_name": kpi_name,
        "anomaly_start": request.get("start_date", "") if isinstance(request, dict) else "",
        "anomaly_end": request.get("end_date", "") if isinstance(request, dict) else "",
        "baseline_start": request.get("baseline_start_date", "") if isinstance(request, dict) else "",
        "baseline_end": request.get("baseline_end_date", "") if isinstance(request, dict) else "",
        "executive_summary": draft_conclusion,
        "incident_context": request.get("description", "No description provided.") if isinstance(request, dict) else "",
        "retrieved_docs": retrieved_docs,
        "hypothesis": plan.get("hypothesis", "N/A") if isinstance(plan, dict) else "N/A",
        "priority_dimensions": plan.get("priority_dimensions", []) if isinstance(plan, dict) else [],
        "plan_steps": plan.get("steps", []) if isinstance(plan, dict) else [],
        "sql_results": sql_results,
        "findings": findings,
        "analysis_results": analysis_results,
        "confidence": confidence,
        "verification_checks": verification.get("checks", []) if isinstance(verification, dict) else [],
        "unsupported_claims": verification.get("unsupported_claims", []) if isinstance(verification, dict) else [],
        "review_status": state.get("review_status", "not_required"),
        "review_reasons": state.get("review_trigger_reasons", []),
        "draft_conclusion": draft_conclusion,
        "follow_ups": follow_ups,
        "version": "0.1.0",
    }


def render_report(state: dict[str, Any], output_dir: Path) -> ReportMetadata:
    """Render the investigation report in markdown, HTML, and JSON formats.

    Args:
        state: The final workflow state dict.
        output_dir: Directory to write report files.

    Returns:
        ReportMetadata with paths to generated files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    ctx = _build_template_context(state)
    run_id = ctx["run_id"]

    # Set up Jinja2
    env = Environment(loader=BaseLoader(), autoescape=False)
    env.filters["basename"] = _basename_filter

    # Render markdown
    md_template = env.from_string(MARKDOWN_TEMPLATE)
    md_content = md_template.render(**ctx)
    md_path = output_dir / f"report_{run_id}.md"
    md_path.write_text(md_content, encoding="utf-8")

    # Render HTML
    html_body = md.markdown(md_content, extensions=["tables", "fenced_code"])
    html_template = env.from_string(HTML_WRAPPER)
    html_content = html_template.render(run_id=run_id, content=html_body)
    html_path = output_dir / f"report_{run_id}.html"
    html_path.write_text(html_content, encoding="utf-8")

    # Save JSON (machine-readable)
    json_output = {
        "run_id": run_id,
        "generated_at": ctx["generated_at"],
        "kpi_name": ctx["kpi_name"],
        "anomaly_window": {"start": str(ctx["anomaly_start"]), "end": str(ctx["anomaly_end"])},
        "baseline_window": {"start": str(ctx["baseline_start"]), "end": str(ctx["baseline_end"])},
        "findings": ctx["findings"],
        "confidence": ctx["confidence"],
        "draft_conclusion": ctx["draft_conclusion"],
        "verification_checks": ctx["verification_checks"],
        "review_status": str(ctx["review_status"]),
        "node_timings": state.get("node_timings", {}),
    }
    json_path = output_dir / f"report_{run_id}.json"
    json_path.write_text(json.dumps(json_output, indent=2, default=str), encoding="utf-8")

    logger.info("report_rendered", md=str(md_path), html=str(html_path), json=str(json_path))

    return ReportMetadata(
        report_id=run_id,
        case_id=state.get("request", {}).get("case_id") if isinstance(state.get("request"), dict) else getattr(state.get("request"), "case_id", None),
        generated_at=datetime.now(),
        markdown_path=str(md_path),
        html_path=str(html_path),
        json_path=str(json_path),
    )
