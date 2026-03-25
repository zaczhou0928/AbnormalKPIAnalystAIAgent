"""LangGraph workflow nodes for the KPI investigation pipeline.

Each node function takes and returns a WorkflowState dict,
following LangGraph conventions.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from agentic_kpi_analyst.analysis.charting import (
    create_contribution_chart,
    create_timeseries_chart,
)
from agentic_kpi_analyst.analysis.contribution import run_contribution_analysis
from agentic_kpi_analyst.analysis.kpi_analysis import (
    baseline_vs_anomaly_comparison,
    concentration_check,
    small_sample_flag,
)
from agentic_kpi_analyst.config import Settings, get_settings
from agentic_kpi_analyst.llm import get_llm_client
from agentic_kpi_analyst.logging_utils import get_logger
from agentic_kpi_analyst.models import (
    AnalysisResult,
    AnomalyCase,
    Confidence,
    EvidenceItem,
    Finding,
    InvestigationPlan,
    InvestigationRequest,
    InvestigationStep,
    ReportMetadata,
    RetrievedDoc,
    ReviewStatus,
    ReviewTriggerReason,
    SQLExecutionResult,
    SQLQuery,
    VerificationCheck,
    VerificationResult,
)
from agentic_kpi_analyst.reports.renderer import render_report
from agentic_kpi_analyst.retrieval.retriever import KnowledgeRetriever
from agentic_kpi_analyst.warehouse.connection import WarehouseConnection
from agentic_kpi_analyst.warehouse.sql_executor import SQLExecutor

logger = get_logger(__name__)


# ──────────────────────────────────────────────
# Shared resources (initialized once per run)
# ──────────────────────────────────────────────

class NodeContext:
    """Holds shared resources for graph nodes. Created once per run."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.llm = get_llm_client(self.settings.llm_mode, api_key=self.settings.openai_api_key, model=self.settings.openai_model)
        self.warehouse = WarehouseConnection(self.settings)
        self.warehouse.initialize()
        self.sql_executor = SQLExecutor(self.warehouse, self.settings)
        self.retriever = KnowledgeRetriever(self.settings.knowledge_abs_dir)

    def close(self) -> None:
        self.warehouse.close()


# ──────────────────────────────────────────────
# Node 1: Intake
# ──────────────────────────────────────────────

def intake_node(state: dict[str, Any], ctx: NodeContext) -> dict[str, Any]:
    """Accept and normalize the investigation request.

    Handles both manual input and pre-labeled anomaly cases.
    Sets baseline window if not provided.
    """
    start = time.monotonic()
    request: InvestigationRequest = state["request"]

    # Set baseline window if not provided
    if request.baseline_start_date is None or request.baseline_end_date is None:
        anomaly_duration = (request.end_date - request.start_date).days + 1
        baseline_duration = max(anomaly_duration, 14)
        request.baseline_end_date = request.start_date - timedelta(days=1)
        request.baseline_start_date = request.baseline_end_date - timedelta(days=baseline_duration - 1)

    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    logger.info(
        "intake_complete",
        kpi=request.kpi_name,
        anomaly_window=f"{request.start_date} to {request.end_date}",
        baseline_window=f"{request.baseline_start_date} to {request.baseline_end_date}",
    )

    return {
        **state,
        "request": request,
        "run_id": run_id,
        "start_time": start,
        "node_timings": {**state.get("node_timings", {}), "intake": time.monotonic() - start},
    }


# ──────────────────────────────────────────────
# Node 2: Context Retrieval
# ──────────────────────────────────────────────

def context_retrieval_node(state: dict[str, Any], ctx: NodeContext) -> dict[str, Any]:
    """Retrieve relevant metric definitions and business rules."""
    start = time.monotonic()
    request: InvestigationRequest = state["request"]

    # Build retrieval query from KPI name and description
    query = f"{request.kpi_name.value} {request.description}"
    docs = ctx.retriever.retrieve(query, top_k=5)

    logger.info("context_retrieved", n_docs=len(docs))

    return {
        **state,
        "retrieved_docs": [d.model_dump() for d in docs],
        "node_timings": {**state.get("node_timings", {}), "context_retrieval": time.monotonic() - start},
    }


# ──────────────────────────────────────────────
# Node 3: Planner
# ──────────────────────────────────────────────

def planner_node(state: dict[str, Any], ctx: NodeContext) -> dict[str, Any]:
    """Create an investigation plan using LLM."""
    start = time.monotonic()
    request: InvestigationRequest = state["request"]
    docs = state.get("retrieved_docs", [])

    # Build prompt with context
    docs_text = "\n".join(
        f"- [{d.get('source', d['source']) if isinstance(d, dict) else d.source}] "
        f"{d.get('content', '')[:200] if isinstance(d, dict) else d.content[:200]}"
        for d in docs
    )

    prompt = f"""Create an investigation plan for this KPI anomaly.

KPI: {request.kpi_name.value}
Anomaly Window: {request.start_date} to {request.end_date}
Baseline Window: {request.baseline_start_date} to {request.baseline_end_date}
Description: {request.description}

Relevant context:
{docs_text}

Available dimensions: channel, region, category, customer_type, payment_type, campaign
Available views: daily_kpi_summary, fact_orders_enriched, channel_performance, category_performance, region_performance, customer_segment_performance, refund_summary

Return a JSON object with:
- hypothesis: string
- priority_dimensions: list of dimension names to investigate
- steps: list of objects with step_number, action, description, dimensions, sql_hint
"""

    result = ctx.llm.complete_json(prompt, system="You are a KPI investigation planner.")

    plan = InvestigationPlan(
        hypothesis=result.get("hypothesis", "Investigating KPI movement"),
        priority_dimensions=result.get("priority_dimensions", ["channel", "region", "category"]),
        steps=[
            InvestigationStep(**s) for s in result.get("steps", [])
        ],
    )

    logger.info("plan_created", n_steps=len(plan.steps), dims=plan.priority_dimensions)

    return {
        **state,
        "plan": plan.model_dump(),
        "node_timings": {**state.get("node_timings", {}), "planner": time.monotonic() - start},
    }


# ──────────────────────────────────────────────
# Node 4: SQL Generation
# ──────────────────────────────────────────────

def sql_generation_node(state: dict[str, Any], ctx: NodeContext) -> dict[str, Any]:
    """Generate SQL queries based on the investigation plan."""
    start = time.monotonic()
    request: InvestigationRequest = state["request"]
    plan_data = state.get("plan", {})
    dims = plan_data.get("priority_dimensions", ["channel", "region", "category"])

    prompt = f"""Generate SQL queries to investigate this KPI anomaly.

KPI: {request.kpi_name.value}
Anomaly Window: {request.start_date} to {request.end_date}
Baseline Window: {request.baseline_start_date} to {request.baseline_end_date}
Dimensions to investigate: {', '.join(dims)}
Description: {request.description}

Available views: daily_kpi_summary, fact_orders_enriched, channel_performance, category_performance, region_performance, customer_segment_performance, refund_summary

Rules:
- Only SELECT queries
- Use the curated views
- Include date filters
- Compare baseline vs anomaly windows

Return JSON with: queries: list of objects with query_id, purpose, target_view, sql
"""

    result = ctx.llm.complete_json(prompt, system="You are a SQL query generator for analytics. Generate safe, read-only SQL.")

    queries = [
        SQLQuery(**q) for q in result.get("queries", [])
    ]

    logger.info("sql_generated", n_queries=len(queries))

    return {
        **state,
        "generated_queries": [q.model_dump() for q in queries],
        "node_timings": {**state.get("node_timings", {}), "sql_generation": time.monotonic() - start},
    }


# ──────────────────────────────────────────────
# Node 5: SQL Validation & Execution
# ──────────────────────────────────────────────

def sql_validation_and_execution_node(state: dict[str, Any], ctx: NodeContext) -> dict[str, Any]:
    """Validate and execute generated SQL queries."""
    start = time.monotonic()
    queries = [SQLQuery(**q) if isinstance(q, dict) else q for q in state.get("generated_queries", [])]

    results: list[dict[str, Any]] = []
    validation_failures = state.get("sql_validation_failure_count", 0)
    evidence: list[dict[str, Any]] = list(state.get("evidence_registry", []))

    for query in queries:
        exec_result = ctx.sql_executor.execute(query)
        results.append(exec_result.model_dump())

        if exec_result.success:
            evidence.append(EvidenceItem(
                evidence_id=f"sql_{query.query_id}",
                source_type="sql_result",
                source_ref=query.query_id,
                description=f"{query.purpose} — {exec_result.row_count} rows returned",
                supports_conclusion=True,
                strength=Confidence.MEDIUM,
            ).model_dump())
        else:
            validation_failures += 1

    logger.info(
        "sql_execution_complete",
        total=len(queries),
        success=sum(1 for r in results if r["success"]),
        failed=sum(1 for r in results if not r["success"]),
    )

    return {
        **state,
        "sql_results": results,
        "sql_validation_failure_count": validation_failures,
        "evidence_registry": evidence,
        "node_timings": {**state.get("node_timings", {}), "sql_execution": time.monotonic() - start},
    }


# ──────────────────────────────────────────────
# Node 6: Python Analysis
# ──────────────────────────────────────────────

def python_analysis_node(state: dict[str, Any], ctx: NodeContext) -> dict[str, Any]:
    """Run contribution analysis, slice comparison, and generate charts."""
    start = time.monotonic()
    request: InvestigationRequest = state["request"]
    plan_data = state.get("plan", {})
    dims = plan_data.get("priority_dimensions", ["channel", "region", "category"])

    # Determine which metric column to analyze
    kpi_metric_map = {
        "gmv": ("order_total", "sum"),
        "order_count": ("order_total", "count"),
        "aov": ("order_total", "mean"),
        "revenue": ("order_total", "sum"),
        "refund_rate": ("is_refunded", "mean"),
        "cancellation_rate": ("is_cancelled", "mean"),
        "new_customer_ratio": ("customer_type", "count"),  # special handling below
        "conversion_rate": ("order_total", "count"),
    }
    metric_col, agg = kpi_metric_map.get(request.kpi_name.value, ("order_total", "sum"))

    # Load order data
    orders = ctx.warehouse.execute_query("SELECT * FROM orders")

    bl_start = str(request.baseline_start_date)
    bl_end = str(request.baseline_end_date)
    an_start = str(request.start_date)
    an_end = str(request.end_date)

    # 1. Baseline vs anomaly aggregate comparison
    agg_comparison = baseline_vs_anomaly_comparison(
        orders, "order_date", bl_start, bl_end, an_start, an_end,
        ["order_total"], agg="sum",
    )

    # Also do count-based comparison
    count_comparison = baseline_vs_anomaly_comparison(
        orders, "order_date", bl_start, bl_end, an_start, an_end,
        ["order_total"], agg="count",
    )

    # 2. Contribution analysis across dimensions
    contribution_results = run_contribution_analysis(
        orders, "order_date", dims, bl_start, bl_end, an_start, an_end,
        metric_col, agg,
    )

    # 3. Generate charts
    charts_dir = ctx.settings.output_abs_dir / "charts" / state.get("run_id", "default")
    for i, ar in enumerate(contribution_results):
        chart_path = create_contribution_chart(ar, charts_dir, chart_id=f"{ar.top_drivers[0].dimension}_{i}" if ar.top_drivers else str(i))
        ar.chart_path = chart_path

    # Time series chart
    ts_chart = create_timeseries_chart(
        orders, "order_date", "order_total",
        bl_start, bl_end, an_start, an_end,
        charts_dir, chart_id="overview",
    )

    # 4. Build evidence from analysis
    evidence: list[dict[str, Any]] = list(state.get("evidence_registry", []))
    analysis_results_dicts: list[dict[str, Any]] = []

    for ar in contribution_results:
        analysis_results_dicts.append(ar.model_dump())

        # Check concentration
        if ar.top_drivers:
            slice_df = pd.DataFrame(ar.table_data)
            conc = concentration_check(slice_df, top_n=1)
            flags = small_sample_flag(slice_df)

            dim = ar.top_drivers[0].dimension
            top_driver = ar.top_drivers[0]

            evidence.append(EvidenceItem(
                evidence_id=f"analysis_{dim}",
                source_type="analysis",
                source_ref=f"contribution_{dim}",
                description=(
                    f"{dim} analysis: top driver is '{top_driver.slice_value}' "
                    f"with {top_driver.contribution_pct:.1f}% contribution "
                    f"({top_driver.percent_change:+.1f}% change). "
                    f"Concentrated: {conc['concentrated']}"
                ),
                supports_conclusion=True,
                strength=Confidence.HIGH if conc["concentrated"] else Confidence.MEDIUM,
            ).model_dump())

            if flags:
                evidence.append(EvidenceItem(
                    evidence_id=f"caveat_{dim}_sample_size",
                    source_type="analysis",
                    source_ref=f"sample_check_{dim}",
                    description=f"Sample size warning: {'; '.join(flags)}",
                    supports_conclusion=False,
                    strength=Confidence.LOW,
                ).model_dump())

    # Add baseline comparison as evidence
    for metric_name, vals in agg_comparison["metrics"].items():
        evidence.append(EvidenceItem(
            evidence_id=f"baseline_comparison_{metric_name}",
            source_type="analysis",
            source_ref="baseline_comparison",
            description=(
                f"Aggregate {metric_name}: baseline_daily={vals['baseline_daily']}, "
                f"anomaly_daily={vals['anomaly_daily']}, "
                f"change={vals['percent_change']:+.1f}%"
            ),
            supports_conclusion=True,
            strength=Confidence.HIGH,
        ).model_dump())

    logger.info("analysis_complete", n_results=len(contribution_results))

    return {
        **state,
        "analysis_results": analysis_results_dicts,
        "evidence_registry": evidence,
        "node_timings": {**state.get("node_timings", {}), "analysis": time.monotonic() - start},
    }


# ──────────────────────────────────────────────
# Node 7: Verifier
# ──────────────────────────────────────────────

def verifier_node(state: dict[str, Any], ctx: NodeContext) -> dict[str, Any]:
    """Verify that conclusions are supported by evidence.

    Combines heuristic checks with optional LLM verification.
    """
    start = time.monotonic()
    evidence = state.get("evidence_registry", [])
    analysis_results = state.get("analysis_results", [])
    sql_results = state.get("sql_results", [])
    request: InvestigationRequest = state["request"]

    checks: list[dict[str, Any]] = []
    unsupported: list[str] = []

    # Check 1: Do we have SQL evidence?
    sql_success = [r for r in sql_results if r.get("success", False)]
    checks.append(VerificationCheck(
        check_name="sql_evidence_exists",
        passed=len(sql_success) > 0,
        detail=f"{len(sql_success)} successful SQL queries out of {len(sql_results)}",
    ).model_dump())

    # Check 2: Do we have analysis results?
    checks.append(VerificationCheck(
        check_name="analysis_results_exist",
        passed=len(analysis_results) > 0,
        detail=f"{len(analysis_results)} analysis results produced",
    ).model_dump())

    # Check 3: Is there at least one supporting evidence item?
    supporting = [e for e in evidence if e.get("supports_conclusion", False)]
    checks.append(VerificationCheck(
        check_name="supporting_evidence",
        passed=len(supporting) >= 2,
        detail=f"{len(supporting)} supporting evidence items",
    ).model_dump())

    # Check 4: Check if any dimension shows concentrated contribution
    has_concentrated = False
    for ar in analysis_results:
        stats = ar.get("summary_stats", {})
        if stats.get("concentrated", 0) > 0:
            has_concentrated = True
            break
    checks.append(VerificationCheck(
        check_name="concentrated_driver_found",
        passed=has_concentrated,
        detail="Concentrated driver found" if has_concentrated else "No single dominant driver — movement may be diffuse",
    ).model_dump())

    # Check 5: Sample size adequacy
    sample_warnings = [e for e in evidence if "sample size" in e.get("description", "").lower()]
    checks.append(VerificationCheck(
        check_name="sample_size_adequate",
        passed=len(sample_warnings) == 0,
        detail=f"{len(sample_warnings)} sample size warnings" if sample_warnings else "All slices have adequate sample sizes",
    ).model_dump())

    # Determine overall assessment
    passed_checks = sum(1 for c in checks if c["passed"])
    total_checks = len(checks)
    all_passed = passed_checks == total_checks

    # Determine confidence
    if passed_checks >= 4:
        suggested_confidence = Confidence.HIGH
    elif passed_checks >= 3:
        suggested_confidence = Confidence.MEDIUM
    elif passed_checks >= 2:
        suggested_confidence = Confidence.LOW
    else:
        suggested_confidence = Confidence.INCONCLUSIVE

    # Determine if human review needed
    needs_review = False
    review_reasons: list[str] = []

    if suggested_confidence in (Confidence.LOW, Confidence.INCONCLUSIVE):
        needs_review = True
        review_reasons.append(ReviewTriggerReason.LOW_CONFIDENCE.value)

    sql_fail_count = state.get("sql_validation_failure_count", 0)
    if sql_fail_count >= 2:
        needs_review = True
        review_reasons.append(ReviewTriggerReason.SQL_VALIDATION_FAILURE.value)

    elapsed = time.monotonic() - state.get("start_time", time.monotonic())
    if elapsed > ctx.settings.max_investigation_cost_seconds:
        needs_review = True
        review_reasons.append(ReviewTriggerReason.COST_EXCEEDED.value)

    if not all_passed:
        needs_review = True
        review_reasons.append(ReviewTriggerReason.WEAK_EVIDENCE.value)

    # Build findings from analysis results
    findings: list[dict[str, Any]] = []
    evidence_ids = [e.get("evidence_id", "") for e in evidence if e.get("supports_conclusion")]

    # Find the top driver across all dimensions
    best_driver = None
    best_contribution = 0.0
    for ar in analysis_results:
        drivers = ar.get("top_drivers", [])
        if drivers:
            top = drivers[0]
            if abs(top.get("contribution_pct", 0)) > abs(best_contribution):
                best_driver = top
                best_contribution = top.get("contribution_pct", 0)

    if best_driver:
        findings.append(Finding(
            finding_id="finding_primary",
            description=(
                f"The {request.kpi_name.value} movement is primarily driven by "
                f"'{best_driver['slice_value']}' in the {best_driver['dimension']} dimension "
                f"({best_driver['percent_change']:+.1f}% change, "
                f"{best_driver['contribution_pct']:.1f}% contribution)."
            ),
            confidence=suggested_confidence,
            evidence_ids=evidence_ids[:5],
            is_primary=True,
            caveats=[
                "Analysis is based on observed correlations, not causal inference.",
            ] + (["Some dimension slices have small sample sizes."] if sample_warnings else []),
        ).model_dump())

    # Draft conclusion
    if best_driver:
        conclusion = (
            f"Evidence suggests the {request.kpi_name.value} anomaly is primarily "
            f"driven by changes in '{best_driver['slice_value']}' ({best_driver['dimension']}). "
            f"This slice accounts for {best_driver['contribution_pct']:.1f}% of the total movement. "
            f"Confidence: {suggested_confidence.value}."
        )
    else:
        conclusion = (
            f"The {request.kpi_name.value} movement could not be attributed to a single dominant driver. "
            f"The change appears diffuse across multiple dimensions. Confidence: inconclusive."
        )

    verification = VerificationResult(
        overall_supported=all_passed,
        checks=[VerificationCheck(**c) for c in checks],
        unsupported_claims=unsupported,
        suggested_confidence=suggested_confidence,
        needs_human_review=needs_review,
        review_reasons=[ReviewTriggerReason(r) for r in review_reasons],
    )

    logger.info(
        "verification_complete",
        passed=f"{passed_checks}/{total_checks}",
        confidence=suggested_confidence.value,
        needs_review=needs_review,
    )

    return {
        **state,
        "verification": verification.model_dump(),
        "findings": findings,
        "draft_conclusion": conclusion,
        "node_timings": {**state.get("node_timings", {}), "verifier": time.monotonic() - start},
    }


# ──────────────────────────────────────────────
# Node 8: Human Review
# ──────────────────────────────────────────────

def human_review_node(state: dict[str, Any], ctx: NodeContext) -> dict[str, Any]:
    """Handle human review gate.

    In auto-approve mode, automatically approves.
    In interactive mode (Streamlit), sets status to pending.
    """
    start = time.monotonic()
    verification = state.get("verification", {})
    needs_review = verification.get("needs_human_review", False)

    if not needs_review:
        return {
            **state,
            "review_status": ReviewStatus.NOT_REQUIRED.value,
            "node_timings": {**state.get("node_timings", {}), "human_review": time.monotonic() - start},
        }

    review_reasons = verification.get("review_reasons", [])

    if ctx.settings.human_review_auto_approve:
        logger.info("human_review_auto_approved", reasons=review_reasons)
        return {
            **state,
            "review_status": ReviewStatus.APPROVED.value,
            "review_trigger_reasons": review_reasons,
            "review_comment": "Auto-approved (human_review_auto_approve=True)",
            "node_timings": {**state.get("node_timings", {}), "human_review": time.monotonic() - start},
        }

    # In non-auto mode, mark as pending (Streamlit or CLI will handle)
    logger.info("human_review_pending", reasons=review_reasons)
    return {
        **state,
        "review_status": ReviewStatus.PENDING.value,
        "review_trigger_reasons": review_reasons,
        "node_timings": {**state.get("node_timings", {}), "human_review": time.monotonic() - start},
    }


# ──────────────────────────────────────────────
# Node 9: Report Generation
# ──────────────────────────────────────────────

def report_generation_node(state: dict[str, Any], ctx: NodeContext) -> dict[str, Any]:
    """Generate markdown, HTML, and JSON reports."""
    start = time.monotonic()
    request: InvestigationRequest = state["request"]
    run_id = state.get("run_id", "unknown")

    report_dir = ctx.settings.output_abs_dir / "reports" / run_id
    report_dir.mkdir(parents=True, exist_ok=True)

    report_meta = render_report(state, report_dir)

    logger.info(
        "report_generated",
        markdown=report_meta.markdown_path,
        html=report_meta.html_path,
    )

    return {
        **state,
        "report": report_meta.model_dump(),
        "end_time": time.monotonic(),
        "node_timings": {**state.get("node_timings", {}), "report_generation": time.monotonic() - start},
    }
