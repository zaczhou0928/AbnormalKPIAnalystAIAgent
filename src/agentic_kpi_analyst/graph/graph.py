"""LangGraph workflow assembly and execution."""

from __future__ import annotations

import json
import time
from datetime import date
from functools import partial
from pathlib import Path
from typing import Any

from langgraph.graph import END, StateGraph

from agentic_kpi_analyst.config import Settings, get_settings
from agentic_kpi_analyst.graph.nodes import (
    NodeContext,
    context_retrieval_node,
    human_review_node,
    intake_node,
    planner_node,
    python_analysis_node,
    report_generation_node,
    sql_generation_node,
    sql_validation_and_execution_node,
    verifier_node,
)
from agentic_kpi_analyst.logging_utils import get_logger
from agentic_kpi_analyst.models import AnomalyCase, InvestigationRequest, KPIName, ReviewStatus
from agentic_kpi_analyst.state import WorkflowState

logger = get_logger(__name__)


def _should_loop_back(state: dict[str, Any]) -> str:
    """Decide whether to loop back for another investigation pass or proceed to report."""
    verification = state.get("verification", {})
    if hasattr(verification, "model_dump"):
        verification = verification.model_dump()

    overall_supported = verification.get("overall_supported", True)
    iteration = state.get("iteration", 0)
    max_iterations = state.get("max_iterations", 2)

    # If not supported and we haven't exceeded max iterations, loop back
    if not overall_supported and iteration < max_iterations:
        return "retry"
    return "proceed"


def _review_routing(state: dict[str, Any]) -> str:
    """Route based on human review status."""
    review_status = state.get("review_status", ReviewStatus.NOT_REQUIRED.value)
    if review_status == ReviewStatus.REJECTED.value:
        return "rejected"
    return "approved"


def build_graph(ctx: NodeContext) -> StateGraph:
    """Build the LangGraph investigation workflow.

    Flow:
    intake -> context_retrieval -> planner -> sql_generation
    -> sql_execution -> python_analysis -> verifier
    -> [retry? -> planner | proceed -> human_review]
    -> [rejected? -> END | approved -> report_generation -> END]
    """

    # Wrap node functions to pass context
    def _wrap(node_fn, state: dict[str, Any]) -> dict[str, Any]:
        return node_fn(state, ctx)

    graph = StateGraph(dict)

    # Add nodes
    graph.add_node("intake", partial(_wrap, intake_node))
    graph.add_node("context_retrieval", partial(_wrap, context_retrieval_node))
    graph.add_node("planner", partial(_wrap, planner_node))
    graph.add_node("sql_generation", partial(_wrap, sql_generation_node))
    graph.add_node("sql_execution", partial(_wrap, sql_validation_and_execution_node))
    graph.add_node("python_analysis", partial(_wrap, python_analysis_node))
    graph.add_node("verifier", partial(_wrap, verifier_node))
    graph.add_node("human_review", partial(_wrap, human_review_node))
    graph.add_node("report_generation", partial(_wrap, report_generation_node))

    # Increment iteration counter node
    def increment_iteration(state: dict[str, Any]) -> dict[str, Any]:
        return {**state, "iteration": state.get("iteration", 0) + 1}

    graph.add_node("increment_iteration", increment_iteration)

    # Set entry point
    graph.set_entry_point("intake")

    # Linear edges
    graph.add_edge("intake", "context_retrieval")
    graph.add_edge("context_retrieval", "planner")
    graph.add_edge("planner", "sql_generation")
    graph.add_edge("sql_generation", "sql_execution")
    graph.add_edge("sql_execution", "python_analysis")
    graph.add_edge("python_analysis", "verifier")

    # Conditional: retry or proceed
    graph.add_conditional_edges(
        "verifier",
        _should_loop_back,
        {
            "retry": "increment_iteration",
            "proceed": "human_review",
        },
    )
    graph.add_edge("increment_iteration", "planner")

    # Conditional: approved or rejected
    graph.add_conditional_edges(
        "human_review",
        _review_routing,
        {
            "approved": "report_generation",
            "rejected": END,
        },
    )
    graph.add_edge("report_generation", END)

    return graph


def run_investigation(
    request: InvestigationRequest,
    settings: Settings | None = None,
) -> WorkflowState:
    """Run the full investigation workflow for a request.

    Returns the final WorkflowState.
    """
    settings = settings or get_settings()
    ctx = NodeContext(settings)

    try:
        graph = build_graph(ctx)
        compiled = graph.compile()

        initial_state: dict[str, Any] = {
            "request": request,
            "retrieved_docs": [],
            "plan": None,
            "generated_queries": [],
            "sql_results": [],
            "sql_validation_failure_count": 0,
            "analysis_results": [],
            "evidence_registry": [],
            "findings": [],
            "draft_conclusion": "",
            "verification": None,
            "review_status": ReviewStatus.NOT_REQUIRED.value,
            "review_trigger_reasons": [],
            "review_comment": "",
            "report": None,
            "run_id": "",
            "iteration": 0,
            "max_iterations": 2,
            "errors": [],
            "start_time": time.monotonic(),
            "end_time": 0.0,
            "node_timings": {},
        }

        logger.info("workflow_started", kpi=request.kpi_name.value, case_id=request.case_id)
        final_state = compiled.invoke(initial_state)
        logger.info("workflow_complete", run_id=final_state.get("run_id"))

        return WorkflowState(**{
            k: final_state.get(k)
            for k in WorkflowState.model_fields
            if k in final_state and final_state.get(k) is not None
        })

    finally:
        ctx.close()


def run_case(case_id: str, settings: Settings | None = None) -> WorkflowState:
    """Run investigation for a labeled anomaly case by ID."""
    settings = settings or get_settings()
    data_dir = settings.data_abs_dir
    cases_path = data_dir / "anomaly_cases.json"

    if not cases_path.exists():
        raise FileNotFoundError(f"Anomaly cases file not found: {cases_path}. Run 'make seed' first.")

    with open(cases_path) as f:
        cases = json.load(f)

    case_data = None
    for c in cases:
        if c["case_id"] == case_id:
            case_data = c
            break

    if case_data is None:
        available = [c["case_id"] for c in cases]
        raise ValueError(f"Case {case_id} not found. Available: {available}")

    case = AnomalyCase(
        case_id=case_data["case_id"],
        kpi_name=KPIName(case_data["kpi_name"]),
        affected_start_date=date.fromisoformat(case_data["affected_start_date"]),
        affected_end_date=date.fromisoformat(case_data["affected_end_date"]),
        description=case_data["description"],
        expected_primary_cause=case_data["expected_primary_cause"],
        expected_secondary_factors=case_data.get("expected_secondary_factors", []),
        recommended_dimensions=case_data.get("recommended_dimensions", []),
        should_trigger_human_review=case_data.get("should_trigger_human_review", False),
    )

    request = InvestigationRequest(
        request_id=case.case_id,
        kpi_name=case.kpi_name,
        start_date=case.affected_start_date,
        end_date=case.affected_end_date,
        description=case.description,
        case_id=case.case_id,
    )

    return run_investigation(request, settings)
