"""Shared workflow state for the LangGraph investigation pipeline."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from agentic_kpi_analyst.models import (
    AnalysisResult,
    Confidence,
    EvidenceItem,
    Finding,
    InvestigationPlan,
    InvestigationRequest,
    ReportMetadata,
    RetrievedDoc,
    ReviewStatus,
    ReviewTriggerReason,
    SQLExecutionResult,
    SQLQuery,
    VerificationResult,
)


class WorkflowState(BaseModel):
    """Strongly typed shared state passed through the LangGraph workflow.

    Every node reads from and writes to this state object.
    """

    # --- Input ---
    request: InvestigationRequest | None = None

    # --- Context Retrieval ---
    retrieved_docs: list[RetrievedDoc] = Field(default_factory=list)

    # --- Planning ---
    plan: InvestigationPlan | None = None

    # --- SQL Generation & Execution ---
    generated_queries: list[SQLQuery] = Field(default_factory=list)
    sql_results: list[SQLExecutionResult] = Field(default_factory=list)
    sql_validation_failure_count: int = 0

    # --- Analysis ---
    analysis_results: list[AnalysisResult] = Field(default_factory=list)

    # --- Evidence ---
    evidence_registry: list[EvidenceItem] = Field(default_factory=list)

    # --- Findings ---
    findings: list[Finding] = Field(default_factory=list)
    draft_conclusion: str = ""

    # --- Verification ---
    verification: VerificationResult | None = None

    # --- Human Review ---
    review_status: ReviewStatus = ReviewStatus.NOT_REQUIRED
    review_trigger_reasons: list[ReviewTriggerReason] = Field(default_factory=list)
    review_comment: str = ""

    # --- Report ---
    report: ReportMetadata | None = None

    # --- Run Metadata ---
    run_id: str = ""
    iteration: int = 0
    max_iterations: int = 2
    errors: list[str] = Field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0
    node_timings: dict[str, float] = Field(default_factory=dict)

    # --- LangGraph compatibility: allow dict-style access ---
    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)
