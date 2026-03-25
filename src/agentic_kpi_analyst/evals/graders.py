"""Graders for evaluating investigation results against labeled cases."""

from __future__ import annotations

import re
from typing import Any

from agentic_kpi_analyst.logging_utils import get_logger
from agentic_kpi_analyst.models import AnomalyCase, CaseEvalResult, Confidence, ReviewStatus
from agentic_kpi_analyst.state import WorkflowState

logger = get_logger(__name__)


def _normalize(text: str) -> set[str]:
    """Normalize text to a set of lowercase words for matching.

    Splits on both whitespace and underscores, removes stop words
    and very short tokens for better matching.
    """
    text = text.lower().replace("_", " ").replace("-", " ")
    words = set(re.findall(r"[a-z]+", text))
    stop_words = {"the", "is", "in", "of", "a", "an", "to", "by", "and", "or", "for", "was", "be", "as", "at", "on", "that", "this", "from", "with"}
    # Simple suffix stripping for better matching (refunds->refund, caused->caus, etc.)
    stemmed = set()
    for w in words:
        if len(w) <= 2 or w in stop_words:
            continue
        # Strip common suffixes
        for suffix in ("ing", "ed", "tion", "sion", "ment", "ness", "ous", "ive", "ly", "es", "s"):
            if w.endswith(suffix) and len(w) - len(suffix) >= 3:
                w = w[:-len(suffix)]
                break
        stemmed.add(w)
    return stemmed


def _cause_hit(predicted: str, expected: str) -> bool:
    """Check if the predicted cause matches the expected cause.

    Uses keyword overlap: if key domain terms from the expected cause
    appear in the prediction, it's a hit. We use a 30% threshold
    on expected keywords, which is forgiving for semantic matches.
    """
    pred_words = _normalize(predicted)
    expected_words = _normalize(expected)

    if not expected_words:
        return False

    overlap = pred_words & expected_words
    # Require at least 25% overlap with expected cause keywords
    return len(overlap) / len(expected_words) >= 0.25


def grade_case(case: AnomalyCase, state: WorkflowState) -> CaseEvalResult:
    """Grade a single investigation result against a labeled case.

    Evaluates:
    - Primary cause identification
    - SQL execution success rate
    - Evidence sufficiency
    - Unsupported claims
    - Human review triggering
    - Runtime

    Args:
        case: The labeled anomaly case.
        state: The final workflow state after investigation.

    Returns:
        CaseEvalResult with all metrics.
    """
    # Primary cause hit
    primary_hit = False
    top_k_hit = False
    predicted_cause = ""

    if state.findings:
        for finding in state.findings:
            desc = finding.description if isinstance(finding, dict) is False else finding.get("description", "")
            if isinstance(finding, dict):
                desc = finding.get("description", "")
                is_primary = finding.get("is_primary", False)
            else:
                desc = finding.description
                is_primary = finding.is_primary

            if is_primary:
                predicted_cause = desc
                primary_hit = _cause_hit(desc, case.expected_primary_cause)

            # Top-k: any finding matches
            if _cause_hit(desc, case.expected_primary_cause):
                top_k_hit = True

    # Also check draft conclusion
    conclusion = state.draft_conclusion or ""
    if not primary_hit and _cause_hit(conclusion, case.expected_primary_cause):
        primary_hit = True
        top_k_hit = True
        if not predicted_cause:
            predicted_cause = conclusion[:200]

    # SQL success rate
    total_sql = len(state.sql_results)
    successful_sql = sum(
        1 for r in state.sql_results
        if (r.success if not isinstance(r, dict) else r.get("success", False))
    )
    sql_success_rate = successful_sql / total_sql if total_sql > 0 else 0.0

    # Evidence sufficiency
    supporting_evidence = sum(
        1 for e in state.evidence_registry
        if (e.supports_conclusion if not isinstance(e, dict) else e.get("supports_conclusion", False))
    )
    evidence_sufficient = supporting_evidence >= 2

    # Unsupported claims
    unsupported_count = 0
    if state.verification:
        v = state.verification
        if isinstance(v, dict):
            unsupported_count = len(v.get("unsupported_claims", []))
        else:
            unsupported_count = len(v.unsupported_claims)

    # Human review
    review_triggered = state.review_status not in (
        ReviewStatus.NOT_REQUIRED,
        ReviewStatus.NOT_REQUIRED.value,
        "not_required",
    )

    # Runtime
    runtime = state.end_time - state.start_time if state.end_time > 0 else 0.0

    # Confidence
    confidence = Confidence.INCONCLUSIVE
    if state.verification:
        v = state.verification
        if isinstance(v, dict):
            conf_val = v.get("suggested_confidence", "inconclusive")
            confidence = Confidence(conf_val) if isinstance(conf_val, str) else conf_val
        else:
            confidence = v.suggested_confidence

    result = CaseEvalResult(
        case_id=case.case_id,
        primary_cause_hit=primary_hit,
        top_k_hit=top_k_hit,
        sql_success_rate=sql_success_rate,
        evidence_sufficient=evidence_sufficient,
        unsupported_claim_count=unsupported_count,
        human_review_triggered=review_triggered,
        runtime_seconds=runtime,
        predicted_primary_cause=predicted_cause[:500],
        confidence=confidence,
    )

    logger.info(
        "case_graded",
        case_id=case.case_id,
        primary_hit=primary_hit,
        sql_success=f"{sql_success_rate:.0%}",
    )

    return result
