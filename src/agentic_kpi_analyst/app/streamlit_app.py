"""Streamlit app for the Agentic KPI Root-Cause Analyst."""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure src is on path
_src = Path(__file__).resolve().parent.parent.parent
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from agentic_kpi_analyst.config import get_settings
from agentic_kpi_analyst.logging_utils import setup_logging
from agentic_kpi_analyst.models import (
    InvestigationRequest,
    KPIName,
    ReviewStatus,
)

setup_logging("WARNING")
settings = get_settings()

st.set_page_config(page_title="KPI Root-Cause Analyst", page_icon="📊", layout="wide")


# ──────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────

def _load_cases() -> list[dict]:
    """Load anomaly cases from JSON."""
    cases_path = settings.data_abs_dir / "anomaly_cases.json"
    if not cases_path.exists():
        return []
    with open(cases_path) as f:
        return json.load(f)


def _safe_get(obj, key, default=None):
    """Get attribute or dict key safely."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _render_results():
    """Render investigation results if available in session state."""
    if "result" not in st.session_state:
        return

    result = st.session_state["result"]

    st.divider()
    st.header("Investigation Results")

    # Status metrics
    cols = st.columns(4)
    confidence = "N/A"
    if result.verification:
        conf = _safe_get(result.verification, "suggested_confidence", "N/A")
        confidence = conf.value if hasattr(conf, "value") else str(conf)
    cols[0].metric("Confidence", confidence)

    sql_ok = sum(1 for r in result.sql_results if _safe_get(r, "success", False))
    cols[1].metric("SQL Queries", f"{sql_ok}/{len(result.sql_results)}")
    cols[2].metric("Evidence Items", str(len(result.evidence_registry)))
    cols[3].metric("Analysis Dimensions", str(len(result.analysis_results)))

    # Conclusion
    st.subheader("Root-Cause Assessment")
    st.info(result.draft_conclusion or "No conclusion reached.")

    # Human Review Gate
    review_val = result.review_status
    if hasattr(review_val, "value"):
        review_val = review_val.value

    if review_val in ("pending", "approved", "rejected"):
        st.subheader("Human Review")
        if review_val == "pending":
            st.warning("This case requires human review.")
            col_a, col_b = st.columns(2)
            if col_a.button("Approve", key="approve"):
                st.session_state["result"].review_status = ReviewStatus.APPROVED
                st.rerun()
            if col_b.button("Reject", key="reject"):
                st.session_state["result"].review_status = ReviewStatus.REJECTED
                st.rerun()
        elif review_val == "approved":
            st.success("Approved")
        else:
            st.error("Rejected")

    # Findings
    if result.findings:
        st.subheader("Findings")
        for f in result.findings:
            desc = _safe_get(f, "description", "")
            conf = _safe_get(f, "confidence", "")
            if hasattr(conf, "value"):
                conf = conf.value
            is_primary = _safe_get(f, "is_primary", False)
            label = "Primary" if is_primary else "Secondary"
            st.markdown(f"**{label} Finding** (confidence: {conf})")
            st.markdown(f"> {desc}")

    # Evidence table
    if result.evidence_registry:
        st.subheader("Evidence Registry")
        evidence_data = [
            e if isinstance(e, dict) else e.model_dump()
            for e in result.evidence_registry
        ]
        edf = pd.DataFrame(evidence_data)
        display_cols = [c for c in ["evidence_id", "source_type", "description", "strength"] if c in edf.columns]
        st.dataframe(edf[display_cols], use_container_width=True)

    # Analysis results
    if result.analysis_results:
        st.subheader("Contribution Analysis")
        for ar in result.analysis_results:
            desc = _safe_get(ar, "description", "")
            table_data = _safe_get(ar, "table_data", [])
            chart_path = _safe_get(ar, "chart_path", "")

            st.markdown(f"**{desc}**")
            if table_data:
                tdf = pd.DataFrame(table_data)
                st.dataframe(tdf, use_container_width=True)

            if chart_path and Path(chart_path).exists():
                st.components.v1.html(Path(chart_path).read_text(), height=400, scrolling=True)

    # Verification checks
    if result.verification:
        st.subheader("Verification Checks")
        checks = _safe_get(result.verification, "checks", [])
        for check in checks:
            name = _safe_get(check, "check_name", "")
            passed = _safe_get(check, "passed", False)
            detail = _safe_get(check, "detail", "")
            icon = "✅" if passed else "❌"
            st.markdown(f"{icon} **{name}**: {detail}")

    # Download reports
    if result.report:
        st.subheader("Download Report")
        md_path = Path(_safe_get(result.report, "markdown_path", ""))
        html_path = Path(_safe_get(result.report, "html_path", ""))
        json_path = Path(_safe_get(result.report, "json_path", ""))

        col1, col2, col3 = st.columns(3)
        if md_path.exists():
            col1.download_button("Markdown", md_path.read_text(), md_path.name, "text/markdown")
        if html_path.exists():
            col2.download_button("HTML", html_path.read_text(), html_path.name, "text/html")
        if json_path.exists():
            col3.download_button("JSON", json_path.read_text(), json_path.name, "application/json")


# ──────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────

st.sidebar.title("KPI Analyst")
mode = st.sidebar.radio("Mode", ["Anomaly Case", "Manual Investigation", "Evaluation"])
st.sidebar.divider()
st.sidebar.markdown(f"**LLM Mode:** `{settings.llm_mode}`")
st.sidebar.markdown(f"**Warehouse:** `{settings.warehouse_path}`")


# ──────────────────────────────────────────────
# Main content
# ──────────────────────────────────────────────

if mode == "Anomaly Case":
    st.title("Anomaly Case Investigation")

    cases = _load_cases()
    if not cases:
        st.error("No anomaly cases found. Run `make seed` to generate data.")
        st.stop()

    case_options = {f"{c['case_id']}: {c['description'][:60]}": c for c in cases}
    selected_label = st.selectbox("Select Case", list(case_options.keys()))
    selected_case = case_options[selected_label]

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**KPI:** `{selected_case['kpi_name']}`")
        st.markdown(f"**Window:** {selected_case['affected_start_date']} to {selected_case['affected_end_date']}")
    with col2:
        st.markdown(f"**Expected Cause:** {selected_case['expected_primary_cause']}")
        st.markdown(f"**Dimensions:** {', '.join(selected_case.get('recommended_dimensions', []))}")

    if st.button("Run Investigation", type="primary", key="run_case"):
        with st.spinner("Running investigation..."):
            from agentic_kpi_analyst.graph.graph import run_case
            result = run_case(selected_case["case_id"], settings)
            st.session_state["result"] = result

    _render_results()

elif mode == "Manual Investigation":
    st.title("Manual KPI Investigation")

    col1, col2 = st.columns(2)
    with col1:
        kpi = st.selectbox("KPI", [k.value for k in KPIName])
        start_date = st.date_input("Anomaly Start", value=date(2025, 7, 10))
        end_date = st.date_input("Anomaly End", value=date(2025, 7, 17))
    with col2:
        description = st.text_area("Description", "Describe the anomaly...")
        bl_start = st.date_input("Baseline Start", value=start_date - timedelta(days=14))
        bl_end = st.date_input("Baseline End", value=start_date - timedelta(days=1))

    if st.button("Run Investigation", type="primary", key="run_manual"):
        request = InvestigationRequest(
            request_id="manual",
            kpi_name=KPIName(kpi),
            start_date=start_date,
            end_date=end_date,
            description=description,
            baseline_start_date=bl_start,
            baseline_end_date=bl_end,
        )
        with st.spinner("Running investigation..."):
            from agentic_kpi_analyst.graph.graph import run_investigation
            result = run_investigation(request, settings)
            st.session_state["result"] = result

    _render_results()

elif mode == "Evaluation":
    st.title("Evaluation Pipeline")

    cases = _load_cases()
    case_ids = [c["case_id"] for c in cases]
    selected_ids = st.multiselect("Select cases (empty = all)", case_ids)

    if st.button("Run Evaluation", type="primary"):
        with st.spinner("Running evaluation pipeline..."):
            from agentic_kpi_analyst.evals.runner import run_evaluation
            summary = run_evaluation(settings, case_ids=selected_ids or None)
            st.session_state["eval_summary"] = summary

    if "eval_summary" in st.session_state:
        summary = st.session_state["eval_summary"]

        cols = st.columns(4)
        cols[0].metric("Primary Hit Rate", f"{summary.primary_hit_rate:.1%}")
        cols[1].metric("SQL Success", f"{summary.avg_sql_success_rate:.1%}")
        cols[2].metric("Evidence Sufficiency", f"{summary.evidence_sufficiency_rate:.1%}")
        cols[3].metric("Avg Runtime", f"{summary.avg_runtime_seconds:.2f}s")

        st.subheader("Per-Case Results")
        df = pd.DataFrame([r.model_dump() for r in summary.per_case])
        display_cols = ["case_id", "primary_cause_hit", "sql_success_rate", "evidence_sufficient", "confidence", "runtime_seconds"]
        st.dataframe(df[display_cols], use_container_width=True)

        eval_dir = settings.output_abs_dir / "evals"
        summary_path = eval_dir / "eval_summary.json"
        if summary_path.exists():
            st.download_button("Download Summary JSON", summary_path.read_text(), "eval_summary.json", "application/json")
