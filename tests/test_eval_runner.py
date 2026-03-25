"""Tests for the evaluation pipeline."""

import pytest

from agentic_kpi_analyst.config import Settings
from agentic_kpi_analyst.evals.runner import run_evaluation


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(
        llm_mode="mock",
        human_review_auto_approve=True,
    )


class TestEvalRunner:
    """Test the evaluation pipeline."""

    def test_eval_runs_subset(self, settings: Settings) -> None:
        """Run eval on a small subset of cases."""
        summary = run_evaluation(settings, case_ids=["ANO-005", "ANO-002"])

        assert summary.total_cases == 2
        assert 0 <= summary.primary_hit_rate <= 1.0
        assert 0 <= summary.avg_sql_success_rate <= 1.0
        assert len(summary.per_case) == 2

    def test_eval_produces_metrics(self, settings: Settings) -> None:
        """Check all expected metrics are present."""
        summary = run_evaluation(settings, case_ids=["ANO-005"])

        assert summary.total_cases == 1
        assert summary.avg_sql_success_rate > 0, "SQL should succeed in mock mode"
        assert summary.evidence_sufficiency_rate >= 0
        assert summary.avg_runtime_seconds >= 0

    def test_per_case_results(self, settings: Settings) -> None:
        """Check per-case eval results are structured correctly."""
        summary = run_evaluation(settings, case_ids=["ANO-005"])

        assert len(summary.per_case) == 1
        case_result = summary.per_case[0]
        assert case_result.case_id == "ANO-005"
        assert case_result.sql_success_rate > 0
        assert case_result.predicted_primary_cause != ""
