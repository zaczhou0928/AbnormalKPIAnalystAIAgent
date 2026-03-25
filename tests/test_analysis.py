"""Tests for the analysis module."""

import pytest
import pandas as pd

from agentic_kpi_analyst.analysis.kpi_analysis import (
    baseline_vs_anomaly_comparison,
    concentration_check,
    dimension_slice_analysis,
    small_sample_flag,
)
from agentic_kpi_analyst.analysis.contribution import run_contribution_analysis
from agentic_kpi_analyst.warehouse.connection import WarehouseConnection


class TestBaselineComparison:
    """Test baseline vs anomaly comparison."""

    def test_comparison_produces_metrics(self, seeded_warehouse: WarehouseConnection) -> None:
        orders = seeded_warehouse.execute_query("SELECT * FROM orders")
        result = baseline_vs_anomaly_comparison(
            orders, "order_date",
            "2025-06-26", "2025-07-09",
            "2025-07-10", "2025-07-17",
            ["order_total"],
            agg="sum",
        )
        assert "metrics" in result
        assert "order_total" in result["metrics"]
        assert "percent_change" in result["metrics"]["order_total"]

    def test_count_agg(self, seeded_warehouse: WarehouseConnection) -> None:
        orders = seeded_warehouse.execute_query("SELECT * FROM orders")
        result = baseline_vs_anomaly_comparison(
            orders, "order_date",
            "2025-06-26", "2025-07-09",
            "2025-07-10", "2025-07-17",
            ["order_total"],
            agg="count",
        )
        assert result["metrics"]["order_total"]["baseline_daily"] > 0


class TestSliceAnalysis:
    """Test dimensional slice analysis."""

    def test_slice_by_payment_type(self, seeded_warehouse: WarehouseConnection) -> None:
        orders = seeded_warehouse.execute_query("SELECT * FROM orders")
        result = dimension_slice_analysis(
            orders, "order_date", "payment_type",
            "2025-06-26", "2025-07-09",
            "2025-07-10", "2025-07-17",
            "order_total", agg="count",
        )
        assert len(result) > 0
        assert "contribution_pct" in result.columns
        assert "dimension_value" in result.columns

    def test_credit_card_is_top_driver_for_ano005(self, seeded_warehouse: WarehouseConnection) -> None:
        """ANO-005: credit card payment failure should show as top driver."""
        orders = seeded_warehouse.execute_query("SELECT * FROM orders")
        result = dimension_slice_analysis(
            orders, "order_date", "payment_type",
            "2025-06-26", "2025-07-09",
            "2025-07-10", "2025-07-17",
            "order_total", agg="count",
        )
        # credit_card should be the top negative driver
        top = result.iloc[0]
        assert top["dimension_value"] == "credit_card"
        assert top["absolute_change"] < 0


class TestConcentrationCheck:
    """Test concentration analysis."""

    def test_concentrated_result(self) -> None:
        df = pd.DataFrame({
            "dimension_value": ["a", "b", "c"],
            "contribution_pct": [80.0, 15.0, 5.0],
        })
        result = concentration_check(df, top_n=1)
        assert result["concentrated"]
        assert result["top_n_contribution_pct"] == 80.0

    def test_not_concentrated(self) -> None:
        df = pd.DataFrame({
            "dimension_value": ["a", "b", "c"],
            "contribution_pct": [30.0, 35.0, 35.0],
        })
        result = concentration_check(df, top_n=1)
        assert not result["concentrated"]


class TestSmallSampleFlag:
    """Test small sample flagging."""

    def test_flags_small_samples(self) -> None:
        df = pd.DataFrame({
            "dimension_value": ["a", "b"],
            "sample_size_baseline": [100, 10],
            "sample_size_anomaly": [80, 5],
        })
        flags = small_sample_flag(df, min_count=30)
        assert len(flags) == 1
        assert "b" in flags[0]

    def test_no_flags_for_large_samples(self) -> None:
        df = pd.DataFrame({
            "dimension_value": ["a", "b"],
            "sample_size_baseline": [100, 50],
            "sample_size_anomaly": [80, 40],
        })
        flags = small_sample_flag(df, min_count=30)
        assert len(flags) == 0


class TestContributionAnalysis:
    """Test end-to-end contribution analysis."""

    def test_runs_across_multiple_dims(self, seeded_warehouse: WarehouseConnection) -> None:
        orders = seeded_warehouse.execute_query("SELECT * FROM orders")
        results = run_contribution_analysis(
            orders, "order_date", ["channel", "region", "payment_type"],
            "2025-06-26", "2025-07-09",
            "2025-07-10", "2025-07-17",
            "order_total", agg="count",
        )
        assert len(results) == 3
        for r in results:
            assert r.analysis_type == "contribution"
            assert len(r.top_drivers) > 0
