"""Contribution analysis: rank dimension slices by their impact on KPI movement."""

from __future__ import annotations

import pandas as pd

from agentic_kpi_analyst.analysis.kpi_analysis import dimension_slice_analysis
from agentic_kpi_analyst.logging_utils import get_logger
from agentic_kpi_analyst.models import AnalysisResult, DimensionContribution

logger = get_logger(__name__)


def run_contribution_analysis(
    df: pd.DataFrame,
    date_col: str,
    dimensions: list[str],
    baseline_start: str,
    baseline_end: str,
    anomaly_start: str,
    anomaly_end: str,
    metric_col: str,
    agg: str = "sum",
) -> list[AnalysisResult]:
    """Run contribution analysis across multiple dimensions.

    For each dimension, breaks down the metric by slice and ranks
    slices by their contribution to the total movement.

    Returns one AnalysisResult per dimension.
    """
    results: list[AnalysisResult] = []

    for dim in dimensions:
        if dim not in df.columns:
            logger.warning("dimension_not_found", dimension=dim)
            continue

        slice_df = dimension_slice_analysis(
            df=df,
            date_col=date_col,
            dimension=dim,
            baseline_start=baseline_start,
            baseline_end=baseline_end,
            anomaly_start=anomaly_start,
            anomaly_end=anomaly_end,
            metric_col=metric_col,
            agg=agg,
        )

        # Build DimensionContribution objects for top drivers
        top_drivers: list[DimensionContribution] = []
        for _, row in slice_df.iterrows():
            top_drivers.append(DimensionContribution(
                dimension=dim,
                slice_value=str(row["dimension_value"]),
                baseline_value=row["baseline_daily"],
                anomaly_value=row["anomaly_daily"],
                absolute_change=row["absolute_change"],
                percent_change=row["percent_change"],
                contribution_pct=row["contribution_pct"],
            ))

        # Summary stats
        total_change = slice_df["absolute_change"].sum()
        top_1_contrib = abs(slice_df.iloc[0]["contribution_pct"]) if len(slice_df) > 0 else 0
        concentrated = top_1_contrib > 50.0

        summary_stats = {
            "total_change": round(total_change, 2),
            "n_slices": len(slice_df),
            "top_1_contribution_pct": round(top_1_contrib, 2),
            "concentrated": float(concentrated),
        }

        results.append(AnalysisResult(
            analysis_type="contribution",
            description=f"Contribution analysis of {metric_col} by {dim}",
            top_drivers=top_drivers,
            summary_stats=summary_stats,
            table_data=slice_df.to_dict(orient="records"),
        ))

        logger.info(
            "contribution_analysis_done",
            dimension=dim,
            n_slices=len(slice_df),
            concentrated=concentrated,
        )

    return results
