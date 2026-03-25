"""Core KPI analysis utilities: baseline comparison and slice analysis."""

from __future__ import annotations

from typing import Any

import pandas as pd

from agentic_kpi_analyst.logging_utils import get_logger

logger = get_logger(__name__)


def baseline_vs_anomaly_comparison(
    df: pd.DataFrame,
    date_col: str,
    baseline_start: str,
    baseline_end: str,
    anomaly_start: str,
    anomaly_end: str,
    metric_cols: list[str],
    agg: str = "sum",
) -> dict[str, Any]:
    """Compare aggregate metrics between baseline and anomaly windows.

    Args:
        df: DataFrame with a date column and metric columns.
        date_col: Name of the date column.
        baseline_start/end: Baseline window boundaries.
        anomaly_start/end: Anomaly window boundaries.
        metric_cols: Columns to aggregate.
        agg: Aggregation function ("sum", "mean", "count").

    Returns:
        Dict with baseline_values, anomaly_values, absolute_change, percent_change per metric.
    """
    bl_mask = (df[date_col] >= baseline_start) & (df[date_col] <= baseline_end)
    an_mask = (df[date_col] >= anomaly_start) & (df[date_col] <= anomaly_end)

    bl_days = (pd.Timestamp(baseline_end) - pd.Timestamp(baseline_start)).days + 1
    an_days = (pd.Timestamp(anomaly_end) - pd.Timestamp(anomaly_start)).days + 1

    results: dict[str, Any] = {
        "baseline_days": bl_days,
        "anomaly_days": an_days,
        "metrics": {},
    }

    for col in metric_cols:
        if agg == "sum":
            bl_val = float(df.loc[bl_mask, col].sum())
            an_val = float(df.loc[an_mask, col].sum())
            # Normalize to daily rate for fair comparison
            bl_daily = bl_val / bl_days if bl_days > 0 else 0
            an_daily = an_val / an_days if an_days > 0 else 0
        elif agg == "mean":
            bl_daily = float(df.loc[bl_mask, col].mean()) if bl_mask.sum() > 0 else 0
            an_daily = float(df.loc[an_mask, col].mean()) if an_mask.sum() > 0 else 0
        else:
            bl_daily = float(df.loc[bl_mask, col].count()) / bl_days if bl_days > 0 else 0
            an_daily = float(df.loc[an_mask, col].count()) / an_days if an_days > 0 else 0

        abs_change = an_daily - bl_daily
        pct_change = (abs_change / bl_daily * 100) if bl_daily != 0 else 0.0

        results["metrics"][col] = {
            "baseline_daily": round(bl_daily, 2),
            "anomaly_daily": round(an_daily, 2),
            "absolute_change": round(abs_change, 2),
            "percent_change": round(pct_change, 2),
        }

    return results


def dimension_slice_analysis(
    df: pd.DataFrame,
    date_col: str,
    dimension: str,
    baseline_start: str,
    baseline_end: str,
    anomaly_start: str,
    anomaly_end: str,
    metric_col: str,
    agg: str = "sum",
) -> pd.DataFrame:
    """Break down a metric by dimension slices, comparing baseline vs anomaly.

    Returns a DataFrame with columns:
        dimension, baseline_daily, anomaly_daily, absolute_change, percent_change,
        contribution_pct, sample_size_baseline, sample_size_anomaly
    """
    bl_mask = (df[date_col] >= baseline_start) & (df[date_col] <= baseline_end)
    an_mask = (df[date_col] >= anomaly_start) & (df[date_col] <= anomaly_end)

    bl_days = (pd.Timestamp(baseline_end) - pd.Timestamp(baseline_start)).days + 1
    an_days = (pd.Timestamp(anomaly_end) - pd.Timestamp(anomaly_start)).days + 1

    bl_df = df.loc[bl_mask]
    an_df = df.loc[an_mask]

    if agg == "sum":
        bl_grouped = bl_df.groupby(dimension)[metric_col].sum() / bl_days
        an_grouped = an_df.groupby(dimension)[metric_col].sum() / an_days
    elif agg == "mean":
        bl_grouped = bl_df.groupby(dimension)[metric_col].mean()
        an_grouped = an_df.groupby(dimension)[metric_col].mean()
    else:
        bl_grouped = bl_df.groupby(dimension)[metric_col].count() / bl_days
        an_grouped = an_df.groupby(dimension)[metric_col].count() / an_days

    # Sample sizes
    bl_counts = bl_df.groupby(dimension)[metric_col].count()
    an_counts = an_df.groupby(dimension)[metric_col].count()

    # Combine
    all_slices = sorted(set(bl_grouped.index) | set(an_grouped.index))
    rows = []
    for s in all_slices:
        bl_val = float(bl_grouped.get(s, 0))
        an_val = float(an_grouped.get(s, 0))
        abs_change = an_val - bl_val
        pct_change = (abs_change / bl_val * 100) if bl_val != 0 else 0.0

        rows.append({
            "dimension_value": s,
            "baseline_daily": round(bl_val, 2),
            "anomaly_daily": round(an_val, 2),
            "absolute_change": round(abs_change, 2),
            "percent_change": round(pct_change, 2),
            "sample_size_baseline": int(bl_counts.get(s, 0)),
            "sample_size_anomaly": int(an_counts.get(s, 0)),
        })

    result = pd.DataFrame(rows)

    # Compute contribution percentage (how much of total change each slice explains)
    total_change = result["absolute_change"].sum()
    if total_change != 0:
        result["contribution_pct"] = round(result["absolute_change"] / total_change * 100, 2)
    else:
        result["contribution_pct"] = 0.0

    return result.sort_values("absolute_change", key=abs, ascending=False).reset_index(drop=True)


def concentration_check(slice_df: pd.DataFrame, top_n: int = 1) -> dict[str, Any]:
    """Check if KPI movement is concentrated in a few slices.

    Args:
        slice_df: Output of dimension_slice_analysis.
        top_n: How many top slices to consider.

    Returns:
        Dict with concentration metrics.
    """
    if slice_df.empty:
        return {"concentrated": False, "top_n_contribution": 0.0, "detail": "No data"}

    sorted_df = slice_df.sort_values("contribution_pct", key=abs, ascending=False)
    top_contrib = sorted_df.head(top_n)["contribution_pct"].abs().sum()
    total_slices = len(sorted_df)

    return {
        "concentrated": top_contrib > 50.0,
        "top_n_contribution_pct": round(top_contrib, 2),
        "total_slices": total_slices,
        "top_slices": sorted_df.head(top_n)["dimension_value"].tolist(),
        "detail": (
            f"Top {top_n} slice(s) explain {top_contrib:.1f}% of total change"
            if top_contrib > 0 else "No significant change detected"
        ),
    }


def small_sample_flag(slice_df: pd.DataFrame, min_count: int = 30) -> list[str]:
    """Flag dimension slices with small sample sizes."""
    flags = []
    for _, row in slice_df.iterrows():
        if row.get("sample_size_anomaly", 0) < min_count or row.get("sample_size_baseline", 0) < min_count:
            flags.append(
                f"Small sample: {row['dimension_value']} "
                f"(baseline={row.get('sample_size_baseline', 0)}, "
                f"anomaly={row.get('sample_size_anomaly', 0)})"
            )
    return flags
