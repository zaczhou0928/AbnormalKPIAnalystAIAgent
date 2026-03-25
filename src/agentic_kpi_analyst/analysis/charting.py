"""Chart generation for investigation reports using plotly."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from agentic_kpi_analyst.logging_utils import get_logger
from agentic_kpi_analyst.models import AnalysisResult

logger = get_logger(__name__)


def create_contribution_chart(
    analysis: AnalysisResult,
    output_dir: str | Path,
    chart_id: str = "",
) -> str:
    """Create a horizontal bar chart showing dimension contributions.

    Returns path to the saved chart image (HTML).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not analysis.top_drivers:
        return ""

    drivers = analysis.top_drivers[:10]  # top 10
    labels = [d.slice_value for d in drivers]
    values = [d.contribution_pct for d in drivers]
    colors = ["#e74c3c" if v < 0 else "#2ecc71" for v in values]

    fig = go.Figure(go.Bar(
        x=values,
        y=labels,
        orientation="h",
        marker_color=colors,
        text=[f"{v:+.1f}%" for v in values],
        textposition="outside",
    ))

    dim_name = drivers[0].dimension if drivers else "dimension"
    fig.update_layout(
        title=f"Contribution by {dim_name}",
        xaxis_title="Contribution to Total Change (%)",
        yaxis_title=dim_name,
        height=max(300, len(labels) * 40 + 100),
        margin=dict(l=150),
        yaxis=dict(autorange="reversed"),
    )

    filename = f"contribution_{chart_id or dim_name}.html"
    path = output_dir / filename
    fig.write_html(str(path), include_plotlyjs="cdn")
    logger.debug("chart_saved", path=str(path))
    return str(path)


def create_timeseries_chart(
    df: pd.DataFrame,
    date_col: str,
    metric_col: str,
    baseline_start: str,
    baseline_end: str,
    anomaly_start: str,
    anomaly_end: str,
    output_dir: str | Path,
    chart_id: str = "",
    group_col: str | None = None,
) -> str:
    """Create a time-series chart highlighting baseline vs anomaly windows.

    Returns path to saved chart.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    fig = go.Figure()

    if group_col and group_col in df.columns:
        for group_val in df[group_col].unique():
            sub = df[df[group_col] == group_val].sort_values(date_col)
            fig.add_trace(go.Scatter(
                x=sub[date_col],
                y=sub[metric_col],
                mode="lines+markers",
                name=str(group_val),
                marker=dict(size=4),
            ))
    else:
        daily = df.groupby(date_col)[metric_col].sum().reset_index().sort_values(date_col)
        fig.add_trace(go.Scatter(
            x=daily[date_col],
            y=daily[metric_col],
            mode="lines+markers",
            name=metric_col,
            marker=dict(size=4),
        ))

    # Add shaded regions
    fig.add_vrect(
        x0=baseline_start, x1=baseline_end,
        fillcolor="blue", opacity=0.08,
        annotation_text="Baseline", annotation_position="top left",
    )
    fig.add_vrect(
        x0=anomaly_start, x1=anomaly_end,
        fillcolor="red", opacity=0.08,
        annotation_text="Anomaly", annotation_position="top left",
    )

    fig.update_layout(
        title=f"{metric_col} Over Time",
        xaxis_title="Date",
        yaxis_title=metric_col,
        height=400,
    )

    filename = f"timeseries_{chart_id or metric_col}.html"
    path = output_dir / filename
    fig.write_html(str(path), include_plotlyjs="cdn")
    logger.debug("chart_saved", path=str(path))
    return str(path)
