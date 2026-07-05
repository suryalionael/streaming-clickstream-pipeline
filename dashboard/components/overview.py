"""Overview dashboard panel with KPI cards and sparklines."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def _sparkline_fig(trace_vals: list[float], color: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            y=trace_vals,
            mode="lines",
            line=dict(color=color, width=1.5),
            fill="tozeroy",
            fillcolor=f"rgba{_hex_to_rgba(color, 0.1)}",
            showlegend=False,
        )
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=50,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(visible=False, showticklabels=False),
        yaxis=dict(visible=False, showticklabels=False),
        hovermode=False,
    )
    return fig


def _hex_to_rgba(hex_color: str, alpha: float) -> tuple[int, int, int, float]:
    h = hex_color.lstrip("#")
    r: int = int(h[0:2], 16)
    g: int = int(h[2:4], 16)
    b: int = int(h[4:6], 16)
    return (r, g, b, alpha)


def _trend_arrow(current: float, previous: float, inverse_good: bool = False) -> str:
    if previous == 0:
        return "→"
    change = (current - previous) / previous
    if abs(change) < 0.01:
        return "→"
    is_good = change > 0
    if inverse_good:
        is_good = not is_good
    return "↑" if is_good else "↓"


def _compute_trend(metrics: dict[str, Any], key: str) -> tuple[float, str]:
    current = metrics.get(key, 0)
    previous = metrics.get(f"{key}_prev", 0)
    arrow = _trend_arrow(float(current), float(previous))
    return float(current), arrow


def render_overview(
    metrics: dict[str, Any],
    timeseries_df: pd.DataFrame | None = None,
) -> None:
    """Render the overview panel with KPI cards and sparklines."""
    st.subheader("Key Metrics")

    kpi_cols = st.columns(4)

    trace_data: list[float] = []
    if timeseries_df is not None and not timeseries_df.empty:
        trace_data = timeseries_df["events_per_second"].tolist()[-30:]

    kpis = [
        ("Events/sec", metrics.get("events_per_second", 0), "", "{:.1f}", "#1f77b4"),
        ("Active Sessions", metrics.get("active_sessions", 0), "", "{:,}", "#ff7f0e"),
        ("Active Users", metrics.get("active_users", 0), "", "{:,}", "#2ca02c"),
        ("Today Revenue", metrics.get("today_revenue", 0), "$", "${:,.2f}", "#d62728"),
    ]

    for col, (label, value, prefix, fmt, color) in zip(kpi_cols, kpis, strict=False):
        with col:
            curr_val = float(value)
            prev_val = float(
                metrics.get(f"{label.lower().replace('/', '_').replace(' ', '_')}_prev", 0)
            )
            arrow = _trend_arrow(curr_val, prev_val)

            st.metric(
                label=label,
                value=f"{prefix}{fmt.format(value)}",
                delta=f"{arrow} {abs(curr_val - prev_val):.1f}" if prev_val > 0 else None,
            )
            if trace_data:
                st.plotly_chart(
                    _sparkline_fig(trace_data, color),
                    use_container_width=True,
                    key=f"sparkline_{label.lower().replace(' ', '_')}",
                )

    kpi_cols2 = st.columns(3)
    kpis2 = [
        ("Conversion Rate", metrics.get("today_conversion_rate", 0), "{:.2f}%", "#9467bd"),
        ("Total Events", metrics.get("total_events", 0), "{:,}", "#8c564b"),
        ("Purchases Today", metrics.get("total_purchases", 0), "{:,}", "#e377c2"),
    ]

    for col, (label, value, fmt, color) in zip(kpi_cols2, kpis2, strict=False):
        with col:
            curr_val = float(value)
            prev_val = float(metrics.get(f"{label.lower().replace(' ', '_')}_prev", 0))
            arrow = _trend_arrow(curr_val, prev_val)

            st.metric(
                label=label,
                value=fmt.format(value),
                delta=f"{arrow} {abs(curr_val - prev_val):.1f}" if prev_val > 0 else None,
            )
            if trace_data:
                st.plotly_chart(
                    _sparkline_fig(trace_data, color),
                    use_container_width=True,
                    key=f"sparkline_{label.lower().replace(' ', '_')}",
                )


def render_pipeline_status(healthy: bool = True) -> None:
    """Render pipeline status indicator with refresh timestamp."""
    status_color = "🟢" if healthy else "🔴"
    status_text = "All Systems Operational" if healthy else "Degraded Performance"
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    st.info(
        f"**Pipeline Status:** {status_color} {status_text} &nbsp;|&nbsp; "
        f"**Last Updated:** {now}"
    )


def render_empty_state() -> None:
    """Render an empty state message when no data is available."""
    st.info(
        "⏳ No data available yet. The pipeline is still initializing or "
        "no events have been produced. Check back shortly."
    )


def render_loading_state() -> None:
    """Render a loading placeholder while data is being fetched."""
    with st.spinner("Fetching latest metrics..."):
        st.markdown("_Loading..._")
