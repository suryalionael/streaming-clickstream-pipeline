"""Overview dashboard panel."""

from typing import Any

import streamlit as st


def render_overview(metrics: dict[str, Any]) -> None:
    """Render the overview panel with KPI cards."""
    st.markdown("## Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Events/sec",
            value=metrics.get("events_per_second", 0),
            delta=metrics.get("events_per_second_delta"),
        )

    with col2:
        st.metric(
            label="Active Sessions",
            value=metrics.get("active_sessions", 0),
        )

    with col3:
        st.metric(
            label="Active Users",
            value=metrics.get("active_users", 0),
        )

    with col4:
        st.metric(
            label="Today Revenue",
            value=f"${metrics.get('today_revenue', 0):,.2f}",
        )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Conversion Rate",
            value=f"{metrics.get('today_conversion_rate', 0):.2f}%",
        )

    with col2:
        st.metric(
            label="Total Events (Today)",
            value=f"{metrics.get('total_events', 0):,}",
        )

    with col3:
        st.metric(
            label="Total Purchases (Today)",
            value=metrics.get("total_purchases", 0),
        )

    with col4:
        pass


def render_pipeline_status(healthy: bool = True) -> None:
    """Render pipeline status indicator."""
    status_color = "🟢" if healthy else "🔴"
    status_text = "All Systems Operational" if healthy else "Degraded Performance"
    st.markdown(f"### Pipeline Status: {status_color} {status_text}")
