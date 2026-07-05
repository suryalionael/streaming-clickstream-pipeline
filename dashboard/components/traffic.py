"""Traffic analysis dashboard panel."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_traffic(timeseries_df: pd.DataFrame) -> None:
    """Render traffic time series charts."""
    st.markdown("## Traffic Analysis")

    if timeseries_df.empty:
        st.info("Waiting for traffic data...")
        return

    col1, col2 = st.columns(2)

    with col1:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=timeseries_df["window_start"],
                y=timeseries_df["page_views"],
                mode="lines+markers",
                name="Page Views",
                line=dict(color="#1f77b4", width=2),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=timeseries_df["window_start"],
                y=timeseries_df["add_to_carts"],
                mode="lines+markers",
                name="Add to Cart",
                line=dict(color="#ff7f0e", width=2),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=timeseries_df["window_start"],
                y=timeseries_df["purchases"],
                mode="lines+markers",
                name="Purchases",
                line=dict(color="#2ca02c", width=2),
            )
        )
        fig.update_layout(
            title="Event Volume Over Time",
            xaxis_title="Time",
            yaxis_title="Count",
            height=400,
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=timeseries_df["window_start"],
                y=timeseries_df["events_per_second"],
                mode="lines",
                name="Events/sec",
                line=dict(color="#9467bd", width=2),
                fill="tozeroy",
            )
        )
        fig.update_layout(
            title="Events Per Second",
            xaxis_title="Time",
            yaxis_title="Events/s",
            height=400,
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)
