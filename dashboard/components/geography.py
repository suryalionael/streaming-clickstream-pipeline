"""Geography dashboard panel."""

import pandas as pd
import plotly.express as px
import streamlit as st


def render_geography(
    geo_df: pd.DataFrame, source_df: pd.DataFrame, device_df: pd.DataFrame
) -> None:
    """Render geographic, traffic source, and device breakdowns."""
    st.markdown("## Geography & Traffic Sources")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Traffic by Country")
        if not geo_df.empty:
            fig = px.choropleth(
                geo_df,
                locations="country",
                locationmode="country names",
                color="total_visits",
                hover_name="country",
                hover_data={
                    "total_visits": ":,",
                    "total_users": ":,",
                    "total_revenue": ":,.2f",
                    "total_purchases": ":,",
                },
                color_continuous_scale="Viridis",
                title="Geographic Distribution",
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for geographic data...")

    with col2:
        st.markdown("### Traffic Sources")
        if not source_df.empty:
            fig = px.pie(
                source_df,
                values="total_visits",
                names="traffic_source",
                title="Traffic Source Distribution",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for traffic source data...")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Device Breakdown")
        if not device_df.empty:
            fig = px.bar(
                device_df,
                x="device",
                y="total_visits",
                title="Visits by Device",
                color="device",
                text_auto=".0f",
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for device data...")

    with col2:
        st.markdown("### Source Performance")
        if not source_df.empty:
            fig = px.bar(
                source_df,
                x="traffic_source",
                y="conversion_rate_pct",
                title="Conversion Rate by Source",
                color="traffic_source",
                text_auto=".1f",
            )
            fig.update_layout(height=350, yaxis_title="Conversion Rate (%)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for source performance data...")
