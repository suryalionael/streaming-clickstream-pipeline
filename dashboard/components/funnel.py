"""Conversion funnel dashboard panel."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_funnel(funnel_df: pd.DataFrame) -> None:
    """Render conversion funnel visualization."""
    st.markdown("## Conversion Funnel")

    if funnel_df.empty:
        st.info("Waiting for funnel data...")
        return

    latest = funnel_df.iloc[0]

    col1, col2 = st.columns(2)

    with col1:
        # Funnel chart
        stages = [
            "Page Views",
            "Product Views",
            "Add to Cart",
            "Checkout",
            "Purchases",
        ]
        values = [
            int(latest.get("page_views", 0)),
            int(latest.get("product_views", 0)),
            int(latest.get("add_to_carts", 0)),
            int(latest.get("checkout_starts", 0)),
            int(latest.get("purchases", 0)),
        ]

        fig = go.Figure(
            go.Funnel(
                y=stages,
                x=values,
                textposition="inside",
                textinfo="value+percent initial",
                marker=dict(color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]),
            )
        )
        fig.update_layout(
            title="Conversion Funnel (Latest Window)",
            height=450,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Key metrics
        conversion_rate = latest.get("conversion_rate", 0) * 100
        abandonment_rate = latest.get("abandonment_rate", 0) * 100
        avg_cart = latest.get("avg_cart_value", 0)
        revenue = latest.get("total_revenue", 0)

        metrics = {
            "Conversion Rate": f"{conversion_rate:.2f}%",
            "Abandonment Rate": f"{abandonment_rate:.2f}%",
            "Avg Cart Value": f"${avg_cart:.2f}",
            "Revenue (Window)": f"${revenue:.2f}",
        }

        for label, value in metrics.items():
            st.metric(label=label, value=value)

        # Mini gauge for conversion rate
        fig = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=conversion_rate,
                number={"suffix": "%", "font": {"size": 24}},
                gauge={
                    "axis": {"range": [0, 20], "tickwidth": 1},
                    "bar": {"color": "#2ca02c"},
                    "steps": [
                        {"range": [0, 5], "color": "#ffcccc"},
                        {"range": [5, 10], "color": "#ffffcc"},
                        {"range": [10, 20], "color": "#ccffcc"},
                    ],
                    "threshold": {
                        "line": {"color": "red", "width": 4},
                        "thickness": 0.75,
                        "value": conversion_rate,
                    },
                },
            )
        )
        fig.update_layout(height=200, margin=dict(l=30, r=30, t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)
