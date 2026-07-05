"""Product analytics dashboard panel."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def render_products(products_df: pd.DataFrame, categories_df: pd.DataFrame) -> None:
    """Render product and category performance charts."""
    st.markdown("## Product & Category Performance")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Top Products by Revenue")
        if not products_df.empty:
            top = products_df.head(10)
            fig = px.bar(
                top,
                x="revenue",
                y="product_id",
                orientation="h",
                title="Top 10 Products by Revenue",
                color="revenue",
                color_continuous_scale="Greens",
                hover_data={
                    "product_id": True,
                    "category": True,
                    "views": ":,",
                    "purchases": ":,",
                    "revenue": ":,.2f",
                },
            )
            fig.update_layout(
                height=500,
                yaxis={"categoryorder": "total ascending"},
                xaxis_title="Revenue ($)",
                yaxis_title="",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for product data...")

    with col2:
        st.markdown("### Product Conversion Rates")
        if not products_df.empty:
            top_conv = products_df.nlargest(10, "conversion_rate")
            fig = px.bar(
                top_conv,
                x="conversion_rate",
                y="product_id",
                orientation="h",
                title="Top 10 Products by Conversion Rate",
                color="conversion_rate",
                color_continuous_scale="Blues",
                hover_data={
                    "product_id": True,
                    "views": ":,",
                    "purchases": ":,",
                    "conversion_rate": ":,.2%",
                },
            )
            fig.update_layout(
                height=500,
                yaxis={"categoryorder": "total ascending"},
                xaxis_title="Conversion Rate",
                yaxis_title="",
                xaxis_tickformat=".0%",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for conversion data...")

    st.markdown("### Category Performance")
    if not categories_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig = px.pie(
                categories_df,
                values="total_revenue",
                names="category",
                title="Revenue by Category",
                hole=0.3,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=categories_df["category"],
                    y=categories_df["total_views"],
                    name="Views",
                    marker_color="#1f77b4",
                )
            )
            fig.add_trace(
                go.Bar(
                    x=categories_df["category"],
                    y=categories_df["total_purchases"],
                    name="Purchases",
                    marker_color="#2ca02c",
                )
            )
            fig.update_layout(
                title="Views vs Purchases by Category",
                xaxis_title="Category",
                yaxis_title="Count",
                barmode="group",
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for category data...")
