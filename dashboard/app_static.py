"""Standalone Streamlit dashboard backed by static Gold-layer snapshots.

Reads the CSV snapshots produced by ``replay/export.py`` instead of a live
DuckDB/Delta Lake connection, so it runs with no Kafka, Spark, or Docker
dependency — suitable for deployment to Streamlit Community Cloud.
Install with ``requirements-static.txt`` and run:

    streamlit run dashboard/app_static.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard.components.funnel import render_funnel
from dashboard.components.geography import render_geography
from dashboard.components.overview import render_empty_state, render_overview
from dashboard.components.products import render_products
from dashboard.components.traffic import render_traffic

PAGE_TITLE = "Clickstream Analytics Pipeline (Static Demo)"
PAGE_ICON = "📊"
SNAPSHOT_DIR = Path(os.getenv("SNAPSHOT_DIR", "replay/snapshots"))

st.set_page_config(
    page_title=PAGE_TITLE,
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data
def _load_table(name: str) -> pd.DataFrame:
    csv_path = SNAPSHOT_DIR / f"{name}.csv"
    if not csv_path.exists():
        return pd.DataFrame()
    return pd.read_csv(csv_path)


def _funnel_ordered(funnel_df: pd.DataFrame) -> pd.DataFrame:
    if funnel_df.empty:
        return funnel_df
    return funnel_df.sort_values("window_start", ascending=False).head(100)


def _timeseries_ordered(funnel_df: pd.DataFrame) -> pd.DataFrame:
    if funnel_df.empty:
        return funnel_df
    return funnel_df.sort_values("window_start", ascending=True).head(200)


def _live_metrics(funnel_df: pd.DataFrame) -> dict[str, float | int]:
    empty = {
        "events_per_second": 0,
        "active_sessions": 0,
        "active_users": 0,
        "today_revenue": 0.0,
        "today_conversion_rate": 0.0,
        "total_events": 0,
        "total_purchases": 0,
    }
    if funnel_df.empty:
        return empty

    df = funnel_df.copy()
    df["window_start"] = pd.to_datetime(df["window_start"])
    latest_day = df["window_start"].max().date()
    today_df = df[df["window_start"].dt.date == latest_day]
    if today_df.empty:
        today_df = df

    return {
        "events_per_second": round(float(today_df["events_per_second"].mean()), 2),
        "active_sessions": int(today_df["sessions"].sum()),
        "active_users": int(today_df["unique_users"].sum()),
        "today_revenue": round(float(today_df["total_revenue"].sum()), 2),
        "today_conversion_rate": round(float(today_df["conversion_rate"].max()) * 100, 2),
        "total_events": int(
            (today_df["page_views"] + today_df["product_views"] + today_df["add_to_carts"]).sum()
        ),
        "total_purchases": int(today_df["purchases"].sum()),
    }


def _top_products(products_df: pd.DataFrame, limit: int = 20) -> pd.DataFrame:
    if products_df.empty:
        return products_df
    grouped = products_df.groupby(["product_id", "category"], as_index=False).agg(
        views=("views", "sum"),
        add_to_carts=("add_to_carts", "sum"),
        purchases=("purchases", "sum"),
        revenue=("revenue", "sum"),
    )
    grouped["conversion_rate"] = grouped["purchases"] / grouped["views"].replace(0, pd.NA)
    grouped["conversion_rate"] = grouped["conversion_rate"].fillna(0)
    return grouped.sort_values("revenue", ascending=False).head(limit)


def _top_categories(products_df: pd.DataFrame) -> pd.DataFrame:
    if products_df.empty:
        return products_df
    grouped = products_df.dropna(subset=["category"]).groupby("category", as_index=False).agg(
        total_views=("views", "sum"),
        total_carts=("add_to_carts", "sum"),
        total_purchases=("purchases", "sum"),
        total_revenue=("revenue", "sum"),
    )
    grouped["conversion_rate"] = grouped["total_purchases"] / grouped["total_views"].replace(
        0, pd.NA
    )
    grouped["conversion_rate"] = grouped["conversion_rate"].fillna(0)
    return grouped.sort_values("total_revenue", ascending=False)


def _geography(traffic_df: pd.DataFrame) -> pd.DataFrame:
    if traffic_df.empty:
        return traffic_df
    grouped = traffic_df.dropna(subset=["country"]).groupby("country", as_index=False).agg(
        total_visits=("visits", "sum"),
        total_users=("users", "sum"),
        total_purchases=("purchases", "sum"),
        total_revenue=("revenue", "sum"),
    )
    return grouped.sort_values("total_visits", ascending=False)


def _traffic_sources(traffic_df: pd.DataFrame) -> pd.DataFrame:
    if traffic_df.empty:
        return traffic_df
    grouped = (
        traffic_df.dropna(subset=["traffic_source"])
        .groupby("traffic_source", as_index=False)
        .agg(
            total_visits=("visits", "sum"),
            total_users=("users", "sum"),
            total_purchases=("purchases", "sum"),
            total_revenue=("revenue", "sum"),
        )
    )
    grouped["conversion_rate_pct"] = grouped["total_purchases"] / grouped["total_visits"].replace(
        0, pd.NA
    )
    grouped["conversion_rate_pct"] = (grouped["conversion_rate_pct"].fillna(0)) * 100
    return grouped.sort_values("total_visits", ascending=False)


def _device_breakdown(traffic_df: pd.DataFrame) -> pd.DataFrame:
    if traffic_df.empty:
        return traffic_df
    grouped = traffic_df.dropna(subset=["device"]).groupby("device", as_index=False).agg(
        total_visits=("visits", "sum"),
        total_users=("users", "sum"),
        total_purchases=("purchases", "sum"),
    )
    return grouped.sort_values("total_visits", ascending=False)


def render_sidebar() -> str:
    with st.sidebar:
        st.title("📊 Clickstream Analytics")
        st.caption("Static demo — no live pipeline required")
        st.markdown("---")

        page = st.radio(
            "Navigation",
            ["Overview", "Traffic", "Funnel", "Geography", "Products"],
            label_visibility="collapsed",
        )

        st.markdown("---")
        st.markdown("### About this demo")
        st.markdown(
            "This is a **static snapshot** of the Gold-layer analytics tables, "
            "exported from the full Kafka → Spark → Delta Lake pipeline by "
            "`replay/export.py`. It has no Kafka, Spark, or Docker dependency."
        )

    return page


def main() -> None:
    funnel_df = _load_table("funnel_metrics")
    products_df = _load_table("product_performance")
    traffic_df = _load_table("traffic_analytics")

    page = render_sidebar()

    st.title(PAGE_TITLE)
    st.markdown("*Real-time e-commerce clickstream analytics — static demo snapshot*")
    st.divider()

    if funnel_df.empty and products_df.empty and traffic_df.empty:
        render_empty_state()
        st.info(
            f"No snapshot found at `{SNAPSHOT_DIR}`. Run `python -m replay.export` "
            "to generate one."
        )
        return

    if page == "Overview":
        ts_df = _timeseries_ordered(funnel_df)
        render_overview(_live_metrics(funnel_df), timeseries_df=ts_df)
        render_traffic(ts_df)
    elif page == "Traffic":
        render_traffic(_timeseries_ordered(funnel_df))
    elif page == "Funnel":
        render_funnel(_funnel_ordered(funnel_df))
    elif page == "Geography":
        render_geography(
            _geography(traffic_df), _traffic_sources(traffic_df), _device_breakdown(traffic_df)
        )
    elif page == "Products":
        render_products(_top_products(products_df), _top_categories(products_df))


if __name__ == "__main__":
    main()
