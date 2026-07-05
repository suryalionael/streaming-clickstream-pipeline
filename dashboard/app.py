"""Streamlit dashboard main application for clickstream analytics."""

import logging
import time

import streamlit as st

from dashboard.components.funnel import render_funnel
from dashboard.components.geography import render_geography
from dashboard.components.infrastructure import render_infrastructure
from dashboard.components.overview import (
    render_empty_state,
    render_overview,
    render_pipeline_status,
)
from dashboard.components.products import render_products
from dashboard.components.traffic import render_traffic
from dashboard.config import config
from storage.queries import MetricsStore

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title=config.page_title,
    page_icon=config.page_icon,
    layout="wide",
    initial_sidebar_state="expanded",
)


def initialize_store() -> MetricsStore:
    store = MetricsStore()
    try:
        store.connect()
    except Exception:
        logger.exception("Failed to connect to metrics store")
        st.error(
            "Could not connect to the metrics database. "
            "Ensure the pipeline is running and Delta tables exist."
        )
        return None  # type: ignore
    return store


def render_sidebar() -> str:
    with st.sidebar:
        st.title("📊 Clickstream Analytics")
        st.markdown("---")

        page = st.radio(
            "Navigation",
            ["Overview", "Traffic", "Funnel", "Geography", "Products", "Infrastructure"],
            label_visibility="collapsed",
        )

        st.markdown("---")
        st.markdown("### System Info")
        st.markdown(
            f"- **Refresh:** Every {config.refresh_interval}s\n"
            f"- **Mode:** Real-time streaming\n"
            f"- **Engine:** Spark Structured Streaming"
        )

        st.markdown("---")
        st.caption("Built with Kafka · Spark · Delta Lake · DuckDB · Streamlit")

        if st.button("🔄 Refresh Now", use_container_width=True):
            st.rerun()

    return page


def _render_dashboard_page(page: str, store: MetricsStore) -> None:
    try:
        if page == "Overview":
            metrics = store.get_live_metrics()
            render_pipeline_status(healthy=True)
            ts_df = store.get_traffic_timeseries()

            if ts_df.empty:
                render_empty_state()
            else:
                render_overview(metrics, timeseries_df=ts_df)
                render_traffic(ts_df)

        elif page == "Traffic":
            ts_df = store.get_traffic_timeseries()
            render_traffic(ts_df)

        elif page == "Funnel":
            funnel_df = store.get_funnel_data()
            render_funnel(funnel_df)

        elif page == "Geography":
            geo_df = store.get_geography_data()
            source_df = store.get_traffic_sources()
            device_df = store.get_device_breakdown()
            render_geography(geo_df, source_df, device_df)

        elif page == "Products":
            products_df = store.get_top_products(20)
            categories_df = store.get_top_categories()
            render_products(products_df, categories_df)

        elif page == "Infrastructure":
            render_infrastructure()
    except Exception:
        logger.exception("Error rendering dashboard page")
        st.error("An unexpected error occurred. The pipeline may still be initializing.")


def main() -> None:
    store = initialize_store()
    if store is None:
        st.stop()

    page = render_sidebar()

    st.title(config.page_title)
    st.markdown("*Real-time e-commerce clickstream analytics*")
    st.divider()

    _render_dashboard_page(page, store)

    time.sleep(config.refresh_interval)
    st.rerun()


if __name__ == "__main__":
    main()
