"""Streamlit dashboard main application."""

import logging
import time

import streamlit as st

from dashboard.components.funnel import render_funnel
from dashboard.components.geography import render_geography
from dashboard.components.infrastructure import render_infrastructure
from dashboard.components.overview import render_overview, render_pipeline_status
from dashboard.components.products import render_products
from dashboard.components.traffic import render_traffic
from dashboard.config import config
from storage.queries import MetricsStore

logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title=config.page_title,
    page_icon=config.page_icon,
    layout="wide",
    initial_sidebar_state="expanded",
)


def initialize_store() -> MetricsStore:
    """Initialize and connect the metrics store."""
    store = MetricsStore()
    try:
        store.connect()
    except Exception:
        logger.exception("Failed to connect to metrics store")
        st.error("Could not connect to the metrics database. Make sure the pipeline is running.")
        store = None  # type: ignore
    return store


def render_sidebar() -> str:
    """Render sidebar with navigation."""
    st.sidebar.title("📊 Clickstream Analytics")
    st.sidebar.markdown("---")

    page = st.sidebar.radio(
        "Navigation",
        ["Overview", "Traffic", "Funnel", "Geography", "Products", "Infrastructure"],
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Pipeline Info")
    st.sidebar.markdown("**Pipeline:** Streaming Clickstream")
    st.sidebar.markdown("**Layer:** Real-time")
    st.sidebar.markdown("**Refresh:** Auto (5s)")

    st.sidebar.markdown("---")
    st.sidebar.markdown("Built with ❤️ using")
    st.sidebar.markdown("Kafka · Spark · Delta Lake · DuckDB · Streamlit")

    # Auto-refresh
    if st.sidebar.button("🔄 Refresh Now"):
        st.rerun()

    return page


def render_overview_page(store: MetricsStore) -> None:
    """Render the overview page."""
    metrics = store.get_live_metrics()
    render_pipeline_status(healthy=True)
    render_overview(metrics)

    # Traffic time series
    ts_df = store.get_traffic_timeseries()
    render_traffic(ts_df)


def render_traffic_page(store: MetricsStore) -> None:
    """Render the traffic analysis page."""
    ts_df = store.get_traffic_timeseries()
    render_traffic(ts_df)


def render_funnel_page(store: MetricsStore) -> None:
    """Render the funnel analysis page."""
    funnel_df = store.get_funnel_data()
    render_funnel(funnel_df)


def render_geography_page(store: MetricsStore) -> None:
    """Render the geography page."""
    geo_df = store.get_geography_data()
    source_df = store.get_traffic_sources()
    device_df = store.get_device_breakdown()
    render_geography(geo_df, source_df, device_df)


def render_products_page(store: MetricsStore) -> None:
    """Render the products page."""
    products_df = store.get_top_products(20)
    categories_df = store.get_top_categories()
    render_products(products_df, categories_df)


def render_infrastructure_page() -> None:
    """Render the infrastructure page."""
    render_infrastructure()


def main() -> None:
    """Main dashboard application."""
    store = initialize_store()

    if store is None:
        st.stop()

    page = render_sidebar()

    st.title(config.page_title)
    st.markdown("*Real-time e-commerce clickstream analytics pipeline*")
    st.markdown("---")

    try:
        if page == "Overview":
            render_overview_page(store)
        elif page == "Traffic":
            render_traffic_page(store)
        elif page == "Funnel":
            render_funnel_page(store)
        elif page == "Geography":
            render_geography_page(store)
        elif page == "Products":
            render_products_page(store)
        elif page == "Infrastructure":
            render_infrastructure_page()
    except Exception as e:
        logger.exception("Error rendering page")
        st.error(f"An error occurred: {e}")

    # Auto-refresh
    if config.refresh_interval > 0:
        time.sleep(config.refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
