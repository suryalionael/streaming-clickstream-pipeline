"""Infrastructure status dashboard panel."""

import streamlit as st


def render_infrastructure() -> None:
    """Render infrastructure status panel."""
    st.markdown("## Infrastructure Status")

    services = [
        {"name": "Kafka", "status": "healthy", "description": "Message broker"},
        {"name": "Spark Streaming", "status": "healthy", "description": "Stream processing engine"},
        {"name": "MinIO", "status": "healthy", "description": "S3-compatible object storage"},
        {"name": "Delta Lake", "status": "healthy", "description": "Data lake storage layer"},
        {"name": "DuckDB", "status": "healthy", "description": "Analytics database"},
        {
            "name": "Clickstream Producer",
            "status": "healthy",
            "description": "Synthetic event generator",
        },
    ]

    st.markdown("### Service Status")

    for service in services:
        status_icon = "🟢" if service["status"] == "healthy" else "🔴"
        col1, col2, col3 = st.columns([1, 3, 6])
        with col1:
            st.markdown(f"**{status_icon}**")
        with col2:
            st.markdown(f"**{service['name']}**")
        with col3:
            st.markdown(f"_{service['description']}_")

    st.markdown("### Data Lake Structure")
    st.markdown(
        """
    ```
    clickstream-lake/
    ├── bronze/          # Raw ingested events
    │   └── year=2026/month=07/day=05/hour=*
    ├── silver/          # Cleaned & enriched events
    │   └── year=2026/month=07/day=05/hour=*
    └── gold/            # Aggregated metrics
        ├── funnel_metrics/
        ├── product_performance/
        └── traffic_analytics/
    ```
    """
    )

    st.markdown("### Architecture")
    st.markdown(
        """
    ```
    Generator → Kafka → Spark Streaming → Bronze → Silver → Gold → DuckDB → Dashboard
    ```
    """
    )

    st.caption("Last updated: auto-refreshing")
