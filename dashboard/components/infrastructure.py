"""Pipeline observability dashboard panel with live health metrics."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import streamlit as st

from storage.queries import MetricsStore

HEALTHY = "🟢"
WARNING = "🟡"
ERROR = "🔴"
UNKNOWN = "⚪"


def _health_indicator(value: float, warn_below: float, error_below: float = 0) -> str:
    if value <= error_below:
        return ERROR
    if value < warn_below:
        return WARNING
    return HEALTHY


def _recency_indicator(ts_str: str | None, max_minutes: int = 5) -> str:
    if ts_str is None:
        return ERROR
    try:
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%fZ",
        ):
            try:
                parsed = datetime.strptime(ts_str[:26], fmt)
                break
            except ValueError:
                continue
        else:
            return WARNING
        age = (datetime.now(UTC) - parsed.replace(tzinfo=UTC)).total_seconds()
        if age < max_minutes * 60:
            return HEALTHY
        if age < max_minutes * 60 * 3:
            return WARNING
        return ERROR
    except Exception:
        return UNKNOWN


def _fmt_ts(ts: str | None) -> str:
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            return ts
    return "—"


def _status_card(
    label: str,
    value: str | None,
    indicator: str,
    detail: str = "",
) -> None:
    col1, col2, col3 = st.columns([1, 2, 5])
    with col1:
        st.markdown(f"**{indicator}**")
    with col2:
        st.markdown(f"**{label}**")
    with col3:
        parts = [value or "—"]
        if detail:
            parts.append(f"<span style='color:#888;font-size:0.9em'>— {detail}</span>")
        st.markdown(" ".join(parts), unsafe_allow_html=True)


def _render_service_health(health: dict[str, Any]) -> None:
    st.markdown("### Service Connectivity")
    st.markdown("*HTTP health checks against pipeline services*")

    import urllib.request

    services: list[dict[str, Any]] = [
        {"name": "Kafka (via Kafka UI)", "url": "http://localhost:8080/actuator/health"},
        {"name": "MinIO (S3 Storage)", "url": "http://localhost:9000/minio/health/live"},
    ]

    for svc in services:
        try:
            resp = urllib.request.urlopen(svc["url"], timeout=5)
            indicator = HEALTHY if resp.getcode() < 400 else ERROR
        except Exception:
            indicator = ERROR
        _status_card(svc["name"], "reachable" if indicator == HEALTHY else "unreachable", indicator)

    _status_card(
        "DuckDB (Analytics)",
        "connected" if health.get("duckdb_connected", False) else "disconnected",
        HEALTHY if health.get("duckdb_connected", False) else ERROR,
    )


def _render_config_section() -> None:
    st.markdown("### Pipeline Configuration")
    st.markdown("*Current runtime settings*")

    from storage.config import config as storage_config

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown(
            f"- **Bronze path:** `{storage_config.delta_bronze_path}`\n"
            f"- **Silver path:** `{storage_config.delta_silver_path}`\n"
            f"- **Gold path:** `{storage_config.delta_gold_path}`\n"
        )
    with col_b:
        st.markdown(
            f"- **DuckDB path:** `{storage_config.duckdb_path}`\n"
            f"- **MinIO endpoint:** `{storage_config.minio_endpoint}`\n"
            f"- **Refresh interval:** {storage_config.dashboard_refresh_interval}s\n"
        )

    with st.expander("Architecture Reference", expanded=False):
        st.markdown(
            """
        ```
        Generator → Kafka → Spark Streaming → Bronze → Silver → Gold → DuckDB → Dashboard
        ```

        - **Batch duration**: 10 seconds
        - **Watermark delay**: 60 minutes
        - **Window interval**: 5 minutes
        - **Kafka partitions**: 6
        - **Dead-letter path**: configured per environment
        """
        )


def _render_footer() -> None:
    from storage.config import config as storage_config

    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    st.caption(f"Auto-refreshes every {storage_config.dashboard_refresh_interval}s · Last updated: {now}")


def render_infrastructure(store: MetricsStore | None = None) -> None:
    """Render the pipeline observability panel with live health data."""
    st.markdown("## Pipeline Health")
    st.markdown(
        "*Live operational metrics for the streaming pipeline*"
    )

    health: dict[str, Any] = {}
    if store is not None:
        health = store.get_pipeline_health()

    # ── Delta Lake Table Status ──
    st.markdown("### Delta Lake Tables")
    st.markdown("*Row counts and freshness by medallion layer*")

    bronze_ok = health.get("bronze_count", 0) > 0
    silver_ok = health.get("silver_count", 0) > 0
    gold_ok = health.get("gold_funnel_count", 0) > 0

    bronze_ind = HEALTHY if bronze_ok else WARNING
    silver_ind = HEALTHY if silver_ok else WARNING
    gold_ind = HEALTHY if gold_ok else WARNING

    _status_card(
        "Bronze",
        f"{health.get('bronze_count', 0):,} rows",
        bronze_ind,
        f"latest: {_fmt_ts(health.get('latest_bronze_ts'))}",
    )
    _status_card(
        "Silver",
        f"{health.get('silver_count', 0):,} rows",
        silver_ind,
        f"latest: {_fmt_ts(health.get('latest_silver_ts'))}",
    )
    _status_card(
        "Gold — Funnel",
        f"{health.get('gold_funnel_count', 0):,} rows",
        gold_ind,
        f"latest: {_fmt_ts(health.get('latest_gold_ts'))}",
    )
    _status_card(
        "Gold — Product",
        f"{health.get('gold_product_count', 0):,} rows",
        HEALTHY if health.get("gold_product_count", 0) > 0 else WARNING,
    )
    _status_card(
        "Gold — Traffic",
        f"{health.get('gold_traffic_count', 0):,} rows",
        HEALTHY if health.get("gold_traffic_count", 0) > 0 else WARNING,
    )
    _status_card(
        "Dead Letter",
        f"{health.get('dead_letter_count', 0):,} rows",
        HEALTHY,
        "invalid messages routed here",
    )

    # ── Streaming Metrics ──
    st.markdown("### Streaming Metrics")
    st.markdown("*Real-time processing performance*")

    eps = health.get("events_per_second", 0.0)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Events / Second", f"{eps:.1f}")
    with col2:
        total_events = (
            health.get("bronze_count", 0)
            + health.get("silver_count", 0)
            + health.get("gold_funnel_count", 0)
        )
        st.metric("Total Events in Lake", f"{total_events:,}")
    with col3:
        total_gold = (
            health.get("gold_funnel_count", 0)
            + health.get("gold_product_count", 0)
            + health.get("gold_traffic_count", 0)
        )
        st.metric("Gold Aggregate Rows", f"{total_gold:,}")

    _render_service_health(health)
    _render_config_section()
    _render_footer()
