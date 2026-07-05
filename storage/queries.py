"""DuckDB queries for dashboard data access."""

import logging
from typing import Any

import duckdb
import pandas as pd

from storage.config import config

logger = logging.getLogger(__name__)


class MetricsStore:
    """DuckDB-based metrics store for dashboard data."""

    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or config.duckdb_path
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._connected = False

    def connect(self) -> None:
        """Connect to DuckDB database."""
        try:
            self._connection = duckdb.connect(self.db_path)
            self._connected = True
            self._init_schema()
            logger.info(f"Connected to DuckDB at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise

    def _init_schema(self) -> None:
        """Initialize DuckDB schema with views over Delta Lake."""
        assert self._connection is not None

        # Install and load parquet extension for Delta Lake compatibility
        try:
            self._connection.execute("INSTALL parquet; LOAD parquet;")
        except Exception:
            pass

        # Create views over Delta Lake tables (read as Parquet)
        paths = {
            "bronze": config.delta_bronze_path,
            "silver": config.delta_silver_path,
            "funnel_metrics": f"{config.delta_gold_path}/funnel_metrics",
            "product_performance": f"{config.delta_gold_path}/product_performance",
            "traffic_analytics": f"{config.delta_gold_path}/traffic_analytics",
        }

        for name, path in paths.items():
            try:
                self._connection.execute(
                    f"""
                    CREATE OR REPLACE VIEW {name} AS
                    SELECT * FROM read_parquet('{path}/*.parquet', union_by_name=true)
                    """
                )
                logger.info(f"Created view: {name} -> {path}")
            except Exception as e:
                logger.warning(f"Could not create view {name}: {e}")
                # Create empty table as placeholder
                self._connection.execute(
                    f"""
                    CREATE OR REPLACE VIEW {name} AS
                    SELECT * FROM (VALUES (NULL)) AS t WHERE 1=0
                    """
                )

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        assert self._connection is not None, "Not connected"
        return self._connection

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        return self.connection.execute(sql).fetchdf()

    # --- Dashboard Queries ---

    def get_live_metrics(self) -> dict[str, Any]:
        """Get current live metrics for overview panel."""
        metrics: dict[str, Any] = {
            "events_per_second": 0,
            "active_sessions": 0,
            "active_users": 0,
            "today_revenue": 0.0,
            "today_conversion_rate": 0.0,
            "total_events": 0,
            "total_purchases": 0,
        }
        try:
            df = self.query(
                """
                SELECT
                    COALESCE(SUM(page_views + product_views + add_to_carts), 0) AS total_events,
                    COALESCE(SUM(purchases), 0) AS total_purchases,
                    COALESCE(SUM(unique_users), 0) AS active_users,
                    COALESCE(SUM(sessions), 0) AS active_sessions,
                    COALESCE(SUM(total_revenue), 0) AS today_revenue,
                    COALESCE(AVG(events_per_second), 0) AS avg_eps,
                    COALESCE(MAX(conversion_rate), 0) AS conversion_rate
                FROM funnel_metrics
                WHERE year = EXTRACT(year FROM CURRENT_DATE)
                  AND month = EXTRACT(month FROM CURRENT_DATE)
                  AND day = EXTRACT(day FROM CURRENT_DATE)
            """
            )
            if not df.empty:
                row = df.iloc[0]
                metrics = {
                    "events_per_second": round(float(row["avg_eps"]), 2),
                    "active_sessions": int(row["active_sessions"]),
                    "active_users": int(row["active_users"]),
                    "today_revenue": round(float(row["today_revenue"]), 2),
                    "today_conversion_rate": round(float(row["conversion_rate"]) * 100, 2),
                    "total_events": int(row["total_events"]),
                    "total_purchases": int(row["total_purchases"]),
                }
        except Exception as e:
            logger.warning(f"Failed to get live metrics: {e}")
        return metrics

    def get_funnel_data(self) -> pd.DataFrame:
        """Get funnel metrics for visualization."""
        try:
            return self.query(
                """
                SELECT
                    window_start,
                    window_end,
                    page_views,
                    product_views,
                    add_to_carts,
                    checkout_starts,
                    purchases,
                    conversion_rate,
                    abandonment_rate,
                    avg_cart_value,
                    total_revenue
                FROM funnel_metrics
                ORDER BY window_start DESC
                LIMIT 100
            """
            )
        except Exception:
            return pd.DataFrame()

    def get_traffic_timeseries(self) -> pd.DataFrame:
        """Get time series data for traffic graphs."""
        try:
            return self.query(
                """
                SELECT
                    window_start,
                    page_views,
                    add_to_carts,
                    purchases,
                    events_per_second
                FROM funnel_metrics
                ORDER BY window_start ASC
                LIMIT 200
            """
            )
        except Exception:
            return pd.DataFrame()

    def get_top_products(self, limit: int = 10) -> pd.DataFrame:
        """Get top performing products."""
        try:
            return self.query(
                f"""
                SELECT
                    product_id,
                    category,
                    views,
                    add_to_carts,
                    purchases,
                    revenue,
                    conversion_rate
                FROM product_performance
                ORDER BY revenue DESC
                LIMIT {limit}
            """
            )
        except Exception:
            return pd.DataFrame()

    def get_top_categories(self) -> pd.DataFrame:
        """Get category performance."""
        try:
            return self.query(
                """
                SELECT
                    category,
                    SUM(views) AS total_views,
                    SUM(add_to_carts) AS total_carts,
                    SUM(purchases) AS total_purchases,
                    SUM(revenue) AS total_revenue,
                    CASE
                        WHEN SUM(views) > 0
                        THEN CAST(SUM(purchases) AS FLOAT) / SUM(views)
                        ELSE 0
                    END AS conversion_rate
                FROM product_performance
                WHERE category IS NOT NULL
                GROUP BY category
                ORDER BY total_revenue DESC
            """
            )
        except Exception:
            return pd.DataFrame()

    def get_geography_data(self) -> pd.DataFrame:
        """Get geographic distribution of traffic."""
        try:
            return self.query(
                """
                SELECT
                    country,
                    SUM(visits) AS total_visits,
                    SUM(users) AS total_users,
                    SUM(purchases) AS total_purchases,
                    SUM(revenue) AS total_revenue
                FROM traffic_analytics
                WHERE country IS NOT NULL
                GROUP BY country
                ORDER BY total_visits DESC
            """
            )
        except Exception:
            return pd.DataFrame()

    def get_traffic_sources(self) -> pd.DataFrame:
        """Get traffic source breakdown."""
        try:
            return self.query(
                """
                SELECT
                    traffic_source,
                    SUM(visits) AS total_visits,
                    SUM(users) AS total_users,
                    SUM(purchases) AS total_purchases,
                    SUM(revenue) AS total_revenue,
                    CASE
                        WHEN SUM(visits) > 0
                        THEN CAST(SUM(purchases) AS FLOAT) / SUM(visits) * 100
                        ELSE 0
                    END AS conversion_rate_pct
                FROM traffic_analytics
                WHERE traffic_source IS NOT NULL
                GROUP BY traffic_source
                ORDER BY total_visits DESC
            """
            )
        except Exception:
            return pd.DataFrame()

    def get_device_breakdown(self) -> pd.DataFrame:
        """Get device type breakdown."""
        try:
            return self.query(
                """
                SELECT
                    device,
                    SUM(visits) AS total_visits,
                    SUM(users) AS total_users,
                    SUM(purchases) AS total_purchases
                FROM traffic_analytics
                WHERE device IS NOT NULL
                GROUP BY device
                ORDER BY total_visits DESC
            """
            )
        except Exception:
            return pd.DataFrame()

    # --- Pipeline Health Queries ---

    def get_pipeline_health(self) -> dict[str, Any]:
        """Return live operational health metrics for the pipeline dashboard."""
        health: dict[str, Any] = {
            "bronze_count": 0,
            "silver_count": 0,
            "gold_funnel_count": 0,
            "gold_product_count": 0,
            "gold_traffic_count": 0,
            "dead_letter_count": 0,
            "events_per_second": 0.0,
            "latest_bronze_ts": None,
            "latest_silver_ts": None,
            "latest_gold_ts": None,
            "duckdb_connected": False,
        }
        try:
            health["duckdb_connected"] = self._connected

            health["bronze_count"] = self._table_row_count("bronze")
            health["silver_count"] = self._table_row_count("silver")
            health["gold_funnel_count"] = self._table_row_count("funnel_metrics")
            health["gold_product_count"] = self._table_row_count("product_performance")
            health["gold_traffic_count"] = self._table_row_count("traffic_analytics")

            health["latest_bronze_ts"] = self._latest_timestamp("bronze", "event_time")
            health["latest_silver_ts"] = self._latest_timestamp("silver", "event_time")

            eps_df = self.query(
                """
                SELECT COALESCE(MAX(events_per_second), 0) AS eps
                FROM funnel_metrics
                """
            )
            if not eps_df.empty:
                health["events_per_second"] = round(float(eps_df.iloc[0]["eps"]), 2)

            ts_df = self.query(
                """
                SELECT COALESCE(MAX(window_end), '') AS latest
                FROM funnel_metrics
                """
            )
            if not ts_df.empty:
                health["latest_gold_ts"] = str(ts_df.iloc[0]["latest"])
        except Exception as e:
            logger.warning(f"Failed to collect pipeline health: {e}")
        return health

    def _table_row_count(self, view_name: str) -> int:
        try:
            df = self.query(f"SELECT COUNT(*) AS cnt FROM {view_name}")
            if not df.empty:
                return int(df.iloc[0]["cnt"])
        except Exception:
            pass
        return 0

    def _latest_timestamp(self, view_name: str, col: str) -> str | None:
        try:
            df = self.query(f"SELECT MAX({col}) AS ts FROM {view_name}")
            if not df.empty and df.iloc[0]["ts"] is not None:
                return str(df.iloc[0]["ts"])
        except Exception:
            pass
        return None

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._connection:
            self._connection.close()
            self._connected = False
            logger.info("DuckDB connection closed")
