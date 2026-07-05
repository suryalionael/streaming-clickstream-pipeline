"""Dashboard configuration."""

import os
from dataclasses import dataclass


@dataclass
class DashboardConfig:
    refresh_interval: int = int(os.getenv("DASHBOARD_REFRESH_INTERVAL", "5"))
    duckdb_path: str = os.getenv("DUCKDB_PATH", "/opt/dashboard/data/metrics.duckdb")
    page_title: str = "Clickstream Analytics Pipeline"
    page_icon: str = "📊"


config = DashboardConfig()
