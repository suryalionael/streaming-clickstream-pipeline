"""Storage layer configuration."""

import os
from dataclasses import dataclass


@dataclass
class StorageConfig:
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "clickstream-lake")
    delta_bronze_path: str = os.getenv("DELTA_BRONZE_PATH", "/opt/spark/data/bronze")
    delta_silver_path: str = os.getenv("DELTA_SILVER_PATH", "/opt/spark/data/silver")
    delta_gold_path: str = os.getenv("DELTA_GOLD_PATH", "/opt/spark/data/gold")
    duckdb_path: str = os.getenv("DUCKDB_PATH", "/opt/dashboard/data/metrics.duckdb")
    dashboard_refresh_interval: int = int(os.getenv("DASHBOARD_REFRESH_INTERVAL", "5"))


config = StorageConfig()
