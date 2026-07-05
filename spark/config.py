"""Spark streaming pipeline configuration."""

import os
from dataclasses import dataclass


@dataclass
class SparkConfig:
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    app_name: str = "ClickstreamStreamingPipeline"
    batch_duration: int = int(os.getenv("SPARK_BATCH_DURATION", "10"))
    checkpoint_dir: str = os.getenv("SPARK_CHECKPOINT_DIR", "/opt/spark/checkpoints")
    watermark_delay: int = int(os.getenv("SPARK_WATERMARK_DELAY", "60"))

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC_CLICKSTREAM", "clickstream-events")
    kafka_dead_letter_topic: str = os.getenv("KAFKA_TOPIC_DEAD_LETTER", "clickstream-dead-letter")
    starting_offsets: str = os.getenv("SPARK_STARTING_OFFSETS", "latest")

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "clickstream-lake")

    bronze_path: str = os.getenv("DELTA_BRONZE_PATH", "/opt/spark/data/bronze")
    silver_path: str = os.getenv("DELTA_SILVER_PATH", "/opt/spark/data/silver")
    gold_path: str = os.getenv("DELTA_GOLD_PATH", "/opt/spark/data/gold")

    @property
    def spark_builder_config(self) -> dict[str, str]:
        return {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.streaming.schemaInference": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.streaming.checkpointLocation": self.checkpoint_dir,
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
            "spark.databricks.delta.vacuum.enabled": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": self.minio_endpoint,
            "spark.hadoop.fs.s3a.access.key": self.minio_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.minio_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl.disable.cache": "true",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            ),
        }


config = SparkConfig()
