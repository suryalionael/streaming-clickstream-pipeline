"""Tests for the streaming pipeline entry points."""


class TestStreamingPipeline:
    def test_spark_config(self) -> None:
        from spark.config import SparkConfig

        config = SparkConfig()
        assert config.app_name == "ClickstreamStreamingPipeline"
        assert config.batch_duration >= 1
        assert config.watermark_delay_minutes >= 1
        assert "minutes" in config.watermark_delay

    def test_config_kafka_settings(self) -> None:
        from spark.config import SparkConfig

        config = SparkConfig()
        assert "bootstrap.servers" not in config.spark_builder_config
        assert config.kafka_bootstrap_servers is not None

    def test_config_has_minio_settings(self) -> None:
        from spark.config import SparkConfig

        config = SparkConfig()
        builder_config = config.spark_builder_config
        assert "spark.hadoop.fs.s3a.endpoint" in builder_config
        assert "spark.hadoop.fs.s3a.access.key" in builder_config
