"""Spark Structured Streaming pipeline for clickstream processing."""

import logging
import sys

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.config import config
from spark.schemas import clickstream_schema
from spark.transforms import (
    add_date_columns,
    add_event_flags,
    add_ingestion_metadata,
    clean_and_enrich,
    compute_funnel_metrics,
    compute_product_performance,
    compute_traffic_analytics,
    parse_event_time,
    validate_event,
)

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create and configure Spark session."""
    builder = (
        SparkSession.builder.appName(config.app_name)
        .master(config.master)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "io.delta:delta-spark_2.12:3.2.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
    )

    for k, v in config.spark_builder_config.items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "Spark session created", extra={"master": config.master, "app_name": config.app_name}
    )
    return spark


def read_from_kafka(spark: SparkSession) -> DataFrame:
    """Read streaming data from Kafka."""
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", config.starting_offsets)
        .option("maxOffsetsPerTrigger", 10000)
        .option("failOnDataLoss", "false")
        .option("kafka.session.timeout.ms", "60000")
        .option("kafka.heartbeat.interval.ms", "10000")
        .option("kafka.auto.offset.reset", "latest")
        .load()
    )
    return df


def parse_kafka_messages(df: DataFrame) -> DataFrame:
    """Parse JSON messages from Kafka and apply schema."""
    parsed = (
        df.select(
            F.col("key").cast("string").alias("message_key"),
            F.from_json(F.col("value").cast("string"), clickstream_schema).alias("event"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("event.*")
        .filter(F.col("event_id").isNotNull())
    )
    return parsed


def write_to_bronze(df: DataFrame, epoch_id: int) -> None:
    """Write raw events to Delta Lake bronze layer."""
    batch_df = add_ingestion_metadata(df)
    batch_df = validate_event(batch_df)[0]
    batch_df.write.format("delta").mode("append").partitionBy(
        "year", "month", "day", "hour"
    ).option("mergeSchema", "true").save(config.bronze_path)
    logger.info("Bronze write complete", extra={"epoch_id": epoch_id, "records": df.count()})


def write_to_silver(df: DataFrame, epoch_id: int) -> None:
    """Clean, enrich, and write to Delta Lake silver layer."""
    batch_df = parse_event_time(df)
    batch_df = clean_and_enrich(batch_df)
    batch_df = add_date_columns(batch_df)
    batch_df = add_event_flags(batch_df)
    batch_df = batch_df.withColumn("ingestion_timestamp", F.current_timestamp().cast("string"))
    batch_df.write.format("delta").mode("append").partitionBy(
        "year", "month", "day", "hour"
    ).option("mergeSchema", "true").save(config.silver_path)
    logger.info("Silver write complete", extra={"epoch_id": epoch_id, "records": df.count()})


def write_to_gold(df: DataFrame, epoch_id: int) -> None:
    """Compute aggregates and write to Gold layer."""
    batch_df = parse_event_time(df)
    batch_df = add_date_columns(batch_df)
    batch_df = add_event_flags(batch_df)
    batch_df = clean_and_enrich(batch_df)

    funnel_df = compute_funnel_metrics(batch_df)
    funnel_df.write.format("delta").mode("append").partitionBy(
        "year", "month", "day", "hour"
    ).option("mergeSchema", "true").save(f"{config.gold_path}/funnel_metrics")

    product_df = compute_product_performance(batch_df)
    product_df.write.format("delta").mode("append").partitionBy(
        "year", "month", "day", "hour"
    ).option("mergeSchema", "true").save(f"{config.gold_path}/product_performance")

    traffic_df = compute_traffic_analytics(batch_df)
    traffic_df.write.format("delta").mode("append").partitionBy(
        "year", "month", "day", "hour"
    ).option("mergeSchema", "true").save(f"{config.gold_path}/traffic_analytics")

    logger.info(
        "Gold write complete",
        extra={
            "epoch_id": epoch_id,
            "funnel_records": funnel_df.count(),
            "product_records": product_df.count(),
            "traffic_records": traffic_df.count(),
        },
    )


def foreach_batch_all(df: DataFrame, epoch_id: int) -> None:
    """Process each micro-batch for all layers in a single pass."""
    try:
        parsed_df = parse_kafka_messages(df)
        if parsed_df.count() == 0:
            return

        write_to_bronze(parsed_df, epoch_id)
        write_to_silver(parsed_df, epoch_id)
        write_to_gold(parsed_df, epoch_id)
    except Exception:
        logger.exception(f"Error processing batch {epoch_id}")


def start_streaming(spark: SparkSession) -> None:
    """Start the streaming pipeline with a single query handling all layers."""
    kafka_df = read_from_kafka(spark)

    (
        kafka_df.writeStream.foreachBatch(foreach_batch_all)
        .queryName("clickstream_pipeline")
        .outputMode("update")
        .option("checkpointLocation", f"{config.checkpoint_dir}/pipeline")
        .trigger(processingTime=f"{config.batch_duration} seconds")
        .start()
    )

    logger.info("Streaming pipeline started")
    spark.streams.awaitAnyTermination()


def run_pipeline() -> None:
    """Initialize and run the streaming pipeline."""
    spark = create_spark_session()

    try:
        initialize_delta_tables(spark)
        start_streaming(spark)
    except Exception:
        logger.exception("Fatal streaming error")
        sys.exit(1)
    finally:
        spark.stop()


def initialize_delta_tables(spark: SparkSession) -> None:
    """Ensure Delta Lake table directories exist."""
    from pyspark.sql.types import StringType, StructField, StructType

    paths = [
        config.bronze_path,
        config.silver_path,
        f"{config.gold_path}/funnel_metrics",
        f"{config.gold_path}/product_performance",
        f"{config.gold_path}/traffic_analytics",
    ]

    init_schema = StructType([StructField("init", StringType(), True)])
    for path in paths:
        try:
            DeltaTable.forPath(spark, path)
            logger.info(f"Delta table exists at {path}")
        except Exception:
            logger.info(f"Initializing empty Delta table at {path}")
            empty_df = spark.createDataFrame([], init_schema)
            empty_df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
            # Remove the init row
            dt = DeltaTable.forPath(spark, path)
            dt.delete(F.col("init").isNotNull())


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_pipeline()


if __name__ == "__main__":
    main()
