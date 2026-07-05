"""Spark Structured Streaming pipeline for clickstream processing."""

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.config import config
from spark.schemas import clickstream_schema
from spark.transforms import (
    add_date_columns,
    add_event_flags,
    add_partition_columns,
    clean_and_enrich,
    compute_funnel_metrics,
    compute_product_performance,
    compute_traffic_analytics,
    deduplicate_events,
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
        "Spark session created",
        extra={
            "master": config.master,
            "app_name": config.app_name,
            "batch_duration": config.batch_duration,
            "watermark_delay": config.watermark_delay,
        },
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


def drop_oos_range(df: DataFrame) -> DataFrame:
    """Drop events outside allowed time range (defense against clock drift)."""
    now = F.current_timestamp()
    return df.filter(
        F.col("event_timestamp").between(
            now - F.expr("INTERVAL 7 DAYS"),
            now + F.expr("INTERVAL 1 DAYS"),
        )
    )


def write_to_dead_letter(df: DataFrame, epoch_id: int) -> None:
    """Write invalid Kafka messages to dead-letter Delta table."""
    dl = (
        df.select(
            F.col("key").cast("string").alias("raw_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.lit("failed_parse").alias("failure_reason"),
            F.current_timestamp().cast("string").alias("failure_timestamp"),
        )
        .withColumn("year", F.year(F.current_timestamp()))
        .withColumn("month", F.month(F.current_timestamp()))
        .withColumn("day", F.dayofmonth(F.current_timestamp()))
        .withColumn("hour", F.hour(F.current_timestamp()))
    )

    dl.write.format("delta").mode("append").partitionBy("year", "month", "day", "hour").save(
        config.dead_letter_path
    )
    logger.info(
        "Dead-letter write complete",
        extra={"epoch_id": epoch_id, "records": dl.count()},
    )


def write_to_bronze(df: DataFrame, epoch_id: int) -> None:
    """Write raw events to Delta Lake bronze layer."""
    batch_df = df.transform(parse_event_time)
    batch_df = batch_df.transform(
        lambda d: d.withColumn("ingestion_timestamp", F.current_timestamp().cast("string"))
    )
    batch_df = batch_df.transform(add_partition_columns)
    batch_df = validate_event(batch_df)[0]

    batch_df.write.format("delta").mode("append").partitionBy(
        "year", "month", "day", "hour"
    ).option("mergeSchema", "true").save(config.bronze_path)
    logger.info("Bronze write complete", extra={"epoch_id": epoch_id, "records": df.count()})


def write_to_silver(df: DataFrame, epoch_id: int) -> None:
    """Clean, enrich, and write to Delta Lake silver layer."""
    batch_df = df.transform(parse_event_time)
    batch_df = batch_df.transform(drop_oos_range)
    batch_df = batch_df.transform(clean_and_enrich)
    batch_df = batch_df.transform(add_date_columns)
    batch_df = batch_df.transform(add_event_flags)
    batch_df = batch_df.transform(deduplicate_events)
    batch_df = batch_df.transform(add_partition_columns)
    batch_df = batch_df.withColumn("ingestion_timestamp", F.current_timestamp().cast("string"))

    batch_df.write.format("delta").mode("append").partitionBy(
        "year", "month", "day", "hour"
    ).option("mergeSchema", "true").save(config.silver_path)
    logger.info("Silver write complete", extra={"epoch_id": epoch_id, "records": df.count()})


def write_to_gold(df: DataFrame, epoch_id: int) -> None:
    """Compute aggregates and write to Gold layer."""
    batch_df = df.transform(parse_event_time)
    batch_df = batch_df.transform(drop_oos_range)
    batch_df = batch_df.transform(add_date_columns)
    batch_df = batch_df.transform(add_event_flags)
    batch_df = batch_df.transform(clean_and_enrich)
    batch_df = batch_df.transform(add_partition_columns)

    window_duration = f"{config.window_interval} minutes"
    slide_duration = f"{config.window_interval} minutes"

    funnel_df = compute_funnel_metrics(batch_df, window_duration, slide_duration)
    if funnel_df.count() > 0:
        funnel_df.write.format("delta").mode("append").partitionBy(
            "year", "month", "day", "hour"
        ).option("mergeSchema", "true").save(f"{config.gold_path}/funnel_metrics")

    product_df = compute_product_performance(batch_df, window_duration)
    if product_df.count() > 0:
        product_df.write.format("delta").mode("append").partitionBy(
            "year", "month", "day", "hour"
        ).option("mergeSchema", "true").save(f"{config.gold_path}/product_performance")

    traffic_df = compute_traffic_analytics(batch_df, window_duration)
    if traffic_df.count() > 0:
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
        row_count = df.count()
        if row_count == 0:
            return

        logger.info("Processing batch", extra={"epoch_id": epoch_id, "records": row_count})

        # Split into valid and invalid messages
        parsed = df.select(
            F.col("key").cast("string").alias("message_key"),
            F.from_json(F.col("value").cast("string"), clickstream_schema).alias("event"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )

        valid = parsed.filter(F.col("event.event_id").isNotNull())
        invalid = parsed.filter(F.col("event.event_id").isNull())

        invalid_count = invalid.count()
        if invalid_count > 0:
            logger.warning(
                "Invalid messages found, routing to dead letter",
                extra={"epoch_id": epoch_id, "count": invalid_count},
            )
            write_to_dead_letter(df, epoch_id)

        if valid.count() == 0:
            return

        parsed_df = valid.select("event.*").filter(F.col("event_id").isNotNull())

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
        start_streaming(spark)
    except Exception:
        logger.exception("Fatal streaming error")
        sys.exit(1)
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_pipeline()


if __name__ == "__main__":
    main()
