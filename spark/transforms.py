"""Transform functions for the streaming pipeline."""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from spark.schemas import VALID_EVENT_TYPES


def parse_event_time(df: DataFrame) -> DataFrame:
    """Parse event_time string to timestamp."""
    return df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Add ingestion timestamp and partition columns."""
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return (
        df.withColumn("ingestion_timestamp", F.lit(now))
        .withColumn(
            "event_timestamp", F.to_timestamp(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        .withColumn("year", F.year("event_timestamp"))
        .withColumn("month", F.month("event_timestamp"))
        .withColumn("day", F.dayofmonth("event_timestamp"))
        .withColumn("hour", F.hour("event_timestamp"))
    )


def add_date_columns(df: DataFrame) -> DataFrame:
    """Add date and time partition columns."""
    return df.withColumn("event_date", F.to_date("event_timestamp")).withColumn(
        "event_hour", F.hour("event_timestamp")
    )


def add_event_flags(df: DataFrame) -> DataFrame:
    """Add boolean flags for common event types."""
    return (
        df.withColumn("is_purchase", F.col("event_type") == "purchase")
        .withColumn("is_cart_add", F.col("event_type") == "add_to_cart")
        .withColumn("is_page_view", F.col("event_type") == "page_view")
        .withColumn(
            "is_checkout", F.col("event_type").isin("begin_checkout", "payment", "purchase")
        )
        .withColumn("is_bounce", F.lit(False))  # Calculated at session level
    )


def validate_event(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split dataframe into valid and invalid events."""
    valid = df.filter(
        F.col("event_type").isin(VALID_EVENT_TYPES)
        & F.col("event_id").isNotNull()
        & (F.col("event_id") != "")
        & F.col("user_id").isNotNull()
        & F.col("session_id").isNotNull()
    )
    invalid = df.subtract(valid)
    return valid, invalid


def clean_and_enrich(df: DataFrame) -> DataFrame:
    """Clean malformed fields and enrich with derived columns."""
    df = df.withColumn(
        "price",
        F.when(F.col("price").isNull() | (F.col("price") < 0), 0.0).otherwise(F.col("price")),
    )
    df = df.withColumn(
        "quantity",
        F.when(F.col("quantity").isNull(), 0).otherwise(
            F.when(F.col("quantity") < -100, 0).otherwise(F.col("quantity"))
        ),
    )
    df = df.withColumn(
        "cart_value",
        F.when(F.col("cart_value").isNull() | (F.col("cart_value") < 0), 0.0).otherwise(
            F.col("cart_value")
        ),
    )
    return df


def compute_funnel_metrics(
    df: DataFrame, window_duration: str = "5 minutes", slide_duration: str = "5 minutes"
) -> DataFrame:
    """Compute funnel metrics over tumbling windows."""
    return (
        df.withWatermark("event_timestamp", "60 minutes")
        .groupBy(
            F.window("event_timestamp", window_duration, slide_duration).alias("window"),
            F.col("year"),
            F.col("month"),
            F.col("day"),
            F.col("hour"),
        )
        .agg(
            F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_views"),
            F.count(F.when(F.col("event_type") == "product_view", 1)).alias("product_views"),
            F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_carts"),
            F.count(F.when(F.col("event_type").isin("begin_checkout", "payment"), 1)).alias(
                "checkout_starts"
            ),
            F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases"),
            F.countDistinct("session_id").alias("sessions"),
            F.countDistinct("user_id").alias("unique_users"),
            F.avg(F.when(F.col("event_type") == "add_to_cart", F.col("cart_value"))).alias(
                "avg_cart_value"
            ),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("cart_value"))).alias(
                "total_revenue"
            ),
        )
        .withColumn(
            "conversion_rate",
            F.when(
                F.col("add_to_carts") > 0,
                F.col("purchases") / F.col("add_to_carts"),
            ).otherwise(0.0),
        )
        .withColumn(
            "abandonment_rate",
            F.when(
                F.col("checkout_starts") > 0,
                1.0 - (F.col("purchases") / F.col("checkout_starts")),
            ).otherwise(0.0),
        )
        .withColumn(
            "events_per_second",
            (F.col("page_views") + F.col("product_views") + F.col("add_to_carts")) / 300.0,
        )
        .withColumn("window_start", F.col("window.start").cast(StringType()))
        .withColumn("window_end", F.col("window.end").cast(StringType()))
        .drop("window")
    )


def compute_product_performance(df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    """Compute product-level performance metrics."""
    return (
        df.filter(F.col("product_id").isNotNull())
        .withWatermark("event_timestamp", "60 minutes")
        .groupBy(
            F.window("event_timestamp", window_duration).alias("window"),
            "product_id",
            "category",
            "year",
            "month",
            "day",
            "hour",
        )
        .agg(
            F.count(F.when(F.col("event_type").isin("product_view", "page_view"), 1)).alias(
                "views"
            ),
            F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_carts"),
            F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("cart_value"))).alias("revenue"),
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("views") > 0, F.col("purchases") / F.col("views")).otherwise(0.0),
        )
        .withColumn("window_start", F.col("window.start").cast(StringType()))
        .withColumn("window_end", F.col("window.end").cast(StringType()))
        .drop("window")
    )


def compute_traffic_analytics(df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    """Compute traffic analytics by country, source, device."""
    return (
        df.withWatermark("event_timestamp", "60 minutes")
        .groupBy(
            F.window("event_timestamp", window_duration).alias("window"),
            "country",
            "traffic_source",
            "device",
            "year",
            "month",
            "day",
            "hour",
        )
        .agg(
            F.count("*").alias("visits"),
            F.countDistinct("user_id").alias("users"),
            F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("cart_value"))).alias("revenue"),
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("visits") > 0, F.col("purchases") / F.col("visits")).otherwise(0.0),
        )
        .withColumn("window_start", F.col("window.start").cast(StringType()))
        .withColumn("window_end", F.col("window.end").cast(StringType()))
        .drop("window")
    )


def write_dead_letter(spark: SparkSession, df: DataFrame, topic: str) -> None:
    """Write invalid events to dead letter Kafka topic."""
    if df.count() > 0:
        df.select(
            F.to_json(F.struct("*")).alias("value"),
            F.col("event_id").alias("key"),
        ).write.format("kafka").option("topic", topic).option(
            "kafka.bootstrap.servers", "localhost:9092"
        ).save()
