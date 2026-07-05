"""PySpark schemas for clickstream events."""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Raw clickstream event schema for Kafka ingestion
clickstream_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("page", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("device", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("operating_system", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("traffic_source", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("cart_value", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("experiment_group", StringType(), True),
        StructField("is_logged_in", BooleanType(), True),
    ]
)

# Bronze layer schema (raw + ingestion metadata)
bronze_schema = (
    clickstream_schema.add(StructField("ingestion_timestamp", StringType(), False))
    .add(StructField("year", IntegerType(), False))
    .add(StructField("month", IntegerType(), False))
    .add(StructField("day", IntegerType(), False))
    .add(StructField("hour", IntegerType(), False))
)

# Silver layer schema (cleaned + enriched)
silver_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("page", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("device", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("operating_system", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("traffic_source", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("cart_value", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("experiment_group", StringType(), True),
        StructField("is_logged_in", BooleanType(), True),
        StructField("event_date", StringType(), False),
        StructField("event_hour", IntegerType(), False),
        StructField("is_purchase", BooleanType(), False),
        StructField("is_cart_add", BooleanType(), False),
        StructField("is_page_view", BooleanType(), False),
        StructField("is_checkout", BooleanType(), False),
        StructField("is_bounce", BooleanType(), False),
        StructField("ingestion_timestamp", StringType(), False),
    ]
)

# Gold layer funnel metrics schema
funnel_metrics_schema = StructType(
    [
        StructField("window_start", StringType(), False),
        StructField("window_end", StringType(), False),
        StructField("page_views", IntegerType(), False),
        StructField("product_views", IntegerType(), False),
        StructField("add_to_carts", IntegerType(), False),
        StructField("checkout_starts", IntegerType(), False),
        StructField("purchases", IntegerType(), False),
        StructField("sessions", IntegerType(), False),
        StructField("unique_users", IntegerType(), False),
        StructField("conversion_rate", DoubleType(), False),
        StructField("abandonment_rate", DoubleType(), False),
        StructField("avg_cart_value", DoubleType(), False),
        StructField("total_revenue", DoubleType(), False),
        StructField("events_per_second", DoubleType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("hour", IntegerType(), False),
    ]
)

# Gold layer product performance schema
product_performance_schema = StructType(
    [
        StructField("window_start", StringType(), False),
        StructField("window_end", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("views", IntegerType(), False),
        StructField("add_to_carts", IntegerType(), False),
        StructField("purchases", IntegerType(), False),
        StructField("revenue", DoubleType(), False),
        StructField("conversion_rate", DoubleType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("hour", IntegerType(), False),
    ]
)

# Gold layer traffic analytics schema
traffic_analytics_schema = StructType(
    [
        StructField("window_start", StringType(), False),
        StructField("window_end", StringType(), False),
        StructField("country", StringType(), True),
        StructField("traffic_source", StringType(), True),
        StructField("device", StringType(), True),
        StructField("visits", IntegerType(), False),
        StructField("users", IntegerType(), False),
        StructField("purchases", IntegerType(), False),
        StructField("revenue", DoubleType(), False),
        StructField("conversion_rate", DoubleType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("hour", IntegerType(), False),
    ]
)

# Valid event types for validation
VALID_EVENT_TYPES = {
    "page_view",
    "search",
    "category_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "begin_checkout",
    "payment",
    "purchase",
    "logout",
}
