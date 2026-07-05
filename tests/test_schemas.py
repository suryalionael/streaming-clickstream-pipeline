"""Tests for Spark schemas and schema validation."""

from spark.schemas import (
    VALID_EVENT_TYPES,
    bronze_schema,
    clickstream_schema,
    funnel_metrics_schema,
    silver_schema,
)


class TestSchemas:
    def test_clickstream_schema_fields(self) -> None:
        field_names = [f.name for f in clickstream_schema.fields]
        assert "event_id" in field_names
        assert "event_type" in field_names
        assert "user_id" in field_names
        assert "session_id" in field_names
        assert "event_time" in field_names
        assert "page" in field_names

    def test_bronze_schema_has_partition_cols(self) -> None:
        field_names = [f.name for f in bronze_schema.fields]
        assert "year" in field_names
        assert "month" in field_names
        assert "day" in field_names
        assert "hour" in field_names
        assert "ingestion_timestamp" in field_names

    def test_silver_schema_has_flags(self) -> None:
        field_names = [f.name for f in silver_schema.fields]
        assert "is_purchase" in field_names
        assert "is_cart_add" in field_names
        assert "is_page_view" in field_names
        assert "is_bounce" in field_names

    def test_funnel_metrics_schema(self) -> None:
        field_names = [f.name for f in funnel_metrics_schema.fields]
        assert "window_start" in field_names
        assert "window_end" in field_names
        assert "page_views" in field_names
        assert "conversion_rate" in field_names
        assert "total_revenue" in field_names

    def test_valid_event_types(self) -> None:
        expected = {
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
        assert expected == VALID_EVENT_TYPES

    def test_clickstream_schema_types(self) -> None:
        for field in clickstream_schema.fields:
            assert field.name in [f.name for f in clickstream_schema.fields]
        # Check specific types
        event_id_field = clickstream_schema["event_id"]
        assert "StringType" in str(event_id_field.dataType)
        assert not event_id_field.nullable

        price_field = clickstream_schema["price"]
        assert "DoubleType" in str(price_field.dataType)
        assert price_field.nullable
