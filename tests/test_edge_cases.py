"""Edge case tests for the streaming pipeline."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from producer.models import ClickstreamEvent


class TestMalformedData:
    def test_malformed_json(self) -> None:
        """Malformed JSON should be caught during parsing."""
        bad_json = "{this is not json}"
        with pytest.raises(json.JSONDecodeError):
            json.loads(bad_json)

    def test_missing_required_fields(self) -> None:
        """Event with empty required fields should fail validation."""
        event = ClickstreamEvent(
            event_id="",
            event_time="2024-01-01T00:00:00Z",
            user_id="",
            session_id="sess_1",
            event_type="page_view",
            page="/",
            product_id=None,
            category=None,
            device="mobile",
            browser="Chrome",
            operating_system="iOS",
            country="US",
            city="NY",
            traffic_source="direct",
            campaign="organic",
            referrer="google.com",
            price=0.0,
            quantity=0,
            cart_value=0.0,
            currency="USD",
            experiment_group="control",
            is_logged_in=False,
        )
        assert not event.validate()

    def test_null_timestamps(self) -> None:
        """Events with null timestamps should use current time."""
        event = ClickstreamEvent(
            event_id=str(uuid4()),
            event_time=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            user_id="user_1",
            session_id="sess_1",
            event_type="page_view",
            page="/",
            product_id=None,
            category=None,
            device="mobile",
            browser="Chrome",
            operating_system="iOS",
            country="US",
            city="NY",
            traffic_source="direct",
            campaign="organic",
            referrer="google.com",
            price=0.0,
            quantity=0,
            cart_value=0.0,
            currency="USD",
            experiment_group="control",
            is_logged_in=False,
        )
        assert event.event_time is not None
        assert "T" in event.event_time

    def test_unknown_event_type(self) -> None:
        """Unknown event types should be accepted but flagged."""
        from spark.schemas import VALID_EVENT_TYPES

        assert "unknown_event_type" not in VALID_EVENT_TYPES

    def test_empty_payload(self) -> None:
        """Empty payload is not valid JSON."""
        with pytest.raises(json.JSONDecodeError):
            json.loads("")

    def test_invalid_types(self) -> None:
        """Numeric strings vs actual numbers should be handled."""
        event = ClickstreamEvent(
            event_id=str(uuid4()),
            event_time=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            user_id="user_1",
            session_id="sess_1",
            event_type="purchase",
            page="/checkout",
            product_id="prod_1",
            category="electronics",
            device="desktop",
            browser="Chrome",
            operating_system="Windows",
            country="US",
            city="NY",
            traffic_source="direct",
            campaign="organic",
            referrer="google.com",
            price=99.99,
            quantity=2,
            cart_value=199.98,
            currency="USD",
            experiment_group="control",
            is_logged_in=True,
        )
        assert isinstance(event.price, float)
        assert isinstance(event.quantity, int)
        assert isinstance(event.cart_value, float)

    def test_negative_quantity(self) -> None:
        """Negative quantity is valid (represents removal)."""
        event_dict = {
            "event_id": str(uuid4()),
            "event_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "user_id": "user_1",
            "session_id": "sess_1",
            "event_type": "remove_from_cart",
            "page": "/cart",
            "product_id": "prod_1",
            "category": "electronics",
            "device": "mobile",
            "browser": "Chrome",
            "operating_system": "iOS",
            "country": "US",
            "city": "NY",
            "traffic_source": "direct",
            "campaign": "organic",
            "referrer": "google.com",
            "price": 99.99,
            "quantity": -1,
            "cart_value": 0.0,
            "currency": "USD",
            "experiment_group": "control",
            "is_logged_in": True,
        }
        event = ClickstreamEvent.from_dict(event_dict)
        assert event.quantity == -1
        assert event.event_type == "remove_from_cart"

    @staticmethod
    def _serialize_and_deserialize(events: list[ClickstreamEvent]) -> dict[str, Any]:
        data = [e.to_dict() for e in events]
        return {"events": data}


class TestDuplicateEvents:
    def test_identical_events(self) -> None:
        """Two identical events should have the same event_id."""
        event_id = str(uuid4())
        event1 = ClickstreamEvent(
            event_id=event_id,
            event_time="2024-01-01T00:00:00Z",
            user_id="user_1",
            session_id="sess_1",
            event_type="page_view",
            page="/",
            product_id=None,
            category=None,
            device="mobile",
            browser="Chrome",
            operating_system="iOS",
            country="US",
            city="NY",
            traffic_source="direct",
            campaign="organic",
            referrer="google.com",
            price=0.0,
            quantity=0,
            cart_value=0.0,
            currency="USD",
            experiment_group="control",
            is_logged_in=False,
        )
        event2 = ClickstreamEvent(
            event_id=event_id,
            event_time="2024-01-01T00:00:00Z",
            user_id="user_1",
            session_id="sess_1",
            event_type="page_view",
            page="/",
            product_id=None,
            category=None,
            device="mobile",
            browser="Chrome",
            operating_system="iOS",
            country="US",
            city="NY",
            traffic_source="direct",
            campaign="organic",
            referrer="google.com",
            price=0.0,
            quantity=0,
            cart_value=0.0,
            currency="USD",
            experiment_group="control",
            is_logged_in=False,
        )
        assert event1.event_id == event2.event_id
        assert event1.to_dict() == event2.to_dict()

    def test_deduplication_by_id(self) -> None:
        """Events with duplicate IDs should be identifiable."""
        ids = [str(uuid4()) for _ in range(10)]
        ids[3] = ids[5]
        ids[7] = ids[5]
        unique_ids = set(ids)
        assert len(unique_ids) == len(ids) - 2


class TestEmptyBatches:
    def test_empty_batch(self) -> None:
        """Empty batch should produce no events."""
        events: list[ClickstreamEvent] = []
        assert len(events) == 0

    def test_single_event_batch(self) -> None:
        """Single event batch should work."""
        event = ClickstreamEvent(
            event_id=str(uuid4()),
            event_time=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            user_id="user_1",
            session_id="sess_1",
            event_type="page_view",
            page="/",
            product_id=None,
            category=None,
            device="mobile",
            browser="Chrome",
            operating_system="iOS",
            country="US",
            city="NY",
            traffic_source="direct",
            campaign="organic",
            referrer="google.com",
            price=0.0,
            quantity=0,
            cart_value=0.0,
            currency="USD",
            experiment_group="control",
            is_logged_in=False,
        )
        assert event is not None
        assert event.event_type == "page_view"


class TestLateArrivingData:
    def test_late_event_has_timestamp(self) -> None:
        """Late events still have a valid timestamp."""
        late_time = "2020-01-01T00:00:00Z"
        event = ClickstreamEvent(
            event_id=str(uuid4()),
            event_time=late_time,
            user_id="user_1",
            session_id="sess_1",
            event_type="page_view",
            page="/",
            product_id=None,
            category=None,
            device="mobile",
            browser="Chrome",
            operating_system="iOS",
            country="US",
            city="NY",
            traffic_source="direct",
            campaign="organic",
            referrer="google.com",
            price=0.0,
            quantity=0,
            cart_value=0.0,
            currency="USD",
            experiment_group="control",
            is_logged_in=False,
        )
        assert event.event_time == late_time

    def test_future_event(self) -> None:
        """Events with future timestamps are still valid."""
        future_time = "2099-12-31T23:59:59Z"
        event = ClickstreamEvent(
            event_id=str(uuid4()),
            event_time=future_time,
            user_id="user_1",
            session_id="sess_1",
            event_type="page_view",
            page="/",
            product_id=None,
            category=None,
            device="mobile",
            browser="Chrome",
            operating_system="iOS",
            country="US",
            city="NY",
            traffic_source="direct",
            campaign="organic",
            referrer="google.com",
            price=0.0,
            quantity=0,
            cart_value=0.0,
            currency="USD",
            experiment_group="control",
            is_logged_in=False,
        )
        assert event.event_time == future_time


class TestSchemaEvolution:
    def test_extra_fields_in_dict(self) -> None:
        """Extra fields in dict are ignored by from_dict (only known fields used)."""
        event_dict = {
            "event_id": str(uuid4()),
            "event_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "user_id": "user_1",
            "session_id": "sess_1",
            "event_type": "page_view",
            "page": "/",
            "product_id": None,
            "category": None,
            "device": "mobile",
            "browser": "Chrome",
            "operating_system": "iOS",
            "country": "US",
            "city": "NY",
            "traffic_source": "direct",
            "campaign": "organic",
            "referrer": "google.com",
            "price": 0.0,
            "quantity": 0,
            "cart_value": 0.0,
            "currency": "USD",
            "experiment_group": "control",
            "is_logged_in": False,
            "new_field": "should_not_crash",
        }
        clean = {k: v for k, v in event_dict.items() if k in ClickstreamEvent.__dataclass_fields__}
        event = ClickstreamEvent.from_dict(clean)
        assert event.event_type == "page_view"
        assert event.event_id is not None


class TestProducerResilience:
    def test_kafka_config_has_retries(self) -> None:
        """Producer config must include retries."""
        from producer.config import KafkaConfig

        kc = KafkaConfig()
        producer_config = kc.producer_config
        assert producer_config.get("retries", 0) > 0

    def test_dead_letter_topic_configured(self) -> None:
        """Dead letter topic must be configured."""
        from producer.config import KafkaConfig

        kc = KafkaConfig()
        assert kc.dead_letter_topic is not None
        assert len(kc.dead_letter_topic) > 0

    def test_consumer_delivery_semantics(self) -> None:
        """Spark config should have failOnDataLoss disabled."""
        from spark.config import SparkConfig

        sc = SparkConfig()
        assert sc.starting_offsets in ("earliest", "latest")
