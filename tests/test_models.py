"""Tests for data models."""

from producer.models import ClickstreamEvent, SessionState


class TestClickstreamEvent:
    def test_create_event(self, sample_event: ClickstreamEvent) -> None:
        assert sample_event.event_id is not None
        assert sample_event.event_type == "page_view"
        assert sample_event.validate()

    def test_to_dict(self, sample_event: ClickstreamEvent) -> None:
        d = sample_event.to_dict()
        assert d["event_id"] == sample_event.event_id
        assert d["event_type"] == "page_view"
        assert d["page"] == "/"
        assert d["currency"] == "USD"

    def test_from_dict(self, sample_event_dict: dict) -> None:
        event = ClickstreamEvent.from_dict(sample_event_dict)
        assert event.event_id == sample_event_dict["event_id"]
        assert event.event_type == "purchase"
        assert event.price == 999.99
        assert event.validate()

    def test_round_trip(self, sample_event: ClickstreamEvent) -> None:
        d = sample_event.to_dict()
        event = ClickstreamEvent.from_dict(d)
        assert event.event_id == sample_event.event_id
        assert event.event_time == sample_event.event_time
        assert event.event_type == sample_event.event_type
        assert event.user_id == sample_event.user_id

    def test_validation_valid(self, sample_event: ClickstreamEvent) -> None:
        assert sample_event.validate()

    def test_validation_invalid_event_type(self, sample_event: ClickstreamEvent) -> None:
        sample_event.event_type = "invalid_type"
        assert not sample_event.validate()

    def test_validation_missing_event_id(self, sample_event: ClickstreamEvent) -> None:
        sample_event.event_id = ""
        assert not sample_event.validate()

    def test_validation_missing_user_id(self, sample_event: ClickstreamEvent) -> None:
        sample_event.user_id = ""
        assert not sample_event.validate()

    def test_create_factory(self) -> None:
        event = ClickstreamEvent.create(
            event_type="purchase",
            user_id="user_000001",
            session_id="sess_123",
            page="/checkout",
            country="US",
            city="NY",
            traffic_source="direct",
            campaign="organic",
            referrer="google.com",
            device="mobile",
            browser="Chrome",
            operating_system="iOS",
            experiment_group="control",
            is_logged_in=True,
            product_id="prod_001",
            category="electronics",
            price=99.99,
            quantity=2,
            cart_value=199.98,
        )
        assert event.event_type == "purchase"
        assert event.price == 99.99
        assert event.quantity == 2
        assert event.cart_value == 199.98
        assert event.validate()

    def test_create_with_metadata(self) -> None:
        event = ClickstreamEvent.create(
            event_type="page_view",
            user_id="user_000001",
            session_id="sess_123",
            page="/",
            country="US",
            city="NY",
            traffic_source="direct",
            campaign="organic",
            referrer="direct",
            device="desktop",
            browser="Chrome",
            operating_system="Windows",
            experiment_group="control",
            is_logged_in=True,
            metadata={"custom_field": "value"},
        )
        assert event.metadata["custom_field"] == "value"


class TestSessionState:
    def test_session_initialization(self) -> None:
        session = SessionState(
            user_id="user_000001",
            session_id="sess_123",
        )
        assert session.user_id == "user_000001"
        assert session.session_id == "sess_123"
        assert session.cart_value == 0.0
        assert len(session.cart_items) == 0
        assert not session.has_purchased

    def test_session_tracking(self) -> None:
        session = SessionState(
            user_id="user_000001",
            session_id="sess_123",
        )
        session.cart_value = 150.0
        session.cart_items.append({"product_id": "prod_001", "price": 50.0, "quantity": 3})
        session.has_purchased = True
        session.session_depth = 10

        assert session.cart_value == 150.0
        assert len(session.cart_items) == 1
        assert session.has_purchased
        assert session.session_depth == 10
