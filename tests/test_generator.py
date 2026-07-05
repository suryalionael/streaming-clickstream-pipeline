"""Tests for the clickstream event generator."""

import random

from producer.config import GeneratorConfig
from producer.generator import ClickstreamGenerator


class TestClickstreamGenerator:
    def test_initialization(self, generator: ClickstreamGenerator) -> None:
        assert generator.config.events_per_second == 10
        assert generator.config.num_users == 50
        assert len(generator.sessions) == 0

    def test_generate_event(self, generator: ClickstreamGenerator) -> None:
        event = generator.generate_event()
        assert event is not None
        assert event.event_id is not None
        assert event.event_type in generator.config.event_types
        assert event.user_id.startswith("user_")
        assert event.event_time.endswith("Z")

    def test_generate_batch(self, generator: ClickstreamGenerator) -> None:
        events = generator.generate_batch(20)
        assert len(events) > 0
        assert len(events) <= 20
        for event in events:
            assert event.event_type in generator.config.event_types

    def test_event_has_required_fields(self, generator: ClickstreamGenerator) -> None:
        event = generator.generate_event()
        assert event is not None
        assert event.event_id
        assert event.event_time
        assert event.user_id
        assert event.session_id
        assert event.event_type
        assert event.page
        assert event.device
        assert event.browser
        assert event.operating_system
        assert event.country
        assert event.city
        assert event.traffic_source
        assert event.currency == "USD"

    def test_event_validation(self, generator: ClickstreamGenerator) -> None:
        event = generator.generate_event()
        assert event is not None
        assert event.validate()

    def test_generator_reuses_sessions(self, generator: ClickstreamGenerator) -> None:
        # Directly test session reuse by calling _get_next_event
        user_id = "user_000001"
        session = generator._create_new_session(user_id)
        generator.sessions[user_id] = session

        events = []
        for _ in range(5):
            event = generator._get_next_event(session)
            if event:
                events.append(event)
                session.events.append(event)

        assert len(events) > 0
        assert all(e.user_id == user_id for e in events)
        assert all(e.session_id == session.session_id for e in events)

    def test_burst_detection(self, generator: ClickstreamGenerator) -> None:
        # Normal mode - burst should be rare
        random.seed(42)
        bursts = sum(1 for _ in range(1000) if generator.should_burst())
        assert bursts < 100  # Should be less than 10%

    def test_burst_mode_multiplier(self) -> None:
        config = GeneratorConfig(burst_mode=True)
        gen = ClickstreamGenerator(config)
        random.seed(42)
        multipliers = [gen.get_burst_multiplier() for _ in range(100)]
        assert any(m > 1 for m in multipliers)
        assert all(m >= 1 for m in multipliers)

    def test_black_friday_mode(self) -> None:
        config = GeneratorConfig(black_friday_mode=True)
        gen = ClickstreamGenerator(config)
        random.seed(42)
        multipliers = [gen.get_burst_multiplier() for _ in range(100)]
        assert any(m >= 3 for m in multipliers)

    def test_different_event_types(self, generator: ClickstreamGenerator) -> None:
        generated_types = set()
        for _ in range(1000):
            event = generator.generate_event()
            if event:
                generated_types.add(event.event_type)

        # With enough events, should generate at least page_view and one more type
        assert len(generated_types) >= 2
        assert "page_view" in generated_types

    def test_user_journey_progression(self, generator: ClickstreamGenerator) -> None:
        """Test that events follow a realistic customer journey pattern."""
        user_id = "user_000001"
        generator.sessions[user_id] = generator._create_new_session(user_id)

        journey_events = []
        for _ in range(30):
            event = generator.generate_event()
            if event and event.user_id == user_id:
                journey_events.append(event)
                if event.event_type == "purchase":
                    break

        assert len(journey_events) > 0
        # Journey should start with page_view or similar browsing event
        assert journey_events[0].event_type in ("page_view", "category_view")

    def test_abandonment_behavior(self) -> None:
        """Test cart abandonment behavior."""
        config = GeneratorConfig(abandonment_rate=0.95, conversion_rate=0.001)
        gen = ClickstreamGenerator(config)
        random.seed(42)

        events = gen.generate_batch(100)
        purchase_count = sum(1 for e in events if e.event_type == "purchase")
        assert purchase_count < 10  # Very few purchases with high abandonment

    def test_conversion_behavior(self) -> None:
        """Test conversion behavior."""
        config = GeneratorConfig(conversion_rate=0.5, abandonment_rate=0.1)
        gen = ClickstreamGenerator(config)
        random.seed(42)

        events = gen.generate_batch(1000)
        # Count events by type
        type_counts: dict[str, int] = {}
        for e in events:
            type_counts[e.event_type] = type_counts.get(e.event_type, 0) + 1

        # With enough events and high conversion, should have some cart activity
        add_to_cart_count = type_counts.get("add_to_cart", 0)
        purchase_count = type_counts.get("purchase", 0)
        checkout_count = type_counts.get("begin_checkout", 0) + type_counts.get("payment", 0)

        # Should have at least some cart interaction
        total_conversion_events = add_to_cart_count + checkout_count + purchase_count
        assert total_conversion_events > 0, f"No conversion events found. Types: {type_counts}"

    def test_session_management(self, generator: ClickstreamGenerator) -> None:
        """Test that sessions are properly created and managed."""
        user_id = "user_000001"
        session1 = generator._create_new_session(user_id)
        assert session1.session_id.startswith("sess_")
        assert session1.user_id == user_id
        assert session1.cart_value == 0.0
        assert not session1.has_purchased
