"""Shared test fixtures for the clickstream pipeline test suite."""

import random
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from producer.config import GeneratorConfig
from producer.generator import ClickstreamGenerator
from producer.models import ClickstreamEvent


@pytest.fixture
def generator_config() -> GeneratorConfig:
    return GeneratorConfig(
        events_per_second=10,
        num_users=50,
        abandonment_rate=0.5,
        conversion_rate=0.1,
        burst_mode=False,
    )


@pytest.fixture
def generator(generator_config: GeneratorConfig) -> ClickstreamGenerator:
    return ClickstreamGenerator(generator_config)


@pytest.fixture
def sample_event() -> ClickstreamEvent:
    return ClickstreamEvent(
        event_id=str(uuid4()),
        event_time=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        user_id="user_000001",
        session_id="sess_test_123",
        event_type="page_view",
        page="/",
        product_id=None,
        category=None,
        device="mobile",
        browser="Chrome",
        operating_system="iOS",
        country="United States",
        city="New York",
        traffic_source="direct",
        campaign="organic",
        referrer="google.com",
        price=0.0,
        quantity=0,
        cart_value=0.0,
        currency="USD",
        experiment_group="control",
        is_logged_in=True,
    )


@pytest.fixture
def sample_events() -> list[ClickstreamEvent]:
    events = []
    for i in range(10):
        event = ClickstreamEvent(
            event_id=str(uuid4()),
            event_time=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            user_id=f"user_{i:06d}",
            session_id=f"sess_{i}_{uuid4().hex[:8]}",
            event_type=random.choice(["page_view", "product_view", "add_to_cart", "purchase"]),
            page=random.choice(["/", "/products", "/cart", "/checkout"]),
            product_id=f"prod_{random.randint(1, 100):03d}",
            category=random.choice(["electronics", "clothing", "home_garden"]),
            device=random.choice(["mobile", "desktop", "tablet"]),
            browser=random.choice(["Chrome", "Safari", "Firefox"]),
            operating_system=random.choice(["iOS", "Android", "Windows", "macOS"]),
            country=random.choice(["United States", "United Kingdom", "Germany"]),
            city=random.choice(["New York", "London", "Berlin"]),
            traffic_source=random.choice(["direct", "organic_search", "social_media"]),
            campaign="test_campaign",
            referrer="google.com",
            price=round(random.uniform(10, 200), 2),
            quantity=random.randint(0, 3),
            cart_value=round(random.uniform(0, 500), 2),
            currency="USD",
            experiment_group=random.choice(["control", "variant_a"]),
            is_logged_in=random.random() < 0.6,
        )
        events.append(event)
    return events


@pytest.fixture
def sample_event_dict() -> dict[str, Any]:
    return {
        "event_id": str(uuid4()),
        "event_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "user_id": "user_000001",
        "session_id": "sess_test_456",
        "event_type": "purchase",
        "page": "/checkout/confirmation",
        "product_id": "prod_001",
        "category": "electronics",
        "device": "desktop",
        "browser": "Chrome",
        "operating_system": "Windows",
        "country": "United States",
        "city": "New York",
        "traffic_source": "email",
        "campaign": "newsletter",
        "referrer": "email.com",
        "price": 999.99,
        "quantity": 1,
        "cart_value": 999.99,
        "currency": "USD",
        "experiment_group": "control",
        "is_logged_in": True,
    }
