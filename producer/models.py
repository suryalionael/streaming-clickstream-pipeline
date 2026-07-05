"""Data models for clickstream events."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


@dataclass
class ClickstreamEvent:
    event_id: str
    event_time: str
    user_id: str
    session_id: str
    event_type: str
    page: str
    product_id: str | None
    category: str | None
    device: str
    browser: str
    operating_system: str
    country: str
    city: str
    traffic_source: str
    campaign: str
    referrer: str
    price: float
    quantity: int
    cart_value: float
    currency: str
    experiment_group: str
    is_logged_in: bool
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        event_type: str,
        user_id: str,
        session_id: str,
        page: str,
        country: str,
        city: str,
        traffic_source: str,
        campaign: str,
        referrer: str,
        device: str,
        browser: str,
        operating_system: str,
        experiment_group: str,
        is_logged_in: bool,
        product_id: str | None = None,
        category: str | None = None,
        price: float = 0.0,
        quantity: int = 0,
        cart_value: float = 0.0,
        metadata: dict[str, Any] | None = None,
    ) -> "ClickstreamEvent":
        return cls(
            event_id=str(uuid4()),
            event_time=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            event_type=event_type,
            user_id=user_id,
            session_id=session_id,
            page=page,
            product_id=product_id,
            category=category,
            device=device,
            browser=browser,
            operating_system=operating_system,
            country=country,
            city=city,
            traffic_source=traffic_source,
            campaign=campaign,
            referrer=referrer,
            price=price,
            quantity=quantity,
            cart_value=cart_value,
            currency="USD",
            experiment_group=experiment_group,
            is_logged_in=is_logged_in,
            metadata=metadata or {},
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for k, v in self.__dict__.items():
            if isinstance(v, datetime):
                result[k] = v.isoformat()
            else:
                result[k] = v
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ClickstreamEvent":
        return cls(**data)

    def validate(self) -> bool:
        """Basic schema validation."""
        required = [
            "event_id",
            "event_time",
            "user_id",
            "session_id",
            "event_type",
            "page",
        ]
        for field_name in required:
            if not getattr(self, field_name, None):
                return False
        valid_types = {
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
        return self.event_type in valid_types


@dataclass
class SessionState:
    user_id: str
    session_id: str
    events: list[ClickstreamEvent] = field(default_factory=list)
    cart_items: list[dict] = field(default_factory=list)
    cart_value: float = 0.0
    current_page: str = "/"
    current_category: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    has_purchased: bool = False
    traffic_source: str = "direct"
    campaign: str = "organic"
    country: str = "United States"
    city: str = "New York"
    device: str = "desktop"
    browser: str = "Chrome"
    operating_system: str = "Windows"
    experiment_group: str = "control"
    is_logged_in: bool = True
    pages_visited: list[str] = field(default_factory=list)
    products_viewed: list[str] = field(default_factory=list)
    searches: int = 0
    session_depth: int = 0
