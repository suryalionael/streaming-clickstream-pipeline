"""Realistic synthetic clickstream event generator with state-machine customer journeys."""

import logging
import random
import time
from datetime import UTC, datetime
from typing import Any

from producer.config import GeneratorConfig
from producer.models import ClickstreamEvent, SessionState

logger = logging.getLogger(__name__)


def weighted_choice(items: list[dict[str, Any]], weight_key: str = "weight") -> dict[str, Any]:
    """Select an item from a weighted list using roulette-wheel selection."""
    total = sum(item[weight_key] for item in items)
    r = random.uniform(0, total)
    cumulative = 0.0
    for item in items:
        cumulative += item[weight_key]
        if r <= cumulative:
            return item
    return items[-1]


class ClickstreamGenerator:
    """Generates realistic synthetic clickstream events with configurable behavior."""

    def __init__(self, config: GeneratorConfig) -> None:
        self.config = config
        self.sessions: dict[str, SessionState] = {}
        self.base_time = datetime.now(UTC)

    def _get_or_create_session(self, user_id: str) -> SessionState:
        existing = self.sessions.get(user_id)
        if existing is not None:
            session = existing
            if session.has_purchased or session.session_depth > random.randint(5, 25):
                if random.random() < 0.7:
                    return self._create_new_session(user_id)
            return session

        if len(self.sessions) > self.config.num_users * 3:
            oldest = sorted(self.sessions.keys(), key=lambda k: self.sessions[k].started_at)[
                : len(self.sessions) // 4
            ]
            for k in oldest:
                del self.sessions[k]

        return self._create_new_session(user_id)

    def _create_new_session(self, user_id: str) -> SessionState:
        country_info = weighted_choice(self.config.countries)
        source_info = weighted_choice(self.config.traffic_sources)

        os_map = {
            "mobile": ["iOS", "Android"],
            "desktop": ["Windows", "macOS", "Linux"],
            "tablet": ["iOS", "Android"],
        }

        device = random.choice(self.config.devices)
        browser = random.choice(self.config.browsers)

        possible_os = os_map.get(device, ["Windows", "macOS"])
        operating_system = random.choice(possible_os)

        # Mobile-heavy traffic: 60% mobile for realistic e-commerce patterns
        if random.random() < 0.6:
            device = "mobile"
            browser = random.choice(self.config.browsers[:3])
            operating_system = random.choice(["iOS", "Android"])

        session_id = f"sess_{user_id}_{int(time.time() * 1000000)}"

        session = SessionState(
            user_id=user_id,
            session_id=session_id,
            traffic_source=source_info["source"],
            campaign=self._get_campaign(source_info["source"]),
            country=country_info["country"],
            city=country_info["city"],
            device=device,
            browser=browser,
            operating_system=operating_system,
            experiment_group=random.choice(self.config.experiment_groups),
            is_logged_in=random.random() < 0.6,
        )
        self.sessions[user_id] = session
        return session

    def _get_campaign(self, source: str) -> str:
        campaigns = {
            "direct": ["organic", "direct"],
            "organic_search": ["organic_seo", "brand_search", "generic_search"],
            "paid_search": ["google_ads", "bing_ads", "shopping_campaign"],
            "social_media": [
                "instagram_campaign",
                "facebook_ads",
                "tiktok_promo",
                "twitter_ad",
            ],
            "email": ["newsletter", "promo_email", "abandoned_cart", "welcome_series"],
            "referral": ["blog_referral", "partner_link", "review_site"],
        }
        return random.choice(campaigns.get(source, ["organic"]))

    def _get_page_for_event(self, event_type: str) -> str:
        pages = {
            "page_view": [
                "/",
                "/products",
                "/categories",
                "/sale",
                "/new-arrivals",
                "/deals",
            ],
            "search": ["/search"],
            "category_view": [
                "/category/electronics",
                "/category/clothing",
                "/category/home_garden",
                "/category/sports",
                "/category/books",
            ],
            "product_view": ["/product/"],
            "add_to_cart": ["/product/"],
            "remove_from_cart": ["/cart"],
            "begin_checkout": ["/checkout"],
            "payment": ["/checkout/payment"],
            "purchase": ["/checkout/confirmation"],
            "logout": ["/logout"],
        }
        return random.choice(pages.get(event_type, ["/"]))

    def _get_category(self, session: SessionState) -> str | None:
        if session.current_category:
            return session.current_category
        return random.choice(self.config.categories)

    def _get_product(self, category: str | None = None) -> tuple[str | None, str | None, float]:
        if category and category in self.config.products:
            product_list = self.config.products[category]
        else:
            cat = random.choice(self.config.categories)
            product_list = self.config.products.get(cat, ["Generic Product"])
            category = cat

        product = random.choice(product_list)
        price_ranges = {
            "electronics": (9.99, 2499.99),
            "clothing": (14.99, 299.99),
            "home_garden": (5.99, 599.99),
            "books": (9.99, 79.99),
            "sports": (9.99, 499.99),
            "beauty": (4.99, 89.99),
            "toys": (9.99, 149.99),
            "groceries": (1.99, 59.99),
            "automotive": (14.99, 399.99),
            "office": (4.99, 199.99),
        }
        price_range = price_ranges.get(category or "electronics", (9.99, 99.99))
        price = round(random.uniform(*price_range), 2)
        return product, category, price

    @staticmethod
    def _transition_event_type(last_event_type: str, is_converting: bool) -> str | None:
        """Determine next event type using a state machine."""
        # Threshold tables: (threshold, event) pairs per source state.
        # A random r is generated per source; the first threshold r < t wins.
        trans: dict[str, list[tuple[float, str]]] = {
            "page_view": [
                (0.30, "search"),
                (0.55, "category_view"),
                (0.80, "product_view"),
                (1.01, "page_view"),
            ],
            "search": [
                (0.60, "product_view"),
                (0.85, "category_view"),
                (1.01, "page_view"),
            ],
            "category_view": [
                (0.65, "product_view"),
                (0.80, "search"),
                (1.01, "page_view"),
            ],
            "product_view": [
                (0.35, "add_to_cart"),
                (0.55, "product_view"),
                (0.65, "category_view"),
                (1.01, "page_view"),
            ],
            "add_to_cart": [
                (0.35, "product_view"),
                (0.50, "remove_from_cart"),
                (0.70 if is_converting else 0.30, "begin_checkout"),
                (1.01, "page_view"),
            ],
            "remove_from_cart": [
                (0.40, "product_view"),
                (0.60 if is_converting else 0.35, "add_to_cart"),
                (0.75, "begin_checkout"),
                (1.01, "page_view"),
            ],
            "begin_checkout": [
                (0.85 if is_converting else 0.30, "payment"),
                (0.50, "page_view"),
                (1.01, "add_to_cart"),
            ],
            "payment": [(1.01, "purchase" if is_converting else "page_view")],
            "purchase": [
                (0.50, "logout"),
                (1.01, "page_view"),
            ],
            "logout": [(1.01, "__END__")],
        }

        if last_event_type not in trans:
            return "page_view"

        table = trans[last_event_type]
        r = random.random()
        for threshold, event in table:
            if r < threshold:
                return None if event == "__END__" else event
        return None

    def _get_next_event(self, session: SessionState) -> ClickstreamEvent | None:
        """Determine the next event in the user journey based on current state."""
        session.session_depth += 1

        # Abandonment: users who haven't purchased may leave after page_view
        if (
            session.session_depth > 2
            and not session.has_purchased
            and random.random() < self.config.abandonment_rate * 0.1
            and random.random() < 0.3
        ):
            return None

        # Compute conversion probability with traffic source bonus
        source_info = next(
            (s for s in self.config.traffic_sources if s["source"] == session.traffic_source),
            None,
        )
        effective_conversion = self.config.conversion_rate + (
            source_info["conversion_bonus"] if source_info else 0.0
        )
        is_converting = random.random() < effective_conversion

        # First event in session is always page_view
        if not session.events:
            event = ClickstreamEvent.create(
                event_type="page_view",
                user_id=session.user_id,
                session_id=session.session_id,
                page=random.choice(["/", "/products", "/new-arrivals", "/sale", "/deals"]),
                country=session.country,
                city=session.city,
                traffic_source=session.traffic_source,
                campaign=session.campaign,
                referrer=random.choice(["google.com", "facebook.com", "direct", "email.com"]),
                device=session.device,
                browser=session.browser,
                operating_system=session.operating_system,
                experiment_group=session.experiment_group,
                is_logged_in=session.is_logged_in,
            )
            session.events.append(event)
            return event

        # State machine transition from last event
        last_event_type = session.events[-1].event_type
        next_type = self._transition_event_type(last_event_type, is_converting)

        if next_type is None:
            return None

        # Build the event payload
        page = self._get_page_for_event(next_type)
        category = self._get_category(session)
        product_id, cat_out, price = self._get_product(category)

        if next_type == "category_view":
            session.current_category = category

        quantity = 0
        cart_value = session.cart_value

        if next_type in ("add_to_cart", "purchase"):
            quantity = random.randint(1, 3)
            session.cart_value += price * quantity
            session.cart_items.append(
                {
                    "product_id": product_id,
                    "product_name": cat_out,
                    "price": price,
                    "quantity": quantity,
                }
            )
            cart_value = session.cart_value

        elif next_type == "remove_from_cart":
            if session.cart_items:
                removed = session.cart_items.pop()
                session.cart_value -= removed["price"] * removed["quantity"]
                cart_value = session.cart_value
                quantity = -removed["quantity"]

        elif next_type == "purchase":
            cart_value = session.cart_value

        # Track page visits
        if page not in session.pages_visited:
            session.pages_visited.append(page)
        if product_id and product_id not in session.products_viewed:
            session.products_viewed.append(product_id)
        if next_type == "search":
            session.searches += 1
        if next_type == "purchase":
            session.has_purchased = True

        event = ClickstreamEvent.create(
            event_type=next_type,
            user_id=session.user_id,
            session_id=session.session_id,
            page=page,
            country=session.country,
            city=session.city,
            traffic_source=session.traffic_source,
            campaign=session.campaign,
            referrer=random.choice(
                ["google.com", "facebook.com", "direct", "email.com", "twitter.com"]
            ),
            device=session.device,
            browser=session.browser,
            operating_system=session.operating_system,
            experiment_group=session.experiment_group,
            is_logged_in=session.is_logged_in,
            product_id=product_id,
            category=category,
            price=price,
            quantity=quantity,
            cart_value=cart_value,
            metadata={
                "session_depth": session.session_depth,
                "page_load_time_ms": random.randint(100, 5000),
                "viewport_width": random.choice([375, 414, 768, 1024, 1440, 1920]),
            },
        )

        session.events.append(event)
        return event

    def generate_event(self) -> ClickstreamEvent | None:
        """Generate a single clickstream event."""
        max_attempts = 100
        for _ in range(max_attempts):
            if self.sessions and random.random() < 0.85:
                user_id = random.choice(list(self.sessions.keys()))
            else:
                user_id = f"user_{random.randint(1, self.config.num_users):06d}"

            session = self._get_or_create_session(user_id)
            event = self._get_next_event(session)

            if event is not None:
                return event

            self.sessions.pop(user_id, None)

        return None

    def generate_batch(self, batch_size: int) -> list[ClickstreamEvent]:
        """Generate a batch of events."""
        events: list[ClickstreamEvent] = []
        for _ in range(batch_size):
            event = self.generate_event()
            if event is not None:
                events.append(event)
        return events

    def should_burst(self) -> bool:
        """Determine if we should generate a traffic burst."""
        if self.config.burst_mode and random.random() < 0.05:
            return True
        if self.config.black_friday_mode:
            return random.random() < 0.02
        return random.random() < 0.003

    def get_burst_multiplier(self) -> int:
        """Get traffic burst multiplier based on current mode."""
        if self.config.black_friday_mode:
            base = self.config.burst_multiplier if self.config.burst_mode else 3
            if random.random() < 0.02:
                return base * 5
            return base * 3
        if self.config.burst_mode and self.should_burst():
            base = self.config.burst_multiplier
            if random.random() < 0.01:
                return base * 4
            return base
        if random.random() < 0.003:
            return 4  # Rare flash sale spike
        return 1 if random.random() < 0.99 else 2
