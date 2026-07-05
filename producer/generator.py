"""Realistic synthetic clickstream event generator."""

import logging
import random
import time
from datetime import datetime, timezone

from producer.config import GeneratorConfig
from producer.models import ClickstreamEvent, SessionState

logger = logging.getLogger(__name__)


def weighted_choice(items: list[dict], weight_key: str = "weight") -> dict:
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
        self.user_agents: dict[str, tuple[str, str, str]] = {}
        self.base_time = datetime.now(timezone.utc)

    def _get_or_create_session(self, user_id: str) -> SessionState:
        if user_id in self.sessions:
            session = self.sessions[user_id]
            if session.has_purchased or session.session_depth > random.randint(5, 20):
                # Session expired or completed
                if random.random() < 0.7:
                    del self.sessions[user_id]
                    return self._create_new_session(user_id)
            return session

        if len(self.sessions) > self.config.num_users * 3:
            # Clean up oldest sessions
            oldest = sorted(self.sessions.keys(), key=lambda k: self.sessions[k].started_at)[
                : len(self.sessions) // 4
            ]
            for k in oldest:
                del self.sessions[k]

        return self._create_new_session(user_id)

    def _create_new_session(self, user_id: str) -> SessionState:
        country_info = weighted_choice(self.config.countries)
        source_info = weighted_choice(self.config.traffic_sources)

        device = random.choice(self.config.devices)
        browser = random.choice(self.config.browsers)
        os_map = {
            "mobile": ["iOS", "Android"],
            "desktop": ["Windows", "macOS", "Linux"],
            "tablet": ["iOS", "Android"],
        }
        # Device-appropriate OS selection
        possible_os = os_map.get(device, ["Windows", "macOS"])
        operating_system = random.choice(possible_os)

        session_id = f"sess_{user_id}_{int(time.time() * 1000000)}"

        # Evening/weekend users more likely to be on mobile
        if random.random() < 0.3:
            device = "mobile"
            browser = "Chrome" if random.random() < 0.6 else "Safari"
            operating_system = "iOS" if random.random() < 0.5 else "Android"

        # Mobile-heavy traffic (60% mobile overall)
        if random.random() < 0.6:
            device = "mobile"
            browser = random.choice(self.config.browsers[:3])
            operating_system = random.choice(["iOS", "Android"])

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
            "direct": ["organic", "direct", "none"],
            "organic_search": ["organic_seo", "brand_search", "generic_search"],
            "paid_search": ["google_ads", "bing_ads", "shopping_campaign"],
            "social_media": ["instagram_campaign", "facebook_ads", "tiktok_promo", "twitter_ad"],
            "email": ["newsletter", "promo_email", "abandoned_cart", "welcome_series"],
            "referral": ["blog_referral", "partner_link", "review_site"],
        }
        return random.choice(campaigns.get(source, ["organic"]))

    def _get_page_for_event(self, event_type: str, session: SessionState) -> str:
        pages = {
            "page_view": [
                "/",
                "/products",
                "/categories",
                "/about",
                "/contact",
                "/sale",
                "/new-arrivals",
                "/deals",
            ],
            "search": ["/search"],
            "category_view": [
                "/category/electronics",
                "/category/clothing",
                "/category/home_garden",
            ],
            "product_view": ["/product/"],
            "add_to_cart": ["/product/"],
            "remove_from_cart": ["/cart"],
            "begin_checkout": ["/checkout"],
            "payment": ["/checkout/payment"],
            "purchase": ["/checkout/confirmation"],
            "logout": ["/logout"],
        }
        base = pages.get(event_type, ["/"])
        return random.choice(base)

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
        # Realistic price ranges by category
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

    def _get_next_event(self, session: SessionState) -> ClickstreamEvent | None:
        """Determine the next event in the user journey based on current state."""
        # Session depth tracking
        session.session_depth += 1

        # Abandonment check
        if session.session_depth > 2 and random.random() < self.config.abandonment_rate * 0.1:
            if not session.has_purchased and random.random() < 0.3:
                return None  # User leaves

        # Conversion check
        is_converting = random.random() < self.config.conversion_rate
        source_info = next(
            (s for s in self.config.traffic_sources if s["source"] == session.traffic_source),
            None,
        )
        if source_info:
            is_converting = random.random() < (
                self.config.conversion_rate + source_info["conversion_bonus"]
            )

        # State machine for user journey
        if not session.events:
            event_type = "page_view"
            page = random.choice(["/", "/products", "/new-arrivals", "/sale"])
            event = ClickstreamEvent.create(
                event_type=event_type,
                user_id=session.user_id,
                session_id=session.session_id,
                page=page,
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

        last_event = session.events[-1]
        event_type = last_event.event_type

        # Journey progression
        # After page_view -> could search, browse category, or view product
        if event_type == "page_view":
            r = random.random()
            if r < 0.3:
                event_type = "search"
            elif r < 0.55:
                event_type = "category_view"
            elif r < 0.75:
                event_type = "product_view"
            else:
                event_type = "page_view"

        elif event_type == "search":
            if random.random() < 0.6:
                event_type = "product_view"
            elif random.random() < 0.3:
                event_type = "category_view"
            else:
                event_type = "page_view"

        elif event_type == "category_view":
            if random.random() < 0.6:
                event_type = "product_view"
            elif random.random() < 0.2:
                event_type = "search"
            else:
                event_type = "page_view"

        elif event_type == "product_view":
            if random.random() < 0.3:
                event_type = "add_to_cart"
            elif random.random() < 0.2:
                event_type = "product_view"  # View another product
            elif random.random() < 0.1:
                event_type = "category_view"
            else:
                event_type = "page_view"

        elif event_type == "add_to_cart":
            if random.random() < 0.4:
                event_type = "product_view"
            elif random.random() < 0.2:
                event_type = "remove_from_cart"
            elif random.random() < 0.2 and is_converting:
                event_type = "begin_checkout"
            else:
                event_type = "page_view"

        elif event_type == "remove_from_cart":
            if random.random() < 0.4:
                event_type = "product_view"
            elif random.random() < 0.2 and is_converting:
                event_type = "add_to_cart"
            elif random.random() < 0.2:
                event_type = "begin_checkout"
            else:
                event_type = "page_view"

        elif event_type == "begin_checkout":
            if random.random() < 0.8 and is_converting:
                event_type = "payment"
            elif random.random() < 0.1:
                event_type = "page_view"
            else:
                event_type = "add_to_cart"

        elif event_type == "payment":
            if is_converting:
                event_type = "purchase"
            else:
                event_type = "page_view"  # Payment failed, go back

        elif event_type == "purchase":
            session.has_purchased = True
            event_type = "logout" if random.random() < 0.5 else "page_view"

        elif event_type == "logout":
            return None  # Session ends

        else:
            event_type = "page_view"

        # Generate event details
        page = self._get_page_for_event(event_type, session)
        category = self._get_category(session)
        product_id, cat, price = self._get_product(category)

        # Update session category if browsing a category
        if event_type == "category_view":
            session.current_category = category

        quantity = 0
        cart_value = session.cart_value

        if event_type in ("add_to_cart", "purchase"):
            quantity = random.randint(1, 3)
            session.cart_value += price * quantity
            session.cart_items.append(
                {"product_id": product_id, "price": price, "quantity": quantity}
            )
            cart_value = session.cart_value

        elif event_type == "remove_from_cart":
            if session.cart_items:
                removed = session.cart_items.pop()
                session.cart_value -= removed["price"] * removed["quantity"]
                cart_value = session.cart_value
                quantity = -removed["quantity"]

        elif event_type == "purchase":
            cart_value = session.cart_value

        elif event_type == "begin_checkout":
            pass

        # Track pages/products visited
        if page not in session.pages_visited:
            session.pages_visited.append(page)
        if product_id and product_id not in session.products_viewed:
            session.products_viewed.append(product_id)
        if event_type == "search":
            session.searches += 1

        event = ClickstreamEvent.create(
            event_type=event_type,
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
            if self.sessions and random.random() < 0.8:
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
        return random.random() < 0.01  # Random spikes

    def get_burst_multiplier(self) -> int:
        base = self.config.burst_multiplier if self.config.burst_mode else 2
        if self.config.black_friday_mode:
            return base * 3
        if random.random() < 0.003:
            return base * 4  # Flash sale
        return base if self.should_burst() else 1
