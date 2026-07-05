"""Configuration management for the clickstream producer."""

import os
from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_CLICKSTREAM", "clickstream-events")
    )
    dead_letter_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_DEAD_LETTER", "clickstream-dead-letter")
    )
    partitions: int = int(os.getenv("KAFKA_PARTITIONS", "6"))
    replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))

    @property
    def producer_config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": "all",
            "retries": 5,
            "batch.num.messages": 1000,
            "linger.ms": 10,
            "compression.type": "snappy",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.ms": 100,
        }


@dataclass
class GeneratorConfig:
    events_per_second: int = int(os.getenv("EVENTS_PER_SECOND", "50"))
    num_users: int = int(os.getenv("NUM_USERS", "1000"))
    abandonment_rate: float = float(os.getenv("ABANDONMENT_RATE", "0.7"))
    conversion_rate: float = float(os.getenv("CONVERSION_RATE", "0.05"))
    burst_mode: bool = os.getenv("BURST_MODE", "false").lower() == "true"
    burst_multiplier: int = int(os.getenv("BURST_MULTIPLIER", "3"))
    black_friday_mode: bool = os.getenv("BLACK_FRIDAY_MODE", "false").lower() == "true"

    # Product catalog - realistic e-commerce categories and products
    categories: list[str] = field(
        default_factory=lambda: [
            "electronics",
            "clothing",
            "home_garden",
            "books",
            "sports",
            "beauty",
            "toys",
            "groceries",
            "automotive",
            "office",
        ]
    )

    countries: list[dict[str, Any]] = field(
        default_factory=lambda: [
            {"country": "United States", "city": "New York", "weight": 25},
            {"country": "United States", "city": "San Francisco", "weight": 15},
            {"country": "United States", "city": "Chicago", "weight": 10},
            {"country": "United Kingdom", "city": "London", "weight": 12},
            {"country": "Germany", "city": "Berlin", "weight": 8},
            {"country": "France", "city": "Paris", "weight": 8},
            {"country": "Canada", "city": "Toronto", "weight": 6},
            {"country": "Australia", "city": "Sydney", "weight": 5},
            {"country": "India", "city": "Mumbai", "weight": 5},
            {"country": "Brazil", "city": "São Paulo", "weight": 4},
            {"country": "Japan", "city": "Tokyo", "weight": 2},
        ]
    )

    traffic_sources: list[dict[str, Any]] = field(
        default_factory=lambda: [
            {"source": "direct", "weight": 25, "conversion_bonus": 0.02},
            {"source": "organic_search", "weight": 25, "conversion_bonus": 0.01},
            {"source": "paid_search", "weight": 15, "conversion_bonus": 0.0},
            {"source": "social_media", "weight": 15, "conversion_bonus": -0.01},
            {"source": "email", "weight": 10, "conversion_bonus": 0.03},
            {"source": "referral", "weight": 10, "conversion_bonus": 0.015},
        ]
    )

    products: dict[str, list[str]] = field(
        default_factory=lambda: {
            "electronics": [
                "Smartphone X1",
                "Laptop Pro 15",
                "Wireless Earbuds",
                "4K Monitor 27",
                "Mechanical Keyboard",
                "Gaming Mouse",
                "USB-C Hub",
                "External SSD 1TB",
                "Webcam HD Pro",
                "Smart Watch Series 5",
            ],
            "clothing": [
                "Classic Denim Jacket",
                "Merino Wool Sweater",
                "Running Shoes Pro",
                "Cotton T-Shirt Pack",
                "Leather Belt",
                "Wool Scarf",
                "Slim Fit Chinos",
                "Winter Parka",
                "Canvas Sneakers",
                "Silk Tie",
            ],
            "home_garden": [
                "Indoor Plant Set",
                "Smart Thermostat",
                "Robot Vacuum V3",
                "Cast Iron Cookware",
                "LED Desk Lamp",
                "Memory Foam Pillow",
                "Bamboo Cutting Board",
                "French Press",
                "Plant Pots Set",
                "Smart Light Bulbs",
            ],
            "books": [
                "Data Engineering at Scale",
                "Machine Learning Fundamentals",
                "Clean Architecture",
                "The Great Novel",
                "Cooking for Everyone",
                "Python for Data Analysis",
                "Designing Data-Intensive Apps",
            ],
            "sports": [
                "Yoga Mat Premium",
                "Resistance Bands Set",
                "Protein Powder 2kg",
                "Water Bottle 1L",
                "Foam Roller",
                "Jump Rope Speed",
                "Adjustable Dumbbells",
                "Exercise Bike",
            ],
        }
    )

    devices: list[str] = field(
        default_factory=lambda: [
            "mobile",
            "mobile",
            "mobile",
            "desktop",
            "desktop",
            "tablet",
        ]
    )

    browsers: list[str] = field(
        default_factory=lambda: [
            "Chrome",
            "Chrome",
            "Chrome",
            "Safari",
            "Safari",
            "Firefox",
            "Edge",
        ]
    )

    operating_systems: list[str] = field(
        default_factory=lambda: [
            "iOS",
            "Android",
            "Android",
            "Windows",
            "macOS",
            "Windows",
            "Linux",
        ]
    )

    event_types: list[str] = field(
        default_factory=lambda: [
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
        ]
    )

    experiment_groups: list[str] = field(
        default_factory=lambda: [
            "control",
            "variant_a",
            "variant_b",
        ]
    )


@dataclass
class LoggingConfig:
    level: str = os.getenv("LOG_LEVEL", "INFO")
    format: Literal["json", "text"] = os.getenv("LOG_FORMAT", "json")  # type: ignore


@dataclass
class AppConfig:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


config = AppConfig()
