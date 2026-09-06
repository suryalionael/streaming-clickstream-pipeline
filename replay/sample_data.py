"""Synthetic static sample data matching the Gold-layer schemas.

Used by replay.export when no local Delta Lake output exists, so a
snapshot is always available for the standalone dashboard to render.
Reuses the same category/country/traffic-source catalog as the live
producer (see producer/config.py) for realistic values.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pandas as pd

from producer.config import GeneratorConfig
from replay.config import config

_catalog = GeneratorConfig()


def _time_windows() -> list[tuple[datetime, datetime]]:
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    start = end - timedelta(hours=config.sample_hours)
    step = timedelta(minutes=config.window_minutes)

    windows = []
    cursor = start
    while cursor < end:
        windows.append((cursor, cursor + step))
        cursor += step
    return windows


def generate_funnel_metrics() -> pd.DataFrame:
    """Generate one funnel row per time window with a diurnal traffic curve."""
    rng = random.Random(config.random_seed)
    rows = []
    for window_start, window_end in _time_windows():
        hour_factor = 0.4 + 0.6 * (1 - abs(((window_start.hour + 6) % 24) - 12) / 12)
        page_views = int(rng.uniform(200, 600) * hour_factor)
        product_views = int(page_views * rng.uniform(0.4, 0.6))
        add_to_carts = int(product_views * rng.uniform(0.15, 0.3))
        checkout_starts = int(add_to_carts * rng.uniform(0.4, 0.6))
        purchases = int(checkout_starts * rng.uniform(0.5, 0.8))
        sessions = max(1, int(page_views * rng.uniform(0.2, 0.3)))
        unique_users = int(sessions * rng.uniform(0.85, 1.0))
        avg_cart_value = round(rng.uniform(40, 150), 2)
        total_revenue = round(purchases * avg_cart_value, 2)

        rows.append(
            {
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "page_views": page_views,
                "product_views": product_views,
                "add_to_carts": add_to_carts,
                "checkout_starts": checkout_starts,
                "purchases": purchases,
                "sessions": sessions,
                "unique_users": unique_users,
                "conversion_rate": round(purchases / sessions, 4) if sessions else 0.0,
                "abandonment_rate": round(1 - purchases / checkout_starts, 4)
                if checkout_starts
                else 0.0,
                "avg_cart_value": avg_cart_value,
                "total_revenue": total_revenue,
                "events_per_second": round(
                    (page_views + product_views + add_to_carts) / (config.window_minutes * 60), 2
                ),
                "year": window_start.year,
                "month": window_start.month,
                "day": window_start.day,
                "hour": window_start.hour,
            }
        )
    return pd.DataFrame(rows)


def generate_product_performance() -> pd.DataFrame:
    """Generate per-product performance rows across the last few windows."""
    rng = random.Random(config.random_seed + 1)
    rows = []
    windows = _time_windows()[-12:]
    for window_start, window_end in windows:
        for category, products in _catalog.products.items():
            for product_name in products:
                views = rng.randint(5, 200)
                add_to_carts = int(views * rng.uniform(0.1, 0.3))
                purchases = int(add_to_carts * rng.uniform(0.2, 0.5))
                revenue = round(purchases * rng.uniform(15, 300), 2)
                rows.append(
                    {
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "product_id": product_name.lower().replace(" ", "_"),
                        "category": category,
                        "views": views,
                        "add_to_carts": add_to_carts,
                        "purchases": purchases,
                        "revenue": revenue,
                        "conversion_rate": round(purchases / views, 4) if views else 0.0,
                        "year": window_start.year,
                        "month": window_start.month,
                        "day": window_start.day,
                        "hour": window_start.hour,
                    }
                )
    return pd.DataFrame(rows)


def generate_traffic_analytics() -> pd.DataFrame:
    """Generate country/source/device traffic rows across the last few windows."""
    rng = random.Random(config.random_seed + 2)
    rows = []
    windows = _time_windows()[-12:]
    devices = ("mobile", "desktop", "tablet")
    for window_start, window_end in windows:
        for country_info in _catalog.countries:
            for source_info in _catalog.traffic_sources:
                for device in devices:
                    visits = int(
                        rng.uniform(1, 40)
                        * (country_info["weight"] / 10)
                        * (source_info["weight"] / 10)
                    )
                    if visits <= 0:
                        continue
                    users = int(visits * rng.uniform(0.7, 0.95))
                    purchases = int(visits * rng.uniform(0.02, 0.08))
                    revenue = round(purchases * rng.uniform(20, 200), 2)
                    rows.append(
                        {
                            "window_start": window_start.isoformat(),
                            "window_end": window_end.isoformat(),
                            "country": country_info["country"],
                            "traffic_source": source_info["source"],
                            "device": device,
                            "visits": visits,
                            "users": users,
                            "purchases": purchases,
                            "revenue": revenue,
                            "conversion_rate": round(purchases / visits, 4) if visits else 0.0,
                            "year": window_start.year,
                            "month": window_start.month,
                            "day": window_start.day,
                            "hour": window_start.hour,
                        }
                    )
    return pd.DataFrame(rows)
