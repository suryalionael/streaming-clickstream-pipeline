"""Tests for the dashboard module."""

import pandas as pd


class TestDashboardComponents:
    def test_overview_render(self) -> None:
        from dashboard.components.overview import render_overview

        metrics = {
            "events_per_second": 42.5,
            "active_sessions": 150,
            "active_users": 85,
            "today_revenue": 1234.56,
            "today_conversion_rate": 3.5,
            "total_events": 50000,
            "total_purchases": 175,
        }
        # Should not raise
        render_overview(metrics)

    def test_overview_with_empty_metrics(self) -> None:
        from dashboard.components.overview import render_overview

        render_overview({})

    def test_overview_with_partial_metrics(self) -> None:
        from dashboard.components.overview import render_overview

        render_overview({"events_per_second": 10})


class TestMetricsStore:
    def test_query_execution(self) -> None:
        from storage.queries import MetricsStore

        store = MetricsStore(db_path=":memory:")
        store.connect()

        result = store.query("SELECT 1 AS test")
        assert len(result) == 1
        assert result.iloc[0]["test"] == 1

        store.close()

    def test_live_metrics_empty(self) -> None:
        from storage.queries import MetricsStore

        store = MetricsStore(db_path=":memory:")
        store.connect()

        metrics = store.get_live_metrics()
        assert isinstance(metrics, dict)
        assert "events_per_second" in metrics
        assert metrics["events_per_second"] == 0

        store.close()

    def test_funnel_data_empty(self) -> None:
        from storage.queries import MetricsStore

        store = MetricsStore(db_path=":memory:")
        store.connect()

        df = store.get_funnel_data()
        assert isinstance(df, pd.DataFrame)

        store.close()

    def test_method_error_handling(self) -> None:
        from storage.queries import MetricsStore

        store = MetricsStore(db_path=":memory:")
        store.connect()

        # Individual methods handle errors gracefully
        metrics = store.get_live_metrics()
        assert isinstance(metrics, dict)

        funnel = store.get_funnel_data()
        assert isinstance(funnel, pd.DataFrame)

        products = store.get_top_products()
        assert isinstance(products, pd.DataFrame)

        store.close()

    def test_query_execution_basic(self) -> None:
        from storage.queries import MetricsStore

        store = MetricsStore(db_path=":memory:")
        store.connect()
        result = store.query("SELECT 1 AS val, 'test' AS name")
        assert len(result) == 1
        assert result.iloc[0]["val"] == 1
        assert result.iloc[0]["name"] == "test"
        store.close()

    def test_connection_close(self) -> None:
        from storage.queries import MetricsStore

        store = MetricsStore(db_path=":memory:")
        store.connect()
        store.close()
        # Should not raise when closing twice
        store.close()
