# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] — 2026-07-05

### Added

- **Synthetic clickstream generator**: State-machine event generation producing ~50 events/sec across 10 event types with realistic customer journeys, burst mode, and Black Friday simulation.
- **Kafka ingestion**: Idempotent producer with exactly-once semantics, dead-letter topic, and delivery callbacks.
- **Spark Structured Streaming pipeline**: Event-time processing with 10-second micro-batches, watermarking, and foreachBatch writing all three medallion layers.
- **Delta Lake medallion architecture**: Bronze (raw) → Silver (clean, enriched, deduplicated) → Gold (aggregated funnel, product, and traffic metrics).
- **Dead-letter queue**: Invalid/unparseable Kafka messages captured with failure metadata for offline reprocessing.
- **DuckDB analytics layer**: Zero-config views over Delta tables, serving dashboard queries.
- **Streamlit dashboard**: 6-page real-time analytics with KPI sparklines, trend arrows, conversion funnels, geographic heatmaps, product tables, and pipeline health monitoring.
- **Pipeline observability page**: Live Delta Lake row counts, freshness timestamps, events-per-second, HTTP health checks for Kafka UI and MinIO, DuckDB connection status.
- **Docker Compose orchestration**: 7 containers (Zookeeper, Kafka, Kafka UI, MinIO, Producer, Spark, Dashboard) with health checks and dependency ordering.
- **CI/CD via GitHub Actions**: Ruff, Black, isort, mypy strict, unit tests, Docker build verification on every push.
- **Test suite**: 74 tests (unit, edge case, Spark integration, Docker smoke).

### Fixed

- Timestamp parsing in `parse_event_time` now handles microsecond fractional seconds (`.954725Z`) via regex stripping before `to_timestamp`.
- Bronze/Silver/Gold partition columns now populate correctly from event timestamps (fixing `__HIVE_DEFAULT_PARTITION__`).

### Security

- Default credentials documented as development-only; `.env.example` provided for configuration.
- Security policy established in `SECURITY.md`.
