# Final Engineering Review: Streaming Clickstream Pipeline

## Architecture Overview

```
┌────────────┐    ┌────────────┐    ┌──────────────────┐    ┌──────────┐    ┌───────────┐
│  Producer  │───▶│   Kafka    │───▶│  Spark Streaming  │───▶│Δ Delta   │───▶│  DuckDB   │
│   (Faker)  │    │ (Broker)   │    │ (Structured Str.) │    │  Lake    │    │ (Analytics)│
└────────────┘    └────────────┘    └──────────────────┘    └──────────┘    └───────────┘
                                          │                                          │
                                          ▼                                          ▼
                                   ┌──────────────┐                          ┌───────────┐
                                   │ Dead Letter   │                          │  Streamlit │
                                   │   Delta Table │                          │ Dashboard  │
                                   └──────────────┘                          └───────────┘
```

- **Medallion Architecture**: Bronze (raw) → Silver (clean, enriched, deduped) → Gold (aggregated metrics)
- **Streaming**: Spark Structured Streaming with `foreachBatch`, single Kafka consumer group
- **Storage**: Delta Lake on MinIO (S3-compatible) with Hive-style partitioning (year/month/day/hour)
- **Serving**: DuckDB reads Delta tables as Parquet; Streamlit renders Plotly visualizations
- **Orchestration**: Docker Compose (7 services) with health checks and depends_on

## Layer Details

### Producer (`producer/`)
- Synthetic clickstream event generation using Faker, ~50 events/sec
- State machine transition model: users browse → search → view → cart → checkout → purchase
- Burst mode (0.3% probability) amplifies event rate
- Thread-safe Kafka producer with graceful shutdown via signal handlers

### Bronze (`spark/streaming.py`: `write_to_bronze`)
- Parses JSON from Kafka, applies schema, drops null event_ids
- Adds `ingestion_timestamp` (current time) and partition columns
- Writes raw events partitioned by `year/month/day/hour`

### Silver (`spark/streaming.py`: `write_to_silver`)
- Time range filtering (drops events outside `[now - 7d, now + 1d]`)
- Parse, validate, clean & enrich (user agent → device/browser/OS, referrer → source)
- Deduplication (drops duplicate `(event_id, event_timestamp)` within window)
- Session flag columns (`is_entry`, `is_exit`, `is_bounce`, `session_number`)
- Partition columns and ingestion timestamp

### Gold (`spark/streaming.py`: `write_to_gold`)
- **Funnel Metrics**: Sliding windows tracking page_views → checkout → purchase conversion with abandonment rate, avg cart value, total revenue
- **Product Performance**: Per-product metrics: views, add_to_carts, purchases, revenue, conversion rate
- **Traffic Analytics**: Per-window aggregates: visits, users, sessions, events/sec, device/country/source breakdown

### Dead Letter Queue (`spark/streaming.py`: `write_to_dead_letter`)
- Captures raw Kafka messages that fail JSON parsing or schema validation
- Stores: raw key, raw value, topic, partition, offset, failure reason, failure timestamp
- Partitioned by `year/month/day/hour`, stored as Delta table

## Production Readiness Checklist

| Criteria | Status | Notes |
|---|---|---|
| Graceful shutdown | ✅ | Signal handlers (SIGTERM/SIGINT) in producer and Spark |
| Data validation | ✅ | Schema enforcement, null checks, time range filtering |
| Deduplication | ✅ | Event_id + timestamp dedup in Silver layer |
| Dead letter queue | ✅ | Invalid messages persisted for reprocessing |
| Checkpointing | ✅ | Exactly-once semantics via Delta + checkpoint dir |
| Partitioning | ✅ | Hive-style year/month/day/hour on all tables |
| Schema evolution | ✅ | `mergeSchema=true` on Delta writes |
| Monitoring | ✅ | Structured logging, dashboard pipeline status |
| Container health checks | ✅ | All 7 services have health checks |
| Resource limits | ⚠️ | Not set in docker-compose (add for production) |
| Secrets management | ⚠️ | Default creds in docker-compose (use .env for prod) |
| AuthN/AuthZ | ❌ | No authentication on Kafka, MinIO, or Streamlit |
| TLS | ❌ | Not configured (add for production) |
| Alerting | ❌ | No alert rules (add PagerDuty/Prometheus) |

## Streaming Guarantees

- **At-least-once**: Delta Lake writes + checkpointing ensure no data loss on restart
- **Exactly-once** (effectively): `foreachBatch` with idempotent Delta writes + Kafka offset tracking
- **Out-of-order handling**: Event timestamp vs ingestion timestamp tracked; time range filter drops events >7d late
- **Watermark**: 10-minute watermark delay for late-arriving events in sliding window aggregations
- **Failover**: Consumer group rebalancing on Spark restart picks up from last committed offset

## Test Coverage

| Suite | Tests | Scope |
|---|---|---|
| Unit (models, config, transforms) | 44 | State machine, event validation, JSON serialization |
| Edge cases | 18 | Malformed JSON, nulls, duplicates, empty batches, late data, schema evolution |
| Spark integration | 6 | Silver/Gold transforms with SparkSession (skipif no Spark) |
| Docker smoke | 6 | Service health, pipeline end-to-end (skipif no Docker) |
| **Total** | **74** | |

## Outstanding Issues

- `spark.sql.adaptive.enabled` warning in Spark logs (benign; AQE is not supported in streaming mode)
- Delta checkpoint cleanup may need VACUUM policy for long-running pipelines
- Docker Compose uses `depends_on` which does not wait for services to be ready (handled via health checks + retries)

## Overall Rating: **8.5/10**

A well-structured streaming pipeline following medallion architecture best practices. Production-ready for development/staging environments. Recommended enhancements for production: secrets management, resource limits, authentication/TLS, and monitoring/alerting integration.
