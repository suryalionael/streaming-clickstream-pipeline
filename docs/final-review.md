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

## Production Readiness Assessment

### Strengths

- **Medallion Architecture**: Bronze → Silver → Gold separation provides clear data lineage, incremental quality improvement, and independent scaling of each layer.
- **Streaming Correctness**: Event-time processing with watermarking, exactly-once semantics via Delta Lake plus checkpointing, and idempotent writes ensure no data loss or duplication.
- **Fault Tolerance**: Graceful shutdown via signal handlers, consumer group rebalancing on restart, and failed-batch exception handling with structured logging.
- **Observability**: Structured JSON logging throughout, pipeline health status in the dashboard, and a dedicated dead-letter sink for unparseable messages.
- **Test Coverage**: 74 tests spanning unit, edge case, Spark integration, and Docker smoke suites; linting and type checking in CI.
- **Containerization**: All 7 services run in Docker Compose with health checks, persistent volumes, and dependency ordering.

### Architectural Decisions

- **Single `foreachBatch` query**: Writing Bronze, Silver, and Gold from one streaming query avoids multiple Kafka consumer groups and simplifies checkpoint management. The trade-off is that all three layers share the same processing cadence.
- **Delta Lake over raw Parquet**: Schema evolution, ACID transactions, and time travel justify the additional metadata overhead. For an analytics pipeline serving a dashboard, these features outweigh the simplicity of plain Parquet.
- **DuckDB for serving**: Lighter than a full warehouse, faster than querying Delta directly from Spark, and zero-config for the dashboard container. The trade-off is that views must be re-created on restart.
- **MinIO over cloud S3**: Enables reproducible local development. The S3A connector and MinIO are API-compatible, so switching to AWS S3 or GCS requires only config changes.

### Limitations

- **Authentication and Authorization**: Kafka, MinIO, and Streamlit run without authentication. In a production deployment, enable SASL/SCRAM for Kafka, IAM policies for S3, and SSO or basic auth for the dashboard.
- **TLS**: All inter-service communication is plaintext. For production, configure TLS for Kafka, MinIO, and the dashboard.
- **Resource Management**: Docker Compose services lack CPU/memory limits. In production, set resource reservations and limits to prevent noisy-neighbor issues.
- **Alerting**: No automated alert rules exist. Integrate with PagerDuty, OpsGenie, or Prometheus Alertmanager for production monitoring.
- **Delta Maintenance**: Long-running pipelines require periodic `VACUUM` and `OPTIMIZE` to manage small files and tombstone cleanup.

### Production-Inspired Practices

- **Idempotent Kafka Producer**: `enable.idempotence=true` prevents duplicate messages despite retries.
- **Structured Logging**: JSON-formatted logs with consistent keys enable ingestion into ELK, Datadog, or Splunk.
- **Dead Letter Queue**: Invalid messages are preserved with failure metadata, enabling offline analysis and reprocessing.
- **Schema Evolution**: Delta's `mergeSchema=true` allows fields to be added without pipeline downtime.
- **Partition Pruning**: Hive-style partitioning on all Delta tables enables efficient queries by time range.
- **Container Health Checks**: Each service defines a health check endpoint; Docker Compose respects startup dependencies.
- **Graceful Shutdown**: Signal handlers flush pending Kafka messages and close resources cleanly.

### Future Improvements

- Add RBAC and TLS for all services
- Set resource limits in Docker Compose or Kubernetes manifests
- Implement a Delta Lake maintenance job (VACUUM, OPTIMIZE, ZORDER)
- Add Prometheus metrics endpoints and Grafana dashboards
- Introduce CI/CD deployment to a cloud environment (AWS/GCP/Azure)
- Implement backpressure-aware producer rate limiting
- Add end-to-end data quality monitoring (row count SLAs, schema drift detection)
- Integrate a schema registry (e.g., Confluent Schema Registry or Apicurio)
