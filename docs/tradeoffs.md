# Design Trade-offs and Technical Decisions

## Pipeline Design

### Medallion Architecture (Bronze/Silver/Gold)

**Why three layers instead of two or four?**

| Layer | Purpose | Transformations |
|-------|---------|-----------------|
| Bronze | Raw data ingestion | Minimal: add ingestion timestamp, partition columns |
| Silver | Cleaned, validated data | Schema validation, type casting, null handling, deduplication |
| Gold | Business-level aggregates | Windowed aggregations, funnel metrics, product analytics |

**Trade-off**: Three layers increase storage (3x replication) but provide clear separation of concerns:
- Bronze: Source of truth, never modified
- Silver: Clean data for ad-hoc analysis
- Gold: Pre-computed metrics for dashboard performance

**Alternative considered**: Single-table design with tags/classification - simpler but creates tight coupling between raw storage and analytics.

### Micro-batch vs Continuous Processing

**Spark Structured Streaming uses micro-batch processing**

| Aspect | Micro-batch | Continuous Processing |
|--------|------------|----------------------|
| Latency | 100ms-10s | Sub-100ms |
| Throughput | Higher | Lower |
| Fault tolerance | Exactly-once | At-least-once |
| Complexity | Simple | Complex |

**Decision**: Micro-batch with 10-second intervals - balances latency requirements for dashboards with throughput needs for 50+ events/sec.

### 5-minute Tumbling Windows

**Why 5 minutes?**
- Smooths out short-term variations for meaningful funnel metrics
- Matches typical e-commerce reporting cadence
- Manageable number of output rows per window

**Trade-off**: Less granular than 1-minute windows but significantly fewer shuffle operations and less storage overhead.

## Infrastructure Decisions

### Docker Compose vs Kubernetes

**Docker Compose** chosen for:
- Simplicity: Single command to start all services
- Portfolio project: No need for orchestration complexity
- Local development: Reproducible environment without cloud costs

**Kubernetes** would be preferred for:
- Production deployment with auto-scaling
- Multi-node Spark clusters
- High availability with rolling updates

### MinIO vs Local Filesystem

**MinIO** chosen because:
- S3-compatible API enables future cloud migration
- Object storage semantics match Delta Lake expectations
- Built-in console for debugging
- Industry standard for data lakes

**Local filesystem** would be:
- Simpler but not cloud-portable
- Missing S3 API compatibility for Spark

### DuckDB vs Trino/Presto

**DuckDB** chosen for:
- Embedded: No separate server to manage
- Performance: Optimized for analytical queries
- Simple: Single binary, no cluster coordination
- Integration: Direct Python API for Streamlit

**Trino/Presto** would be better for:
- Multi-user workloads
- Querying across multiple data sources
- Larger-than-memory datasets

### JSON over Avro/Protobuf

**JSON** chosen for:
- Human-readable: Debugging and development simplicity
- Schema flexibility: Schema evolution without registry
- Python integration: Native `json` module, no serialization overhead

**Avro/Protobuf** would provide:
- Smaller message sizes (30-50% reduction)
- Stronger schema enforcement
- Schema registry for evolution management

## Generator Design

### State Machine vs Event Probability

**State machine** approach chosen:
- Tracks user session state (pages viewed, cart contents, purchase status)
- Transitions between states based on configurable probabilities
- Produces realistic customer journeys (browse → search → view → cart → checkout)

**Pure probability** approach would:
- Be simpler to implement
- Not capture session context dependencies
- Produce unrealistic event sequences (e.g., purchase without browsing)

### In-process vs External Session Store

**In-process session store** chosen because:
- Single producer process handles all sessions
- No external dependency (Redis/DynamoDB) needed
- Simpler for portfolio deployment

**External session store** would be needed for:
- Multi-producer deployments
- Preserving sessions across producer restarts
- Sharing session state across instances

## Dashboard Approach

### Streamlit vs Dash/Tableau

**Streamlit** chosen because:
- Python-native: Same language as pipeline code
- Quick prototyping: Minimal boilerplate
- Auto-refresh: Native caching and rerun
- Portfolio: Demonstrates full-stack Python capability

**Dash** would provide more customization, Tableau would need a separate server.

### DuckDB for Dashboard Data

**Why not query Delta Lake directly?**
- Delta Lake requires Spark for optimal read performance
- DuckDB provides faster ad-hoc queries through columnar engine
- DuckDB views over Delta Parquet files balance freshness with performance

## Testing Strategy

### Unit vs Integration Test Split

**Unit tests** cover:
- Generator logic and statistics
- Model validation and serialization
- Schema definitions
- Dashboard component rendering

**Integration/smoke tests** cover:
- End-to-end Docker compose deployment
- Kafka connectivity and topic creation
- Spark streaming with real Kafka data
- Dashboard HTTP health checks

**Trade-off**: Integration tests require Docker, making CI more complex but providing higher confidence.

## Security Considerations

This is a local development pipeline:
- No authentication between services (Kafka, MinIO)
- Plaintext communication (no TLS)
- Default credentials (minioadmin/minioadmin)

**Production changes needed**:
- Enable Kafka SSL/SASL authentication
- MinIO with TLS and strong credentials
- Network isolation between services
- Secrets management (Vault/HashiCorp)

## Monitoring and Observability

Current implementation provides:
- Structured JSON logging
- Producer metrics (messages/sec, errors)
- Kafka consumer lag monitoring
- Dead letter queue

**Missing** (future improvements):
- Prometheus metrics endpoint
- Grafana dashboards
- Distributed tracing
- Alerting rules
