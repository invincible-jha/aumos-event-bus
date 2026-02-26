# AumOS Event Bus

Apache Kafka-based event streaming backbone for the AumOS enterprise AI platform.
Provides topic lifecycle management, Protobuf schema registry integration,
tenant-aware partitioning, and dead letter queue management for all AumOS services.

## Overview

Every AumOS service publishes and consumes Kafka events. The Event Bus management
service is the control plane for that infrastructure:

- **Topic management** — provision, configure, and retire Kafka topics via REST API
- **Schema registry** — register Protobuf schemas with Confluent Schema Registry; enforce compatibility
- **Tenant partitioning** — deterministic SHA-256-based assignment ensures all events for a tenant land on the same partition, guaranteeing per-tenant ordering without global ordering overhead
- **Dead letter queue** — capture failed messages, inspect them, and retry or resolve with exponential backoff

## Architecture

```
aumos-common ──┐
aumos-proto ───┤
               ▼
       aumos-event-bus          ← this service
       ┌──────────────────┐
       │  FastAPI REST API │
       │  api/router.py   │
       └────────┬─────────┘
                │
       ┌────────▼──────────────────────┐
       │  core/services.py             │
       │  TopicManagementService       │
       │  SchemaValidationService      │
       │  DLQManagementService         │
       └────────┬──────────────────────┘
                │
   ┌────────────┼────────────────┐
   ▼            ▼                ▼
adapters/    adapters/        adapters/
repositories kafka_admin.py   schema_registry.py
.py          (confluent-kafka) (httpx → SR REST)
(SQLAlchemy)
```

## Getting Started

### Prerequisites

- Python 3.11+
- Docker + Docker Compose
- Access to AumOS internal PyPI (for `aumos-common` and `aumos-proto`)

### Local Development

```bash
# Install dependencies
make install

# Copy and configure environment
cp .env.example .env
# Edit .env with your local broker and registry settings

# Start local Kafka (KRaft), Schema Registry, and PostgreSQL
make docker-run

# Run the service
uvicorn aumos_event_bus.main:app --reload --host 0.0.0.0 --port 8000
```

### Verify Setup

```bash
make lint       # Ruff — should pass with no errors
make typecheck  # mypy strict — should pass
make test       # pytest with coverage
```

## API Reference

All endpoints require a Bearer JWT token and `X-Tenant-ID` header.

### Topics

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/topics` | Create and provision a Kafka topic |
| `GET` | `/api/v1/topics` | List topics for the current tenant |
| `GET` | `/api/v1/topics/{topic_id}` | Get a topic definition |
| `DELETE` | `/api/v1/topics/{topic_id}` | Delete a topic |
| `GET` | `/api/v1/topics/{topic_id}/metrics` | Get live topic metrics |

### Schemas

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/schemas` | Register a Protobuf schema |
| `GET` | `/api/v1/schemas/{subject}` | Get latest schema version |
| `GET` | `/api/v1/schemas/{subject}/versions` | List all schema versions |
| `POST` | `/api/v1/schemas/{subject}/compatibility` | Check schema compatibility |

### Dead Letter Queue

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/dlq` | List pending DLQ entries |
| `POST` | `/api/v1/dlq/{entry_id}/retry` | Schedule entry for immediate retry |
| `POST` | `/api/v1/dlq/{entry_id}/resolve` | Mark entry as resolved |
| `POST` | `/api/v1/dlq/{entry_id}/abandon` | Abandon entry (no further retries) |

### Partitioning

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/partitions/{topic_name}/tenant/{tenant_id}` | Get partition assignment for a tenant |

## Tenant-Aware Partitioning

The `TenantPartitioner` provides deterministic partition assignment:

```python
from aumos_event_bus.core.tenant_partitioner import TenantPartitioner

partitioner = TenantPartitioner(seed=42)
assignment = partitioner.assign("tenant-uuid-1234", "aumos.agents", partitions=12)
# assignment.partition → e.g., 7 (stable across calls)
```

**Important:** The `tenant_partitioner_seed` setting MUST NOT change after topics are
provisioned. Changing it will shift all tenant-to-partition mappings and break ordering.

## Dead Letter Queue

Messages that fail processing are captured in the DLQ with exponential backoff retry:

- Initial backoff: 1 second
- Max backoff: 60 seconds
- Backoff multiplier: 2× per retry
- Max retries: 5 (configurable)

After `max_retries` attempts, entries are automatically marked `ABANDONED`.
Operators can manually `resolve` or `abandon` entries via the management API.

## Configuration

All settings use the `AUMOS_EVENTBUS_` prefix. See `.env.example` for the full list.

Key settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `AUMOS_KAFKA__BROKERS` | `localhost:9092` | Kafka broker addresses |
| `AUMOS_KAFKA__SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry URL |
| `AUMOS_EVENTBUS_DLQ_MAX_RETRIES` | `5` | Maximum DLQ retry attempts |
| `AUMOS_EVENTBUS_TENANT_PARTITIONER_SEED` | `42` | Partition hash seed (do not change) |
| `AUMOS_EVENTBUS_SCHEMA_COMPATIBILITY_DEFAULT` | `BACKWARD` | Default compatibility mode |

## Development

```bash
make lint        # Ruff lint + format check
make format      # Auto-format with ruff
make typecheck   # mypy strict type checking
make test        # Full test suite with coverage
make docker-build # Build Docker image
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
