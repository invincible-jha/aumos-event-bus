# CLAUDE.md — AumOS Event Bus

## Project Overview

AumOS Enterprise is a composable enterprise AI platform with 9 products + 2 services
across 62 repositories. This repo (`aumos-event-bus`) is part of **Foundation Infrastructure**:
shared infrastructure services that every AumOS product depends on.

**Release Tier:** A (Fully Open)
**Product Mapping:** Foundation Service — Event Streaming Infrastructure
**Phase:** 1A (Months 1-4)

## Repo Purpose

Provides the Kafka-based event streaming backbone for all AumOS services. Manages
topic lifecycle, Protobuf schema registry integration with compatibility enforcement,
tenant-aware partition assignment (ensuring per-tenant message ordering), and a
dead letter queue system with configurable exponential backoff retry.

## Architecture Position

```
aumos-common ──────────────────────────────────────────┐
aumos-proto ────────────────────────────────────────────┤
                                                         ▼
                                          aumos-event-bus (THIS REPO)
                                                         │
                        ┌────────────────────────────────┤
                        ▼                ▼               ▼
               aumos-platform-core  aumos-data-layer  aumos-auth-gateway
               aumos-model-registry aumos-observability (all services)
```

**Upstream dependencies (this repo IMPORTS from):**
- `aumos-common` — auth, database, events, errors, config, health, pagination
- `aumos-proto` — Protobuf message definitions for Kafka events

**Downstream dependents (other repos IMPORT from this):**
- ALL AumOS repos — every service publishes and/or consumes Kafka events
- Services use the topic names, schema subjects, and partition keys this service defines

## Tech Stack (DO NOT DEVIATE)

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.11+ | Runtime |
| FastAPI | 0.110+ | REST API framework |
| SQLAlchemy | 2.0+ (async) | Database ORM |
| asyncpg | 0.29+ | PostgreSQL async driver |
| Pydantic | 2.6+ | Data validation, settings, API schemas |
| confluent-kafka | 2.3+ | Kafka AdminClient + producer/consumer |
| httpx | 0.27+ | Confluent Schema Registry REST client |
| structlog | 24.1+ | Structured JSON logging |
| OpenTelemetry | 1.23+ | Distributed tracing |
| pytest | 8.0+ | Testing framework |
| ruff | 0.3+ | Linting and formatting |
| mypy | 1.8+ | Type checking |

## Coding Standards

### ABSOLUTE RULES (violations will break integration with other repos)

1. **Import aumos-common, never reimplement.** If aumos-common provides it, use it.
   ```python
   from aumos_common.auth import get_current_tenant, get_current_user
   from aumos_common.database import get_db_session, Base, AumOSModel, BaseRepository
   from aumos_common.events import EventPublisher, Topics
   from aumos_common.errors import NotFoundError, ErrorCode
   from aumos_common.config import AumOSSettings
   from aumos_common.health import create_health_router
   from aumos_common.pagination import PageRequest, PageResponse, paginate
   from aumos_common.app import create_app
   ```

2. **Type hints on EVERY function.** No exceptions.

3. **Pydantic models for ALL API inputs/outputs.** Never return raw dicts from endpoints.

4. **RLS tenant isolation via aumos-common.** Never write raw SQL that bypasses RLS.

5. **Structured logging via structlog.** Never use `print()` or `logging.getLogger()`.

6. **Publish domain events to Kafka after state changes.**

7. **Async by default.** All I/O operations must be async.

8. **Google-style docstrings** on all public classes and functions.

### Style Rules

- Max line length: **120 characters**
- Import order: stdlib → third-party → aumos-common → local
- Linter: `ruff` (select E, W, F, I, N, UP, ANN, B, A, COM, C4, PT, RUF)
- Type checker: `mypy` strict mode
- Formatter: `ruff format`

### File Structure

```
src/aumos_event_bus/
├── __init__.py
├── main.py                   # FastAPI app entry point
├── settings.py               # Extends AumOSSettings (prefix: AUMOS_EVENTBUS_)
├── api/
│   ├── __init__.py
│   ├── router.py             # All API routes
│   └── schemas.py            # Pydantic request/response models
├── core/
│   ├── __init__.py
│   ├── models.py             # SQLAlchemy ORM models (table prefix: evt_)
│   ├── services.py           # Business logic (TopicMgmt, SchemaValidation, DLQMgmt)
│   ├── interfaces.py         # Protocol classes for adapters
│   └── tenant_partitioner.py # Deterministic tenant → partition mapping
└── adapters/
    ├── __init__.py
    ├── repositories.py       # SQLAlchemy repos (TopicRepo, SchemaRepo, DLQRepo)
    ├── kafka_admin.py        # KafkaAdminAdapter (confluent-kafka AdminClient)
    ├── schema_registry.py    # SchemaRegistryAdapter (httpx → Confluent SR REST)
    └── kafka.py              # EventBusEventPublisher (audit events)
```

## API Conventions

- All endpoints under `/api/v1/` prefix
- Auth: Bearer JWT token (validated by aumos-common)
- Tenant: `X-Tenant-ID` header (set by auth middleware)
- Request ID: `X-Request-ID` header (auto-generated if missing)
- Pagination: `?skip=0&limit=50`
- Errors: Standard `ErrorResponse` from aumos-common
- Content-Type: `application/json` (always)

## Database Conventions

- Table prefix: `evt_`
- ALL tenant-scoped tables extend `AumOSModel`
- `TenantPartitionMapping` extends `Base` directly (it IS the tenant mapping table)
- Migration naming: `{timestamp}_evt_{description}.py`

## Kafka Conventions

- Publish events via `EventPublisher` from aumos-common
- Use `Topics.*` constants for topic names
- Always include `tenant_id` in events
- Use `TenantPartitioner.get_partition_key(tenant_id)` as message key for tenant ordering
- Schema Registry subjects follow pattern: `{topic_name}-value`

## Repo-Specific Context

### Strimzi Kafka Operator
This service manages topics that are provisioned in a Kubernetes cluster running
Strimzi. Topic config aligns with Strimzi KafkaTopic CRD conventions.

### Protobuf Schema Registry
All schemas are PROTOBUF type by default. The `schema_definition` field expects
valid Protobuf IDL. Set `schema_type=PROTOBUF` explicitly. Schema compatibility
defaults to BACKWARD.

### Tenant-Aware Partitioning
`TenantPartitioner` uses SHA-256 (first 8 bytes) with a stable seed. The seed
(`tenant_partitioner_seed`) MUST NOT change after topics are provisioned, or
tenant-to-partition assignments will shift and break message ordering guarantees.

### DLQ Management
DLQ entries use exponential backoff. Background workers should call
`DLQRepository.get_entries_due_for_retry()` on a timer. The management API
provides manual retry/resolve/abandon operations for operators.

### Environment Variable Prefix
`AUMOS_EVENTBUS_` for all settings in this service (e.g., `AUMOS_EVENTBUS_SCHEMA_REGISTRY_URL`).

## What Claude Code Should NOT Do

1. **Do NOT reimplement anything in aumos-common.**
2. **Do NOT use print().** Use `get_logger(__name__)`.
3. **Do NOT return raw dicts from API endpoints.** Use Pydantic models.
4. **Do NOT write raw SQL.** Use SQLAlchemy ORM with BaseRepository.
5. **Do NOT hardcode configuration.** Use Pydantic Settings with env vars.
6. **Do NOT skip type hints.**
7. **Do NOT import AGPL/GPL licensed packages** without explicit approval.
8. **Do NOT put business logic in API routes.**
9. **Do NOT create new exception classes** unless adding to aumos-common ErrorCode.
10. **Do NOT bypass RLS.**
11. **Do NOT change the tenant_partitioner_seed** without a full resharding migration.
