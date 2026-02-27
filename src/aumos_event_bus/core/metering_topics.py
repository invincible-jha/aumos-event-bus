"""Kafka topic configurations for AumOS metering events.

Defines the canonical topic names, partition counts, replication factors,
and retention policies for all metering event streams. These configurations
are authoritative — the event bus management API reads them when provisioning
topics on startup (when auto_create_standard_topics=True).

Retention policy: 90 days for all metering topics to support quarterly billing
cycle look-backs and audit requirements.

Partition counts reflect expected throughput at enterprise scale:
  - api-calls and token-usage: high throughput → 12 partitions
  - model-inference, storage-ops, synthetic-records: moderate → 6 partitions

All topics use replication factor 3 (minimum ISR 2) for durability.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Retention constant
# ---------------------------------------------------------------------------

_90_DAYS_MS: int = 90 * 24 * 3600 * 1000  # 7_776_000_000 ms


# ---------------------------------------------------------------------------
# Metering topic definitions
# ---------------------------------------------------------------------------

METERING_TOPICS: list[dict] = [
    {
        "name": "aumos.metering.api-calls",
        "display_name": "Metering: API Calls",
        "description": (
            "Records every service API call with tenant, project, team context, "
            "HTTP method, path, status code, and response latency."
        ),
        "partitions": 12,
        "replication_factor": 3,
        "retention_ms": _90_DAYS_MS,
        "cleanup_policy": "delete",
        "compression_type": "lz4",
        "min_isr": 2,
        "is_system_topic": True,
    },
    {
        "name": "aumos.metering.token-usage",
        "display_name": "Metering: Token Usage",
        "description": (
            "Records LLM token consumption events including model ID, provider, "
            "input/output token counts, and estimated cost per request."
        ),
        "partitions": 12,
        "replication_factor": 3,
        "retention_ms": _90_DAYS_MS,
        "cleanup_policy": "delete",
        "compression_type": "lz4",
        "min_isr": 2,
        "is_system_topic": True,
    },
    {
        "name": "aumos.metering.model-inference",
        "display_name": "Metering: Model Inference",
        "description": (
            "Records GPU/CPU inference usage including latency, compute time, "
            "and hardware tier for non-token-based billing."
        ),
        "partitions": 6,
        "replication_factor": 3,
        "retention_ms": _90_DAYS_MS,
        "cleanup_policy": "delete",
        "compression_type": "lz4",
        "min_isr": 2,
        "is_system_topic": True,
    },
    {
        "name": "aumos.metering.storage-ops",
        "display_name": "Metering: Storage Operations",
        "description": (
            "Records storage read/write operations with byte counts and storage tier "
            "for object storage, vector store, and model artifact billing."
        ),
        "partitions": 6,
        "replication_factor": 3,
        "retention_ms": _90_DAYS_MS,
        "cleanup_policy": "delete",
        "compression_type": "lz4",
        "min_isr": 2,
        "is_system_topic": True,
    },
    {
        "name": "aumos.metering.synthetic-records",
        "display_name": "Metering: Synthetic Data Generation",
        "description": (
            "Records synthetic data generation events with record counts, "
            "data modality, and generation duration for usage-based billing."
        ),
        "partitions": 6,
        "replication_factor": 3,
        "retention_ms": _90_DAYS_MS,
        "cleanup_policy": "delete",
        "compression_type": "lz4",
        "min_isr": 2,
        "is_system_topic": True,
    },
]

# ---------------------------------------------------------------------------
# Topic name constants — use these in application code instead of raw strings
# ---------------------------------------------------------------------------


class MeteringTopics:
    """Canonical metering topic name constants.

    Usage:
        from aumos_event_bus.core.metering_topics import MeteringTopics
        publisher.publish(MeteringTopics.TOKEN_USAGE, event)
    """

    API_CALLS: str = "aumos.metering.api-calls"
    TOKEN_USAGE: str = "aumos.metering.token-usage"
    MODEL_INFERENCE: str = "aumos.metering.model-inference"
    STORAGE_OPS: str = "aumos.metering.storage-ops"
    SYNTHETIC_RECORDS: str = "aumos.metering.synthetic-records"

    @classmethod
    def all_topics(cls) -> list[str]:
        """Return all metering topic names.

        Returns:
            List of topic name strings in definition order.
        """
        return [
            cls.API_CALLS,
            cls.TOKEN_USAGE,
            cls.MODEL_INFERENCE,
            cls.STORAGE_OPS,
            cls.SYNTHETIC_RECORDS,
        ]
