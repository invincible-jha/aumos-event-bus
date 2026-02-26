"""Event Bus service-specific settings extending AumOS base configuration.

All standard AumOS configuration (database, Kafka, Keycloak, OTEL) is
inherited from AumOSSettings. Settings here control the management API,
schema registry integration, and topic provisioning behaviour.
"""

from pydantic_settings import SettingsConfigDict

from aumos_common.config import AumOSSettings


class Settings(AumOSSettings):
    """Configuration for the AumOS Event Bus management service.

    Extends base AumOS settings with event-bus-specific configuration
    for schema registry, topic defaults, DLQ handling, and audit archival.

    Environment variable prefix: AUMOS_EVENTBUS_
    """

    service_name: str = "aumos-event-bus"

    # -------------------------------------------------------------------------
    # Confluent Schema Registry
    # -------------------------------------------------------------------------
    schema_registry_url: str = "http://localhost:8081"
    schema_registry_username: str = ""
    schema_registry_password: str = ""
    schema_compatibility_default: str = "BACKWARD"

    # -------------------------------------------------------------------------
    # Kafka topic provisioning defaults
    # -------------------------------------------------------------------------
    topic_default_replication_factor: int = 3
    topic_default_min_isr: int = 2
    topic_default_compression_type: str = "lz4"
    topic_default_segment_bytes: int = 1_073_741_824  # 1 GB
    topic_creation_timeout_ms: int = 30_000

    # -------------------------------------------------------------------------
    # Tenant partitioning
    # -------------------------------------------------------------------------
    tenant_partitioner_seed: int = 42
    max_tenants_per_partition: int = 10

    # -------------------------------------------------------------------------
    # Dead Letter Queue
    # -------------------------------------------------------------------------
    dlq_max_retries: int = 5
    dlq_initial_backoff_ms: int = 1_000
    dlq_max_backoff_ms: int = 60_000
    dlq_backoff_multiplier: float = 2.0
    dlq_retention_ms: int = 2_592_000_000  # 30 days

    # -------------------------------------------------------------------------
    # Audit log archival (S3 sink)
    # -------------------------------------------------------------------------
    audit_s3_bucket: str = "aumos-audit-archive"
    audit_s3_prefix: str = "events/audit"
    audit_sink_flush_size: int = 10_000
    audit_sink_flush_interval_ms: int = 60_000

    # -------------------------------------------------------------------------
    # Management API
    # -------------------------------------------------------------------------
    management_api_enabled: bool = True
    auto_create_standard_topics: bool = True

    model_config = SettingsConfigDict(env_prefix="AUMOS_EVENTBUS_")
