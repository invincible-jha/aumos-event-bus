"""Pydantic request/response models for the Event Bus management API.

All API schemas are defined here. Routes in router.py import these models
to ensure typed inputs and outputs for every endpoint.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Topic schemas
# ---------------------------------------------------------------------------


class TopicCreateRequest(BaseModel):
    """Request body for creating a new Kafka topic."""

    topic_name: str = Field(..., min_length=1, max_length=255, description="Kafka topic name")
    display_name: str = Field(..., min_length=1, max_length=255)
    description: str = Field(default="")
    partitions: int = Field(default=6, ge=1, le=1024)
    replication_factor: int = Field(default=3, ge=1)
    retention_ms: int = Field(default=604_800_000, ge=1, description="Retention in milliseconds")
    cleanup_policy: str = Field(default="delete", pattern="^(delete|compact|delete,compact)$")
    compression_type: str = Field(default="lz4")
    min_isr: int = Field(default=2, ge=1)
    schema_subject: str | None = Field(default=None, max_length=255)
    config_overrides: dict[str, str] = Field(default_factory=dict)


class TopicResponse(BaseModel):
    """Response body for a single Kafka topic definition."""

    id: uuid.UUID
    tenant_id: str
    topic_name: str
    display_name: str
    description: str
    partitions: int
    replication_factor: int
    retention_ms: int
    cleanup_policy: str
    compression_type: str
    min_isr: int
    status: str
    is_system_topic: bool
    schema_subject: str | None
    config_overrides: dict[str, str]
    created_at: Any
    updated_at: Any


class TopicListResponse(BaseModel):
    """Paginated list of topic definitions."""

    items: list[TopicResponse]
    total: int
    skip: int
    limit: int


class TopicMetricsResponse(BaseModel):
    """Metrics for a specific Kafka topic."""

    topic_id: uuid.UUID
    topic_name: str
    partition_count: int
    message_count: int | None = None
    bytes_in_per_sec: float | None = None
    bytes_out_per_sec: float | None = None
    consumer_groups: list[dict[str, Any]] = Field(default_factory=list)
    config: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Schema registry schemas
# ---------------------------------------------------------------------------


class SchemaRegisterRequest(BaseModel):
    """Request body for registering a Protobuf schema."""

    subject: str = Field(..., min_length=1, max_length=255)
    schema_definition: str = Field(..., min_length=1)
    schema_type: str = Field(default="PROTOBUF")
    compatibility: str = Field(default="BACKWARD")
    topic_id: uuid.UUID | None = Field(default=None)


class SchemaResponse(BaseModel):
    """Response body for a schema version."""

    id: uuid.UUID
    tenant_id: str
    topic_id: uuid.UUID | None
    subject: str
    schema_version: int
    schema_id: int
    schema_definition: str
    schema_type: str
    compatibility: str
    is_active: bool
    metadata: dict[str, Any]
    created_at: Any
    updated_at: Any


class SchemaVersionsResponse(BaseModel):
    """List of schema versions for a subject."""

    subject: str
    versions: list[SchemaResponse]


class SchemaCompatibilityRequest(BaseModel):
    """Request body for checking schema compatibility."""

    schema_definition: str = Field(..., min_length=1)


class SchemaCompatibilityResponse(BaseModel):
    """Result of a schema compatibility check."""

    subject: str
    is_compatible: bool
    message: str = ""


# ---------------------------------------------------------------------------
# DLQ schemas
# ---------------------------------------------------------------------------


class DLQEntryResponse(BaseModel):
    """Response body for a DLQ entry."""

    id: uuid.UUID
    tenant_id: str
    topic_id: uuid.UUID
    source_topic: str
    dlq_topic: str
    message_key: str | None
    message_value: str
    message_headers: dict[str, Any]
    original_offset: int | None
    original_partition: int | None
    failure_reason: str
    failure_details: dict[str, Any]
    status: str
    retry_count: int
    max_retries: int
    next_retry_at: int | None
    resolved_at: int | None
    consumer_group: str | None
    correlation_id: str | None
    created_at: Any
    updated_at: Any


class DLQListResponse(BaseModel):
    """Paginated list of DLQ entries."""

    items: list[DLQEntryResponse]
    total: int
    skip: int
    limit: int


class DLQRetryResponse(BaseModel):
    """Result of a DLQ retry operation."""

    entry_id: uuid.UUID
    status: str
    next_retry_at: int | None
    retry_count: int
    message: str = ""


class DLQResolveResponse(BaseModel):
    """Result of resolving or abandoning a DLQ entry."""

    entry_id: uuid.UUID
    status: str
    message: str = ""


# ---------------------------------------------------------------------------
# Partition schemas
# ---------------------------------------------------------------------------


class TenantPartitionResponse(BaseModel):
    """Tenant-to-partition assignment for a topic."""

    tenant_id: str
    topic_name: str
    assigned_partition: int
    total_partitions: int
    hash_value: int


# ---------------------------------------------------------------------------
# Stream processing schemas (Gap #23)
# ---------------------------------------------------------------------------


class StreamJobCreateRequest(BaseModel):
    """Request body for creating a stream processing job."""

    name: str = Field(..., min_length=1, max_length=255, description="Job name")
    query_text: str = Field(..., min_length=1, description="SQL query (ksqldb) or job config")
    input_topics: list[str] = Field(default_factory=list)
    output_topic: str = Field(..., min_length=1)
    backend: str = Field(default="ksqldb", pattern="^(ksqldb|flink)$")
    properties: dict[str, Any] = Field(default_factory=dict)


class StreamJobResponse(BaseModel):
    """Response body for a stream processing job."""

    name: str
    tenant_id: str
    backend: str
    backend_job_id: str
    query_text: str
    input_topics: list[str]
    output_topic: str
    status: str


class StreamJobListResponse(BaseModel):
    """List of stream processing jobs."""

    jobs: list[dict[str, Any]]
    total: int


# ---------------------------------------------------------------------------
# Kafka Connect connector schemas (Gap #24)
# ---------------------------------------------------------------------------


class ConnectorCreateRequest(BaseModel):
    """Request body for creating a Kafka Connect connector."""

    name: str = Field(..., min_length=1, max_length=255)
    config: dict[str, Any] = Field(..., description="Connector configuration")


class ConnectorResponse(BaseModel):
    """Response body for a Kafka Connect connector."""

    name: str
    config: dict[str, Any] = Field(default_factory=dict)
    status: dict[str, Any] = Field(default_factory=dict)


class ConnectorListResponse(BaseModel):
    """List of Kafka Connect connectors."""

    items: list[dict[str, Any]]
    total: int


# ---------------------------------------------------------------------------
# Consumer group monitoring schemas (Gap #25)
# ---------------------------------------------------------------------------


class ConsumerGroupSummary(BaseModel):
    """Summary of a consumer group."""

    group_id: str
    offsets: dict[str, Any] = Field(default_factory=dict)


class ConsumerGroupLagResponse(BaseModel):
    """Per-partition lag information for a consumer group."""

    group_id: str
    tenant_id: str
    total_lag: int
    partitions: list[dict[str, Any]]


class ConsumerGroupListResponse(BaseModel):
    """List of consumer groups."""

    groups: list[ConsumerGroupSummary]
    total: int


# ---------------------------------------------------------------------------
# Event replay schemas (Gap #26)
# ---------------------------------------------------------------------------


class ReplayJobCreateRequest(BaseModel):
    """Request body for creating an event replay job."""

    group_id: str = Field(..., min_length=1)
    topic: str = Field(..., min_length=1)
    from_offset_type: str = Field(..., pattern="^(earliest|latest|timestamp)$")
    from_timestamp: str | None = Field(default=None, description="ISO8601 timestamp")


class ReplayJobResponse(BaseModel):
    """Response body for a replay job."""

    group_id: str
    tenant_id: str
    topic: str
    from_offset_type: str
    from_timestamp: str | None
    status: str


# ---------------------------------------------------------------------------
# Geo-replication schemas (Gap #27)
# ---------------------------------------------------------------------------


class GeoReplicationFlowCreateRequest(BaseModel):
    """Request body for creating a geo-replication flow."""

    name: str = Field(..., min_length=1, max_length=255)
    source_bootstrap: str = Field(..., min_length=1)
    target_bootstrap: str = Field(..., min_length=1)
    topics_pattern: str = Field(default=".*")


class GeoReplicationFlowResponse(BaseModel):
    """Response body for a geo-replication flow."""

    name: str
    tenant_id: str
    source_bootstrap: str
    target_bootstrap: str
    topics_pattern: str
    status: str


# ---------------------------------------------------------------------------
# Tiered storage schemas (Gap #28)
# ---------------------------------------------------------------------------


class TieredStorageConfigRequest(BaseModel):
    """Request body for configuring tiered storage on a topic."""

    enabled: bool = Field(default=True)
    tier_local_hot_ms: int = Field(default=86_400_000, ge=1)
    s3_bucket: str = Field(default="")


class TieredStorageConfigResponse(BaseModel):
    """Response body for tiered storage configuration."""

    topic_name: str
    tenant_id: str
    enabled: bool
    tier_local_hot_ms: int
    s3_bucket: str


# ---------------------------------------------------------------------------
# Schema evolution schemas (Gap #30)
# ---------------------------------------------------------------------------


class SchemaEvolutionPreviewRequest(BaseModel):
    """Request body for schema evolution preview."""

    subject: str = Field(..., min_length=1)
    new_schema_definition: str = Field(..., min_length=1)


class SchemaEvolutionPreviewResponse(BaseModel):
    """Response body for schema evolution preview."""

    subject: str
    tenant_id: str
    is_compatible: bool
    diff: dict[str, Any]
    recommendation: str
    existing_schema: str
    new_schema: str
