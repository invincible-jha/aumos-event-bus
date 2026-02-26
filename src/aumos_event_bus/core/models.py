"""SQLAlchemy ORM models for the Event Bus management service.

Covers topic registry, schema registry entries, and DLQ records.
All tenant-scoped tables extend AumOSModel for id/tenant_id/timestamps.
Table prefix: evt_
"""

from __future__ import annotations

import enum
import uuid

from sqlalchemy import BigInteger, Boolean, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from aumos_common.database import AumOSModel, Base


class TopicStatus(str, enum.Enum):
    """Lifecycle status of a managed Kafka topic."""

    ACTIVE = "active"
    CREATING = "creating"
    DELETING = "deleting"
    ERROR = "error"
    DEPRECATED = "deprecated"


class DLQStatus(str, enum.Enum):
    """Processing status of a dead letter queue entry."""

    PENDING = "pending"
    RETRYING = "retrying"
    RESOLVED = "resolved"
    ABANDONED = "abandoned"


class SchemaCompatibility(str, enum.Enum):
    """Schema Registry compatibility modes."""

    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"


class TopicDefinition(AumOSModel):
    """Managed Kafka topic definition with metadata.

    Tracks all tenant-scoped topics provisioned through the Event Bus
    management API. Mirrors the actual Kafka topic configuration.
    """

    __tablename__ = "evt_topic_definitions"
    __table_args__ = (
        UniqueConstraint("tenant_id", "topic_name", name="uq_evt_topic_tenant_name"),
    )

    topic_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    partitions: Mapped[int] = mapped_column(Integer, nullable=False, default=6)
    replication_factor: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    retention_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, default=604_800_000)  # 7 days
    cleanup_policy: Mapped[str] = mapped_column(String(50), nullable=False, default="delete")
    compression_type: Mapped[str] = mapped_column(String(20), nullable=False, default="lz4")
    min_isr: Mapped[int] = mapped_column(Integer, nullable=False, default=2)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=TopicStatus.ACTIVE.value)
    is_system_topic: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    schema_subject: Mapped[str | None] = mapped_column(String(255), nullable=True)
    config_overrides: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Relationships
    schemas: Mapped[list[SchemaVersion]] = relationship(
        "SchemaVersion", back_populates="topic", cascade="all, delete-orphan"
    )
    dlq_entries: Mapped[list[DLQEntry]] = relationship(
        "DLQEntry", back_populates="topic", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<TopicDefinition {self.topic_name} partitions={self.partitions}>"


class SchemaVersion(AumOSModel):
    """Protobuf schema version registered for a topic.

    Tracks all schema versions in the Confluent Schema Registry,
    providing a management layer over the registry API.
    """

    __tablename__ = "evt_schema_versions"
    __table_args__ = (
        UniqueConstraint("topic_id", "schema_version", name="uq_evt_schema_topic_version"),
    )

    topic_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("evt_topic_definitions.id"), nullable=False, index=True
    )
    subject: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_id: Mapped[int] = mapped_column(Integer, nullable=False)  # Schema Registry assigned ID
    schema_definition: Mapped[str] = mapped_column(Text, nullable=False)
    schema_type: Mapped[str] = mapped_column(String(20), nullable=False, default="PROTOBUF")
    compatibility: Mapped[str] = mapped_column(
        String(30), nullable=False, default=SchemaCompatibility.BACKWARD.value
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    metadata: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Relationships
    topic: Mapped[TopicDefinition] = relationship("TopicDefinition", back_populates="schemas")

    def __repr__(self) -> str:
        return f"<SchemaVersion {self.subject} v{self.schema_version}>"


class DLQEntry(AumOSModel):
    """Dead letter queue entry for failed message processing.

    Records messages that could not be processed successfully, along with
    the original topic, failure reason, and retry history.
    """

    __tablename__ = "evt_dlq_entries"

    topic_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("evt_topic_definitions.id"), nullable=False, index=True
    )
    source_topic: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    dlq_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    message_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_value: Mapped[str] = mapped_column(Text, nullable=False)
    message_headers: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    original_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    original_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    failure_reason: Mapped[str] = mapped_column(Text, nullable=False)
    failure_details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=DLQStatus.PENDING.value, index=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    next_retry_at: Mapped[int | None] = mapped_column(BigInteger, nullable=True)  # Unix epoch ms
    resolved_at: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    consumer_group: Mapped[str | None] = mapped_column(String(255), nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)

    # Relationships
    topic: Mapped[TopicDefinition] = relationship("TopicDefinition", back_populates="dlq_entries")

    def __repr__(self) -> str:
        return f"<DLQEntry {self.source_topic} status={self.status} retries={self.retry_count}>"


class TenantPartitionMapping(Base):
    """Maps tenant IDs to Kafka partition assignments.

    Not tenant-scoped (it IS the tenant-to-partition mapping table),
    so it extends Base directly rather than AumOSModel.
    """

    __tablename__ = "evt_tenant_partition_mappings"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    topic_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    assigned_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    hash_value: Mapped[int] = mapped_column(BigInteger, nullable=False)
    total_partitions: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint("tenant_id", "topic_name", name="uq_evt_tenant_topic_partition"),
    )

    def __repr__(self) -> str:
        return f"<TenantPartitionMapping tenant={self.tenant_id} topic={self.topic_name} partition={self.assigned_partition}>"
