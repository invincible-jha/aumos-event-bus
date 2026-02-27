"""Abstract interfaces (Protocol classes) for the Event Bus service.

Defines contracts that adapters must implement. Core services depend on
these interfaces, not on concrete implementations (hexagonal architecture).
"""

from __future__ import annotations

import uuid
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class ITopicRepository(Protocol):
    """Interface for Kafka topic definition persistence."""

    async def get_by_id(self, topic_id: uuid.UUID, tenant_id: str) -> Any | None:
        """Retrieve a topic definition by ID."""
        ...

    async def get_by_name(self, topic_name: str, tenant_id: str) -> Any | None:
        """Retrieve a topic definition by Kafka topic name."""
        ...

    async def list_all(self, tenant_id: str, skip: int = 0, limit: int = 50) -> list[Any]:
        """List all topic definitions for a tenant."""
        ...

    async def create(self, data: dict[str, Any]) -> Any:
        """Persist a new topic definition."""
        ...

    async def update(self, topic_id: uuid.UUID, tenant_id: str, updates: dict[str, Any]) -> Any | None:
        """Update an existing topic definition."""
        ...

    async def delete(self, topic_id: uuid.UUID, tenant_id: str) -> bool:
        """Delete a topic definition."""
        ...


@runtime_checkable
class ISchemaRepository(Protocol):
    """Interface for schema version persistence."""

    async def get_by_id(self, schema_id: uuid.UUID, tenant_id: str) -> Any | None:
        """Retrieve a schema version by ID."""
        ...

    async def get_latest_by_subject(self, subject: str, tenant_id: str) -> Any | None:
        """Retrieve the latest active schema version for a subject."""
        ...

    async def list_versions(self, subject: str, tenant_id: str) -> list[Any]:
        """List all schema versions for a subject."""
        ...

    async def create(self, data: dict[str, Any]) -> Any:
        """Persist a new schema version."""
        ...

    async def deactivate_version(self, schema_id: uuid.UUID, tenant_id: str) -> bool:
        """Mark a schema version as inactive."""
        ...


@runtime_checkable
class IDLQRepository(Protocol):
    """Interface for dead letter queue entry persistence."""

    async def get_by_id(self, entry_id: uuid.UUID, tenant_id: str) -> Any | None:
        """Retrieve a DLQ entry by ID."""
        ...

    async def list_pending(
        self, tenant_id: str, source_topic: str | None = None, skip: int = 0, limit: int = 50
    ) -> list[Any]:
        """List DLQ entries pending retry."""
        ...

    async def create(self, data: dict[str, Any]) -> Any:
        """Persist a new DLQ entry."""
        ...

    async def update_status(
        self, entry_id: uuid.UUID, tenant_id: str, status: str, retry_count: int | None = None
    ) -> bool:
        """Update the status and retry count of a DLQ entry."""
        ...

    async def get_entries_due_for_retry(self, current_epoch_ms: int, limit: int = 100) -> list[Any]:
        """Retrieve DLQ entries whose next_retry_at has passed."""
        ...


@runtime_checkable
class IKafkaAdmin(Protocol):
    """Interface for Kafka administrative operations."""

    async def create_topic(
        self,
        topic_name: str,
        partitions: int,
        replication_factor: int,
        config: dict[str, str],
    ) -> None:
        """Create a Kafka topic with given configuration."""
        ...

    async def delete_topic(self, topic_name: str) -> None:
        """Delete a Kafka topic."""
        ...

    async def describe_topic(self, topic_name: str) -> dict[str, Any]:
        """Return metadata about an existing Kafka topic."""
        ...

    async def list_topics(self) -> list[str]:
        """Return all topic names in the Kafka cluster."""
        ...

    async def alter_topic_config(self, topic_name: str, config: dict[str, str]) -> None:
        """Update configuration parameters of an existing topic."""
        ...

    async def get_consumer_group_offsets(self, group_id: str) -> dict[str, Any]:
        """Return consumer group lag information."""
        ...


@runtime_checkable
class ISchemaRegistryClient(Protocol):
    """Interface for Confluent Schema Registry interactions."""

    async def register_schema(
        self, subject: str, schema_definition: str, schema_type: str = "PROTOBUF"
    ) -> int:
        """Register a schema and return the assigned schema ID."""
        ...

    async def get_schema(self, schema_id: int) -> dict[str, Any]:
        """Retrieve schema by its registry-assigned ID."""
        ...

    async def get_latest_schema(self, subject: str) -> dict[str, Any]:
        """Retrieve the latest version for a subject."""
        ...

    async def list_subjects(self) -> list[str]:
        """Return all registered subjects."""
        ...

    async def set_compatibility(self, subject: str, compatibility: str) -> None:
        """Set the compatibility mode for a subject."""
        ...

    async def check_compatibility(self, subject: str, schema_definition: str) -> bool:
        """Check whether a schema is compatible with existing versions."""
        ...

    async def delete_subject(self, subject: str, permanent: bool = False) -> list[int]:
        """Delete all versions of a subject."""
        ...


@runtime_checkable
class IDLQHandler(Protocol):
    """Contract for dead letter queue lifecycle management."""

    async def capture_failure(
        self,
        tenant_id: str,
        source_topic: str,
        message_key: str | None,
        message_value: str,
        failure_reason: str,
        message_headers: dict[str, str] | None,
        original_offset: int | None,
        original_partition: int | None,
        consumer_group: str | None,
        correlation_id: str | None,
        failure_details: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Capture a failed message into the DLQ."""
        ...

    async def schedule_retry(
        self,
        entry_id: uuid.UUID,
        tenant_id: str,
    ) -> dict[str, Any]:
        """Schedule the next retry for a DLQ entry with exponential backoff."""
        ...

    async def get_dlq_depth(
        self,
        tenant_id: str,
        source_topic: str | None,
    ) -> dict[str, Any]:
        """Return the current depth of the DLQ for monitoring."""
        ...

    async def replay_entry(
        self,
        entry_id: uuid.UUID,
        tenant_id: str,
        target_topic: str | None,
    ) -> dict[str, Any]:
        """Manually replay a DLQ entry to its source or a target topic."""
        ...

    async def filter_entries(
        self,
        tenant_id: str,
        source_topic: str | None,
        status: str | None,
        consumer_group: str | None,
        skip: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Inspect and filter DLQ entries for operator tooling."""
        ...


@runtime_checkable
class IEventVersionManager(Protocol):
    """Contract for event schema version management."""

    async def register_version(
        self,
        subject: str,
        schema_definition: str,
        tenant_id: str,
        topic_id: uuid.UUID | None,
        compatibility: Any | None,
        schema_type: str,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Register a new schema version with compatibility validation."""
        ...

    async def check_compatibility(
        self,
        subject: str,
        schema_definition: str,
        mode: Any | None,
    ) -> dict[str, Any]:
        """Check whether a schema definition is compatible with existing versions."""
        ...

    async def get_version_history(
        self,
        subject: str,
        tenant_id: str,
    ) -> list[dict[str, Any]]:
        """Return the full version history for a schema subject."""
        ...

    async def negotiate_version(
        self,
        subject: str,
        tenant_id: str,
        producer_version: int,
        consumer_min_version: int,
    ) -> dict[str, Any]:
        """Negotiate a compatible schema version between producer and consumer."""
        ...

    async def detect_deprecated_versions(
        self,
        tenant_id: str,
    ) -> list[dict[str, Any]]:
        """Detect and report deprecated schema versions across all subjects."""
        ...


@runtime_checkable
class IEventBusMonitoringDashboard(Protocol):
    """Contract for event bus monitoring and metrics aggregation."""

    async def get_full_snapshot(self, tenant_id: str) -> dict[str, Any]:
        """Return a complete monitoring snapshot for dashboard rendering."""
        ...

    async def get_topic_throughput(
        self,
        topic_name: str,
        window_seconds: int,
    ) -> dict[str, Any]:
        """Return throughput metrics for a specific topic."""
        ...

    async def get_consumer_lag_for_group(
        self,
        group_id: str,
        topic_name: str | None,
    ) -> dict[str, Any]:
        """Return consumer lag metrics for a specific consumer group."""
        ...

    async def export_dashboard_json(self, tenant_id: str) -> dict[str, Any]:
        """Export a complete dashboard payload for rendering."""
        ...


@runtime_checkable
class IConsumerGroupManager(Protocol):
    """Contract for Kafka consumer group lifecycle management."""

    async def create_group_config(
        self,
        group_id: str,
        tenant_id: str,
        description: str,
        topics: list[str] | None,
        auto_offset_reset: str,
        session_timeout_ms: int,
        heartbeat_interval_ms: int,
        max_poll_interval_ms: int,
    ) -> dict[str, Any]:
        """Register a consumer group configuration."""
        ...

    async def reset_offsets(
        self,
        group_id: str,
        topic: str,
        position: str,
        specific_offsets: dict[int, int] | None,
    ) -> dict[str, Any]:
        """Reset consumer group offsets for a topic."""
        ...

    async def get_group_lag(self, group_id: str) -> dict[str, Any]:
        """Return consumer lag metrics for a group."""
        ...

    async def get_performance_analytics(self, group_id: str) -> dict[str, Any]:
        """Return performance analytics for a consumer group."""
        ...

    async def get_group_health_status(self, group_id: str) -> dict[str, Any]:
        """Return aggregated health status for a consumer group."""
        ...
