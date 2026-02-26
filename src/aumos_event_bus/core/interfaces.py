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
