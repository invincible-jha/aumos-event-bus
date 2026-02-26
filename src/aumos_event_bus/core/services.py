"""Core business logic services for the Event Bus management API.

Services orchestrate between repositories (persistence) and adapters
(Kafka admin, Schema Registry). All methods are async; no framework deps.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from aumos_common.errors import NotFoundError, ValidationError
from aumos_common.events import AuditEvent, Topics
from aumos_common.observability import get_logger

from aumos_event_bus.core.interfaces import (
    IDLQRepository,
    IKafkaAdmin,
    ISchemaRegistryClient,
    ISchemaRepository,
    ITopicRepository,
)
from aumos_event_bus.core.models import DLQStatus, SchemaCompatibility, TopicStatus
from aumos_event_bus.core.tenant_partitioner import TenantPartitioner

logger = get_logger(__name__)


@dataclass
class TopicCreateRequest:
    """Input for topic creation."""

    topic_name: str
    display_name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    cleanup_policy: str = "delete"
    compression_type: str = "lz4"
    min_isr: int = 2
    description: str = ""
    schema_subject: str | None = None
    config_overrides: dict[str, str] = field(default_factory=dict)


@dataclass
class SchemaRegisterRequest:
    """Input for schema registration."""

    subject: str
    schema_definition: str
    schema_type: str = "PROTOBUF"
    compatibility: str = SchemaCompatibility.BACKWARD.value
    topic_id: uuid.UUID | None = None


@dataclass
class DLQRequeueRequest:
    """Input for requeueing a DLQ entry."""

    entry_id: uuid.UUID
    target_topic: str | None = None  # If None, re-publish to original topic


class TopicManagementService:
    """Manages Kafka topic lifecycle: provisioning, configuration, deletion.

    Coordinates between the topic repository (database) and the Kafka admin
    client (actual broker). Publishes audit events after state changes.
    """

    def __init__(
        self,
        topic_repo: ITopicRepository,
        kafka_admin: IKafkaAdmin,
        partitioner: TenantPartitioner,
        event_publisher: Any | None = None,
    ) -> None:
        """Initialise with injected dependencies.

        Args:
            topic_repo: Persistence layer for topic definitions.
            kafka_admin: Kafka admin client for broker operations.
            partitioner: Tenant-aware partition calculator.
            event_publisher: Optional EventPublisher for audit events.
        """
        self._topic_repo = topic_repo
        self._kafka_admin = kafka_admin
        self._partitioner = partitioner
        self._event_publisher = event_publisher

    async def create_topic(
        self, request: TopicCreateRequest, tenant_id: str, actor: str
    ) -> dict[str, Any]:
        """Provision a new Kafka topic and persist its definition.

        Creates the topic on the Kafka broker, then records the definition
        in the database. If Kafka creation fails, the database record is
        not persisted (atomic in spirit; Kafka is the source of truth).

        Args:
            request: Topic configuration parameters.
            tenant_id: Owning tenant UUID string.
            actor: User or service creating the topic.

        Returns:
            The persisted TopicDefinition as a dict.

        Raises:
            ValidationError: If topic_name is already registered for tenant.
        """
        # Check for duplicate
        existing = await self._topic_repo.get_by_name(request.topic_name, tenant_id)
        if existing:
            raise ValidationError(
                message=f"Topic '{request.topic_name}' already exists for this tenant",
                field="topic_name",
            )

        # Build Kafka config dict
        kafka_config: dict[str, str] = {
            "retention.ms": str(request.retention_ms),
            "cleanup.policy": request.cleanup_policy,
            "compression.type": request.compression_type,
            "min.insync.replicas": str(request.min_isr),
            "segment.bytes": "1073741824",  # 1 GB
        }
        kafka_config.update(request.config_overrides)

        logger.info(
            "Provisioning Kafka topic",
            topic_name=request.topic_name,
            partitions=request.partitions,
            tenant_id=tenant_id,
        )

        # Create on Kafka broker first
        await self._kafka_admin.create_topic(
            topic_name=request.topic_name,
            partitions=request.partitions,
            replication_factor=request.replication_factor,
            config=kafka_config,
        )

        # Persist to database
        topic = await self._topic_repo.create({
            "tenant_id": tenant_id,
            "topic_name": request.topic_name,
            "display_name": request.display_name,
            "description": request.description,
            "partitions": request.partitions,
            "replication_factor": request.replication_factor,
            "retention_ms": request.retention_ms,
            "cleanup_policy": request.cleanup_policy,
            "compression_type": request.compression_type,
            "min_isr": request.min_isr,
            "status": TopicStatus.ACTIVE.value,
            "is_system_topic": False,
            "schema_subject": request.schema_subject,
            "config_overrides": request.config_overrides,
        })

        logger.info("Topic created", topic_name=request.topic_name, topic_id=str(topic["id"]))

        await self._publish_audit(
            tenant_id=tenant_id,
            actor=actor,
            resource=f"topic:{request.topic_name}",
            action="create",
            outcome="success",
            details={"partitions": request.partitions, "retention_ms": request.retention_ms},
        )

        return topic

    async def get_topic(self, topic_id: uuid.UUID, tenant_id: str) -> dict[str, Any]:
        """Retrieve a topic definition by ID.

        Args:
            topic_id: UUID of the topic definition.
            tenant_id: Tenant context for RLS enforcement.

        Returns:
            Topic definition as a dict.

        Raises:
            NotFoundError: If topic does not exist for this tenant.
        """
        topic = await self._topic_repo.get_by_id(topic_id, tenant_id)
        if not topic:
            raise NotFoundError(resource="topic", resource_id=str(topic_id))
        return topic

    async def list_topics(
        self, tenant_id: str, skip: int = 0, limit: int = 50
    ) -> list[dict[str, Any]]:
        """List all topic definitions for a tenant.

        Args:
            tenant_id: Tenant context.
            skip: Pagination offset.
            limit: Maximum results to return.

        Returns:
            List of topic definitions.
        """
        return await self._topic_repo.list_all(tenant_id, skip=skip, limit=limit)

    async def delete_topic(
        self, topic_id: uuid.UUID, tenant_id: str, actor: str, force: bool = False
    ) -> None:
        """Delete a topic from Kafka and remove its database record.

        Args:
            topic_id: UUID of the topic to delete.
            tenant_id: Tenant context.
            actor: User or service requesting deletion.
            force: If True, delete even if topic has active consumers.

        Raises:
            NotFoundError: If topic does not exist.
        """
        topic = await self._topic_repo.get_by_id(topic_id, tenant_id)
        if not topic:
            raise NotFoundError(resource="topic", resource_id=str(topic_id))

        topic_name = topic["topic_name"]

        logger.info("Deleting Kafka topic", topic_name=topic_name, tenant_id=tenant_id)

        await self._kafka_admin.delete_topic(topic_name)
        await self._topic_repo.delete(topic_id, tenant_id)

        await self._publish_audit(
            tenant_id=tenant_id,
            actor=actor,
            resource=f"topic:{topic_name}",
            action="delete",
            outcome="success",
            details={"force": force},
        )

    async def get_tenant_partition(
        self, tenant_id: str, topic_name: str, partitions: int
    ) -> int:
        """Calculate the partition assignment for a tenant on a topic.

        Args:
            tenant_id: Tenant UUID string.
            topic_name: Kafka topic name.
            partitions: Total partition count for the topic.

        Returns:
            Assigned partition number.
        """
        assignment = self._partitioner.assign(tenant_id, topic_name, partitions)
        return assignment.partition

    async def _publish_audit(
        self,
        tenant_id: str,
        actor: str,
        resource: str,
        action: str,
        outcome: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Publish an audit event if publisher is configured."""
        if not self._event_publisher:
            return
        try:
            event = AuditEvent(
                tenant_id=tenant_id,
                actor=actor,
                resource=resource,
                action=action,
                outcome=outcome,
                details=details or {},
                source_service="aumos-event-bus",
            )
            await self._event_publisher.publish(Topics.audit(tenant_id), event)
        except Exception:
            logger.exception("Failed to publish audit event", resource=resource, action=action)


class SchemaValidationService:
    """Manages Protobuf schema registration and compatibility enforcement.

    Wraps the Confluent Schema Registry client with tenant-aware operations
    and persists schema metadata for the management API.
    """

    def __init__(
        self,
        schema_repo: ISchemaRepository,
        registry_client: ISchemaRegistryClient,
        event_publisher: Any | None = None,
    ) -> None:
        """Initialise with injected dependencies.

        Args:
            schema_repo: Persistence layer for schema version metadata.
            registry_client: Confluent Schema Registry HTTP client.
            event_publisher: Optional publisher for audit events.
        """
        self._schema_repo = schema_repo
        self._registry = registry_client
        self._event_publisher = event_publisher

    async def register_schema(
        self, request: SchemaRegisterRequest, tenant_id: str, actor: str
    ) -> dict[str, Any]:
        """Register a Protobuf schema with the Schema Registry.

        Checks compatibility before registering, then persists metadata.

        Args:
            request: Schema definition and configuration.
            tenant_id: Tenant context for database records.
            actor: User or service performing the registration.

        Returns:
            Persisted SchemaVersion as a dict.

        Raises:
            ValidationError: If schema fails compatibility check.
        """
        # Check compatibility against existing versions
        is_compatible = await self._registry.check_compatibility(
            request.subject, request.schema_definition
        )
        if not is_compatible:
            raise ValidationError(
                message=f"Schema is not compatible with existing versions for subject '{request.subject}'",
                field="schema_definition",
            )

        # Register with Confluent Schema Registry
        schema_id = await self._registry.register_schema(
            subject=request.subject,
            schema_definition=request.schema_definition,
            schema_type=request.schema_type,
        )

        # Get the version number assigned by the registry
        latest = await self._registry.get_latest_schema(request.subject)
        schema_version = latest.get("version", 1)

        # Persist metadata
        schema = await self._schema_repo.create({
            "tenant_id": tenant_id,
            "topic_id": request.topic_id,
            "subject": request.subject,
            "schema_version": schema_version,
            "schema_id": schema_id,
            "schema_definition": request.schema_definition,
            "schema_type": request.schema_type,
            "compatibility": request.compatibility,
            "is_active": True,
            "metadata": {"registered_by": actor},
        })

        logger.info(
            "Schema registered",
            subject=request.subject,
            schema_id=schema_id,
            version=schema_version,
        )

        return schema

    async def get_latest_schema(self, subject: str, tenant_id: str) -> dict[str, Any]:
        """Retrieve the latest active schema version for a subject.

        Args:
            subject: Schema Registry subject name.
            tenant_id: Tenant context.

        Returns:
            SchemaVersion dict.

        Raises:
            NotFoundError: If no schema exists for subject.
        """
        schema = await self._schema_repo.get_latest_by_subject(subject, tenant_id)
        if not schema:
            raise NotFoundError(resource="schema", resource_id=subject)
        return schema

    async def check_compatibility(self, subject: str, schema_definition: str) -> bool:
        """Test whether a schema definition is compatible with existing versions.

        Args:
            subject: Schema Registry subject.
            schema_definition: Protobuf schema string to test.

        Returns:
            True if compatible, False otherwise.
        """
        return await self._registry.check_compatibility(subject, schema_definition)


class DLQManagementService:
    """Manages dead letter queue entries: inspection, retry, and resolution.

    Implements exponential backoff retry scheduling and provides tooling
    for operators to inspect and manually resolve failed messages.
    """

    def __init__(
        self,
        dlq_repo: IDLQRepository,
        kafka_admin: IKafkaAdmin,
        max_retries: int = 5,
        initial_backoff_ms: int = 1_000,
        max_backoff_ms: int = 60_000,
        backoff_multiplier: float = 2.0,
        event_publisher: Any | None = None,
    ) -> None:
        """Initialise with injected dependencies and retry configuration.

        Args:
            dlq_repo: Persistence layer for DLQ entries.
            kafka_admin: Kafka admin client.
            max_retries: Maximum retry attempts before marking abandoned.
            initial_backoff_ms: Initial delay before first retry.
            max_backoff_ms: Maximum delay cap for exponential backoff.
            backoff_multiplier: Factor to multiply delay by on each retry.
            event_publisher: Optional publisher for audit events.
        """
        self._dlq_repo = dlq_repo
        self._kafka_admin = kafka_admin
        self._max_retries = max_retries
        self._initial_backoff_ms = initial_backoff_ms
        self._max_backoff_ms = max_backoff_ms
        self._backoff_multiplier = backoff_multiplier
        self._event_publisher = event_publisher

    def calculate_next_retry_ms(self, retry_count: int) -> int:
        """Calculate the epoch ms timestamp for the next retry attempt.

        Uses exponential backoff with jitter: delay = min(initial * multiplier^n, max)

        Args:
            retry_count: Number of retries already attempted.

        Returns:
            Unix epoch milliseconds for next retry.
        """
        delay_ms = min(
            int(self._initial_backoff_ms * (self._backoff_multiplier ** retry_count)),
            self._max_backoff_ms,
        )
        return int(time.time() * 1000) + delay_ms

    async def record_failure(
        self,
        tenant_id: str,
        source_topic: str,
        dlq_topic: str,
        message_key: str | None,
        message_value: str,
        message_headers: dict[str, str],
        failure_reason: str,
        original_offset: int | None = None,
        original_partition: int | None = None,
        consumer_group: str | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Record a failed message in the DLQ.

        Args:
            tenant_id: Tenant context.
            source_topic: Original topic the message came from.
            dlq_topic: DLQ topic name where it was published.
            message_key: Original message key.
            message_value: Serialised message payload.
            message_headers: Original message headers.
            failure_reason: Human-readable failure description.
            original_offset: Kafka offset of the failed message.
            original_partition: Kafka partition of the failed message.
            consumer_group: Consumer group that failed.
            correlation_id: Tracing correlation ID.

        Returns:
            The persisted DLQEntry as a dict.
        """
        next_retry_at = self.calculate_next_retry_ms(0)

        entry = await self._dlq_repo.create({
            "tenant_id": tenant_id,
            "source_topic": source_topic,
            "dlq_topic": dlq_topic,
            "message_key": message_key,
            "message_value": message_value,
            "message_headers": message_headers,
            "original_offset": original_offset,
            "original_partition": original_partition,
            "failure_reason": failure_reason,
            "failure_details": {},
            "status": DLQStatus.PENDING.value,
            "retry_count": 0,
            "max_retries": self._max_retries,
            "next_retry_at": next_retry_at,
            "consumer_group": consumer_group,
            "correlation_id": correlation_id,
        })

        logger.warning(
            "Message sent to DLQ",
            source_topic=source_topic,
            failure_reason=failure_reason,
            tenant_id=tenant_id,
        )
        return entry

    async def list_pending_entries(
        self, tenant_id: str, source_topic: str | None = None, skip: int = 0, limit: int = 50
    ) -> list[dict[str, Any]]:
        """List DLQ entries awaiting retry or resolution.

        Args:
            tenant_id: Tenant context.
            source_topic: Optional filter by source topic.
            skip: Pagination offset.
            limit: Maximum results.

        Returns:
            List of DLQEntry dicts.
        """
        return await self._dlq_repo.list_pending(tenant_id, source_topic, skip=skip, limit=limit)

    async def mark_resolved(
        self, entry_id: uuid.UUID, tenant_id: str, actor: str
    ) -> dict[str, Any]:
        """Manually mark a DLQ entry as resolved.

        Args:
            entry_id: UUID of the DLQ entry.
            tenant_id: Tenant context.
            actor: Operator resolving the entry.

        Returns:
            Updated DLQEntry dict.

        Raises:
            NotFoundError: If entry does not exist.
        """
        entry = await self._dlq_repo.get_by_id(entry_id, tenant_id)
        if not entry:
            raise NotFoundError(resource="dlq_entry", resource_id=str(entry_id))

        await self._dlq_repo.update_status(
            entry_id, tenant_id, status=DLQStatus.RESOLVED.value
        )
        logger.info("DLQ entry marked resolved", entry_id=str(entry_id), actor=actor)
        return {**entry, "status": DLQStatus.RESOLVED.value}

    async def mark_abandoned(
        self, entry_id: uuid.UUID, tenant_id: str, actor: str
    ) -> dict[str, Any]:
        """Abandon a DLQ entry — no further retries will be attempted.

        Args:
            entry_id: UUID of the DLQ entry.
            tenant_id: Tenant context.
            actor: Operator abandoning the entry.

        Returns:
            Updated DLQEntry dict.

        Raises:
            NotFoundError: If entry does not exist.
        """
        entry = await self._dlq_repo.get_by_id(entry_id, tenant_id)
        if not entry:
            raise NotFoundError(resource="dlq_entry", resource_id=str(entry_id))

        await self._dlq_repo.update_status(
            entry_id, tenant_id, status=DLQStatus.ABANDONED.value
        )
        logger.info("DLQ entry abandoned", entry_id=str(entry_id), actor=actor)
        return {**entry, "status": DLQStatus.ABANDONED.value}
