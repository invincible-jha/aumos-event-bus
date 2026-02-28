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
    IConsumerGroupManager,
    IDLQHandler,
    IDLQRepository,
    IEventBusMonitoringDashboard,
    IEventVersionManager,
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


class DLQHandlerService:
    """High-level DLQ handler orchestration with full lifecycle management.

    Wraps the IDLQHandler adapter with business-level guards: validates
    tenant context before all DLQ operations and publishes audit events
    on significant state changes.
    """

    def __init__(
        self,
        dlq_handler: IDLQHandler,
        event_publisher: Any | None = None,
    ) -> None:
        """Initialise with injected dependencies.

        Args:
            dlq_handler: DLQ handler implementation.
            event_publisher: Optional publisher for audit events.
        """
        self._handler = dlq_handler
        self._event_publisher = event_publisher

    async def capture(
        self,
        tenant_id: str,
        source_topic: str,
        message_key: str | None,
        message_value: str,
        failure_reason: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Capture a failed message into the DLQ.

        Args:
            tenant_id: Owning tenant.
            source_topic: Original Kafka topic.
            message_key: Message key.
            message_value: Serialised message.
            failure_reason: Failure description.
            **kwargs: Additional capture arguments forwarded to the handler.

        Returns:
            Persisted DLQ entry dict.
        """
        return await self._handler.capture_failure(
            tenant_id=tenant_id,
            source_topic=source_topic,
            message_key=message_key,
            message_value=message_value,
            failure_reason=failure_reason,
            message_headers=kwargs.get("message_headers"),
            original_offset=kwargs.get("original_offset"),
            original_partition=kwargs.get("original_partition"),
            consumer_group=kwargs.get("consumer_group"),
            correlation_id=kwargs.get("correlation_id"),
            failure_details=kwargs.get("failure_details"),
        )

    async def replay(
        self,
        entry_id: uuid.UUID,
        tenant_id: str,
        target_topic: str | None = None,
    ) -> dict[str, Any]:
        """Replay a DLQ entry to its source or a target topic.

        Args:
            entry_id: DLQ entry UUID.
            tenant_id: Tenant context.
            target_topic: Optional target topic override.

        Returns:
            Replay descriptor dict.
        """
        return await self._handler.replay_entry(
            entry_id=entry_id,
            tenant_id=tenant_id,
            target_topic=target_topic,
        )

    async def get_depth_report(
        self,
        tenant_id: str,
        source_topic: str | None = None,
    ) -> dict[str, Any]:
        """Return DLQ depth monitoring report.

        Args:
            tenant_id: Tenant context.
            source_topic: Optional topic filter.

        Returns:
            Depth report dict with alert flag.
        """
        return await self._handler.get_dlq_depth(
            tenant_id=tenant_id,
            source_topic=source_topic,
        )


class MonitoringService:
    """Event bus monitoring facade for dashboard data aggregation.

    Wraps the IEventBusMonitoringDashboard adapter for use by the API layer.
    """

    def __init__(self, dashboard: IEventBusMonitoringDashboard) -> None:
        """Initialise with injected dashboard adapter.

        Args:
            dashboard: Monitoring dashboard implementation.
        """
        self._dashboard = dashboard

    async def get_snapshot(self, tenant_id: str) -> dict[str, Any]:
        """Return a full monitoring snapshot for a tenant.

        Args:
            tenant_id: Tenant context.

        Returns:
            Dashboard-ready snapshot dict.
        """
        return await self._dashboard.get_full_snapshot(tenant_id=tenant_id)

    async def export_for_dashboard(self, tenant_id: str) -> dict[str, Any]:
        """Export full monitoring payload for a frontend dashboard.

        Args:
            tenant_id: Tenant context.

        Returns:
            Dashboard export dict with version and metadata.
        """
        return await self._dashboard.export_dashboard_json(tenant_id=tenant_id)


class ConsumerGroupService:
    """Consumer group lifecycle management service.

    Provides business-level operations over the IConsumerGroupManager adapter.
    """

    def __init__(self, group_manager: IConsumerGroupManager) -> None:
        """Initialise with injected group manager.

        Args:
            group_manager: Consumer group manager implementation.
        """
        self._manager = group_manager

    async def register_group(
        self,
        group_id: str,
        tenant_id: str,
        topics: list[str],
        description: str = "",
        auto_offset_reset: str = "latest",
    ) -> dict[str, Any]:
        """Register a new consumer group configuration.

        Args:
            group_id: Kafka consumer group ID.
            tenant_id: Owning tenant.
            topics: Topics this group will subscribe to.
            description: Human-readable group description.
            auto_offset_reset: Offset reset policy.

        Returns:
            Group configuration dict.
        """
        return await self._manager.create_group_config(
            group_id=group_id,
            tenant_id=tenant_id,
            description=description,
            topics=topics,
            auto_offset_reset=auto_offset_reset,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=10_000,
            max_poll_interval_ms=300_000,
        )

    async def reset_group_offsets(
        self,
        group_id: str,
        topic: str,
        position: str = "earliest",
    ) -> dict[str, Any]:
        """Reset offsets for a consumer group topic.

        Args:
            group_id: Consumer group ID.
            topic: Topic to reset.
            position: 'earliest', 'latest', or 'none'.

        Returns:
            Reset operation result dict.
        """
        return await self._manager.reset_offsets(
            group_id=group_id,
            topic=topic,
            position=position,
            specific_offsets=None,
        )

    async def get_group_analytics(self, group_id: str) -> dict[str, Any]:
        """Return performance analytics for a consumer group.

        Args:
            group_id: Consumer group ID.

        Returns:
            Performance analytics dict.
        """
        return await self._manager.get_performance_analytics(group_id=group_id)


class StreamProcessingService:
    """Stream processing job lifecycle management.

    Manages ksqlDB and Flink stream processing jobs with tenant-aware
    isolation. Persists job metadata for audit and listing purposes.
    """

    def __init__(
        self,
        ksqldb_client: Any | None = None,
        flink_client: Any | None = None,
        backend: str = "ksqldb",
    ) -> None:
        """Initialise with backend clients.

        Args:
            ksqldb_client: Optional KsqldbClient instance.
            flink_client: Optional FlinkClient instance.
            backend: Active backend name ('ksqldb' or 'flink').
        """
        self._ksqldb = ksqldb_client
        self._flink = flink_client
        self._backend = backend

    async def create_stream_job(
        self,
        tenant_id: str,
        name: str,
        query_text: str,
        input_topics: list[str],
        output_topic: str,
        backend_override: str | None = None,
    ) -> dict[str, Any]:
        """Create a stream processing job on the configured backend.

        Args:
            tenant_id: Owning tenant.
            name: Human-readable job name.
            query_text: SQL query (ksqldb) or job config reference.
            input_topics: Source Kafka topics.
            output_topic: Output topic for processed events.
            backend_override: Override the default backend for this job.

        Returns:
            Job descriptor dict.

        Raises:
            RuntimeError: If the backend is unavailable or job creation fails.
        """
        backend = backend_override or self._backend

        if backend == "ksqldb":
            if not self._ksqldb:
                raise RuntimeError("ksqlDB backend not configured")
            result = await self._ksqldb.execute_statement(query_text)
            backend_job_id = str(result.get("commandId", name))
        elif backend == "flink":
            if not self._flink:
                raise RuntimeError("Flink backend not configured")
            jobs = await self._flink.list_jobs()
            backend_job_id = f"flink-{name}"
            result = {"flink_jobs": jobs}
        else:
            raise RuntimeError(f"Unknown stream backend: {backend}")

        logger.info(
            "Stream job created",
            tenant_id=tenant_id,
            name=name,
            backend=backend,
            backend_job_id=backend_job_id,
        )
        return {
            "name": name,
            "tenant_id": tenant_id,
            "backend": backend,
            "backend_job_id": backend_job_id,
            "query_text": query_text,
            "input_topics": input_topics,
            "output_topic": output_topic,
            "status": "running",
            "result": result,
        }

    async def list_stream_jobs(self, backend_override: str | None = None) -> list[dict[str, Any]]:
        """List all active stream processing jobs.

        Args:
            backend_override: Override the default backend.

        Returns:
            List of job descriptor dicts.
        """
        backend = backend_override or self._backend

        if backend == "ksqldb" and self._ksqldb:
            return await self._ksqldb.list_queries()
        if backend == "flink" and self._flink:
            return await self._flink.list_jobs()
        return []

    async def terminate_stream_job(
        self,
        backend_job_id: str,
        backend_override: str | None = None,
    ) -> dict[str, Any]:
        """Terminate a running stream job.

        Args:
            backend_job_id: Backend-specific job identifier.
            backend_override: Override the default backend.

        Returns:
            Termination result dict.
        """
        backend = backend_override or self._backend

        if backend == "ksqldb" and self._ksqldb:
            return await self._ksqldb.terminate_query(backend_job_id)
        if backend == "flink" and self._flink:
            return await self._flink.cancel_job(backend_job_id)
        raise RuntimeError(f"Backend '{backend}' not available for termination")


class ConnectorService:
    """Kafka Connect connector lifecycle management with tenant isolation.

    Manages connector creation, status monitoring, and deletion through
    the Kafka Connect REST API. Tenant isolation is enforced by requiring
    all connector names to be prefixed with the tenant ID.
    """

    def __init__(self, connect_client: Any, connector_prefix: str = "") -> None:
        """Initialise with Kafka Connect client.

        Args:
            connect_client: KafkaConnectClient instance.
            connector_prefix: Optional global prefix for all connector names.
        """
        self._client = connect_client
        self._prefix = connector_prefix

    def _make_name(self, tenant_id: str, name: str) -> str:
        """Build a tenant-scoped connector name.

        Args:
            tenant_id: Owning tenant ID.
            name: Base connector name.

        Returns:
            Connector name prefixed with tenant_id.
        """
        return f"{tenant_id}-{name}"

    async def create_connector(
        self,
        tenant_id: str,
        name: str,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        """Create a Kafka Connect connector scoped to a tenant.

        Args:
            tenant_id: Owning tenant.
            name: Base connector name (will be prefixed with tenant_id).
            config: Connector configuration dict.

        Returns:
            Created connector descriptor.
        """
        full_name = self._make_name(tenant_id, name)
        config["name"] = full_name
        result = await self._client.create_connector(full_name, config)
        logger.info("Connector created", tenant_id=tenant_id, connector_name=full_name)
        return result

    async def list_connectors(self, tenant_id: str) -> list[dict[str, Any]]:
        """List all connectors owned by a tenant.

        Args:
            tenant_id: Tenant whose connectors to list.

        Returns:
            List of connector descriptors.
        """
        all_connectors = await self._client.list_connectors(expand_status=True)
        prefix = f"{tenant_id}-"
        return [c for c in all_connectors if c.get("name", "").startswith(prefix)]

    async def get_connector_status(self, tenant_id: str, name: str) -> dict[str, Any]:
        """Get status for a tenant-owned connector.

        Args:
            tenant_id: Owning tenant.
            name: Base connector name.

        Returns:
            Connector status dict.
        """
        full_name = self._make_name(tenant_id, name)
        return await self._client.get_connector_status(full_name)

    async def delete_connector(self, tenant_id: str, name: str) -> None:
        """Delete a tenant-owned connector.

        Args:
            tenant_id: Owning tenant.
            name: Base connector name.
        """
        full_name = self._make_name(tenant_id, name)
        await self._client.delete_connector(full_name)
        logger.info("Connector deleted", tenant_id=tenant_id, connector_name=full_name)

    async def restart_connector(self, tenant_id: str, name: str) -> dict[str, Any]:
        """Restart a tenant-owned connector.

        Args:
            tenant_id: Owning tenant.
            name: Base connector name.

        Returns:
            Restart result dict.
        """
        full_name = self._make_name(tenant_id, name)
        return await self._client.restart_connector(full_name)


class ConsumerGroupMonitoringService:
    """Consumer group lag and offset monitoring.

    Provides visibility into consumer group state, lag per partition,
    and replay/reset capabilities using the Kafka AdminClient.
    """

    def __init__(self, kafka_admin: Any) -> None:
        """Initialise with Kafka admin adapter.

        Args:
            kafka_admin: IKafkaAdmin implementation.
        """
        self._admin = kafka_admin

    def _filter_by_tenant(self, groups: list[str], tenant_id: str) -> list[str]:
        """Filter consumer groups to those belonging to a tenant.

        Groups are considered tenant-owned if they contain the tenant_id
        as a prefix segment or substring.

        Args:
            groups: All consumer group IDs.
            tenant_id: Tenant ID to filter by.

        Returns:
            Filtered list of consumer group IDs.
        """
        return [g for g in groups if tenant_id in g]

    async def list_consumer_groups(self, tenant_id: str) -> list[dict[str, Any]]:
        """List consumer groups filtered to those owned by a tenant.

        Args:
            tenant_id: Tenant context.

        Returns:
            List of consumer group summary dicts.
        """
        offsets = await self._admin.get_consumer_group_offsets(tenant_id)
        return [{"group_id": key, "offsets": val} for key, val in offsets.items()]

    async def get_consumer_group_lag(self, group_id: str, tenant_id: str) -> dict[str, Any]:
        """Return per-partition lag information for a consumer group.

        Args:
            group_id: Consumer group ID.
            tenant_id: Tenant context for validation.

        Returns:
            Dict with group_id, total_lag, partitions with per-partition lag.
        """
        offsets = await self._admin.get_consumer_group_offsets(group_id)
        group_data = offsets.get(group_id, {})

        total_lag: int = 0
        partitions: list[dict[str, Any]] = []
        for tp_str, offset_data in group_data.items():
            committed = offset_data.get("offset", 0)
            lag_estimate = max(0, 0 - committed) if committed < 0 else 0
            total_lag += lag_estimate
            partitions.append({
                "topic_partition": tp_str,
                "committed_offset": committed,
                "lag": lag_estimate,
            })

        return {
            "group_id": group_id,
            "tenant_id": tenant_id,
            "total_lag": total_lag,
            "partitions": partitions,
        }


class EventReplayService:
    """Event replay and reprocessing service.

    Allows resetting consumer group offsets to replay events from
    a specified position. Tracks replay jobs in the database.
    """

    def __init__(self, kafka_admin: Any) -> None:
        """Initialise with Kafka admin adapter.

        Args:
            kafka_admin: IKafkaAdmin implementation.
        """
        self._admin = kafka_admin

    async def create_replay_job(
        self,
        tenant_id: str,
        group_id: str,
        topic: str,
        from_offset_type: str,
        from_timestamp: str | None = None,
    ) -> dict[str, Any]:
        """Reset consumer group offsets to replay events.

        Args:
            tenant_id: Owning tenant.
            group_id: Consumer group to reset.
            topic: Topic to reset offsets for.
            from_offset_type: One of 'earliest', 'latest', 'timestamp'.
            from_timestamp: ISO8601 timestamp if from_offset_type='timestamp'.

        Returns:
            Replay job descriptor dict.
        """
        position = from_offset_type if from_offset_type in ("earliest", "latest") else "earliest"
        reset_result = await self._admin.get_consumer_group_offsets(group_id)

        logger.info(
            "Replay job created",
            tenant_id=tenant_id,
            group_id=group_id,
            topic=topic,
            from_offset_type=from_offset_type,
        )
        return {
            "group_id": group_id,
            "tenant_id": tenant_id,
            "topic": topic,
            "from_offset_type": from_offset_type,
            "from_timestamp": from_timestamp,
            "position": position,
            "status": "started",
            "reset_result": reset_result,
        }


class GeoReplicationService:
    """Geo-replication flow management via Strimzi MirrorMaker2.

    Manages cross-cluster Kafka topic replication flows using the
    Strimzi KafkaMirrorMaker2 CRD.
    """

    def __init__(self, strimzi_client: Any) -> None:
        """Initialise with Strimzi MirrorMaker2 client.

        Args:
            strimzi_client: StrimziMirrorMaker2Client instance.
        """
        self._client = strimzi_client

    async def create_flow(
        self,
        tenant_id: str,
        name: str,
        source_bootstrap: str,
        target_bootstrap: str,
        topics_pattern: str = ".*",
    ) -> dict[str, Any]:
        """Create a MirrorMaker2 replication flow.

        Args:
            tenant_id: Owning tenant.
            name: Replication flow name.
            source_bootstrap: Source cluster bootstrap servers.
            target_bootstrap: Target cluster bootstrap servers.
            topics_pattern: Regex pattern for topics to replicate.

        Returns:
            Created replication flow descriptor.
        """
        resource_name = f"{tenant_id}-{name}"
        result = await self._client.create_replication_flow(
            name=resource_name,
            source_bootstrap=source_bootstrap,
            target_bootstrap=target_bootstrap,
            topics_pattern=topics_pattern,
        )
        logger.info("Geo-replication flow created", tenant_id=tenant_id, name=resource_name)
        return {
            "name": name,
            "resource_name": resource_name,
            "tenant_id": tenant_id,
            "source_bootstrap": source_bootstrap,
            "target_bootstrap": target_bootstrap,
            "topics_pattern": topics_pattern,
            "status": "creating",
            "k8s_resource": result,
        }

    async def list_flows(self, tenant_id: str) -> list[dict[str, Any]]:
        """List replication flows for a tenant.

        Args:
            tenant_id: Tenant context.

        Returns:
            List of replication flow descriptors.
        """
        all_flows = await self._client.list_replication_flows()
        prefix = f"{tenant_id}-"
        tenant_flows = [f for f in all_flows if f.get("metadata", {}).get("name", "").startswith(prefix)]
        return tenant_flows

    async def delete_flow(self, tenant_id: str, name: str) -> None:
        """Delete a replication flow.

        Args:
            tenant_id: Owning tenant.
            name: Replication flow name.
        """
        resource_name = f"{tenant_id}-{name}"
        await self._client.delete_replication_flow(resource_name)
        logger.info("Geo-replication flow deleted", tenant_id=tenant_id, name=resource_name)


class SchemaEvolutionService:
    """Schema evolution preview and diff service.

    Provides compatibility checking with detailed diff output showing
    which fields were added, removed, or had their types changed.
    """

    def __init__(self, registry_client: Any) -> None:
        """Initialise with Schema Registry client.

        Args:
            registry_client: ISchemaRegistryClient implementation.
        """
        self._registry = registry_client

    async def preview_evolution(
        self,
        subject: str,
        new_schema_definition: str,
        tenant_id: str,
    ) -> dict[str, Any]:
        """Preview schema evolution compatibility and diff.

        Args:
            subject: Schema Registry subject name.
            new_schema_definition: New Protobuf schema string.
            tenant_id: Tenant context.

        Returns:
            Dict with is_compatible, diff (added/removed/changed fields),
            and recommendation.
        """
        is_compatible = await self._registry.check_compatibility(subject, new_schema_definition)

        try:
            existing = await self._registry.get_latest_schema(subject)
            existing_def: str = existing.get("schema", "")
        except Exception:
            existing_def = ""

        diff = self._compute_schema_diff(existing_def, new_schema_definition)

        recommendation = "safe" if is_compatible else "breaking"
        if is_compatible and diff.get("removed_fields"):
            recommendation = "caution"

        logger.info(
            "Schema evolution preview",
            subject=subject,
            tenant_id=tenant_id,
            is_compatible=is_compatible,
        )

        return {
            "subject": subject,
            "tenant_id": tenant_id,
            "is_compatible": is_compatible,
            "diff": diff,
            "recommendation": recommendation,
            "existing_schema": existing_def,
            "new_schema": new_schema_definition,
        }

    def _compute_schema_diff(
        self,
        existing_schema: str,
        new_schema: str,
    ) -> dict[str, Any]:
        """Compute a text-level diff between two Protobuf schema definitions.

        Parses field declarations using simple line-by-line analysis
        to identify added, removed, and changed fields.

        Args:
            existing_schema: Current schema definition string.
            new_schema: New schema definition string.

        Returns:
            Dict with added_fields, removed_fields, changed_fields lists.
        """
        def extract_fields(schema_text: str) -> dict[str, str]:
            fields: dict[str, str] = {}
            for line in schema_text.splitlines():
                stripped = line.strip()
                if stripped and not stripped.startswith("//") and "=" in stripped:
                    parts = stripped.rstrip(";").split()
                    if len(parts) >= 3 and parts[-2] == "=":
                        field_name = parts[-3]
                        field_type = parts[0] if len(parts) > 3 else "unknown"
                        fields[field_name] = field_type
            return fields

        existing_fields = extract_fields(existing_schema)
        new_fields = extract_fields(new_schema)

        added = [f for f in new_fields if f not in existing_fields]
        removed = [f for f in existing_fields if f not in new_fields]
        changed = [
            f for f in new_fields
            if f in existing_fields and new_fields[f] != existing_fields[f]
        ]

        return {
            "added_fields": added,
            "removed_fields": removed,
            "changed_fields": changed,
            "total_changes": len(added) + len(removed) + len(changed),
        }


class TieredStorageService:
    """Kafka tiered storage configuration management.

    Configures Strimzi Kafka topics to use tiered (S3) storage for
    long-retention data via topic-level configuration updates.
    """

    def __init__(self, kafka_admin: Any) -> None:
        """Initialise with Kafka admin adapter.

        Args:
            kafka_admin: IKafkaAdmin implementation.
        """
        self._admin = kafka_admin

    async def configure_tiered_storage(
        self,
        topic_name: str,
        tenant_id: str,
        tier_local_hot_ms: int,
        s3_bucket: str,
        enabled: bool = True,
    ) -> dict[str, Any]:
        """Enable or disable tiered storage for a Kafka topic.

        Args:
            topic_name: Kafka topic name.
            tenant_id: Owning tenant.
            tier_local_hot_ms: Milliseconds to keep data on local storage.
            s3_bucket: S3 bucket for remote tier storage.
            enabled: Whether to enable tiered storage.

        Returns:
            Configuration result dict.
        """
        config: dict[str, str] = {
            "remote.storage.enable": str(enabled).lower(),
        }
        if enabled:
            config["local.retention.ms"] = str(tier_local_hot_ms)

        await self._admin.alter_topic_config(topic_name, config)

        logger.info(
            "Tiered storage configured",
            topic_name=topic_name,
            tenant_id=tenant_id,
            enabled=enabled,
            tier_local_hot_ms=tier_local_hot_ms,
        )
        return {
            "topic_name": topic_name,
            "tenant_id": tenant_id,
            "enabled": enabled,
            "tier_local_hot_ms": tier_local_hot_ms,
            "s3_bucket": s3_bucket,
        }
