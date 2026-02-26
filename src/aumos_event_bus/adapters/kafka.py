"""Kafka event publisher for Event Bus internal audit events.

Thin wrapper around aumos-common EventPublisher, publishing domain events
emitted by the Event Bus management service itself (topic created, schema
registered, DLQ entry resolved, etc.).
"""

from __future__ import annotations

from typing import Any

from aumos_common.events import EventPublisher, Topics
from aumos_common.observability import get_logger

logger = get_logger(__name__)


class EventBusEventPublisher:
    """Publishes internal Event Bus domain events to Kafka.

    Delegates to aumos-common EventPublisher for all actual Kafka I/O.
    This class exists to provide a clear named publisher specific to
    the event-bus service boundary.
    """

    def __init__(self, publisher: EventPublisher) -> None:
        """Initialise with an aumos-common EventPublisher.

        Args:
            publisher: Configured aumos-common EventPublisher instance.
        """
        self._publisher = publisher

    async def publish_topic_created(
        self,
        tenant_id: str,
        topic_name: str,
        partitions: int,
        actor: str,
    ) -> None:
        """Publish a topic-created audit event.

        Args:
            tenant_id: Owning tenant UUID string.
            topic_name: Name of the newly created topic.
            partitions: Partition count of the new topic.
            actor: Identity of the user or service that created the topic.
        """
        try:
            await self._publisher.publish(
                Topics.audit(tenant_id),
                {
                    "event_type": "topic.created",
                    "tenant_id": tenant_id,
                    "topic_name": topic_name,
                    "partitions": partitions,
                    "actor": actor,
                    "source_service": "aumos-event-bus",
                },
            )
        except Exception:
            logger.exception(
                "Failed to publish topic_created event",
                topic_name=topic_name,
                tenant_id=tenant_id,
            )

    async def publish_topic_deleted(
        self,
        tenant_id: str,
        topic_name: str,
        actor: str,
    ) -> None:
        """Publish a topic-deleted audit event.

        Args:
            tenant_id: Owning tenant UUID string.
            topic_name: Name of the deleted topic.
            actor: Identity of the user or service that deleted the topic.
        """
        try:
            await self._publisher.publish(
                Topics.audit(tenant_id),
                {
                    "event_type": "topic.deleted",
                    "tenant_id": tenant_id,
                    "topic_name": topic_name,
                    "actor": actor,
                    "source_service": "aumos-event-bus",
                },
            )
        except Exception:
            logger.exception(
                "Failed to publish topic_deleted event",
                topic_name=topic_name,
                tenant_id=tenant_id,
            )

    async def publish_schema_registered(
        self,
        tenant_id: str,
        subject: str,
        schema_id: int,
        schema_version: int,
        actor: str,
    ) -> None:
        """Publish a schema-registered audit event.

        Args:
            tenant_id: Owning tenant UUID string.
            subject: Schema Registry subject name.
            schema_id: Registry-assigned integer schema ID.
            schema_version: Version number assigned by the registry.
            actor: Identity of the registering user or service.
        """
        try:
            await self._publisher.publish(
                Topics.audit(tenant_id),
                {
                    "event_type": "schema.registered",
                    "tenant_id": tenant_id,
                    "subject": subject,
                    "schema_id": schema_id,
                    "schema_version": schema_version,
                    "actor": actor,
                    "source_service": "aumos-event-bus",
                },
            )
        except Exception:
            logger.exception(
                "Failed to publish schema_registered event",
                subject=subject,
                tenant_id=tenant_id,
            )

    async def publish_dlq_resolved(
        self,
        tenant_id: str,
        entry_id: str,
        source_topic: str,
        resolution: str,
        actor: str,
    ) -> None:
        """Publish a DLQ-entry-resolved audit event.

        Args:
            tenant_id: Owning tenant UUID string.
            entry_id: UUID string of the resolved DLQ entry.
            source_topic: Original topic the message came from.
            resolution: Resolution type — "resolved" or "abandoned".
            actor: Identity of the operator.
        """
        try:
            await self._publisher.publish(
                Topics.audit(tenant_id),
                {
                    "event_type": f"dlq.{resolution}",
                    "tenant_id": tenant_id,
                    "entry_id": entry_id,
                    "source_topic": source_topic,
                    "resolution": resolution,
                    "actor": actor,
                    "source_service": "aumos-event-bus",
                },
            )
        except Exception:
            logger.exception(
                "Failed to publish dlq_resolved event",
                entry_id=entry_id,
                tenant_id=tenant_id,
            )
