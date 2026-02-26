"""SQLAlchemy repositories for the Event Bus management service.

Each repository extends BaseRepository from aumos-common, which provides
tenant-aware CRUD operations with automatic RLS enforcement.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aumos_common.database import BaseRepository

from aumos_event_bus.core.models import DLQEntry, DLQStatus, SchemaVersion, TopicDefinition


class TopicRepository(BaseRepository[TopicDefinition]):
    """Repository for Kafka topic definitions.

    Persists and queries TopicDefinition records in the evt_topic_definitions table.
    All methods enforce tenant isolation via aumos-common RLS context.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialise with a database session.

        Args:
            session: Async SQLAlchemy session with RLS context set.
        """
        super().__init__(session=session, model=TopicDefinition)

    async def get_by_id(self, topic_id: uuid.UUID, tenant_id: str) -> dict[str, Any] | None:
        """Retrieve a topic definition by primary key and tenant.

        Args:
            topic_id: UUID primary key of the topic.
            tenant_id: Tenant context for RLS verification.

        Returns:
            Topic as dict, or None if not found.
        """
        stmt = select(TopicDefinition).where(
            TopicDefinition.id == topic_id,
            TopicDefinition.tenant_id == tenant_id,
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return self._to_dict(row) if row else None

    async def get_by_name(self, topic_name: str, tenant_id: str) -> dict[str, Any] | None:
        """Retrieve a topic definition by Kafka topic name.

        Args:
            topic_name: Kafka topic name string.
            tenant_id: Tenant context for RLS verification.

        Returns:
            Topic as dict, or None if not found.
        """
        stmt = select(TopicDefinition).where(
            TopicDefinition.topic_name == topic_name,
            TopicDefinition.tenant_id == tenant_id,
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return self._to_dict(row) if row else None

    async def list_all(self, tenant_id: str, skip: int = 0, limit: int = 50) -> list[dict[str, Any]]:
        """List all topic definitions for a tenant, paginated.

        Args:
            tenant_id: Tenant context.
            skip: Number of rows to skip.
            limit: Maximum rows to return.

        Returns:
            List of topic dicts.
        """
        stmt = (
            select(TopicDefinition)
            .where(TopicDefinition.tenant_id == tenant_id)
            .offset(skip)
            .limit(limit)
            .order_by(TopicDefinition.created_at.desc())
        )
        result = await self._session.execute(stmt)
        return [self._to_dict(row) for row in result.scalars().all()]

    async def create(self, data: dict[str, Any]) -> dict[str, Any]:
        """Persist a new topic definition.

        Args:
            data: Field values for the new TopicDefinition row.

        Returns:
            Created topic as dict.
        """
        topic = TopicDefinition(**data)
        self._session.add(topic)
        await self._session.flush()
        await self._session.refresh(topic)
        return self._to_dict(topic)

    async def update(
        self, topic_id: uuid.UUID, tenant_id: str, updates: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Update an existing topic definition.

        Args:
            topic_id: UUID of the topic to update.
            tenant_id: Tenant context.
            updates: Fields to update.

        Returns:
            Updated topic as dict, or None if not found.
        """
        stmt = (
            update(TopicDefinition)
            .where(TopicDefinition.id == topic_id, TopicDefinition.tenant_id == tenant_id)
            .values(**updates)
            .returning(TopicDefinition)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return self._to_dict(row) if row else None

    async def delete(self, topic_id: uuid.UUID, tenant_id: str) -> bool:
        """Delete a topic definition by ID.

        Args:
            topic_id: UUID of the topic to delete.
            tenant_id: Tenant context.

        Returns:
            True if deleted, False if not found.
        """
        row = await self.get_by_id(topic_id, tenant_id)
        if not row:
            return False
        stmt = select(TopicDefinition).where(
            TopicDefinition.id == topic_id,
            TopicDefinition.tenant_id == tenant_id,
        )
        result = await self._session.execute(stmt)
        instance = result.scalar_one_or_none()
        if instance:
            await self._session.delete(instance)
            await self._session.flush()
            return True
        return False

    def _to_dict(self, obj: TopicDefinition) -> dict[str, Any]:
        """Serialize a TopicDefinition ORM object to a plain dict.

        Args:
            obj: SQLAlchemy ORM instance.

        Returns:
            Dict representation with all mapped columns.
        """
        return {
            "id": obj.id,
            "tenant_id": obj.tenant_id,
            "topic_name": obj.topic_name,
            "display_name": obj.display_name,
            "description": obj.description,
            "partitions": obj.partitions,
            "replication_factor": obj.replication_factor,
            "retention_ms": obj.retention_ms,
            "cleanup_policy": obj.cleanup_policy,
            "compression_type": obj.compression_type,
            "min_isr": obj.min_isr,
            "status": obj.status,
            "is_system_topic": obj.is_system_topic,
            "schema_subject": obj.schema_subject,
            "config_overrides": obj.config_overrides,
            "created_at": obj.created_at,
            "updated_at": obj.updated_at,
        }


class SchemaRepository(BaseRepository[SchemaVersion]):
    """Repository for Protobuf schema version metadata.

    Persists SchemaVersion records that mirror entries in Confluent Schema Registry.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialise with a database session.

        Args:
            session: Async SQLAlchemy session with RLS context set.
        """
        super().__init__(session=session, model=SchemaVersion)

    async def get_by_id(self, schema_id: uuid.UUID, tenant_id: str) -> dict[str, Any] | None:
        """Retrieve a schema version by primary key.

        Args:
            schema_id: UUID primary key of the schema version.
            tenant_id: Tenant context.

        Returns:
            Schema version as dict, or None if not found.
        """
        stmt = select(SchemaVersion).where(
            SchemaVersion.id == schema_id,
            SchemaVersion.tenant_id == tenant_id,
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return self._to_dict(row) if row else None

    async def get_latest_by_subject(self, subject: str, tenant_id: str) -> dict[str, Any] | None:
        """Retrieve the latest active schema version for a subject.

        Args:
            subject: Schema Registry subject string.
            tenant_id: Tenant context.

        Returns:
            Latest active SchemaVersion as dict, or None.
        """
        stmt = (
            select(SchemaVersion)
            .where(
                SchemaVersion.subject == subject,
                SchemaVersion.tenant_id == tenant_id,
                SchemaVersion.is_active.is_(True),
            )
            .order_by(SchemaVersion.schema_version.desc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return self._to_dict(row) if row else None

    async def list_versions(self, subject: str, tenant_id: str) -> list[dict[str, Any]]:
        """List all schema versions for a subject.

        Args:
            subject: Schema Registry subject string.
            tenant_id: Tenant context.

        Returns:
            All versions ordered by version number descending.
        """
        stmt = (
            select(SchemaVersion)
            .where(SchemaVersion.subject == subject, SchemaVersion.tenant_id == tenant_id)
            .order_by(SchemaVersion.schema_version.desc())
        )
        result = await self._session.execute(stmt)
        return [self._to_dict(row) for row in result.scalars().all()]

    async def create(self, data: dict[str, Any]) -> dict[str, Any]:
        """Persist a new schema version record.

        Args:
            data: Field values for the new SchemaVersion row.

        Returns:
            Created schema version as dict.
        """
        schema = SchemaVersion(**data)
        self._session.add(schema)
        await self._session.flush()
        await self._session.refresh(schema)
        return self._to_dict(schema)

    async def deactivate_version(self, schema_id: uuid.UUID, tenant_id: str) -> bool:
        """Mark a schema version as inactive.

        Args:
            schema_id: UUID of the schema version to deactivate.
            tenant_id: Tenant context.

        Returns:
            True if deactivated, False if not found.
        """
        stmt = (
            update(SchemaVersion)
            .where(SchemaVersion.id == schema_id, SchemaVersion.tenant_id == tenant_id)
            .values(is_active=False)
        )
        result = await self._session.execute(stmt)
        return result.rowcount > 0

    def _to_dict(self, obj: SchemaVersion) -> dict[str, Any]:
        """Serialize a SchemaVersion ORM object to a plain dict.

        Args:
            obj: SQLAlchemy ORM instance.

        Returns:
            Dict representation with all mapped columns.
        """
        return {
            "id": obj.id,
            "tenant_id": obj.tenant_id,
            "topic_id": obj.topic_id,
            "subject": obj.subject,
            "schema_version": obj.schema_version,
            "schema_id": obj.schema_id,
            "schema_definition": obj.schema_definition,
            "schema_type": obj.schema_type,
            "compatibility": obj.compatibility,
            "is_active": obj.is_active,
            "metadata": obj.metadata,
            "created_at": obj.created_at,
            "updated_at": obj.updated_at,
        }


class DLQRepository(BaseRepository[DLQEntry]):
    """Repository for dead letter queue entries.

    Persists DLQEntry records tracking failed messages with retry metadata.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialise with a database session.

        Args:
            session: Async SQLAlchemy session with RLS context set.
        """
        super().__init__(session=session, model=DLQEntry)

    async def get_by_id(self, entry_id: uuid.UUID, tenant_id: str) -> dict[str, Any] | None:
        """Retrieve a DLQ entry by primary key.

        Args:
            entry_id: UUID primary key of the DLQ entry.
            tenant_id: Tenant context.

        Returns:
            DLQ entry as dict, or None if not found.
        """
        stmt = select(DLQEntry).where(
            DLQEntry.id == entry_id,
            DLQEntry.tenant_id == tenant_id,
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return self._to_dict(row) if row else None

    async def list_pending(
        self,
        tenant_id: str,
        source_topic: str | None = None,
        skip: int = 0,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List DLQ entries with PENDING or RETRYING status.

        Args:
            tenant_id: Tenant context.
            source_topic: Optional filter by source topic name.
            skip: Pagination offset.
            limit: Maximum results.

        Returns:
            List of DLQ entry dicts.
        """
        pending_statuses = [DLQStatus.PENDING.value, DLQStatus.RETRYING.value]
        stmt = select(DLQEntry).where(
            DLQEntry.tenant_id == tenant_id,
            DLQEntry.status.in_(pending_statuses),
        )
        if source_topic:
            stmt = stmt.where(DLQEntry.source_topic == source_topic)
        stmt = stmt.offset(skip).limit(limit).order_by(DLQEntry.created_at.desc())
        result = await self._session.execute(stmt)
        return [self._to_dict(row) for row in result.scalars().all()]

    async def create(self, data: dict[str, Any]) -> dict[str, Any]:
        """Persist a new DLQ entry.

        Args:
            data: Field values for the new DLQEntry row.

        Returns:
            Created DLQ entry as dict.
        """
        entry = DLQEntry(**data)
        self._session.add(entry)
        await self._session.flush()
        await self._session.refresh(entry)
        return self._to_dict(entry)

    async def update_status(
        self,
        entry_id: uuid.UUID,
        tenant_id: str,
        status: str,
        retry_count: int | None = None,
    ) -> bool:
        """Update the status (and optionally retry count) of a DLQ entry.

        Args:
            entry_id: UUID of the DLQ entry.
            tenant_id: Tenant context.
            status: New status string.
            retry_count: Optional new retry count value.

        Returns:
            True if updated, False if not found.
        """
        values: dict[str, Any] = {"status": status}
        if retry_count is not None:
            values["retry_count"] = retry_count
        stmt = (
            update(DLQEntry)
            .where(DLQEntry.id == entry_id, DLQEntry.tenant_id == tenant_id)
            .values(**values)
        )
        result = await self._session.execute(stmt)
        return result.rowcount > 0

    async def get_entries_due_for_retry(
        self, current_epoch_ms: int, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Retrieve DLQ entries whose next_retry_at has passed.

        Used by background retry workers to pick up scheduled retries.

        Args:
            current_epoch_ms: Current time as Unix epoch milliseconds.
            limit: Maximum entries to retrieve per batch.

        Returns:
            List of DLQ entry dicts ready for retry.
        """
        stmt = (
            select(DLQEntry)
            .where(
                DLQEntry.status == DLQStatus.PENDING.value,
                DLQEntry.next_retry_at <= current_epoch_ms,
                DLQEntry.retry_count < DLQEntry.max_retries,
            )
            .limit(limit)
            .order_by(DLQEntry.next_retry_at.asc())
        )
        result = await self._session.execute(stmt)
        return [self._to_dict(row) for row in result.scalars().all()]

    def _to_dict(self, obj: DLQEntry) -> dict[str, Any]:
        """Serialize a DLQEntry ORM object to a plain dict.

        Args:
            obj: SQLAlchemy ORM instance.

        Returns:
            Dict representation with all mapped columns.
        """
        return {
            "id": obj.id,
            "tenant_id": obj.tenant_id,
            "topic_id": obj.topic_id,
            "source_topic": obj.source_topic,
            "dlq_topic": obj.dlq_topic,
            "message_key": obj.message_key,
            "message_value": obj.message_value,
            "message_headers": obj.message_headers,
            "original_offset": obj.original_offset,
            "original_partition": obj.original_partition,
            "failure_reason": obj.failure_reason,
            "failure_details": obj.failure_details,
            "status": obj.status,
            "retry_count": obj.retry_count,
            "max_retries": obj.max_retries,
            "next_retry_at": obj.next_retry_at,
            "resolved_at": obj.resolved_at,
            "consumer_group": obj.consumer_group,
            "correlation_id": obj.correlation_id,
            "created_at": obj.created_at,
            "updated_at": obj.updated_at,
        }
