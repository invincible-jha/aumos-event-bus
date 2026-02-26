"""FastAPI router for the Event Bus management API.

All routes are thin — they extract request context, delegate to core services,
and return typed Pydantic responses. No business logic lives here.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aumos_common.auth import get_current_tenant, get_current_user
from aumos_common.database import get_db_session
from aumos_common.errors import NotFoundError

from aumos_event_bus.adapters.kafka_admin import KafkaAdminAdapter
from aumos_event_bus.adapters.repositories import DLQRepository, SchemaRepository, TopicRepository
from aumos_event_bus.adapters.schema_registry import SchemaRegistryAdapter
from aumos_event_bus.api.schemas import (
    DLQEntryResponse,
    DLQListResponse,
    DLQResolveResponse,
    DLQRetryResponse,
    SchemaCompatibilityRequest,
    SchemaCompatibilityResponse,
    SchemaRegisterRequest,
    SchemaResponse,
    SchemaVersionsResponse,
    TenantPartitionResponse,
    TopicCreateRequest,
    TopicListResponse,
    TopicMetricsResponse,
    TopicResponse,
)
from aumos_event_bus.core.services import (
    DLQManagementService,
    SchemaRegisterRequest as CoreSchemaRequest,
    SchemaValidationService,
    TopicCreateRequest as CoreTopicRequest,
    TopicManagementService,
)
from aumos_event_bus.core.tenant_partitioner import TenantPartitioner
from aumos_event_bus.settings import Settings

router = APIRouter(tags=["event-bus"])

settings = Settings()
_partitioner = TenantPartitioner(seed=settings.tenant_partitioner_seed)


# ---------------------------------------------------------------------------
# Dependency factories
# ---------------------------------------------------------------------------


def get_topic_service(session: Annotated[AsyncSession, Depends(get_db_session)]) -> TopicManagementService:
    """Construct TopicManagementService with injected dependencies.

    Args:
        session: Async database session from DI.

    Returns:
        Configured TopicManagementService instance.
    """
    kafka_admin = KafkaAdminAdapter(settings=settings)
    return TopicManagementService(
        topic_repo=TopicRepository(session),
        kafka_admin=kafka_admin,
        partitioner=_partitioner,
    )


def get_schema_service(session: Annotated[AsyncSession, Depends(get_db_session)]) -> SchemaValidationService:
    """Construct SchemaValidationService with injected dependencies.

    Args:
        session: Async database session from DI.

    Returns:
        Configured SchemaValidationService instance.
    """
    registry_client = SchemaRegistryAdapter(settings=settings)
    return SchemaValidationService(
        schema_repo=SchemaRepository(session),
        registry_client=registry_client,
    )


def get_dlq_service(session: Annotated[AsyncSession, Depends(get_db_session)]) -> DLQManagementService:
    """Construct DLQManagementService with injected dependencies.

    Args:
        session: Async database session from DI.

    Returns:
        Configured DLQManagementService instance.
    """
    kafka_admin = KafkaAdminAdapter(settings=settings)
    return DLQManagementService(
        dlq_repo=DLQRepository(session),
        kafka_admin=kafka_admin,
        max_retries=settings.dlq_max_retries,
        initial_backoff_ms=settings.dlq_initial_backoff_ms,
        max_backoff_ms=settings.dlq_max_backoff_ms,
        backoff_multiplier=settings.dlq_backoff_multiplier,
    )


# ---------------------------------------------------------------------------
# Topic endpoints
# ---------------------------------------------------------------------------


@router.post("/topics", response_model=TopicResponse, status_code=201)
async def create_topic(
    body: TopicCreateRequest,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    actor: Annotated[str, Depends(get_current_user)],
    service: Annotated[TopicManagementService, Depends(get_topic_service)],
) -> TopicResponse:
    """Create and provision a new Kafka topic.

    Args:
        body: Topic configuration parameters.
        tenant_id: Tenant extracted from auth context.
        actor: User or service identity from auth context.
        service: Injected topic management service.

    Returns:
        The created topic definition.
    """
    core_request = CoreTopicRequest(
        topic_name=body.topic_name,
        display_name=body.display_name,
        partitions=body.partitions,
        replication_factor=body.replication_factor,
        retention_ms=body.retention_ms,
        cleanup_policy=body.cleanup_policy,
        compression_type=body.compression_type,
        min_isr=body.min_isr,
        description=body.description,
        schema_subject=body.schema_subject,
        config_overrides=body.config_overrides,
    )
    result = await service.create_topic(core_request, tenant_id=tenant_id, actor=actor)
    return TopicResponse(**result)


@router.get("/topics", response_model=TopicListResponse)
async def list_topics(
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[TopicManagementService, Depends(get_topic_service)],
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
) -> TopicListResponse:
    """List all topic definitions for the current tenant.

    Args:
        tenant_id: Tenant extracted from auth context.
        service: Injected topic management service.
        skip: Pagination offset.
        limit: Maximum results to return.

    Returns:
        Paginated list of topic definitions.
    """
    items = await service.list_topics(tenant_id=tenant_id, skip=skip, limit=limit)
    return TopicListResponse(items=[TopicResponse(**t) for t in items], total=len(items), skip=skip, limit=limit)


@router.get("/topics/{topic_id}", response_model=TopicResponse)
async def get_topic(
    topic_id: uuid.UUID,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[TopicManagementService, Depends(get_topic_service)],
) -> TopicResponse:
    """Retrieve a topic definition by ID.

    Args:
        topic_id: UUID of the topic.
        tenant_id: Tenant extracted from auth context.
        service: Injected topic management service.

    Returns:
        Topic definition.

    Raises:
        404: If topic does not exist for this tenant.
    """
    result = await service.get_topic(topic_id=topic_id, tenant_id=tenant_id)
    return TopicResponse(**result)


@router.delete("/topics/{topic_id}", status_code=204)
async def delete_topic(
    topic_id: uuid.UUID,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    actor: Annotated[str, Depends(get_current_user)],
    service: Annotated[TopicManagementService, Depends(get_topic_service)],
    force: bool = Query(default=False),
) -> None:
    """Delete a topic from Kafka and remove its database record.

    Args:
        topic_id: UUID of the topic to delete.
        tenant_id: Tenant extracted from auth context.
        actor: User or service identity.
        service: Injected topic management service.
        force: If True, delete even with active consumers.

    Raises:
        404: If topic does not exist.
    """
    await service.delete_topic(topic_id=topic_id, tenant_id=tenant_id, actor=actor, force=force)


@router.get("/topics/{topic_id}/metrics", response_model=TopicMetricsResponse)
async def get_topic_metrics(
    topic_id: uuid.UUID,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[TopicManagementService, Depends(get_topic_service)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TopicMetricsResponse:
    """Get runtime metrics for a Kafka topic.

    Fetches live partition info and consumer group offsets from the broker.

    Args:
        topic_id: UUID of the topic.
        tenant_id: Tenant extracted from auth context.
        service: Injected topic management service.
        session: Database session for topic lookup.

    Returns:
        Live metrics for the topic.

    Raises:
        404: If topic does not exist.
    """
    topic = await service.get_topic(topic_id=topic_id, tenant_id=tenant_id)
    topic_repo = TopicRepository(session)
    _ = topic_repo  # repo available for future enrichment
    kafka_admin = KafkaAdminAdapter(settings=settings)
    topic_name = topic["topic_name"]
    try:
        topic_info = await kafka_admin.describe_topic(topic_name)
    except Exception:
        topic_info = {}

    return TopicMetricsResponse(
        topic_id=topic_id,
        topic_name=topic_name,
        partition_count=topic.get("partitions", 0),
        config=topic_info.get("config", {}),
        consumer_groups=topic_info.get("consumer_groups", []),
    )


# ---------------------------------------------------------------------------
# Schema endpoints
# ---------------------------------------------------------------------------


@router.post("/schemas", response_model=SchemaResponse, status_code=201)
async def register_schema(
    body: SchemaRegisterRequest,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    actor: Annotated[str, Depends(get_current_user)],
    service: Annotated[SchemaValidationService, Depends(get_schema_service)],
) -> SchemaResponse:
    """Register a Protobuf schema with the Schema Registry.

    Args:
        body: Schema definition and configuration.
        tenant_id: Tenant extracted from auth context.
        actor: User or service identity.
        service: Injected schema validation service.

    Returns:
        The registered schema version.
    """
    core_request = CoreSchemaRequest(
        subject=body.subject,
        schema_definition=body.schema_definition,
        schema_type=body.schema_type,
        compatibility=body.compatibility,
        topic_id=body.topic_id,
    )
    result = await service.register_schema(core_request, tenant_id=tenant_id, actor=actor)
    return SchemaResponse(**result)


@router.get("/schemas/{subject}", response_model=SchemaResponse)
async def get_latest_schema(
    subject: str,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[SchemaValidationService, Depends(get_schema_service)],
) -> SchemaResponse:
    """Retrieve the latest active schema version for a subject.

    Args:
        subject: Schema Registry subject name.
        tenant_id: Tenant extracted from auth context.
        service: Injected schema validation service.

    Returns:
        Latest active schema version.

    Raises:
        404: If no schema exists for the subject.
    """
    result = await service.get_latest_schema(subject=subject, tenant_id=tenant_id)
    return SchemaResponse(**result)


@router.get("/schemas/{subject}/versions", response_model=SchemaVersionsResponse)
async def list_schema_versions(
    subject: str,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> SchemaVersionsResponse:
    """List all schema versions for a subject.

    Args:
        subject: Schema Registry subject name.
        tenant_id: Tenant extracted from auth context.
        session: Database session.

    Returns:
        All versions for the subject.
    """
    schema_repo = SchemaRepository(session)
    versions = await schema_repo.list_versions(subject=subject, tenant_id=tenant_id)
    return SchemaVersionsResponse(
        subject=subject,
        versions=[SchemaResponse(**v) for v in versions],
    )


@router.post("/schemas/{subject}/compatibility", response_model=SchemaCompatibilityResponse)
async def check_schema_compatibility(
    subject: str,
    body: SchemaCompatibilityRequest,
    service: Annotated[SchemaValidationService, Depends(get_schema_service)],
) -> SchemaCompatibilityResponse:
    """Check whether a schema definition is compatible with existing versions.

    Args:
        subject: Schema Registry subject name.
        body: Schema definition to test.
        service: Injected schema validation service.

    Returns:
        Compatibility check result.
    """
    is_compatible = await service.check_compatibility(
        subject=subject, schema_definition=body.schema_definition
    )
    message = "Schema is compatible" if is_compatible else "Schema is not compatible with existing versions"
    return SchemaCompatibilityResponse(subject=subject, is_compatible=is_compatible, message=message)


# ---------------------------------------------------------------------------
# DLQ endpoints
# ---------------------------------------------------------------------------


@router.get("/dlq", response_model=DLQListResponse)
async def list_dlq_entries(
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[DLQManagementService, Depends(get_dlq_service)],
    source_topic: str | None = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
) -> DLQListResponse:
    """List dead letter queue entries for the current tenant.

    Args:
        tenant_id: Tenant extracted from auth context.
        service: Injected DLQ management service.
        source_topic: Optional filter by source topic name.
        skip: Pagination offset.
        limit: Maximum results.

    Returns:
        Paginated list of DLQ entries.
    """
    items = await service.list_pending_entries(
        tenant_id=tenant_id, source_topic=source_topic, skip=skip, limit=limit
    )
    return DLQListResponse(items=[DLQEntryResponse(**e) for e in items], total=len(items), skip=skip, limit=limit)


@router.post("/dlq/{entry_id}/retry", response_model=DLQRetryResponse)
async def retry_dlq_entry(
    entry_id: uuid.UUID,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    actor: Annotated[str, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> DLQRetryResponse:
    """Schedule a DLQ entry for immediate retry.

    Resets the next_retry_at to now and updates status to RETRYING.

    Args:
        entry_id: UUID of the DLQ entry.
        tenant_id: Tenant extracted from auth context.
        actor: Operator triggering the retry.
        session: Database session.

    Returns:
        Updated retry status.

    Raises:
        404: If entry does not exist.
    """
    dlq_repo = DLQRepository(session)
    entry = await dlq_repo.get_by_id(entry_id=entry_id, tenant_id=tenant_id)
    if not entry:
        raise NotFoundError(resource="dlq_entry", resource_id=str(entry_id))

    import time as _time

    next_retry_at = int(_time.time() * 1000)
    new_retry_count = entry.get("retry_count", 0) + 1
    await dlq_repo.update_status(
        entry_id=entry_id,
        tenant_id=tenant_id,
        status="retrying",
        retry_count=new_retry_count,
    )
    return DLQRetryResponse(
        entry_id=entry_id,
        status="retrying",
        next_retry_at=next_retry_at,
        retry_count=new_retry_count,
        message=f"Entry scheduled for retry by {actor}",
    )


@router.post("/dlq/{entry_id}/resolve", response_model=DLQResolveResponse)
async def resolve_dlq_entry(
    entry_id: uuid.UUID,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    actor: Annotated[str, Depends(get_current_user)],
    service: Annotated[DLQManagementService, Depends(get_dlq_service)],
) -> DLQResolveResponse:
    """Mark a DLQ entry as manually resolved.

    Args:
        entry_id: UUID of the DLQ entry.
        tenant_id: Tenant extracted from auth context.
        actor: Operator resolving the entry.
        service: Injected DLQ management service.

    Returns:
        Updated resolution status.
    """
    result = await service.mark_resolved(entry_id=entry_id, tenant_id=tenant_id, actor=actor)
    return DLQResolveResponse(entry_id=entry_id, status=result["status"], message="Entry resolved successfully")


@router.post("/dlq/{entry_id}/abandon", response_model=DLQResolveResponse)
async def abandon_dlq_entry(
    entry_id: uuid.UUID,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    actor: Annotated[str, Depends(get_current_user)],
    service: Annotated[DLQManagementService, Depends(get_dlq_service)],
) -> DLQResolveResponse:
    """Mark a DLQ entry as abandoned — no further retries.

    Args:
        entry_id: UUID of the DLQ entry.
        tenant_id: Tenant extracted from auth context.
        actor: Operator abandoning the entry.
        service: Injected DLQ management service.

    Returns:
        Updated abandonment status.
    """
    result = await service.mark_abandoned(entry_id=entry_id, tenant_id=tenant_id, actor=actor)
    return DLQResolveResponse(entry_id=entry_id, status=result["status"], message="Entry abandoned")


# ---------------------------------------------------------------------------
# Partition endpoints
# ---------------------------------------------------------------------------


@router.get("/partitions/{topic_name}/tenant/{tenant_id_param}", response_model=TenantPartitionResponse)
async def get_tenant_partition(
    topic_name: str,
    tenant_id_param: str,
    current_tenant: Annotated[str, Depends(get_current_tenant)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TenantPartitionResponse:
    """Get the partition assignment for a specific tenant on a topic.

    Args:
        topic_name: Kafka topic name.
        tenant_id_param: Tenant ID to look up (path param).
        current_tenant: Authenticated tenant context.
        session: Database session for topic lookup.

    Returns:
        Partition assignment for the tenant.

    Raises:
        404: If topic does not exist.
    """
    topic_repo = TopicRepository(session)
    topic = await topic_repo.get_by_name(topic_name=topic_name, tenant_id=current_tenant)
    if not topic:
        raise NotFoundError(resource="topic", resource_id=topic_name)

    partitions = topic.get("partitions", 6)
    assignment = _partitioner.assign(
        tenant_id=tenant_id_param,
        topic_name=topic_name,
        partitions=partitions,
    )
    return TenantPartitionResponse(
        tenant_id=tenant_id_param,
        topic_name=topic_name,
        assigned_partition=assignment.partition,
        total_partitions=assignment.total_partitions,
        hash_value=assignment.hash_value,
    )
