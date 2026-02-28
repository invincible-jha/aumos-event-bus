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
    ConnectorCreateRequest,
    ConnectorListResponse,
    ConnectorResponse,
    ConsumerGroupLagResponse,
    ConsumerGroupListResponse,
    DLQEntryResponse,
    DLQListResponse,
    DLQResolveResponse,
    DLQRetryResponse,
    GeoReplicationFlowCreateRequest,
    GeoReplicationFlowResponse,
    ReplayJobCreateRequest,
    ReplayJobResponse,
    SchemaCompatibilityRequest,
    SchemaCompatibilityResponse,
    SchemaEvolutionPreviewRequest,
    SchemaEvolutionPreviewResponse,
    SchemaRegisterRequest,
    SchemaResponse,
    SchemaVersionsResponse,
    StreamJobCreateRequest,
    StreamJobListResponse,
    StreamJobResponse,
    TenantPartitionResponse,
    TieredStorageConfigRequest,
    TieredStorageConfigResponse,
    TopicCreateRequest,
    TopicListResponse,
    TopicMetricsResponse,
    TopicResponse,
)
from aumos_event_bus.adapters.kafka_connect_client import KafkaConnectClient
from aumos_event_bus.adapters.ksqldb_client import KsqldbClient
from aumos_event_bus.adapters.flink_client import FlinkClient
from aumos_event_bus.adapters.strimzi_client import StrimziMirrorMaker2Client
from aumos_event_bus.core.services import (
    ConnectorService,
    ConsumerGroupMonitoringService,
    DLQManagementService,
    EventReplayService,
    GeoReplicationService,
    SchemaEvolutionService,
    SchemaRegisterRequest as CoreSchemaRequest,
    SchemaValidationService,
    StreamProcessingService,
    TieredStorageService,
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
# Stream processing endpoints (Gap #23)
# ---------------------------------------------------------------------------


def get_stream_service() -> StreamProcessingService:
    """Construct StreamProcessingService with configured backend clients.

    Returns:
        Configured StreamProcessingService instance.
    """
    ksqldb_client = KsqldbClient(
        base_url=settings.ksqldb_url,
        username=settings.ksqldb_username,
        password=settings.ksqldb_password,
    ) if settings.stream_backend == "ksqldb" else None
    flink_client = FlinkClient(base_url=settings.flink_rest_url) if settings.stream_backend == "flink" else None
    return StreamProcessingService(
        ksqldb_client=ksqldb_client,
        flink_client=flink_client,
        backend=settings.stream_backend,
    )


@router.post("/streams", response_model=StreamJobResponse, status_code=201)
async def create_stream_job(
    body: StreamJobCreateRequest,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[StreamProcessingService, Depends(get_stream_service)],
) -> StreamJobResponse:
    """Create a stream processing job on the configured backend (ksqlDB or Flink).

    Args:
        body: Stream job configuration.
        tenant_id: Tenant extracted from auth context.
        service: Injected stream processing service.

    Returns:
        Created stream job descriptor.
    """
    result = await service.create_stream_job(
        tenant_id=tenant_id,
        name=body.name,
        query_text=body.query_text,
        input_topics=body.input_topics,
        output_topic=body.output_topic,
        backend_override=body.backend,
    )
    return StreamJobResponse(**result)


@router.get("/streams", response_model=StreamJobListResponse)
async def list_stream_jobs(
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[StreamProcessingService, Depends(get_stream_service)],
) -> StreamJobListResponse:
    """List active stream processing jobs.

    Args:
        tenant_id: Tenant extracted from auth context.
        service: Injected stream processing service.

    Returns:
        List of active stream jobs.
    """
    jobs = await service.list_stream_jobs()
    return StreamJobListResponse(jobs=jobs, total=len(jobs))


@router.delete("/streams/{backend_job_id}", status_code=200)
async def terminate_stream_job(
    backend_job_id: str,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[StreamProcessingService, Depends(get_stream_service)],
) -> dict:
    """Terminate a running stream processing job.

    Args:
        backend_job_id: Backend-specific job ID.
        tenant_id: Tenant extracted from auth context.
        service: Injected stream processing service.

    Returns:
        Termination result.
    """
    result = await service.terminate_stream_job(backend_job_id=backend_job_id)
    return result


# ---------------------------------------------------------------------------
# Kafka Connect connector endpoints (Gap #24)
# ---------------------------------------------------------------------------


def get_connector_service() -> ConnectorService:
    """Construct ConnectorService with Kafka Connect client.

    Returns:
        Configured ConnectorService instance.
    """
    client = KafkaConnectClient(base_url=settings.kafka_connect_url)
    return ConnectorService(connect_client=client)


@router.post("/connectors", response_model=ConnectorResponse, status_code=201)
async def create_connector(
    body: ConnectorCreateRequest,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[ConnectorService, Depends(get_connector_service)],
) -> ConnectorResponse:
    """Create a Kafka Connect connector scoped to the current tenant.

    Args:
        body: Connector name and configuration.
        tenant_id: Tenant extracted from auth context.
        service: Injected connector service.

    Returns:
        Created connector descriptor.
    """
    result = await service.create_connector(
        tenant_id=tenant_id,
        name=body.name,
        config=body.config,
    )
    return ConnectorResponse(name=result.get("name", body.name), config=result.get("config", body.config))


@router.get("/connectors", response_model=ConnectorListResponse)
async def list_connectors(
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[ConnectorService, Depends(get_connector_service)],
) -> ConnectorListResponse:
    """List all Kafka Connect connectors owned by the current tenant.

    Args:
        tenant_id: Tenant extracted from auth context.
        service: Injected connector service.

    Returns:
        Paginated list of connectors.
    """
    items = await service.list_connectors(tenant_id=tenant_id)
    return ConnectorListResponse(items=items, total=len(items))


@router.get("/connectors/{name}/status", response_model=ConnectorResponse)
async def get_connector_status(
    name: str,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[ConnectorService, Depends(get_connector_service)],
) -> ConnectorResponse:
    """Get runtime status for a tenant-owned connector.

    Args:
        name: Base connector name.
        tenant_id: Tenant extracted from auth context.
        service: Injected connector service.

    Returns:
        Connector status.
    """
    status = await service.get_connector_status(tenant_id=tenant_id, name=name)
    return ConnectorResponse(name=name, status=status)


@router.delete("/connectors/{name}", status_code=204)
async def delete_connector(
    name: str,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[ConnectorService, Depends(get_connector_service)],
) -> None:
    """Delete a tenant-owned Kafka Connect connector.

    Args:
        name: Base connector name.
        tenant_id: Tenant extracted from auth context.
        service: Injected connector service.
    """
    await service.delete_connector(tenant_id=tenant_id, name=name)


@router.post("/connectors/{name}/restart")
async def restart_connector(
    name: str,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[ConnectorService, Depends(get_connector_service)],
) -> dict:
    """Restart a tenant-owned Kafka Connect connector.

    Args:
        name: Base connector name.
        tenant_id: Tenant extracted from auth context.
        service: Injected connector service.

    Returns:
        Restart result.
    """
    result = await service.restart_connector(tenant_id=tenant_id, name=name)
    return result


# ---------------------------------------------------------------------------
# Consumer group monitoring endpoints (Gap #25)
# ---------------------------------------------------------------------------


def get_consumer_monitoring_service() -> ConsumerGroupMonitoringService:
    """Construct ConsumerGroupMonitoringService.

    Returns:
        Configured ConsumerGroupMonitoringService instance.
    """
    kafka_admin = KafkaAdminAdapter(settings=settings)
    return ConsumerGroupMonitoringService(kafka_admin=kafka_admin)


@router.get("/consumer-groups", response_model=ConsumerGroupListResponse)
async def list_consumer_groups(
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[ConsumerGroupMonitoringService, Depends(get_consumer_monitoring_service)],
) -> ConsumerGroupListResponse:
    """List consumer groups filtered to the current tenant.

    Args:
        tenant_id: Tenant extracted from auth context.
        service: Injected consumer group monitoring service.

    Returns:
        List of consumer group summaries.
    """
    from aumos_event_bus.api.schemas import ConsumerGroupSummary
    groups = await service.list_consumer_groups(tenant_id=tenant_id)
    summaries = [ConsumerGroupSummary(**g) for g in groups]
    return ConsumerGroupListResponse(groups=summaries, total=len(summaries))


@router.get("/consumer-groups/{group_id}/lag", response_model=ConsumerGroupLagResponse)
async def get_consumer_group_lag(
    group_id: str,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[ConsumerGroupMonitoringService, Depends(get_consumer_monitoring_service)],
) -> ConsumerGroupLagResponse:
    """Get per-partition lag for a consumer group.

    Args:
        group_id: Consumer group ID.
        tenant_id: Tenant extracted from auth context.
        service: Injected consumer group monitoring service.

    Returns:
        Consumer group lag information.
    """
    result = await service.get_consumer_group_lag(group_id=group_id, tenant_id=tenant_id)
    return ConsumerGroupLagResponse(**result)


# ---------------------------------------------------------------------------
# Event replay endpoints (Gap #26)
# ---------------------------------------------------------------------------


def get_replay_service() -> EventReplayService:
    """Construct EventReplayService.

    Returns:
        Configured EventReplayService instance.
    """
    kafka_admin = KafkaAdminAdapter(settings=settings)
    return EventReplayService(kafka_admin=kafka_admin)


@router.post("/consumer-groups/{group_id}/replay", response_model=ReplayJobResponse, status_code=201)
async def create_replay_job(
    group_id: str,
    body: ReplayJobCreateRequest,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[EventReplayService, Depends(get_replay_service)],
) -> ReplayJobResponse:
    """Create an event replay job for a consumer group.

    Args:
        group_id: Consumer group ID to replay.
        body: Replay configuration.
        tenant_id: Tenant extracted from auth context.
        service: Injected event replay service.

    Returns:
        Replay job descriptor.
    """
    result = await service.create_replay_job(
        tenant_id=tenant_id,
        group_id=group_id,
        topic=body.topic,
        from_offset_type=body.from_offset_type,
        from_timestamp=body.from_timestamp,
    )
    return ReplayJobResponse(**result)


# ---------------------------------------------------------------------------
# Geo-replication endpoints (Gap #27)
# ---------------------------------------------------------------------------


def get_geo_replication_service() -> GeoReplicationService:
    """Construct GeoReplicationService with Strimzi client.

    Returns:
        Configured GeoReplicationService instance.
    """
    strimzi_client = StrimziMirrorMaker2Client(namespace=settings.strimzi_namespace)
    return GeoReplicationService(strimzi_client=strimzi_client)


@router.post("/geo-replication", response_model=GeoReplicationFlowResponse, status_code=201)
async def create_geo_replication_flow(
    body: GeoReplicationFlowCreateRequest,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[GeoReplicationService, Depends(get_geo_replication_service)],
) -> GeoReplicationFlowResponse:
    """Create a MirrorMaker2 geo-replication flow.

    Args:
        body: Replication flow configuration.
        tenant_id: Tenant extracted from auth context.
        service: Injected geo-replication service.

    Returns:
        Created replication flow descriptor.
    """
    result = await service.create_flow(
        tenant_id=tenant_id,
        name=body.name,
        source_bootstrap=body.source_bootstrap,
        target_bootstrap=body.target_bootstrap,
        topics_pattern=body.topics_pattern,
    )
    return GeoReplicationFlowResponse(**result)


@router.get("/geo-replication", response_model=list[GeoReplicationFlowResponse])
async def list_geo_replication_flows(
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[GeoReplicationService, Depends(get_geo_replication_service)],
) -> list[GeoReplicationFlowResponse]:
    """List geo-replication flows for the current tenant.

    Args:
        tenant_id: Tenant extracted from auth context.
        service: Injected geo-replication service.

    Returns:
        List of replication flow descriptors.
    """
    flows = await service.list_flows(tenant_id=tenant_id)
    return [
        GeoReplicationFlowResponse(
            name=f.get("metadata", {}).get("name", ""),
            tenant_id=tenant_id,
            source_bootstrap="",
            target_bootstrap="",
            topics_pattern=".*",
            status=f.get("status", {}).get("conditions", [{}])[-1].get("type", "unknown"),
        )
        for f in flows
    ]


@router.delete("/geo-replication/{name}", status_code=204)
async def delete_geo_replication_flow(
    name: str,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[GeoReplicationService, Depends(get_geo_replication_service)],
) -> None:
    """Delete a geo-replication flow.

    Args:
        name: Replication flow name.
        tenant_id: Tenant extracted from auth context.
        service: Injected geo-replication service.
    """
    await service.delete_flow(tenant_id=tenant_id, name=name)


# ---------------------------------------------------------------------------
# Tiered storage endpoints (Gap #28)
# ---------------------------------------------------------------------------


def get_tiered_storage_service() -> TieredStorageService:
    """Construct TieredStorageService.

    Returns:
        Configured TieredStorageService instance.
    """
    kafka_admin = KafkaAdminAdapter(settings=settings)
    return TieredStorageService(kafka_admin=kafka_admin)


@router.put("/topics/{topic_name}/tiered-storage", response_model=TieredStorageConfigResponse)
async def configure_tiered_storage(
    topic_name: str,
    body: TieredStorageConfigRequest,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[TieredStorageService, Depends(get_tiered_storage_service)],
) -> TieredStorageConfigResponse:
    """Configure tiered (S3) storage for a Kafka topic.

    Args:
        topic_name: Kafka topic name.
        body: Tiered storage configuration.
        tenant_id: Tenant extracted from auth context.
        service: Injected tiered storage service.

    Returns:
        Updated tiered storage configuration.
    """
    s3_bucket = body.s3_bucket or settings.tiered_storage_s3_bucket
    result = await service.configure_tiered_storage(
        topic_name=topic_name,
        tenant_id=tenant_id,
        tier_local_hot_ms=body.tier_local_hot_ms,
        s3_bucket=s3_bucket,
        enabled=body.enabled,
    )
    return TieredStorageConfigResponse(**result)


# ---------------------------------------------------------------------------
# Schema evolution endpoints (Gap #30)
# ---------------------------------------------------------------------------


def get_schema_evolution_service() -> SchemaEvolutionService:
    """Construct SchemaEvolutionService.

    Returns:
        Configured SchemaEvolutionService instance.
    """
    from aumos_event_bus.adapters.schema_registry import SchemaRegistryAdapter
    registry_client = SchemaRegistryAdapter(settings=settings)
    return SchemaEvolutionService(registry_client=registry_client)


@router.post("/schemas/preview-evolution", response_model=SchemaEvolutionPreviewResponse)
async def preview_schema_evolution(
    body: SchemaEvolutionPreviewRequest,
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    service: Annotated[SchemaEvolutionService, Depends(get_schema_evolution_service)],
) -> SchemaEvolutionPreviewResponse:
    """Preview schema evolution compatibility and field diff.

    Args:
        body: Schema evolution preview request.
        tenant_id: Tenant extracted from auth context.
        service: Injected schema evolution service.

    Returns:
        Compatibility result, field diff, and recommendation.
    """
    result = await service.preview_evolution(
        subject=body.subject,
        new_schema_definition=body.new_schema_definition,
        tenant_id=tenant_id,
    )
    return SchemaEvolutionPreviewResponse(**result)


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
