"""Dead Letter Queue (DLQ) handler adapter for the AumOS Event Bus.

Manages the full DLQ lifecycle: capturing failed messages with error context,
per-source-topic DLQ topic provisioning, exponential-backoff retry scheduling,
manual replay, DLQ depth monitoring, growth alerting, and message inspection
with filtering capabilities.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from typing import Any

from aumos_common.errors import NotFoundError
from aumos_common.observability import get_logger

from aumos_event_bus.core.interfaces import IDLQRepository, IKafkaAdmin
from aumos_event_bus.core.models import DLQStatus

logger = get_logger(__name__)

# DLQ topic suffix convention
_DLQ_TOPIC_SUFFIX = ".dlq"

# Alert threshold: notify when DLQ depth crosses this count
_DLQ_DEPTH_ALERT_THRESHOLD = 100

# Exponential backoff defaults
_INITIAL_BACKOFF_MS = 1_000
_MAX_BACKOFF_MS = 300_000  # 5 minutes
_BACKOFF_MULTIPLIER = 2.0


class DLQHandler:
    """Dead Letter Queue lifecycle manager for the AumOS Event Bus.

    Handles failed message capture, DLQ topic provisioning per source topic,
    exponential-backoff retry scheduling, manual replay operations, and
    monitoring of DLQ depth with configurable growth alerts.

    Usage::

        handler = DLQHandler(dlq_repo=dlq_repo, kafka_admin=kafka_admin)
        entry = await handler.capture_failure(
            tenant_id="t-001",
            source_topic="aumos.payments",
            message_key="order-123",
            message_value='{"amount": 99.99}',
            failure_reason="Deserialization error: unexpected field",
        )
    """

    def __init__(
        self,
        dlq_repo: IDLQRepository,
        kafka_admin: IKafkaAdmin,
        max_retries: int = 5,
        initial_backoff_ms: int = _INITIAL_BACKOFF_MS,
        max_backoff_ms: int = _MAX_BACKOFF_MS,
        backoff_multiplier: float = _BACKOFF_MULTIPLIER,
        depth_alert_threshold: int = _DLQ_DEPTH_ALERT_THRESHOLD,
        dlq_partitions: int = 3,
        dlq_replication_factor: int = 3,
        dlq_retention_ms: int = 7 * 24 * 60 * 60 * 1000,  # 7 days
    ) -> None:
        """Initialise the DLQ handler.

        Args:
            dlq_repo: Persistence layer for DLQ entry records.
            kafka_admin: Kafka admin client for DLQ topic provisioning.
            max_retries: Maximum retry attempts before abandoning a message.
            initial_backoff_ms: Initial retry delay in milliseconds.
            max_backoff_ms: Maximum backoff cap in milliseconds.
            backoff_multiplier: Exponential backoff growth factor.
            depth_alert_threshold: DLQ depth count that triggers a warning log.
            dlq_partitions: Partition count for auto-provisioned DLQ topics.
            dlq_replication_factor: Replication factor for DLQ topics.
            dlq_retention_ms: Retention for DLQ topic messages in ms.
        """
        self._dlq_repo = dlq_repo
        self._kafka_admin = kafka_admin
        self._max_retries = max_retries
        self._initial_backoff_ms = initial_backoff_ms
        self._max_backoff_ms = max_backoff_ms
        self._backoff_multiplier = backoff_multiplier
        self._depth_alert_threshold = depth_alert_threshold
        self._dlq_partitions = dlq_partitions
        self._dlq_replication_factor = dlq_replication_factor
        self._dlq_retention_ms = dlq_retention_ms

        # Track which DLQ topics have already been provisioned in this session
        self._provisioned_dlq_topics: set[str] = set()

    async def capture_failure(
        self,
        tenant_id: str,
        source_topic: str,
        message_key: str | None,
        message_value: str,
        failure_reason: str,
        message_headers: dict[str, str] | None = None,
        original_offset: int | None = None,
        original_partition: int | None = None,
        consumer_group: str | None = None,
        correlation_id: str | None = None,
        failure_details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Capture a failed message into the DLQ.

        Automatically provisions the DLQ topic for the source topic if it does
        not yet exist. Records full error context for operator inspection.

        Args:
            tenant_id: Owning tenant identifier.
            source_topic: Original Kafka topic the message came from.
            message_key: Original message key (may be None).
            message_value: Serialised message payload string.
            failure_reason: Human-readable failure description.
            message_headers: Original message headers dict.
            original_offset: Kafka partition offset of the failed message.
            original_partition: Kafka partition of the failed message.
            consumer_group: Consumer group that failed to process the message.
            correlation_id: Distributed tracing correlation ID.
            failure_details: Structured failure context (stack trace, error type, etc.).

        Returns:
            Persisted DLQ entry as a dict.
        """
        dlq_topic = self._get_dlq_topic_name(source_topic)

        # Ensure DLQ topic exists on the broker
        await self._ensure_dlq_topic_exists(dlq_topic)

        next_retry_at = self._calculate_next_retry_ms(retry_count=0)

        entry = await self._dlq_repo.create(
            {
                "tenant_id": tenant_id,
                "source_topic": source_topic,
                "dlq_topic": dlq_topic,
                "message_key": message_key,
                "message_value": message_value,
                "message_headers": message_headers or {},
                "original_offset": original_offset,
                "original_partition": original_partition,
                "failure_reason": failure_reason,
                "failure_details": failure_details or {},
                "status": DLQStatus.PENDING.value,
                "retry_count": 0,
                "max_retries": self._max_retries,
                "next_retry_at": next_retry_at,
                "consumer_group": consumer_group,
                "correlation_id": correlation_id,
            }
        )

        logger.warning(
            "Message captured in DLQ",
            source_topic=source_topic,
            dlq_topic=dlq_topic,
            tenant_id=tenant_id,
            failure_reason=failure_reason[:200],
            correlation_id=correlation_id,
        )

        # Check depth and emit alert if needed
        await self._check_and_alert_depth(tenant_id=tenant_id, source_topic=source_topic)

        return entry

    async def schedule_retry(
        self,
        entry_id: uuid.UUID,
        tenant_id: str,
    ) -> dict[str, Any]:
        """Schedule the next retry for a DLQ entry with exponential backoff.

        Increments the retry count and sets the next_retry_at timestamp.
        Marks the entry as ABANDONED if max_retries is exceeded.

        Args:
            entry_id: UUID of the DLQ entry to schedule retry for.
            tenant_id: Tenant context for repository access.

        Returns:
            Updated DLQ entry dict.

        Raises:
            NotFoundError: If the entry does not exist.
        """
        entry = await self._dlq_repo.get_by_id(entry_id, tenant_id)
        if not entry:
            raise NotFoundError(resource="dlq_entry", resource_id=str(entry_id))

        retry_count = (entry.get("retry_count") or 0) + 1
        max_retries = entry.get("max_retries") or self._max_retries

        if retry_count > max_retries:
            logger.warning(
                "DLQ entry exceeded max retries — marking abandoned",
                entry_id=str(entry_id),
                retry_count=retry_count,
                max_retries=max_retries,
            )
            await self._dlq_repo.update_status(
                entry_id, tenant_id, status=DLQStatus.ABANDONED.value, retry_count=retry_count
            )
            return {**entry, "status": DLQStatus.ABANDONED.value, "retry_count": retry_count}

        next_retry_at = self._calculate_next_retry_ms(retry_count=retry_count)

        await self._dlq_repo.update_status(
            entry_id,
            tenant_id,
            status=DLQStatus.RETRYING.value,
            retry_count=retry_count,
        )

        logger.info(
            "DLQ retry scheduled",
            entry_id=str(entry_id),
            retry_count=retry_count,
            next_retry_at=next_retry_at,
        )
        return {
            **entry,
            "status": DLQStatus.RETRYING.value,
            "retry_count": retry_count,
            "next_retry_at": next_retry_at,
        }

    async def get_entries_due_for_retry(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return DLQ entries whose retry window has elapsed.

        Args:
            limit: Maximum entries to return per poll cycle.

        Returns:
            List of DLQ entry dicts ready for reprocessing.
        """
        current_epoch_ms = int(time.time() * 1000)
        entries = await self._dlq_repo.get_entries_due_for_retry(
            current_epoch_ms=current_epoch_ms, limit=limit
        )
        if entries:
            logger.info("DLQ entries due for retry", count=len(entries))
        return entries

    async def replay_entry(
        self,
        entry_id: uuid.UUID,
        tenant_id: str,
        target_topic: str | None = None,
    ) -> dict[str, Any]:
        """Manually replay a DLQ entry to its source or a target topic.

        Marks the entry as RESOLVED after scheduling the replay. The actual
        Kafka produce call must be performed by the caller using the returned
        message_value and message_headers.

        Args:
            entry_id: UUID of the DLQ entry to replay.
            tenant_id: Tenant context.
            target_topic: Topic to replay to. Defaults to the entry's source_topic.

        Returns:
            Dict with replay_topic, message_key, message_value, headers.

        Raises:
            NotFoundError: If the entry does not exist.
        """
        entry = await self._dlq_repo.get_by_id(entry_id, tenant_id)
        if not entry:
            raise NotFoundError(resource="dlq_entry", resource_id=str(entry_id))

        replay_topic = target_topic or entry.get("source_topic", "")

        await self._dlq_repo.update_status(
            entry_id, tenant_id, status=DLQStatus.RESOLVED.value
        )

        logger.info(
            "DLQ entry scheduled for manual replay",
            entry_id=str(entry_id),
            replay_topic=replay_topic,
            tenant_id=tenant_id,
        )

        return {
            "entry_id": str(entry_id),
            "replay_topic": replay_topic,
            "message_key": entry.get("message_key"),
            "message_value": entry.get("message_value"),
            "message_headers": entry.get("message_headers", {}),
            "original_failure_reason": entry.get("failure_reason"),
        }

    async def get_dlq_depth(
        self,
        tenant_id: str,
        source_topic: str | None = None,
    ) -> dict[str, Any]:
        """Return the current depth of the DLQ for monitoring.

        Args:
            tenant_id: Tenant context.
            source_topic: Optional filter by source topic.

        Returns:
            Dict with total depth, breakdown by status, and alert flag.
        """
        pending_entries = await self._dlq_repo.list_pending(
            tenant_id=tenant_id,
            source_topic=source_topic,
            skip=0,
            limit=10_000,
        )

        status_breakdown: dict[str, int] = {}
        for entry in pending_entries:
            status = entry.get("status", "unknown")
            status_breakdown[status] = status_breakdown.get(status, 0) + 1

        total_depth = len(pending_entries)
        alert_triggered = total_depth >= self._depth_alert_threshold

        if alert_triggered:
            logger.warning(
                "DLQ depth alert threshold reached",
                tenant_id=tenant_id,
                source_topic=source_topic or "all",
                depth=total_depth,
                threshold=self._depth_alert_threshold,
            )

        return {
            "tenant_id": tenant_id,
            "source_topic": source_topic or "all",
            "total_depth": total_depth,
            "status_breakdown": status_breakdown,
            "alert_triggered": alert_triggered,
            "alert_threshold": self._depth_alert_threshold,
            "measured_at": datetime.now(UTC).isoformat(),
        }

    async def filter_entries(
        self,
        tenant_id: str,
        source_topic: str | None = None,
        status: str | None = None,
        consumer_group: str | None = None,
        skip: int = 0,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Inspect and filter DLQ entries for operator tooling.

        Args:
            tenant_id: Tenant context.
            source_topic: Optional filter by source topic.
            status: Optional filter by DLQ status.
            consumer_group: Optional filter by consumer group.
            skip: Pagination offset.
            limit: Maximum entries to return.

        Returns:
            Filtered list of DLQ entry dicts.
        """
        entries = await self._dlq_repo.list_pending(
            tenant_id=tenant_id,
            source_topic=source_topic,
            skip=skip,
            limit=limit,
        )

        filtered: list[dict[str, Any]] = []
        for entry in entries:
            if status and entry.get("status") != status:
                continue
            if consumer_group and entry.get("consumer_group") != consumer_group:
                continue
            filtered.append(entry)

        return filtered

    async def get_dlq_topic_name(self, source_topic: str) -> str:
        """Return the DLQ topic name for a given source topic.

        Args:
            source_topic: The source Kafka topic name.

        Returns:
            Corresponding DLQ topic name.
        """
        return self._get_dlq_topic_name(source_topic)

    def _get_dlq_topic_name(self, source_topic: str) -> str:
        """Compute the DLQ topic name for a source topic.

        Args:
            source_topic: Original topic name.

        Returns:
            DLQ topic name with suffix appended.
        """
        return f"{source_topic}{_DLQ_TOPIC_SUFFIX}"

    async def _ensure_dlq_topic_exists(self, dlq_topic: str) -> None:
        """Provision a DLQ topic on Kafka if it does not already exist.

        Args:
            dlq_topic: DLQ topic name to create if missing.
        """
        if dlq_topic in self._provisioned_dlq_topics:
            return

        try:
            await self._kafka_admin.create_topic(
                topic_name=dlq_topic,
                partitions=self._dlq_partitions,
                replication_factor=self._dlq_replication_factor,
                config={
                    "retention.ms": str(self._dlq_retention_ms),
                    "cleanup.policy": "delete",
                    "compression.type": "lz4",
                    "min.insync.replicas": "2",
                },
            )
            logger.info("DLQ topic provisioned", dlq_topic=dlq_topic)
        except RuntimeError as exc:
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                logger.debug("DLQ topic already exists", dlq_topic=dlq_topic)
            else:
                logger.warning(
                    "Failed to provision DLQ topic — messages will still be recorded in DB",
                    dlq_topic=dlq_topic,
                    error=str(exc),
                )

        self._provisioned_dlq_topics.add(dlq_topic)

    def _calculate_next_retry_ms(self, retry_count: int) -> int:
        """Calculate the epoch millisecond timestamp for the next retry.

        Applies exponential backoff with configurable cap:
        delay = min(initial_backoff_ms * multiplier^retry_count, max_backoff_ms)

        Args:
            retry_count: Number of retries already attempted.

        Returns:
            Unix epoch milliseconds for the next retry window.
        """
        delay_ms = min(
            int(self._initial_backoff_ms * (self._backoff_multiplier ** retry_count)),
            self._max_backoff_ms,
        )
        return int(time.time() * 1000) + delay_ms

    async def _check_and_alert_depth(self, tenant_id: str, source_topic: str) -> None:
        """Check DLQ depth and emit a structured warning if threshold is crossed.

        Args:
            tenant_id: Tenant context.
            source_topic: Source topic to check depth for.
        """
        try:
            entries = await self._dlq_repo.list_pending(
                tenant_id=tenant_id,
                source_topic=source_topic,
                skip=0,
                limit=self._depth_alert_threshold + 1,
            )
            depth = len(entries)
            if depth >= self._depth_alert_threshold:
                logger.warning(
                    "DLQ depth alert",
                    tenant_id=tenant_id,
                    source_topic=source_topic,
                    depth=depth,
                    threshold=self._depth_alert_threshold,
                )
        except Exception as exc:
            logger.debug("DLQ depth check failed", error=str(exc))
