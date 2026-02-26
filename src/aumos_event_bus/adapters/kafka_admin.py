"""Kafka admin adapter implementing IKafkaAdmin.

Wraps the confluent-kafka AdminClient to provide topic lifecycle management,
consumer group inspection, and topic configuration operations. All I/O is
run in a thread pool to satisfy the async contract.
"""

from __future__ import annotations

import asyncio
from typing import Any

from aumos_common.observability import get_logger

from aumos_event_bus.core.interfaces import IKafkaAdmin
from aumos_event_bus.settings import Settings

logger = get_logger(__name__)


class KafkaAdminAdapter:
    """Kafka administrative operations via confluent-kafka AdminClient.

    Implements IKafkaAdmin using the synchronous confluent-kafka AdminClient,
    offloading blocking calls to a thread pool executor to maintain async
    compatibility with the rest of the service.

    Usage::

        adapter = KafkaAdminAdapter(settings=settings)
        await adapter.create_topic("my-topic", partitions=6, replication_factor=3, config={})
    """

    def __init__(self, settings: Settings) -> None:
        """Initialise the admin client from service settings.

        Args:
            settings: Event Bus settings containing Kafka broker configuration.
        """
        self._settings = settings
        self._client: Any = None

    def _get_client(self) -> Any:
        """Lazily initialise the confluent-kafka AdminClient.

        Returns:
            Configured AdminClient instance.
        """
        if self._client is None:
            from confluent_kafka.admin import AdminClient  # type: ignore[import]

            broker_list = (
                self._settings.kafka.brokers
                if hasattr(self._settings, "kafka")
                else "localhost:9092"
            )
            if isinstance(broker_list, list):
                broker_list = ",".join(broker_list)

            self._client = AdminClient({"bootstrap.servers": broker_list})
        return self._client

    async def create_topic(
        self,
        topic_name: str,
        partitions: int,
        replication_factor: int,
        config: dict[str, str],
    ) -> None:
        """Create a Kafka topic on the broker.

        Args:
            topic_name: Name of the topic to create.
            partitions: Number of partitions.
            replication_factor: Replication factor (should be <= broker count).
            config: Additional topic-level configuration overrides.

        Raises:
            RuntimeError: If topic creation fails on the broker.
        """
        from confluent_kafka.admin import NewTopic  # type: ignore[import]

        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=config,
        )
        client = self._get_client()

        loop = asyncio.get_event_loop()
        futures = await loop.run_in_executor(None, lambda: client.create_topics([new_topic]))

        for name, future in futures.items():
            try:
                await loop.run_in_executor(None, future.result)
                logger.info("Kafka topic created", topic_name=name)
            except Exception as exc:
                # KafkaException subclass for TOPIC_ALREADY_EXISTS is acceptable
                if "TOPIC_ALREADY_EXISTS" in str(exc):
                    logger.warning("Topic already exists on broker", topic_name=name)
                else:
                    logger.error("Failed to create Kafka topic", topic_name=name, error=str(exc))
                    raise RuntimeError(f"Failed to create topic '{name}': {exc}") from exc

    async def delete_topic(self, topic_name: str) -> None:
        """Delete a Kafka topic from the broker.

        Args:
            topic_name: Name of the topic to delete.

        Raises:
            RuntimeError: If topic deletion fails.
        """
        client = self._get_client()
        loop = asyncio.get_event_loop()
        futures = await loop.run_in_executor(None, lambda: client.delete_topics([topic_name]))

        for name, future in futures.items():
            try:
                await loop.run_in_executor(None, future.result)
                logger.info("Kafka topic deleted", topic_name=name)
            except Exception as exc:
                logger.error("Failed to delete Kafka topic", topic_name=name, error=str(exc))
                raise RuntimeError(f"Failed to delete topic '{name}': {exc}") from exc

    async def describe_topic(self, topic_name: str) -> dict[str, Any]:
        """Return metadata about an existing Kafka topic.

        Args:
            topic_name: Name of the topic to describe.

        Returns:
            Dict with partition info and topic configuration.
        """
        client = self._get_client()
        loop = asyncio.get_event_loop()

        def _describe() -> dict[str, Any]:
            metadata = client.list_topics(topic=topic_name, timeout=10)
            topic_meta = metadata.topics.get(topic_name)
            if topic_meta is None:
                return {}
            partitions_info = [
                {
                    "partition": p.id,
                    "leader": p.leader,
                    "replicas": list(p.replicas),
                    "isrs": list(p.isrs),
                }
                for p in topic_meta.partitions.values()
            ]
            return {
                "topic_name": topic_name,
                "partition_count": len(partitions_info),
                "partitions": partitions_info,
                "config": {},
                "consumer_groups": [],
            }

        return await loop.run_in_executor(None, _describe)

    async def list_topics(self) -> list[str]:
        """Return all topic names in the Kafka cluster.

        Returns:
            Sorted list of topic name strings.
        """
        client = self._get_client()
        loop = asyncio.get_event_loop()

        def _list() -> list[str]:
            metadata = client.list_topics(timeout=10)
            return sorted(metadata.topics.keys())

        return await loop.run_in_executor(None, _list)

    async def alter_topic_config(self, topic_name: str, config: dict[str, str]) -> None:
        """Update configuration parameters of an existing Kafka topic.

        Args:
            topic_name: Target topic name.
            config: Config key-value pairs to update.
        """
        from confluent_kafka.admin import ConfigResource, ConfigSource  # type: ignore[import]
        from confluent_kafka import ConfigEntry  # type: ignore[import]

        resource = ConfigResource("topic", topic_name)
        for key, value in config.items():
            resource.set_config(key, value)

        client = self._get_client()
        loop = asyncio.get_event_loop()
        futures = await loop.run_in_executor(None, lambda: client.alter_configs([resource]))

        for res, future in futures.items():
            try:
                await loop.run_in_executor(None, future.result)
                logger.info("Topic config updated", topic_name=topic_name)
            except Exception as exc:
                logger.error("Failed to update topic config", topic_name=topic_name, error=str(exc))
                raise RuntimeError(f"Failed to alter config for '{topic_name}': {exc}") from exc

    async def get_consumer_group_offsets(self, group_id: str) -> dict[str, Any]:
        """Return consumer group lag information.

        Args:
            group_id: Consumer group ID string.

        Returns:
            Dict mapping topic-partition to offset/lag data.
        """
        client = self._get_client()
        loop = asyncio.get_event_loop()

        def _get_offsets() -> dict[str, Any]:
            futures = client.list_consumer_group_offsets([group_id])
            result: dict[str, Any] = {}
            for group, future in futures.items():
                try:
                    offsets = future.result()
                    result[group] = {
                        str(tp): {"offset": offset.offset}
                        for tp, offset in offsets.topic_partitions.items()
                    }
                except Exception as exc:
                    logger.warning(
                        "Could not fetch offsets for group",
                        group_id=group,
                        error=str(exc),
                    )
                    result[group] = {}
            return result

        return await loop.run_in_executor(None, _get_offsets)


# Satisfy IKafkaAdmin structural typing
_: IKafkaAdmin = KafkaAdminAdapter.__new__(KafkaAdminAdapter)  # type: ignore[assignment]
