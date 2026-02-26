"""Tenant-aware Kafka partitioner.

Provides deterministic partition assignment so all events for a given
tenant always land on the same partition set. This guarantees ordering
within a tenant without requiring global ordering across the entire topic.

Algorithm:
1. Hash tenant_id using xxhash (fast, high-quality distribution)
2. Modulo by partition count to get the base partition
3. Maintain a stable mapping in the database for auditability

Design constraints:
- Must be deterministic: same tenant_id always maps to same partition
- Must be uniform: tenants should be evenly distributed
- Must be seedable: allows controlled resharding if partition count changes
"""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass


@dataclass(frozen=True)
class PartitionAssignment:
    """Immutable result of a partition calculation."""

    tenant_id: str
    topic_name: str
    partition: int
    total_partitions: int
    hash_value: int


class TenantPartitioner:
    """Assigns Kafka partitions to tenants deterministically.

    Uses xxhash-style bit mixing (via hashlib sha256 truncated to 64-bit)
    to achieve high-quality distribution without external dependencies.

    Usage:
        partitioner = TenantPartitioner(seed=42)
        assignment = partitioner.assign("tenant-uuid-1234", "aumos.agents", partitions=12)
        print(assignment.partition)  # e.g., 7
    """

    def __init__(self, seed: int = 42) -> None:
        """Initialise the partitioner with a stable seed.

        Args:
            seed: Seed value mixed into the hash for controlled distribution.
                  MUST NOT change after topics are provisioned.
        """
        self._seed = seed
        # Pre-compute seed bytes for mixing
        self._seed_bytes = struct.pack(">q", seed & 0xFFFFFFFFFFFFFFFF)

    def assign(self, tenant_id: str, topic_name: str, partitions: int) -> PartitionAssignment:
        """Calculate the partition for a tenant on a given topic.

        Args:
            tenant_id: Tenant UUID string.
            topic_name: Kafka topic name (e.g., "aumos.agents").
            partitions: Total number of partitions on the topic.

        Returns:
            PartitionAssignment with the calculated partition and metadata.

        Raises:
            ValueError: If partitions is less than 1.
        """
        if partitions < 1:
            raise ValueError(f"partitions must be >= 1, got {partitions}")

        hash_value = self._compute_hash(tenant_id, topic_name)
        partition = hash_value % partitions

        return PartitionAssignment(
            tenant_id=tenant_id,
            topic_name=topic_name,
            partition=partition,
            total_partitions=partitions,
            hash_value=hash_value,
        )

    def get_partition_key(self, tenant_id: str) -> bytes:
        """Generate a Kafka message key that routes to the tenant's partition.

        Used by producers as the message key so that confluent-kafka's
        default murmur2 partitioner routes to the correct partition.
        Note: for exact tenant partitioning, use the custom partitioner
        in the SDK rather than relying on this key alone.

        Args:
            tenant_id: Tenant UUID string.

        Returns:
            Bytes key that encodes the tenant_id for partitioner use.
        """
        return tenant_id.encode("utf-8")

    def compute_partition_distribution(
        self, tenant_ids: list[str], topic_name: str, partitions: int
    ) -> dict[int, list[str]]:
        """Compute how a set of tenants would be distributed across partitions.

        Useful for capacity planning — identify hot partitions before provisioning.

        Args:
            tenant_ids: List of tenant ID strings to distribute.
            topic_name: Kafka topic name.
            partitions: Number of partitions to distribute across.

        Returns:
            Mapping of partition number to list of tenant IDs assigned to it.
        """
        distribution: dict[int, list[str]] = {p: [] for p in range(partitions)}
        for tenant_id in tenant_ids:
            assignment = self.assign(tenant_id, topic_name, partitions)
            distribution[assignment.partition].append(tenant_id)
        return distribution

    def _compute_hash(self, tenant_id: str, topic_name: str) -> int:
        """Compute a stable 64-bit hash combining tenant_id, topic, and seed.

        Uses SHA-256 truncated to 8 bytes for platform-independent results.

        Args:
            tenant_id: Tenant identifier.
            topic_name: Topic name (prevents cross-topic collision).

        Returns:
            Unsigned 64-bit integer hash value.
        """
        # Combine seed + topic + tenant for a unique, stable hash
        input_bytes = self._seed_bytes + topic_name.encode("utf-8") + b"|" + tenant_id.encode("utf-8")
        digest = hashlib.sha256(input_bytes).digest()
        # Take first 8 bytes, interpret as big-endian unsigned int
        hash_value = struct.unpack(">Q", digest[:8])[0]
        return hash_value


class ConfluentKafkaTenantPartitioner:
    """Drop-in partitioner for confluent-kafka producer configuration.

    Implements the callable interface expected by confluent-kafka's
    `partitioner` config option: partitioner(key, all_partitions, available_partitions)

    Usage with confluent-kafka::

        partitioner = ConfluentKafkaTenantPartitioner(seed=42)

        producer = Producer({
            "bootstrap.servers": "localhost:9092",
            "partitioner": partitioner,
        })
    """

    def __init__(self, seed: int = 42) -> None:
        """Initialise with the same seed as TenantPartitioner.

        Args:
            seed: Must match the seed used for database partition mappings.
        """
        self._inner = TenantPartitioner(seed=seed)

    def __call__(
        self,
        key: bytes | None,
        all_partitions: list[int],
        available_partitions: list[int],
    ) -> int:
        """Confluent-kafka partitioner callable.

        Args:
            key: Message key bytes (should be tenant_id.encode()).
            all_partitions: All partition IDs for the topic.
            available_partitions: Partitions with available leaders.

        Returns:
            Partition ID to route this message to.
        """
        if not key or not all_partitions:
            # Fall back to round-robin when no key provided
            return available_partitions[0] if available_partitions else all_partitions[0]

        tenant_id = key.decode("utf-8", errors="replace")
        # Use all_partitions for stable assignment regardless of availability
        partitions = len(all_partitions)
        hash_value = self._inner._compute_hash(tenant_id, "")
        return hash_value % partitions
