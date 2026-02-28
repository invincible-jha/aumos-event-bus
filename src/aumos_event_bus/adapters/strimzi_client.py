"""Strimzi Kubernetes operator client for MirrorMaker2 geo-replication management.

Wraps the Kubernetes API to create, update, and delete Strimzi
KafkaMirrorMaker2 custom resources for geo-replication between clusters.
"""

from __future__ import annotations

from typing import Any

from aumos_common.observability import get_logger

logger = get_logger(__name__)

_MIRROR_MAKER2_GROUP = "kafka.strimzi.io"
_MIRROR_MAKER2_VERSION = "v1beta2"
_MIRROR_MAKER2_PLURAL = "kafkamirrormaker2s"


class StrimziMirrorMaker2Client:
    """Kubernetes CRD client for Strimzi KafkaMirrorMaker2 resources.

    Manages cross-cluster Kafka topic replication via MirrorMaker 2 by
    creating/updating/deleting KafkaMirrorMaker2 CRD objects through the
    Kubernetes custom resources API.

    Usage::

        client = StrimziMirrorMaker2Client(namespace="kafka")
        await client.create_replication_flow("flow-1", source_cluster_config, target_cluster_config)
    """

    def __init__(self, namespace: str = "kafka") -> None:
        """Initialise with the Kubernetes namespace where Strimzi is deployed.

        Args:
            namespace: Kubernetes namespace containing Strimzi Kafka resources.
        """
        self._namespace = namespace
        self._api: Any = None

    def _get_api(self) -> Any:
        """Lazily initialise the Kubernetes custom objects API client.

        Returns:
            CustomObjectsApi instance.
        """
        if self._api is None:
            from kubernetes import client as k8s_client, config as k8s_config  # type: ignore[import]

            try:
                k8s_config.load_incluster_config()
            except Exception:
                k8s_config.load_kube_config()
            self._api = k8s_client.CustomObjectsApi()
        return self._api

    def _build_mirror_maker2_manifest(
        self,
        name: str,
        source_bootstrap: str,
        target_bootstrap: str,
        topics_pattern: str,
        replicas: int,
    ) -> dict[str, Any]:
        """Build a KafkaMirrorMaker2 custom resource manifest.

        Args:
            name: Resource name.
            source_bootstrap: Source Kafka bootstrap servers.
            target_bootstrap: Target Kafka bootstrap servers.
            topics_pattern: Regex pattern for topics to replicate.
            replicas: Number of MirrorMaker2 replicas.

        Returns:
            KafkaMirrorMaker2 manifest dict.
        """
        return {
            "apiVersion": f"{_MIRROR_MAKER2_GROUP}/{_MIRROR_MAKER2_VERSION}",
            "kind": "KafkaMirrorMaker2",
            "metadata": {
                "name": name,
                "namespace": self._namespace,
            },
            "spec": {
                "version": "3.7.0",
                "replicas": replicas,
                "connectCluster": "target",
                "clusters": [
                    {
                        "alias": "source",
                        "bootstrapServers": source_bootstrap,
                    },
                    {
                        "alias": "target",
                        "bootstrapServers": target_bootstrap,
                    },
                ],
                "mirrors": [
                    {
                        "sourceCluster": "source",
                        "targetCluster": "target",
                        "sourceConnector": {
                            "config": {
                                "replication.factor": "3",
                                "offset-syncs.topic.replication.factor": "3",
                                "sync.topic.acls.enabled": "false",
                                "topics": topics_pattern,
                            }
                        },
                        "heartbeatConnector": {
                            "config": {
                                "heartbeats.topic.replication.factor": "3",
                            }
                        },
                        "checkpointConnector": {
                            "config": {
                                "checkpoints.topic.replication.factor": "3",
                            }
                        },
                        "topicsPattern": topics_pattern,
                    }
                ],
            },
        }

    async def create_replication_flow(
        self,
        name: str,
        source_bootstrap: str,
        target_bootstrap: str,
        topics_pattern: str = ".*",
        replicas: int = 1,
    ) -> dict[str, Any]:
        """Create a KafkaMirrorMaker2 replication flow.

        Args:
            name: Unique name for the replication flow resource.
            source_bootstrap: Source Kafka cluster bootstrap servers.
            target_bootstrap: Target Kafka cluster bootstrap servers.
            topics_pattern: Regex pattern for topics to replicate.
            replicas: Number of MirrorMaker2 connector replicas.

        Returns:
            Created KafkaMirrorMaker2 resource dict.
        """
        import asyncio

        api = self._get_api()
        manifest = self._build_mirror_maker2_manifest(
            name=name,
            source_bootstrap=source_bootstrap,
            target_bootstrap=target_bootstrap,
            topics_pattern=topics_pattern,
            replicas=replicas,
        )

        loop = asyncio.get_event_loop()

        def _create() -> Any:
            return api.create_namespaced_custom_object(
                group=_MIRROR_MAKER2_GROUP,
                version=_MIRROR_MAKER2_VERSION,
                namespace=self._namespace,
                plural=_MIRROR_MAKER2_PLURAL,
                body=manifest,
            )

        result: dict[str, Any] = await loop.run_in_executor(None, _create)
        logger.info("Created KafkaMirrorMaker2 resource", name=name)
        return result

    async def delete_replication_flow(self, name: str) -> None:
        """Delete a KafkaMirrorMaker2 replication flow.

        Args:
            name: Name of the KafkaMirrorMaker2 resource to delete.
        """
        import asyncio

        api = self._get_api()
        loop = asyncio.get_event_loop()

        def _delete() -> Any:
            return api.delete_namespaced_custom_object(
                group=_MIRROR_MAKER2_GROUP,
                version=_MIRROR_MAKER2_VERSION,
                namespace=self._namespace,
                plural=_MIRROR_MAKER2_PLURAL,
                name=name,
            )

        await loop.run_in_executor(None, _delete)
        logger.info("Deleted KafkaMirrorMaker2 resource", name=name)

    async def list_replication_flows(self) -> list[dict[str, Any]]:
        """List all KafkaMirrorMaker2 resources in the namespace.

        Returns:
            List of KafkaMirrorMaker2 resource dicts.
        """
        import asyncio

        api = self._get_api()
        loop = asyncio.get_event_loop()

        def _list() -> Any:
            return api.list_namespaced_custom_object(
                group=_MIRROR_MAKER2_GROUP,
                version=_MIRROR_MAKER2_VERSION,
                namespace=self._namespace,
                plural=_MIRROR_MAKER2_PLURAL,
            )

        result: dict[str, Any] = await loop.run_in_executor(None, _list)
        items: list[dict[str, Any]] = result.get("items", [])
        return items

    async def get_replication_flow(self, name: str) -> dict[str, Any]:
        """Get a specific KafkaMirrorMaker2 resource.

        Args:
            name: Resource name.

        Returns:
            KafkaMirrorMaker2 resource dict.
        """
        import asyncio

        api = self._get_api()
        loop = asyncio.get_event_loop()

        def _get() -> Any:
            return api.get_namespaced_custom_object(
                group=_MIRROR_MAKER2_GROUP,
                version=_MIRROR_MAKER2_VERSION,
                namespace=self._namespace,
                plural=_MIRROR_MAKER2_PLURAL,
                name=name,
            )

        result: dict[str, Any] = await loop.run_in_executor(None, _get)
        return result
