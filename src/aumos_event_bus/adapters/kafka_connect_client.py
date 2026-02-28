"""Kafka Connect REST API client for connector lifecycle management.

Wraps the Kafka Connect REST API to create, list, update, delete, and
restart connectors. Used by ConnectorService for tenant-scoped connector management.
"""

from __future__ import annotations

from typing import Any

import httpx

from aumos_common.observability import get_logger

logger = get_logger(__name__)


class KafkaConnectClient:
    """HTTP client for the Kafka Connect REST API.

    Provides async wrappers around the Kafka Connect distributed REST API
    for full connector lifecycle management.

    Usage::

        client = KafkaConnectClient(base_url="http://kafka-connect:8083")
        connectors = await client.list_connectors()
    """

    def __init__(self, base_url: str) -> None:
        """Initialise with Kafka Connect REST API base URL.

        Args:
            base_url: Kafka Connect REST API base URL (e.g. http://kafka-connect:8083).
        """
        self._base_url = base_url.rstrip("/")

    def _make_client(self) -> httpx.AsyncClient:
        """Build an httpx AsyncClient pointed at Kafka Connect.

        Returns:
            Configured httpx.AsyncClient.
        """
        return httpx.AsyncClient(
            base_url=self._base_url,
            timeout=30.0,
            headers={"Content-Type": "application/json"},
        )

    async def list_connectors(self, expand_status: bool = False) -> list[dict[str, Any]]:
        """List all connectors registered with Kafka Connect.

        Args:
            expand_status: If True, include connector status in the response.

        Returns:
            List of connector name strings, or expanded connector dicts.
        """
        params = {"expand": "status"} if expand_status else {}
        async with self._make_client() as client:
            response = await client.get("/connectors", params=params)
            if response.status_code != 200:
                raise RuntimeError(f"Kafka Connect error listing connectors: {response.status_code} {response.text}")
            data = response.json()
            if isinstance(data, list):
                return [{"name": name} for name in data]
            if isinstance(data, dict):
                return [{"name": name, **info} for name, info in data.items()]
            return []

    async def get_connector(self, name: str) -> dict[str, Any]:
        """Get configuration for a specific connector.

        Args:
            name: Connector name.

        Returns:
            Connector descriptor dict with name, config, tasks.

        Raises:
            RuntimeError: If the connector does not exist.
        """
        async with self._make_client() as client:
            response = await client.get(f"/connectors/{name}")
            if response.status_code == 404:
                raise RuntimeError(f"Connector '{name}' not found")
            if response.status_code != 200:
                raise RuntimeError(f"Kafka Connect error: {response.status_code} {response.text}")
            connector: dict[str, Any] = response.json()
            return connector

    async def get_connector_status(self, name: str) -> dict[str, Any]:
        """Get runtime status for a connector and its tasks.

        Args:
            name: Connector name.

        Returns:
            Status dict with connector state, tasks states.
        """
        async with self._make_client() as client:
            response = await client.get(f"/connectors/{name}/status")
            if response.status_code == 404:
                raise RuntimeError(f"Connector '{name}' not found")
            if response.status_code != 200:
                raise RuntimeError(f"Kafka Connect error: {response.status_code} {response.text}")
            status: dict[str, Any] = response.json()
            return status

    async def create_connector(self, name: str, config: dict[str, Any]) -> dict[str, Any]:
        """Create a new connector with the given configuration.

        Args:
            name: Connector name.
            config: Connector configuration dict (connector.class, tasks.max, etc.).

        Returns:
            Created connector descriptor.

        Raises:
            RuntimeError: If creation fails (e.g. duplicate name or bad config).
        """
        payload = {"name": name, "config": config}
        async with self._make_client() as client:
            response = await client.post("/connectors", json=payload)
            if response.status_code not in (200, 201):
                raise RuntimeError(
                    f"Failed to create connector '{name}': {response.status_code} {response.text}"
                )
            created: dict[str, Any] = response.json()
            return created

    async def update_connector_config(self, name: str, config: dict[str, Any]) -> dict[str, Any]:
        """Update configuration for an existing connector.

        Args:
            name: Connector name.
            config: New configuration dict.

        Returns:
            Updated connector descriptor.
        """
        async with self._make_client() as client:
            response = await client.put(f"/connectors/{name}/config", json=config)
            if response.status_code not in (200, 201):
                raise RuntimeError(
                    f"Failed to update connector '{name}': {response.status_code} {response.text}"
                )
            updated: dict[str, Any] = response.json()
            return updated

    async def delete_connector(self, name: str) -> None:
        """Delete a connector and stop all its tasks.

        Args:
            name: Connector name.

        Raises:
            RuntimeError: If deletion fails or connector not found.
        """
        async with self._make_client() as client:
            response = await client.delete(f"/connectors/{name}")
            if response.status_code == 404:
                raise RuntimeError(f"Connector '{name}' not found")
            if response.status_code not in (200, 204):
                raise RuntimeError(
                    f"Failed to delete connector '{name}': {response.status_code} {response.text}"
                )

    async def restart_connector(self, name: str, include_tasks: bool = True) -> dict[str, Any]:
        """Restart a connector and optionally its tasks.

        Args:
            name: Connector name.
            include_tasks: If True, restart all tasks as well.

        Returns:
            Dict with restart result.
        """
        params = {"includeTasks": str(include_tasks).lower(), "onlyFailed": "false"}
        async with self._make_client() as client:
            response = await client.post(f"/connectors/{name}/restart", params=params)
            if response.status_code not in (200, 202, 204):
                raise RuntimeError(
                    f"Failed to restart connector '{name}': {response.status_code} {response.text}"
                )
            return {"name": name, "status": "restarting"}

    async def pause_connector(self, name: str) -> None:
        """Pause a connector.

        Args:
            name: Connector name.
        """
        async with self._make_client() as client:
            response = await client.put(f"/connectors/{name}/pause")
            if response.status_code not in (202, 204):
                raise RuntimeError(f"Failed to pause connector '{name}': {response.status_code}")

    async def resume_connector(self, name: str) -> None:
        """Resume a paused connector.

        Args:
            name: Connector name.
        """
        async with self._make_client() as client:
            response = await client.put(f"/connectors/{name}/resume")
            if response.status_code not in (202, 204):
                raise RuntimeError(f"Failed to resume connector '{name}': {response.status_code}")
