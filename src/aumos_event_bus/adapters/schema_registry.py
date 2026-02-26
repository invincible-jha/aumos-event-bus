"""Schema Registry adapter implementing ISchemaRegistryClient.

Uses httpx.AsyncClient to communicate with Confluent Schema Registry's
REST API. Supports PROTOBUF, AVRO, and JSON schema types.
"""

from __future__ import annotations

import base64
from typing import Any

import httpx

from aumos_common.observability import get_logger

from aumos_event_bus.core.interfaces import ISchemaRegistryClient
from aumos_event_bus.settings import Settings

logger = get_logger(__name__)


class SchemaRegistryAdapter:
    """Confluent Schema Registry client using httpx.

    Implements ISchemaRegistryClient against the Confluent Schema Registry
    REST API (v1). Supports optional basic authentication.

    Usage::

        adapter = SchemaRegistryAdapter(settings=settings)
        schema_id = await adapter.register_schema("my-topic-value", proto_str)
    """

    def __init__(self, settings: Settings) -> None:
        """Initialise the client from service settings.

        Args:
            settings: Event Bus settings with schema_registry_url and credentials.
        """
        self._base_url = settings.schema_registry_url.rstrip("/")
        self._username = settings.schema_registry_username
        self._password = settings.schema_registry_password

    def _make_client(self) -> httpx.AsyncClient:
        """Build an httpx client with optional basic auth.

        Returns:
            Configured httpx.AsyncClient.
        """
        headers: dict[str, str] = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        auth: httpx.BasicAuth | None = None
        if self._username and self._password:
            auth = httpx.BasicAuth(self._username, self._password)
        return httpx.AsyncClient(base_url=self._base_url, headers=headers, auth=auth, timeout=15.0)

    async def register_schema(
        self,
        subject: str,
        schema_definition: str,
        schema_type: str = "PROTOBUF",
    ) -> int:
        """Register a schema and return the assigned schema ID.

        If the schema already exists (idempotent registration), the existing
        schema ID is returned.

        Args:
            subject: Schema Registry subject name (e.g., "my-topic-value").
            schema_definition: Protobuf IDL or JSON schema string.
            schema_type: Schema type — PROTOBUF, AVRO, or JSON.

        Returns:
            Integer schema ID assigned by the registry.

        Raises:
            httpx.HTTPStatusError: On non-2xx response from registry.
        """
        payload: dict[str, Any] = {"schema": schema_definition}
        if schema_type != "AVRO":
            payload["schemaType"] = schema_type

        async with self._make_client() as client:
            response = await client.post(f"/subjects/{subject}/versions", json=payload)
            response.raise_for_status()
            data = response.json()
            schema_id: int = data["id"]
            logger.info("Schema registered", subject=subject, schema_id=schema_id)
            return schema_id

    async def get_schema(self, schema_id: int) -> dict[str, Any]:
        """Retrieve schema by its registry-assigned ID.

        Args:
            schema_id: Integer schema ID from the registry.

        Returns:
            Dict with id, schema, and schemaType fields.
        """
        async with self._make_client() as client:
            response = await client.get(f"/schemas/ids/{schema_id}")
            response.raise_for_status()
            return dict(response.json())

    async def get_latest_schema(self, subject: str) -> dict[str, Any]:
        """Retrieve the latest version for a subject.

        Args:
            subject: Schema Registry subject name.

        Returns:
            Dict with id, version, schema, and subject fields.
        """
        async with self._make_client() as client:
            response = await client.get(f"/subjects/{subject}/versions/latest")
            response.raise_for_status()
            return dict(response.json())

    async def list_subjects(self) -> list[str]:
        """Return all registered subjects.

        Returns:
            Sorted list of subject name strings.
        """
        async with self._make_client() as client:
            response = await client.get("/subjects")
            response.raise_for_status()
            subjects: list[str] = response.json()
            return sorted(subjects)

    async def set_compatibility(self, subject: str, compatibility: str) -> None:
        """Set the compatibility mode for a subject.

        Args:
            subject: Schema Registry subject name.
            compatibility: Mode string — BACKWARD, FORWARD, FULL, NONE, etc.
        """
        async with self._make_client() as client:
            response = await client.put(
                f"/config/{subject}",
                json={"compatibility": compatibility},
            )
            response.raise_for_status()
            logger.info("Schema compatibility set", subject=subject, compatibility=compatibility)

    async def check_compatibility(self, subject: str, schema_definition: str) -> bool:
        """Check whether a schema is compatible with existing versions.

        Returns True when no versions exist yet (first registration is always
        compatible) or when the registry confirms compatibility.

        Args:
            subject: Schema Registry subject name.
            schema_definition: Schema string to test.

        Returns:
            True if compatible (or no existing versions), False otherwise.
        """
        async with self._make_client() as client:
            # Check if subject exists first
            check_response = await client.get(f"/subjects/{subject}/versions")
            if check_response.status_code == 404:
                # No existing versions — first registration is always compatible
                return True

            response = await client.post(
                f"/compatibility/subjects/{subject}/versions/latest",
                json={"schema": schema_definition},
            )
            if response.status_code == 404:
                return True

            response.raise_for_status()
            data = response.json()
            is_compatible: bool = data.get("is_compatible", False)
            return is_compatible

    async def delete_subject(self, subject: str, permanent: bool = False) -> list[int]:
        """Delete all versions of a subject.

        Args:
            subject: Schema Registry subject name to delete.
            permanent: If True, perform a hard delete (permanent=true).

        Returns:
            List of deleted version numbers.
        """
        params = {"permanent": "true"} if permanent else {}
        async with self._make_client() as client:
            response = await client.delete(f"/subjects/{subject}", params=params)
            response.raise_for_status()
            deleted_versions: list[int] = response.json()
            logger.info(
                "Schema subject deleted",
                subject=subject,
                versions_deleted=deleted_versions,
                permanent=permanent,
            )
            return deleted_versions


# Satisfy ISchemaRegistryClient structural typing
_: ISchemaRegistryClient = SchemaRegistryAdapter.__new__(SchemaRegistryAdapter)  # type: ignore[assignment]
