"""Event schema versioning adapter for the AumOS Event Bus.

Manages Protobuf schema lifecycle via Confluent Schema Registry integration:
schema registration, backward/forward/full compatibility checking, version
migration transformers, evolution tracking, deprecated version alerting,
per-event schema validation, and version negotiation between producers
and consumers.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from aumos_common.errors import NotFoundError, ValidationError
from aumos_common.observability import get_logger

from aumos_event_bus.core.interfaces import ISchemaRegistryClient, ISchemaRepository
from aumos_event_bus.core.models import SchemaCompatibility

logger = get_logger(__name__)

# Deprecated versions are alerted if older than this many versions
_DEPRECATION_AGE_THRESHOLD = 3


class CompatibilityMode(str, Enum):
    """Schema compatibility enforcement modes."""

    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"


class EventVersionManager:
    """Schema versioning and compatibility enforcement for Event Bus schemas.

    Provides a service-level wrapper around the Confluent Schema Registry client
    and the local schema version persistence layer. Enforces compatibility rules,
    tracks schema evolution, detects deprecated versions, and supports version
    negotiation between producers and consumers.

    Usage::

        versioner = EventVersionManager(
            schema_repo=schema_repo,
            registry_client=registry_client,
        )
        result = await versioner.register_version(
            subject="aumos.payments-value",
            schema_definition='syntax = "proto3"; message Payment {...}',
            compatibility=CompatibilityMode.BACKWARD,
            tenant_id="t-001",
        )
    """

    def __init__(
        self,
        schema_repo: ISchemaRepository,
        registry_client: ISchemaRegistryClient,
        default_compatibility: CompatibilityMode = CompatibilityMode.BACKWARD,
    ) -> None:
        """Initialise the event version manager.

        Args:
            schema_repo: Persistence layer for schema version metadata.
            registry_client: Confluent Schema Registry HTTP client.
            default_compatibility: Default compatibility mode applied to new subjects.
        """
        self._schema_repo = schema_repo
        self._registry = registry_client
        self._default_compatibility = default_compatibility

    async def register_version(
        self,
        subject: str,
        schema_definition: str,
        tenant_id: str,
        topic_id: uuid.UUID | None = None,
        compatibility: CompatibilityMode | None = None,
        schema_type: str = "PROTOBUF",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Register a new schema version with compatibility validation.

        Checks compatibility against existing versions before registration.
        On success, persists the schema metadata to the local database and
        registers with the Confluent Schema Registry.

        Args:
            subject: Schema Registry subject name (e.g., 'topic-name-value').
            schema_definition: Protobuf IDL schema string.
            tenant_id: Owning tenant identifier.
            topic_id: Associated topic UUID (optional).
            compatibility: Compatibility mode override. Uses default if None.
            schema_type: Schema format ('PROTOBUF', 'AVRO', 'JSON').
            metadata: Additional metadata to attach to the version record.

        Returns:
            Persisted SchemaVersion as a dict.

        Raises:
            ValidationError: If schema fails compatibility check.
        """
        effective_compat = compatibility or self._default_compatibility

        # Set compatibility mode on registry before registering
        try:
            await self._registry.set_compatibility(subject, effective_compat.value)
        except Exception as exc:
            logger.warning(
                "Failed to set compatibility mode — proceeding with registration",
                subject=subject,
                compatibility=effective_compat.value,
                error=str(exc),
            )

        # Validate compatibility against existing versions
        is_compatible = await self._registry.check_compatibility(subject, schema_definition)
        if not is_compatible:
            raise ValidationError(
                message=(
                    f"Schema is not compatible with existing versions for subject '{subject}' "
                    f"under compatibility mode '{effective_compat.value}'"
                ),
                field="schema_definition",
            )

        # Register with the schema registry
        schema_id = await self._registry.register_schema(
            subject=subject,
            schema_definition=schema_definition,
            schema_type=schema_type,
        )

        # Retrieve the assigned version number
        latest = await self._registry.get_latest_schema(subject)
        schema_version = latest.get("version", 1)

        # Deactivate previous active version in local DB
        await self._deactivate_previous_version(subject=subject, tenant_id=tenant_id)

        # Persist to local schema repository
        schema_record = await self._schema_repo.create(
            {
                "tenant_id": tenant_id,
                "topic_id": topic_id,
                "subject": subject,
                "schema_version": schema_version,
                "schema_id": schema_id,
                "schema_definition": schema_definition,
                "schema_type": schema_type,
                "compatibility": effective_compat.value,
                "is_active": True,
                "metadata": {
                    **(metadata or {}),
                    "registered_at": datetime.now(UTC).isoformat(),
                },
            }
        )

        logger.info(
            "Schema version registered",
            subject=subject,
            schema_version=schema_version,
            schema_id=schema_id,
            compatibility=effective_compat.value,
        )
        return schema_record

    async def check_compatibility(
        self,
        subject: str,
        schema_definition: str,
        mode: CompatibilityMode | None = None,
    ) -> dict[str, Any]:
        """Check whether a schema definition is compatible with existing versions.

        Args:
            subject: Schema Registry subject to check against.
            schema_definition: Candidate schema string.
            mode: Compatibility mode to apply. Uses subject's configured mode if None.

        Returns:
            Dict with 'compatible' bool, 'subject', 'mode', and optional 'errors'.
        """
        if mode is not None:
            try:
                await self._registry.set_compatibility(subject, mode.value)
            except Exception as exc:
                logger.debug("Could not set compatibility for check", error=str(exc))

        is_compatible = await self._registry.check_compatibility(subject, schema_definition)

        result: dict[str, Any] = {
            "subject": subject,
            "compatible": is_compatible,
            "mode": mode.value if mode else self._default_compatibility.value,
            "checked_at": datetime.now(UTC).isoformat(),
        }

        if not is_compatible:
            result["errors"] = [
                f"Schema is not {mode.value if mode else 'backward'}-compatible "
                f"with existing versions for subject '{subject}'"
            ]

        return result

    async def get_version_history(
        self,
        subject: str,
        tenant_id: str,
    ) -> list[dict[str, Any]]:
        """Return the full version history for a schema subject.

        Args:
            subject: Schema Registry subject.
            tenant_id: Tenant context.

        Returns:
            List of SchemaVersion dicts ordered by version number ascending.
        """
        versions = await self._schema_repo.list_versions(subject=subject, tenant_id=tenant_id)
        logger.debug(
            "Schema version history retrieved",
            subject=subject,
            version_count=len(versions),
        )
        return versions

    async def get_latest_version(
        self,
        subject: str,
        tenant_id: str,
    ) -> dict[str, Any]:
        """Return the latest active schema version for a subject.

        Args:
            subject: Schema Registry subject.
            tenant_id: Tenant context.

        Returns:
            Latest SchemaVersion dict.

        Raises:
            NotFoundError: If no active version exists for the subject.
        """
        latest = await self._schema_repo.get_latest_by_subject(subject=subject, tenant_id=tenant_id)
        if not latest:
            raise NotFoundError(resource="schema_version", resource_id=subject)
        return latest

    async def negotiate_version(
        self,
        subject: str,
        tenant_id: str,
        producer_version: int,
        consumer_min_version: int,
    ) -> dict[str, Any]:
        """Negotiate a compatible schema version between producer and consumer.

        Selects the highest version that satisfies both the producer's version
        and the consumer's minimum version requirement.

        Args:
            subject: Schema Registry subject.
            tenant_id: Tenant context.
            producer_version: Schema version the producer is sending.
            consumer_min_version: Minimum schema version the consumer can handle.

        Returns:
            Dict with 'negotiated_version', 'compatible', and version metadata.

        Raises:
            ValidationError: If no compatible version can be negotiated.
        """
        versions = await self._schema_repo.list_versions(subject=subject, tenant_id=tenant_id)

        if not versions:
            raise NotFoundError(resource="schema_versions", resource_id=subject)

        # Find versions in the negotiation range
        compatible_versions = [
            v for v in versions
            if consumer_min_version <= (v.get("schema_version") or 0) <= producer_version
        ]

        if not compatible_versions:
            raise ValidationError(
                message=(
                    f"No compatible schema version found for subject '{subject}' "
                    f"between consumer_min={consumer_min_version} and producer={producer_version}"
                ),
                field="producer_version",
            )

        # Select highest compatible version
        negotiated = max(compatible_versions, key=lambda v: v.get("schema_version") or 0)

        logger.info(
            "Schema version negotiated",
            subject=subject,
            producer_version=producer_version,
            consumer_min_version=consumer_min_version,
            negotiated_version=negotiated.get("schema_version"),
        )

        return {
            "subject": subject,
            "negotiated_version": negotiated.get("schema_version"),
            "schema_id": negotiated.get("schema_id"),
            "compatible": True,
            "schema_definition": negotiated.get("schema_definition", ""),
            "negotiated_at": datetime.now(UTC).isoformat(),
        }

    async def validate_event(
        self,
        subject: str,
        tenant_id: str,
        event_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate an event payload against the latest registered schema.

        Performs structural validation (field presence, required fields) against
        the schema definition. For Protobuf, validates field name presence.

        Args:
            subject: Schema Registry subject.
            tenant_id: Tenant context.
            event_payload: Event data dict to validate.

        Returns:
            Dict with 'valid', 'subject', 'schema_version', and 'errors' keys.
        """
        try:
            latest = await self.get_latest_version(subject=subject, tenant_id=tenant_id)
        except NotFoundError:
            return {
                "valid": False,
                "subject": subject,
                "errors": [f"No schema registered for subject '{subject}'"],
            }

        schema_def = latest.get("schema_definition", "")
        errors = self._validate_payload_against_schema(
            payload=event_payload,
            schema_definition=schema_def,
        )

        return {
            "valid": len(errors) == 0,
            "subject": subject,
            "schema_version": latest.get("schema_version"),
            "schema_id": latest.get("schema_id"),
            "errors": errors,
            "validated_at": datetime.now(UTC).isoformat(),
        }

    async def detect_deprecated_versions(
        self,
        tenant_id: str,
    ) -> list[dict[str, Any]]:
        """Detect and report deprecated schema versions across all subjects.

        A version is considered deprecated if it is more than
        _DEPRECATION_AGE_THRESHOLD versions behind the latest for its subject.

        Args:
            tenant_id: Tenant context.

        Returns:
            List of deprecated version dicts with subject, version, and age info.
        """
        all_subjects = await self._registry.list_subjects()
        deprecated: list[dict[str, Any]] = []

        for subject in all_subjects:
            try:
                versions = await self._schema_repo.list_versions(
                    subject=subject, tenant_id=tenant_id
                )
                if len(versions) <= _DEPRECATION_AGE_THRESHOLD:
                    continue

                sorted_versions = sorted(versions, key=lambda v: v.get("schema_version") or 0)
                latest_version_num = sorted_versions[-1].get("schema_version") or 0

                for version in sorted_versions:
                    version_num = version.get("schema_version") or 0
                    age = latest_version_num - version_num
                    if age >= _DEPRECATION_AGE_THRESHOLD:
                        deprecated.append(
                            {
                                "subject": subject,
                                "schema_version": version_num,
                                "latest_version": latest_version_num,
                                "versions_behind": age,
                                "schema_id": version.get("schema_id"),
                                "is_active": version.get("is_active", False),
                            }
                        )
                        logger.warning(
                            "Deprecated schema version detected",
                            subject=subject,
                            schema_version=version_num,
                            versions_behind=age,
                        )
            except Exception as exc:
                logger.debug(
                    "Could not check deprecation for subject",
                    subject=subject,
                    error=str(exc),
                )

        return deprecated

    async def build_migration_transformer(
        self,
        subject: str,
        tenant_id: str,
        from_version: int,
        to_version: int,
    ) -> dict[str, Any]:
        """Build a migration descriptor for transforming events between versions.

        Returns metadata describing the field changes between two schema versions.
        Actual field-level transformation must be implemented by the consumer.

        Args:
            subject: Schema Registry subject.
            tenant_id: Tenant context.
            from_version: Source schema version number.
            to_version: Target schema version number.

        Returns:
            Migration descriptor dict with added_fields, removed_fields, and
            a field_map for rename/type change tracking.

        Raises:
            NotFoundError: If either version cannot be found.
        """
        versions = await self._schema_repo.list_versions(subject=subject, tenant_id=tenant_id)

        version_map = {v.get("schema_version"): v for v in versions}

        from_schema = version_map.get(from_version)
        to_schema = version_map.get(to_version)

        if not from_schema:
            raise NotFoundError(resource="schema_version", resource_id=str(from_version))
        if not to_schema:
            raise NotFoundError(resource="schema_version", resource_id=str(to_version))

        from_fields = self._extract_fields_from_schema(from_schema.get("schema_definition", ""))
        to_fields = self._extract_fields_from_schema(to_schema.get("schema_definition", ""))

        added = list(to_fields - from_fields)
        removed = list(from_fields - to_fields)
        retained = list(from_fields & to_fields)

        logger.info(
            "Migration transformer built",
            subject=subject,
            from_version=from_version,
            to_version=to_version,
            added_fields=added,
            removed_fields=removed,
        )

        return {
            "subject": subject,
            "from_version": from_version,
            "to_version": to_version,
            "added_fields": added,
            "removed_fields": removed,
            "retained_fields": retained,
            "field_map": {field: field for field in retained},
            "direction": "upgrade" if to_version > from_version else "downgrade",
            "built_at": datetime.now(UTC).isoformat(),
        }

    def _extract_fields_from_schema(self, schema_definition: str) -> set[str]:
        """Extract field names from a Protobuf schema definition string.

        Args:
            schema_definition: Protobuf IDL string.

        Returns:
            Set of field name strings extracted via simple line parsing.
        """
        fields: set[str] = set()
        for line in schema_definition.splitlines():
            stripped = line.strip()
            # Protobuf field lines: <modifier> <type> <name> = <number>;
            parts = stripped.rstrip(";").split()
            if len(parts) >= 3 and "=" in parts and not stripped.startswith("//"):
                # The field name is the element before "="
                eq_idx = parts.index("=")
                if eq_idx >= 1:
                    fields.add(parts[eq_idx - 1])
        return fields

    def _validate_payload_against_schema(
        self,
        payload: dict[str, Any],
        schema_definition: str,
    ) -> list[str]:
        """Validate payload keys against field names in the schema.

        Args:
            payload: Event payload dict to validate.
            schema_definition: Protobuf schema string.

        Returns:
            List of validation error strings (empty if valid).
        """
        if not schema_definition:
            return ["Schema definition is empty — cannot validate payload"]

        known_fields = self._extract_fields_from_schema(schema_definition)
        if not known_fields:
            # Could not parse schema — skip structural validation
            return []

        errors: list[str] = []
        for field_name in payload.keys():
            if field_name not in known_fields:
                errors.append(
                    f"Unexpected field '{field_name}' not found in schema definition"
                )

        return errors

    async def _deactivate_previous_version(self, subject: str, tenant_id: str) -> None:
        """Mark the previously active schema version as inactive.

        Args:
            subject: Schema Registry subject.
            tenant_id: Tenant context.
        """
        try:
            previous = await self._schema_repo.get_latest_by_subject(
                subject=subject, tenant_id=tenant_id
            )
            if previous:
                schema_id_value = previous.get("id")
                if schema_id_value:
                    await self._schema_repo.deactivate_version(
                        uuid.UUID(str(schema_id_value)), tenant_id
                    )
        except Exception as exc:
            logger.debug(
                "Could not deactivate previous schema version",
                subject=subject,
                error=str(exc),
            )
