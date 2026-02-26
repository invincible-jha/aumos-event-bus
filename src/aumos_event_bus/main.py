"""AumOS Event Bus service entry point.

Bootstraps the FastAPI application with Kafka admin client and Schema Registry
connections during lifespan, then tears them down on shutdown.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from aumos_common.app import create_app
from aumos_common.database import init_database
from aumos_common.health import HealthCheck
from aumos_common.observability import get_logger

from aumos_event_bus.settings import Settings

logger = get_logger(__name__)

settings = Settings()


async def _check_kafka() -> bool:
    """Verify connectivity to the Kafka broker.

    Returns:
        True if broker is reachable, False otherwise.
    """
    try:
        from aumos_event_bus.adapters.kafka_admin import KafkaAdminAdapter

        admin = KafkaAdminAdapter(settings=settings)
        topics = await admin.list_topics()
        logger.debug("Kafka health check passed", topic_count=len(topics))
        return True
    except Exception as exc:
        logger.warning("Kafka health check failed", error=str(exc))
        return False


async def _check_schema_registry() -> bool:
    """Verify connectivity to Confluent Schema Registry.

    Returns:
        True if registry responds, False otherwise.
    """
    try:
        from aumos_event_bus.adapters.schema_registry import SchemaRegistryAdapter

        registry = SchemaRegistryAdapter(settings=settings)
        await registry.list_subjects()
        logger.debug("Schema Registry health check passed")
        return True
    except Exception as exc:
        logger.warning("Schema Registry health check failed", error=str(exc))
        return False


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage startup and shutdown of infrastructure connections.

    Startup:
    - Initialises SQLAlchemy async engine and connection pool
    - Validates Kafka broker connectivity
    - Validates Schema Registry connectivity

    Shutdown:
    - Releases connection pool resources

    Args:
        app: The FastAPI application instance.

    Yields:
        None — control returns to FastAPI while the app serves requests.
    """
    logger.info("Starting AumOS Event Bus", version="0.1.0")

    # Initialise database connection pool
    init_database(settings.database)
    logger.info("Database connection pool initialised")

    # Warm-up connectivity checks (non-fatal — log and continue)
    try:
        await _check_kafka()
    except Exception:
        logger.warning("Kafka not reachable at startup — continuing without broker")

    try:
        await _check_schema_registry()
    except Exception:
        logger.warning("Schema Registry not reachable at startup — continuing")

    logger.info("AumOS Event Bus ready to serve requests")

    yield

    logger.info("AumOS Event Bus shutting down")


app = create_app(
    service_name="aumos-event-bus",
    version="0.1.0",
    settings=settings,
    lifespan=lifespan,
    health_checks=[
        HealthCheck(name="kafka", check_fn=_check_kafka),
        HealthCheck(name="schema_registry", check_fn=_check_schema_registry),
    ],
)

# Register API routes
from aumos_event_bus.api.router import router  # noqa: E402

app.include_router(router, prefix="/api/v1")
