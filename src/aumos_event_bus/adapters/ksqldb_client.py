"""ksqlDB REST API client for stream processing management.

Wraps the ksqlDB REST API to create, list, and terminate persistent queries.
Used by StreamProcessingService when the ksqldb backend is configured.
"""

from __future__ import annotations

from typing import Any

import httpx

from aumos_common.observability import get_logger

logger = get_logger(__name__)


class KsqldbClient:
    """HTTP client for the ksqlDB REST API.

    Provides async wrappers around ksqlDB's REST endpoints for executing
    KSQL statements, listing queries, and terminating persistent queries.

    Usage::

        client = KsqldbClient(base_url="http://ksqldb-server:8088")
        await client.execute_statement("CREATE STREAM s AS SELECT * FROM t;")
    """

    def __init__(self, base_url: str, username: str = "", password: str = "") -> None:
        """Initialise with ksqlDB server URL and optional credentials.

        Args:
            base_url: ksqlDB REST API base URL (e.g. http://ksqldb-server:8088).
            username: Optional HTTP basic auth username.
            password: Optional HTTP basic auth password.
        """
        self._base_url = base_url.rstrip("/")
        self._auth = (username, password) if username else None

    def _make_client(self) -> httpx.AsyncClient:
        """Build an httpx AsyncClient with auth if configured.

        Returns:
            Configured httpx.AsyncClient.
        """
        kwargs: dict[str, Any] = {"base_url": self._base_url, "timeout": 30.0}
        if self._auth:
            kwargs["auth"] = self._auth
        return httpx.AsyncClient(**kwargs)

    async def execute_statement(self, sql: str, properties: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a KSQL statement (CREATE STREAM, CREATE TABLE, etc.).

        Args:
            sql: KSQL statement to execute.
            properties: Optional stream properties to include in the request.

        Returns:
            ksqlDB response payload as a dict.

        Raises:
            RuntimeError: If the ksqlDB server returns an error response.
        """
        payload: dict[str, Any] = {"ksql": sql}
        if properties:
            payload["streamsProperties"] = properties

        async with self._make_client() as client:
            response = await client.post("/ksql", json=payload)
            if response.status_code not in (200, 201):
                logger.error(
                    "ksqlDB statement failed",
                    sql=sql,
                    status=response.status_code,
                    body=response.text,
                )
                raise RuntimeError(f"ksqlDB error {response.status_code}: {response.text}")
            data: dict[str, Any] = response.json()
            if isinstance(data, list):
                return {"results": data}
            return data

    async def list_queries(self) -> list[dict[str, Any]]:
        """List all persistent queries running on the ksqlDB server.

        Returns:
            List of query descriptor dicts with id, queryString, sinks.
        """
        result = await self.execute_statement("SHOW QUERIES;")
        queries: list[dict[str, Any]] = result.get("queries", [])
        if not queries and "results" in result:
            for item in result["results"]:
                queries.extend(item.get("queries", []))
        return queries

    async def terminate_query(self, query_id: str) -> dict[str, Any]:
        """Terminate a persistent ksqlDB query.

        Args:
            query_id: The persistent query ID (e.g. CSAS_MY_STREAM_0).

        Returns:
            Termination result dict.

        Raises:
            RuntimeError: If termination fails.
        """
        async with self._make_client() as client:
            response = await client.post(
                "/close-query",
                json={"queryId": query_id},
            )
            if response.status_code not in (200, 204):
                raise RuntimeError(
                    f"Failed to terminate ksqlDB query '{query_id}': {response.status_code} {response.text}"
                )
            if response.content:
                terminated: dict[str, Any] = response.json()
                return terminated
            return {"query_id": query_id, "status": "terminated"}

    async def health_check(self) -> bool:
        """Check whether the ksqlDB server is reachable.

        Returns:
            True if the server responds with HTTP 200.
        """
        try:
            async with self._make_client() as client:
                response = await client.get("/info")
                return response.status_code == 200
        except Exception as exc:
            logger.warning("ksqlDB health check failed", error=str(exc))
            return False
