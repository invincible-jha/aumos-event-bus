"""Apache Flink REST API client for stream processing job management.

Wraps the Flink JobManager REST API to submit, list, and cancel Flink jobs.
Used by StreamProcessingService when the flink backend is configured.
"""

from __future__ import annotations

from typing import Any

import httpx

from aumos_common.observability import get_logger

logger = get_logger(__name__)


class FlinkClient:
    """HTTP client for the Apache Flink REST API.

    Provides async wrappers around Flink's JobManager REST API for
    submitting JAR-based jobs, listing jobs, and cancelling them.

    Usage::

        client = FlinkClient(base_url="http://flink-jobmanager:8081")
        jobs = await client.list_jobs()
    """

    def __init__(self, base_url: str) -> None:
        """Initialise with Flink JobManager REST API base URL.

        Args:
            base_url: Flink REST API base URL (e.g. http://flink-jobmanager:8081).
        """
        self._base_url = base_url.rstrip("/")

    def _make_client(self) -> httpx.AsyncClient:
        """Build an httpx AsyncClient pointed at the Flink JobManager.

        Returns:
            Configured httpx.AsyncClient.
        """
        return httpx.AsyncClient(base_url=self._base_url, timeout=60.0)

    async def list_jobs(self) -> list[dict[str, Any]]:
        """List all jobs currently known to the Flink cluster.

        Returns:
            List of job descriptor dicts with jid, name, state, start-time.

        Raises:
            RuntimeError: If the Flink API returns an error.
        """
        async with self._make_client() as client:
            response = await client.get("/jobs/overview")
            if response.status_code != 200:
                raise RuntimeError(f"Flink API error listing jobs: {response.status_code} {response.text}")
            data: dict[str, Any] = response.json()
            jobs: list[dict[str, Any]] = data.get("jobs", [])
            return jobs

    async def get_job(self, job_id: str) -> dict[str, Any]:
        """Get details for a specific Flink job.

        Args:
            job_id: Flink job ID (hex string).

        Returns:
            Job detail dict with jid, name, state, vertices.

        Raises:
            RuntimeError: If the job does not exist or API fails.
        """
        async with self._make_client() as client:
            response = await client.get(f"/jobs/{job_id}")
            if response.status_code == 404:
                raise RuntimeError(f"Flink job '{job_id}' not found")
            if response.status_code != 200:
                raise RuntimeError(f"Flink API error: {response.status_code} {response.text}")
            job: dict[str, Any] = response.json()
            return job

    async def cancel_job(self, job_id: str) -> dict[str, Any]:
        """Cancel a running Flink job.

        Args:
            job_id: Flink job ID (hex string).

        Returns:
            Dict with job_id and status.

        Raises:
            RuntimeError: If cancellation fails.
        """
        async with self._make_client() as client:
            response = await client.patch(f"/jobs/{job_id}", params={"mode": "cancel"})
            if response.status_code not in (200, 202):
                raise RuntimeError(
                    f"Failed to cancel Flink job '{job_id}': {response.status_code} {response.text}"
                )
            return {"job_id": job_id, "status": "cancelling"}

    async def upload_and_run_jar(
        self,
        jar_content: bytes,
        jar_filename: str,
        entry_class: str | None = None,
        program_args: list[str] | None = None,
        parallelism: int = 1,
    ) -> dict[str, Any]:
        """Upload a JAR file and submit it as a Flink job.

        Args:
            jar_content: Raw bytes of the JAR file.
            jar_filename: Filename for the JAR (used in Flink UI).
            entry_class: Optional main class override.
            program_args: Optional list of program arguments.
            parallelism: Job parallelism level.

        Returns:
            Dict with job_id of the submitted job.

        Raises:
            RuntimeError: If upload or submission fails.
        """
        async with self._make_client() as client:
            # Upload the JAR
            upload_response = await client.post(
                "/jars/upload",
                files={"jarfile": (jar_filename, jar_content, "application/java-archive")},
            )
            if upload_response.status_code != 200:
                raise RuntimeError(
                    f"Failed to upload JAR to Flink: {upload_response.status_code} {upload_response.text}"
                )
            upload_data: dict[str, Any] = upload_response.json()
            jar_id: str = upload_data.get("filename", "").split("/")[-1]

            # Run the uploaded JAR
            run_payload: dict[str, Any] = {"parallelism": parallelism}
            if entry_class:
                run_payload["entryClass"] = entry_class
            if program_args:
                run_payload["programArgs"] = " ".join(program_args)

            run_response = await client.post(f"/jars/{jar_id}/run", json=run_payload)
            if run_response.status_code != 200:
                raise RuntimeError(
                    f"Failed to run Flink JAR '{jar_id}': {run_response.status_code} {run_response.text}"
                )
            run_data: dict[str, Any] = run_response.json()
            return {"job_id": run_data.get("jobid", ""), "jar_id": jar_id}

    async def health_check(self) -> bool:
        """Check whether the Flink JobManager is reachable.

        Returns:
            True if the JobManager responds with HTTP 200 on /overview.
        """
        try:
            async with self._make_client() as client:
                response = await client.get("/overview")
                return response.status_code == 200
        except Exception as exc:
            logger.warning("Flink health check failed", error=str(exc))
            return False
