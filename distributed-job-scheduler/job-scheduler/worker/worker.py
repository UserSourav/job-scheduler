"""
Worker service.

This process is intentionally decoupled from the API's database session --
it talks to the scheduler exclusively through the REST API (claim / start /
complete / fail / heartbeat). That keeps the worker deployable as a totally
separate service (different machine, container, language even) and means
horizontal scaling is just "run more of this process".

Run with:  python worker.py --queues emails,reports --concurrency 5
"""
import argparse
import asyncio
import logging
import signal
import time
import uuid

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("worker")


class JobWorker:
    def __init__(self, api_base: str, queue_names: list[str], concurrency: int, poll_interval: float):
        self.api_base = api_base.rstrip("/")
        self.queue_names = queue_names
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.worker_id: uuid.UUID | None = None
        self.active_jobs = 0
        self.shutting_down = False
        self.client = httpx.AsyncClient(base_url=self.api_base, timeout=30.0)

    async def register(self):
        import socket

        resp = await self.client.post(
            "/api/v1/workers/register",
            json={
                "hostname": socket.gethostname(),
                "queue_names": self.queue_names,
                "concurrency": self.concurrency,
            },
        )
        resp.raise_for_status()
        self.worker_id = uuid.UUID(resp.json()["id"])
        log.info("Registered as worker %s for queues %s", self.worker_id, self.queue_names)

    async def heartbeat_loop(self):
        while not self.shutting_down:
            try:
                await self.client.post(
                    f"/api/v1/workers/{self.worker_id}/heartbeat",
                    json={"active_job_count": self.active_jobs},
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("Heartbeat failed: %s", exc)
            await asyncio.sleep(5.0)

    async def execute_job(self, job: dict):
        """
        Runs one job. The actual "business logic" lives in `run_handler` --
        replace it with real task dispatch (e.g. a registry of job `name` ->
        Python callables) for a production deployment.
        """
        job_id = job["id"]
        self.active_jobs += 1
        try:
            start_resp = await self.client.post(
                f"/api/v1/jobs/{job_id}/start", params={"worker_id": str(self.worker_id)}
            )
            start_resp.raise_for_status()
            execution_id = start_resp.json()["id"]

            try:
                result = await self.run_handler(job)
            except Exception as exc:  # noqa: BLE001
                log.error("Job %s failed: %s", job_id, exc)
                await self.client.post(
                    f"/api/v1/jobs/{job_id}/fail",
                    json={"execution_id": execution_id, "error": str(exc), "retryable": True},
                )
                return

            await self.client.post(
                f"/api/v1/jobs/{job_id}/complete",
                json={"execution_id": execution_id, "result": result},
            )
            log.info("Job %s completed", job_id)
        finally:
            self.active_jobs -= 1

    async def run_handler(self, job: dict) -> dict:
        """
        Placeholder task dispatcher. Sleeps briefly to simulate work.
        Swap this out for real handlers keyed by `job['name']`.
        """
        await asyncio.sleep(0.2)
        return {"handled": job["name"], "payload_echo": job["payload"]}

    async def poll_loop(self):
        while not self.shutting_down:
            try:
                free_slots = self.concurrency - self.active_jobs
                if free_slots > 0:
                    resp = await self.client.post(
                        "/api/v1/jobs/claim",
                        json={
                            "worker_id": str(self.worker_id),
                            "queue_names": self.queue_names,
                            "max_jobs": free_slots,
                        },
                    )
                    resp.raise_for_status()
                    jobs = resp.json()
                    for job in jobs:
                        asyncio.create_task(self.execute_job(job))
            except Exception as exc:  # noqa: BLE001
                log.warning("Poll failed: %s", exc)
            await asyncio.sleep(self.poll_interval)

    async def run(self):
        await self.register()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        await asyncio.gather(self.poll_loop(), self.heartbeat_loop())

    async def shutdown(self):
        if self.shutting_down:
            return
        log.info("Graceful shutdown requested: draining (%d active jobs)...", self.active_jobs)
        self.shutting_down = True
        try:
            await self.client.post(f"/api/v1/workers/{self.worker_id}/drain")
        except Exception:  # noqa: BLE001
            pass
        while self.active_jobs > 0:
            await asyncio.sleep(0.5)
        try:
            await self.client.post(f"/api/v1/workers/{self.worker_id}/offline")
        except Exception:  # noqa: BLE001
            pass
        await self.client.aclose()
        log.info("Worker shut down cleanly.")
        raise SystemExit(0)


def main():
    parser = argparse.ArgumentParser(description="Job scheduler worker")
    parser.add_argument("--api-base", default="http://127.0.0.1:8000")
    parser.add_argument("--queues", required=True, help="Comma-separated queue names")
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    args = parser.parse_args()

    worker = JobWorker(
        api_base=args.api_base,
        queue_names=[q.strip() for q in args.queues.split(",")],
        concurrency=args.concurrency,
        poll_interval=args.poll_interval,
    )
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
