"""
Scheduler process.

Runs alongside the API and workers as its own long-lived process. Two jobs:

1. Recurring job spawning: for every RECURRING "template" job, compute its
   next due run from the cron expression and, once due, insert a fresh
   QUEUED job row for a worker to pick up (the template itself is never
   claimed/executed -- only its spawned children are).
2. Stale-job reclamation: safety net for workers that crash mid-job without
   heartbeating; see `job_service.reclaim_stale_jobs`.

Run with:  python -m app.services.scheduler   (from backend/)
"""
import asyncio
import logging
import uuid
from datetime import datetime, timezone

from croniter import croniter
from sqlalchemy import select

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.enums import JobStatus, JobType
from app.models.job import Job
from app.services import job_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scheduler")


async def spawn_due_recurring_jobs():
    async with AsyncSessionLocal() as db:
        templates = (
            await db.execute(
                select(Job).where(Job.job_type == JobType.RECURRING, Job.cron_expression.is_not(None))
            )
        ).scalars().all()

        now = datetime.now(timezone.utc)
        for template in templates:
            cron = croniter(template.cron_expression, template.run_at)
            next_fire = cron.get_next(datetime)
            if next_fire > now:
                continue

            # Avoid duplicate spawns: check whether a child for this fire time
            # already exists.
            existing = (
                await db.execute(
                    select(Job).where(
                        Job.parent_recurring_job_id == template.id,
                        Job.run_at == next_fire,
                    )
                )
            ).scalar_one_or_none()
            if existing:
                continue

            child = Job(
                queue_id=template.queue_id,
                job_type=JobType.RECURRING,
                status=JobStatus.QUEUED,
                name=template.name,
                payload=template.payload,
                priority=template.priority,
                run_at=next_fire,
                parent_recurring_job_id=template.id,
                max_attempts=template.max_attempts,
            )
            db.add(child)
            # Advance the template's own run_at so the next tick computes the
            # following occurrence instead of re-firing the same one.
            template.run_at = next_fire
            log.info("Spawned recurring job child for template %s at %s", template.id, next_fire)

        await db.commit()


async def reclaim_stale():
    async with AsyncSessionLocal() as db:
        count = await job_service.reclaim_stale_jobs(db, settings.WORKER_STALE_HEARTBEAT_SECONDS)
        if count:
            log.info("Reclaimed %d stale job(s) back to QUEUED", count)


async def main_loop():
    log.info("Scheduler started (poll interval=%ss)", settings.SCHEDULER_POLL_INTERVAL_SECONDS)
    while True:
        try:
            await spawn_due_recurring_jobs()
            await reclaim_stale()
        except Exception:  # noqa: BLE001
            log.exception("Scheduler tick failed")
        await asyncio.sleep(settings.SCHEDULER_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main_loop())
