"""
Core job-lifecycle logic shared by the API layer and (indirectly) exercised
by the worker service through the API.

The single most important guarantee in this file is `claim_jobs`: it must
never let two workers claim the same job. That's done with a single
UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) statement so the
claim is atomic at the database level -- no application-level locking needed,
and it scales across many worker processes/machines.
"""
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import ExecutionStatus, JobStatus, LogLevel
from app.models.job import DeadLetterEntry, Job, JobExecution, JobLog
from app.models.queue import Queue, RetryPolicy


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def claim_jobs(
    db: AsyncSession, queue_ids: list[uuid.UUID], worker_id: uuid.UUID, max_jobs: int
) -> list[Job]:
    """
    Atomically claim up to `max_jobs` due jobs from the given queues.

    Uses SELECT ... FOR UPDATE SKIP LOCKED so concurrent workers polling the
    same queue never see (or block on) rows another worker is mid-claim on.
    Respects each queue's concurrency_limit by counting jobs already in
    CLAIMED/RUNNING state for that queue.
    """
    claimed: list[Job] = []
    remaining = max_jobs

    for queue_id in queue_ids:
        if remaining <= 0:
            break

        queue = (await db.execute(select(Queue).where(Queue.id == queue_id))).scalar_one_or_none()
        if queue is None or queue.is_paused:
            continue

        in_flight = (
            await db.execute(
                select(Job).where(
                    Job.queue_id == queue_id,
                    Job.status.in_([JobStatus.CLAIMED, JobStatus.RUNNING]),
                )
            )
        ).scalars().all()
        available_slots = max(queue.concurrency_limit - len(in_flight), 0)
        take = min(remaining, available_slots)
        if take <= 0:
            continue

        # Sub-select the candidate rows first (ordered, locked, skip-locked),
        # then UPDATE just those ids. This two-step form is what lets us use
        # ORDER BY + LIMIT together with FOR UPDATE SKIP LOCKED portably.
        candidate_ids_result = await db.execute(
            select(Job.id)
            .where(
                Job.queue_id == queue_id,
                Job.status.in_([JobStatus.QUEUED, JobStatus.SCHEDULED, JobStatus.RETRYING]),
                Job.run_at <= utcnow(),
            )
            .order_by(Job.priority.desc(), Job.run_at.asc())
            .limit(take)
            .with_for_update(skip_locked=True)
        )
        candidate_ids = [row[0] for row in candidate_ids_result.all()]
        if not candidate_ids:
            continue

        await db.execute(
            update(Job)
            .where(Job.id.in_(candidate_ids))
            .values(
                status=JobStatus.CLAIMED,
                claimed_by_worker_id=worker_id,
                claimed_at=utcnow(),
            )
        )
        await db.flush()

        rows = (await db.execute(select(Job).where(Job.id.in_(candidate_ids)))).scalars().all()
        claimed.extend(rows)
        remaining -= len(rows)

    await db.commit()
    return claimed


async def mark_running(db: AsyncSession, job: Job, worker_id: uuid.UUID) -> JobExecution:
    job.status = JobStatus.RUNNING
    job.started_at = utcnow()
    job.attempt_count += 1

    execution = JobExecution(
        job_id=job.id,
        worker_id=worker_id,
        attempt_number=job.attempt_count,
        status=ExecutionStatus.RUNNING,
        started_at=utcnow(),
    )
    db.add(execution)
    db.add(JobLog(job_id=job.id, level=LogLevel.INFO, message=f"Attempt {job.attempt_count} started"))
    await db.commit()
    await db.refresh(execution)
    return execution


async def mark_completed(db: AsyncSession, job: Job, execution: JobExecution, result: dict | None) -> Job:
    now = utcnow()
    execution.status = ExecutionStatus.SUCCEEDED
    execution.finished_at = now
    execution.duration_ms = int((now - execution.started_at).total_seconds() * 1000)
    execution.result = result

    job.status = JobStatus.COMPLETED
    job.completed_at = now
    db.add(JobLog(job_id=job.id, level=LogLevel.INFO, message="Job completed successfully"))
    await db.commit()
    await db.refresh(job)
    return job


async def mark_failed(
    db: AsyncSession, job: Job, execution: JobExecution, error: str, retryable: bool
) -> Job:
    """
    Handles both the "retry" and "permanently failed -> DLQ" branches,
    computing the next retry delay from the queue's retry policy.
    """
    now = utcnow()
    execution.status = ExecutionStatus.FAILED
    execution.finished_at = now
    execution.duration_ms = int((now - execution.started_at).total_seconds() * 1000)
    execution.error = error
    job.last_error = error

    queue = (await db.execute(select(Queue).where(Queue.id == job.queue_id))).scalar_one()
    retry_policy = None
    if queue.retry_policy_id:
        retry_policy = (
            await db.execute(select(RetryPolicy).where(RetryPolicy.id == queue.retry_policy_id))
        ).scalar_one_or_none()

    max_retries = job.max_attempts if job.max_attempts is not None else (
        retry_policy.max_retries if retry_policy else 0
    )

    if retryable and job.attempt_count <= max_retries:
        delay = retry_policy.compute_delay_seconds(job.attempt_count) if retry_policy else 30
        job.status = JobStatus.RETRYING
        job.run_at = now + timedelta(seconds=delay)
        job.claimed_by_worker_id = None
        job.claimed_at = None
        db.add(
            JobLog(
                job_id=job.id,
                level=LogLevel.WARNING,
                message=f"Attempt {job.attempt_count} failed: {error}. Retrying in {delay}s.",
            )
        )
    else:
        job.status = JobStatus.DEAD_LETTER
        db.add(
            DeadLetterEntry(
                job_id=job.id,
                queue_id=job.queue_id,
                reason=error,
                original_payload=job.payload,
                attempt_count=job.attempt_count,
                moved_at=now,
            )
        )
        db.add(
            JobLog(
                job_id=job.id,
                level=LogLevel.ERROR,
                message=f"Attempt {job.attempt_count} failed permanently: {error}. Moved to DLQ.",
            )
        )

    await db.commit()
    await db.refresh(job)
    return job


async def reclaim_stale_jobs(db: AsyncSession, stale_after_seconds: int) -> int:
    """
    Safety net: if a worker dies mid-job without heartbeating, its CLAIMED/
    RUNNING jobs would otherwise be stuck forever. Anything claimed longer
    than `stale_after_seconds` ago with no completion is put back in the
    queue. Called periodically by the scheduler process.
    """
    cutoff = utcnow() - timedelta(seconds=stale_after_seconds)
    result = await db.execute(
        update(Job)
        .where(
            Job.status.in_([JobStatus.CLAIMED, JobStatus.RUNNING]),
            Job.claimed_at < cutoff,
        )
        .values(status=JobStatus.QUEUED, claimed_by_worker_id=None, claimed_at=None)
        .returning(Job.id)
    )
    reclaimed_ids = result.all()
    await db.commit()
    return len(reclaimed_ids)
