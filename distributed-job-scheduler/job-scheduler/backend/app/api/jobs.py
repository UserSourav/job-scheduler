import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.enums import ExecutionStatus, JobStatus, JobType
from app.models.job import Job, JobExecution, JobLog
from app.models.user import User
from app.schemas.job import (
    BatchJobCreate,
    JobClaimRequest,
    JobCompleteRequest,
    JobCreate,
    JobExecutionOut,
    JobFailRequest,
    JobLogOut,
    JobOut,
)
from app.services import job_service

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


def _compute_run_at(payload: JobCreate) -> datetime:
    now = datetime.now(timezone.utc)
    if payload.job_type == JobType.DELAYED:
        return now + timedelta(seconds=payload.delay_seconds or 0)
    if payload.job_type in (JobType.SCHEDULED, JobType.RECURRING):
        return payload.scheduled_at or now
    return now


@router.post("", response_model=JobOut, status_code=201)
async def create_job(
    payload: JobCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    if payload.idempotency_key:
        existing = (
            await db.execute(select(Job).where(Job.idempotency_key == payload.idempotency_key))
        ).scalar_one_or_none()
        if existing:
            return existing

    status_ = JobStatus.SCHEDULED if payload.job_type != JobType.IMMEDIATE else JobStatus.QUEUED
    job = Job(
        queue_id=payload.queue_id,
        job_type=payload.job_type,
        status=status_,
        name=payload.name,
        payload=payload.payload,
        priority=payload.priority,
        run_at=_compute_run_at(payload),
        cron_expression=payload.cron_expression,
        batch_id=payload.batch_id,
        idempotency_key=payload.idempotency_key,
        max_attempts=payload.max_attempts,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)
    return job


@router.post("/batch", response_model=list[JobOut], status_code=201)
async def create_batch(
    payload: BatchJobCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    batch_id = uuid.uuid4()
    created = []
    for item in payload.jobs:
        item.job_type = JobType.BATCH
        item.queue_id = payload.queue_id
        item.batch_id = batch_id
        job = Job(
            queue_id=item.queue_id,
            job_type=JobType.BATCH,
            status=JobStatus.QUEUED,
            name=item.name,
            payload=item.payload,
            priority=item.priority,
            run_at=datetime.now(timezone.utc),
            batch_id=batch_id,
            idempotency_key=item.idempotency_key,
            max_attempts=item.max_attempts,
        )
        db.add(job)
        created.append(job)
    await db.commit()
    for job in created:
        await db.refresh(job)
    return created


@router.get("", response_model=list[JobOut])
async def list_jobs(
    queue_id: uuid.UUID | None = None,
    status: JobStatus | None = None,
    job_type: JobType | None = None,
    batch_id: uuid.UUID | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    query = select(Job)
    if queue_id:
        query = query.where(Job.queue_id == queue_id)
    if status:
        query = query.where(Job.status == status)
    if job_type:
        query = query.where(Job.job_type == job_type)
    if batch_id:
        query = query.where(Job.batch_id == batch_id)
    query = query.order_by(Job.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/{job_id}", response_model=JobOut)
async def get_job(
    job_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    job = (await db.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/{job_id}/executions", response_model=list[JobExecutionOut])
async def get_job_executions(
    job_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    result = await db.execute(
        select(JobExecution).where(JobExecution.job_id == job_id).order_by(JobExecution.attempt_number)
    )
    return result.scalars().all()


@router.get("/{job_id}/logs", response_model=list[JobLogOut])
async def get_job_logs(
    job_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    result = await db.execute(select(JobLog).where(JobLog.job_id == job_id).order_by(JobLog.created_at))
    return result.scalars().all()


@router.post("/{job_id}/cancel", response_model=JobOut)
async def cancel_job(
    job_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    job = (await db.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status in (JobStatus.COMPLETED, JobStatus.DEAD_LETTER, JobStatus.CANCELLED):
        raise HTTPException(status_code=400, detail=f"Cannot cancel a job in status {job.status.value}")
    job.status = JobStatus.CANCELLED
    await db.commit()
    await db.refresh(job)
    return job


@router.post("/{job_id}/retry", response_model=JobOut)
async def retry_job(
    job_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    """Manually re-queue a FAILED / DEAD_LETTER job from the dashboard."""
    job = (await db.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in (JobStatus.FAILED, JobStatus.DEAD_LETTER, JobStatus.CANCELLED):
        raise HTTPException(status_code=400, detail=f"Cannot retry a job in status {job.status.value}")
    job.status = JobStatus.QUEUED
    job.run_at = datetime.now(timezone.utc)
    job.claimed_by_worker_id = None
    job.claimed_at = None
    db.add(JobLog(job_id=job.id, level="info", message="Manually re-queued from dashboard"))
    await db.commit()
    await db.refresh(job)
    return job


# --- Worker-facing endpoints -------------------------------------------------
# These are called by the worker service, not end users / the dashboard.

@router.post("/claim", response_model=list[JobOut])
async def claim_jobs(
    payload: JobClaimRequest, db: AsyncSession = Depends(get_db)
):
    from app.models.queue import Queue

    queue_rows = (
        await db.execute(select(Queue.id).where(Queue.name.in_(payload.queue_names)))
    ).all()
    queue_ids = [row[0] for row in queue_rows]
    if not queue_ids:
        return []
    jobs = await job_service.claim_jobs(db, queue_ids, payload.worker_id, payload.max_jobs)
    return jobs


@router.post("/{job_id}/start", response_model=JobExecutionOut)
async def start_job(
    job_id: uuid.UUID, worker_id: uuid.UUID, db: AsyncSession = Depends(get_db)
):
    job = (await db.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    execution = await job_service.mark_running(db, job, worker_id)
    return execution


@router.post("/{job_id}/complete", response_model=JobOut)
async def complete_job(
    job_id: uuid.UUID, payload: JobCompleteRequest, db: AsyncSession = Depends(get_db)
):
    job = (await db.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    execution = (
        await db.execute(select(JobExecution).where(JobExecution.id == payload.execution_id))
    ).scalar_one_or_none()
    if not job or not execution:
        raise HTTPException(status_code=404, detail="Job or execution not found")
    return await job_service.mark_completed(db, job, execution, payload.result)


@router.post("/{job_id}/fail", response_model=JobOut)
async def fail_job(
    job_id: uuid.UUID, payload: JobFailRequest, db: AsyncSession = Depends(get_db)
):
    job = (await db.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    execution = (
        await db.execute(select(JobExecution).where(JobExecution.id == payload.execution_id))
    ).scalar_one_or_none()
    if not job or not execution:
        raise HTTPException(status_code=404, detail="Job or execution not found")
    return await job_service.mark_failed(db, job, execution, payload.error, payload.retryable)
