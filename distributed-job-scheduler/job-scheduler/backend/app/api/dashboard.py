import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.enums import JobStatus, WorkerStatus
from app.models.job import DeadLetterEntry, Job
from app.models.queue import Queue
from app.models.user import User
from app.models.worker import Worker
from app.schemas.job import JobOut

router = APIRouter(prefix="/api/v1/dashboard", tags=["dashboard"])


@router.get("/overview")
async def overview(
    project_id: uuid.UUID | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """High-level counts used by the dashboard's top summary cards."""
    job_query = select(Job.status, func.count(Job.id))
    if project_id:
        job_query = job_query.join(Queue, Queue.id == Job.queue_id).where(Queue.project_id == project_id)
    job_query = job_query.group_by(Job.status)
    job_counts_rows = (await db.execute(job_query)).all()
    job_counts = {s.value: 0 for s in JobStatus}
    for status_val, count in job_counts_rows:
        job_counts[status_val.value] = count

    worker_query = select(Worker.status, func.count(Worker.id)).group_by(Worker.status)
    worker_rows = (await db.execute(worker_query)).all()
    worker_counts = {s.value: 0 for s in WorkerStatus}
    for status_val, count in worker_rows:
        worker_counts[status_val.value] = count

    queue_query = select(func.count(Queue.id))
    if project_id:
        queue_query = queue_query.where(Queue.project_id == project_id)
    total_queues = (await db.execute(queue_query)).scalar_one()

    return {
        "job_counts": job_counts,
        "worker_counts": worker_counts,
        "total_queues": total_queues,
        "total_jobs": sum(job_counts.values()),
    }


@router.get("/throughput")
async def throughput(
    hours: int = 24,
    project_id: uuid.UUID | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Completed vs failed job counts bucketed by hour, for the last `hours`
    hours -- powers the dashboard's throughput chart.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    bucket = func.date_trunc("hour", Job.completed_at)
    query = (
        select(bucket.label("bucket"), Job.status, func.count(Job.id))
        .where(Job.completed_at.is_not(None), Job.completed_at >= since)
        .group_by(bucket, Job.status)
        .order_by(bucket)
    )
    if project_id:
        query = query.join(Queue, Queue.id == Job.queue_id).where(Queue.project_id == project_id)
    rows = (await db.execute(query)).all()

    buckets: dict[str, dict[str, int]] = {}
    for bucket_ts, status_val, count in rows:
        key = bucket_ts.isoformat()
        buckets.setdefault(key, {"completed": 0, "dead_letter": 0})
        if status_val == JobStatus.COMPLETED:
            buckets[key]["completed"] = count
        elif status_val == JobStatus.DEAD_LETTER:
            buckets[key]["dead_letter"] = count
    return [{"bucket": k, **v} for k, v in sorted(buckets.items())]


@router.get("/dead-letter-queue", response_model=list[JobOut])
async def dead_letter_queue(
    queue_id: uuid.UUID | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    query = select(Job).where(Job.status == JobStatus.DEAD_LETTER)
    if queue_id:
        query = query.where(Job.queue_id == queue_id)
    query = query.order_by(Job.completed_at.desc().nullslast())
    result = await db.execute(query)
    return result.scalars().all()
