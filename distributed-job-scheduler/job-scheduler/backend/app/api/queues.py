import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.enums import JobStatus
from app.models.job import Job
from app.models.queue import Queue
from app.models.user import User
from app.schemas.queue import QueueCreate, QueueOut, QueueStats, QueueUpdate

router = APIRouter(prefix="/api/v1/queues", tags=["queues"])


@router.post("", response_model=QueueOut, status_code=201)
async def create_queue(
    payload: QueueCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    queue = Queue(**payload.model_dump())
    db.add(queue)
    await db.commit()
    await db.refresh(queue)
    return queue


@router.get("", response_model=list[QueueOut])
async def list_queues(
    project_id: uuid.UUID | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    query = select(Queue)
    if project_id:
        query = query.where(Queue.project_id == project_id)
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/{queue_id}", response_model=QueueOut)
async def get_queue(
    queue_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    queue = (await db.execute(select(Queue).where(Queue.id == queue_id))).scalar_one_or_none()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    return queue


@router.patch("/{queue_id}", response_model=QueueOut)
async def update_queue(
    queue_id: uuid.UUID,
    payload: QueueUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    queue = (await db.execute(select(Queue).where(Queue.id == queue_id))).scalar_one_or_none()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(queue, field, value)
    await db.commit()
    await db.refresh(queue)
    return queue


@router.post("/{queue_id}/pause", response_model=QueueOut)
async def pause_queue(
    queue_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    queue = (await db.execute(select(Queue).where(Queue.id == queue_id))).scalar_one_or_none()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    queue.is_paused = True
    await db.commit()
    await db.refresh(queue)
    return queue


@router.post("/{queue_id}/resume", response_model=QueueOut)
async def resume_queue(
    queue_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    queue = (await db.execute(select(Queue).where(Queue.id == queue_id))).scalar_one_or_none()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    queue.is_paused = False
    await db.commit()
    await db.refresh(queue)
    return queue


@router.delete("/{queue_id}", status_code=204)
async def delete_queue(
    queue_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    queue = (await db.execute(select(Queue).where(Queue.id == queue_id))).scalar_one_or_none()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    await db.delete(queue)
    await db.commit()


@router.get("/{queue_id}/stats", response_model=QueueStats)
async def queue_stats(
    queue_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    result = await db.execute(
        select(Job.status, func.count(Job.id)).where(Job.queue_id == queue_id).group_by(Job.status)
    )
    counts = {status.value: 0 for status in JobStatus}
    for status_val, count in result.all():
        counts[status_val.value if hasattr(status_val, "value") else status_val] = count

    return QueueStats(
        queue_id=queue_id,
        queued=counts[JobStatus.QUEUED.value],
        scheduled=counts[JobStatus.SCHEDULED.value],
        claimed=counts[JobStatus.CLAIMED.value],
        running=counts[JobStatus.RUNNING.value],
        completed=counts[JobStatus.COMPLETED.value],
        retrying=counts[JobStatus.RETRYING.value],
        failed=counts[JobStatus.FAILED.value],
        dead_letter=counts[JobStatus.DEAD_LETTER.value],
        cancelled=counts[JobStatus.CANCELLED.value],
    )
