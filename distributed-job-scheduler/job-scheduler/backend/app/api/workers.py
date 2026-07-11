import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.enums import WorkerStatus
from app.models.user import User
from app.models.worker import Worker, WorkerHeartbeat
from app.schemas.worker import WorkerHeartbeatIn, WorkerOut, WorkerRegister

router = APIRouter(prefix="/api/v1/workers", tags=["workers"])


@router.post("/register", response_model=WorkerOut, status_code=201)
async def register_worker(payload: WorkerRegister, db: AsyncSession = Depends(get_db)):
    worker = Worker(
        hostname=payload.hostname,
        queue_names=payload.queue_names,
        concurrency=payload.concurrency,
        status=WorkerStatus.ONLINE,
        last_heartbeat_at=datetime.now(timezone.utc),
    )
    db.add(worker)
    await db.commit()
    await db.refresh(worker)
    return worker


@router.post("/{worker_id}/heartbeat", response_model=WorkerOut)
async def heartbeat(worker_id: uuid.UUID, payload: WorkerHeartbeatIn, db: AsyncSession = Depends(get_db)):
    worker = (await db.execute(select(Worker).where(Worker.id == worker_id))).scalar_one_or_none()
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    worker.last_heartbeat_at = datetime.now(timezone.utc)
    worker.active_job_count = payload.active_job_count
    if worker.status != WorkerStatus.DRAINING:
        worker.status = WorkerStatus.ONLINE
    db.add(
        WorkerHeartbeat(
            worker_id=worker.id,
            active_job_count=payload.active_job_count,
            cpu_percent=payload.cpu_percent,
            memory_mb=payload.memory_mb,
        )
    )
    await db.commit()
    await db.refresh(worker)
    return worker


@router.post("/{worker_id}/drain", response_model=WorkerOut)
async def drain_worker(worker_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Mark a worker as draining so it finishes in-flight jobs but claims no new ones.
    Called either by the worker itself during graceful shutdown, or by an admin."""
    worker = (await db.execute(select(Worker).where(Worker.id == worker_id))).scalar_one_or_none()
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    worker.status = WorkerStatus.DRAINING
    await db.commit()
    await db.refresh(worker)
    return worker


@router.post("/{worker_id}/offline", response_model=WorkerOut)
async def mark_offline(worker_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    worker = (await db.execute(select(Worker).where(Worker.id == worker_id))).scalar_one_or_none()
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    worker.status = WorkerStatus.OFFLINE
    await db.commit()
    await db.refresh(worker)
    return worker


@router.get("", response_model=list[WorkerOut])
async def list_workers(db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    result = await db.execute(select(Worker).order_by(Worker.created_at.desc()))
    return result.scalars().all()
