import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.models.enums import WorkerStatus


class WorkerRegister(BaseModel):
    hostname: str
    queue_names: list[str]
    concurrency: int = Field(default=5, ge=1)


class WorkerOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    hostname: str
    queue_names: list[str]
    status: WorkerStatus
    concurrency: int
    active_job_count: int
    last_heartbeat_at: datetime | None
    created_at: datetime


class WorkerHeartbeatIn(BaseModel):
    active_job_count: int
    cpu_percent: float | None = None
    memory_mb: float | None = None
