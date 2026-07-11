import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.models.enums import ExecutionStatus, JobStatus, JobType, LogLevel


class JobCreate(BaseModel):
    queue_id: uuid.UUID
    name: str
    payload: dict = Field(default_factory=dict)
    job_type: JobType = JobType.IMMEDIATE
    priority: int = 0

    # delayed: run this many seconds from now
    delay_seconds: int | None = Field(default=None, ge=0)
    # scheduled: run at this exact timestamp
    scheduled_at: datetime | None = None
    # recurring: cron expression, e.g. "*/5 * * * *"
    cron_expression: str | None = None
    # batch: client-supplied id to group sibling jobs; if omitted for a BATCH
    # job, the server generates one and returns it.
    batch_id: uuid.UUID | None = None

    idempotency_key: str | None = None
    max_attempts: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_type_fields(self):
        if self.job_type == JobType.DELAYED and self.delay_seconds is None:
            raise ValueError("delay_seconds is required for delayed jobs")
        if self.job_type == JobType.SCHEDULED and self.scheduled_at is None:
            raise ValueError("scheduled_at is required for scheduled jobs")
        if self.job_type == JobType.RECURRING and not self.cron_expression:
            raise ValueError("cron_expression is required for recurring jobs")
        return self


class BatchJobCreate(BaseModel):
    queue_id: uuid.UUID
    jobs: list[JobCreate] = Field(min_length=1)


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    queue_id: uuid.UUID
    job_type: JobType
    status: JobStatus
    name: str
    payload: dict
    priority: int
    run_at: datetime
    cron_expression: str | None
    batch_id: uuid.UUID | None
    attempt_count: int
    max_attempts: int | None
    claimed_by_worker_id: uuid.UUID | None
    claimed_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    last_error: str | None
    created_at: datetime


class JobExecutionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    job_id: uuid.UUID
    worker_id: uuid.UUID | None
    attempt_number: int
    status: ExecutionStatus
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    result: dict | None
    error: str | None


class JobLogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    job_id: uuid.UUID
    execution_id: uuid.UUID | None
    level: LogLevel
    message: str
    created_at: datetime


class JobClaimRequest(BaseModel):
    worker_id: uuid.UUID
    queue_names: list[str]
    max_jobs: int = Field(default=1, ge=1, le=50)


class JobCompleteRequest(BaseModel):
    execution_id: uuid.UUID
    result: dict | None = None


class JobFailRequest(BaseModel):
    execution_id: uuid.UUID
    error: str
    retryable: bool = True
