import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.models.enums import RetryStrategy


class RetryPolicyCreate(BaseModel):
    name: str
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    max_retries: int = Field(default=3, ge=0, le=50)
    base_delay_seconds: int = Field(default=5, ge=1)
    max_delay_seconds: int = Field(default=3600, ge=1)


class RetryPolicyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    name: str
    strategy: RetryStrategy
    max_retries: int
    base_delay_seconds: int
    max_delay_seconds: int


class QueueCreate(BaseModel):
    project_id: uuid.UUID
    name: str
    priority: int = 0
    concurrency_limit: int = Field(default=10, ge=1)
    retry_policy_id: uuid.UUID | None = None


class QueueUpdate(BaseModel):
    priority: int | None = None
    concurrency_limit: int | None = Field(default=None, ge=1)
    is_paused: bool | None = None
    retry_policy_id: uuid.UUID | None = None


class QueueOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    priority: int
    concurrency_limit: int
    is_paused: bool
    retry_policy_id: uuid.UUID | None
    created_at: datetime


class QueueStats(BaseModel):
    queue_id: uuid.UUID
    queued: int
    scheduled: int
    claimed: int
    running: int
    completed: int
    retrying: int
    failed: int
    dead_letter: int
    cancelled: int
