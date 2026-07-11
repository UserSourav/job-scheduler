import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base
from app.models.enums import ExecutionStatus, JobStatus, JobType, LogLevel
from app.models.mixins import TimestampMixin, UUIDPKMixin


class Job(Base, UUIDPKMixin, TimestampMixin):
    __tablename__ = "jobs"
    __table_args__ = (
        # This composite index is what makes atomic claiming fast: workers filter
        # by queue + status + due-time and order by priority.
        Index("ix_jobs_claim_lookup", "queue_id", "status", "run_at"),
    )

    queue_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("queues.id", ondelete="CASCADE"), index=True
    )
    job_type: Mapped[JobType] = mapped_column(default=JobType.IMMEDIATE, nullable=False)
    status: Mapped[JobStatus] = mapped_column(default=JobStatus.QUEUED, nullable=False, index=True)

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)

    priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # When the job becomes eligible to run. For immediate jobs this is set to
    # created_at; for delayed/scheduled jobs it's set in the future.
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Recurring jobs only: the cron expression and the "template" they spawn from.
    cron_expression: Mapped[str | None] = mapped_column(String(120), nullable=True)
    parent_recurring_job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True
    )

    # Batch jobs only: groups sibling jobs submitted together.
    batch_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)

    # Idempotency: if set, the API rejects/returns-existing on duplicate submission.
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)

    max_attempts: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Overrides the queue's retry policy max_retries if set"
    )
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Atomic-claim bookkeeping.
    claimed_by_worker_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workers.id", ondelete="SET NULL"), nullable=True
    )
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    queue: Mapped["Queue"] = relationship(back_populates="jobs")
    executions: Mapped[list["JobExecution"]] = relationship(
        back_populates="job", cascade="all, delete-orphan", order_by="JobExecution.attempt_number"
    )
    logs: Mapped[list["JobLog"]] = relationship(back_populates="job", cascade="all, delete-orphan")


class JobExecution(Base, UUIDPKMixin, TimestampMixin):
    """One row per attempt. Gives full retry history for a job."""

    __tablename__ = "job_executions"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    worker_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workers.id", ondelete="SET NULL"), nullable=True
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[ExecutionStatus] = mapped_column(default=ExecutionStatus.RUNNING, nullable=False)

    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)

    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped["Job"] = relationship(back_populates="executions")


class JobLog(Base, UUIDPKMixin, TimestampMixin):
    __tablename__ = "job_logs"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    execution_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job_executions.id", ondelete="CASCADE"), nullable=True
    )
    level: Mapped[LogLevel] = mapped_column(default=LogLevel.INFO, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)

    job: Mapped["Job"] = relationship(back_populates="logs")


class DeadLetterEntry(Base, UUIDPKMixin, TimestampMixin):
    """
    Permanent record of a job that exhausted all retries. Kept separate from
    `jobs` so the hot job table stays small and DLQ history is easy to audit
    or replay from.
    """

    __tablename__ = "dead_letter_entries"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    queue_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("queues.id", ondelete="CASCADE"), index=True
    )
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    original_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False)
    moved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
