import uuid
from datetime import datetime

from sqlalchemy import ARRAY, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base
from app.models.enums import WorkerStatus
from app.models.mixins import TimestampMixin, UUIDPKMixin


class Worker(Base, UUIDPKMixin, TimestampMixin):
    __tablename__ = "workers"

    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    # Which queue names this worker instance polls (by name, not FK, so a worker
    # can be started before queues exist / span queues across projects).
    queue_names: Mapped[list[str]] = mapped_column(ARRAY(String), default=list, nullable=False)
    status: Mapped[WorkerStatus] = mapped_column(default=WorkerStatus.ONLINE, nullable=False, index=True)
    concurrency: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    active_job_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    heartbeats: Mapped[list["WorkerHeartbeat"]] = relationship(
        back_populates="worker", cascade="all, delete-orphan"
    )


class WorkerHeartbeat(Base, UUIDPKMixin, TimestampMixin):
    """Time-series of heartbeats; lets the dashboard chart worker health over time."""

    __tablename__ = "worker_heartbeats"

    worker_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workers.id", ondelete="CASCADE"), nullable=False, index=True
    )
    active_job_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cpu_percent: Mapped[float | None] = mapped_column(nullable=True)
    memory_mb: Mapped[float | None] = mapped_column(nullable=True)

    worker: Mapped["Worker"] = relationship(back_populates="heartbeats")
