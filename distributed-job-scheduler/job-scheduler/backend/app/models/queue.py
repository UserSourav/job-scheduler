import uuid

from sqlalchemy import Boolean, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base
from app.models.enums import RetryStrategy
from app.models.mixins import TimestampMixin, UUIDPKMixin


class RetryPolicy(Base, UUIDPKMixin, TimestampMixin):
    """
    A named, reusable retry configuration. A queue has a default retry policy;
    individual jobs may override it.
    """

    __tablename__ = "retry_policies"

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    strategy: Mapped[RetryStrategy] = mapped_column(default=RetryStrategy.EXPONENTIAL, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    base_delay_seconds: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    max_delay_seconds: Mapped[int] = mapped_column(Integer, default=3600, nullable=False)

    def compute_delay_seconds(self, attempt_number: int) -> int:
        """attempt_number is 1-indexed (the retry attempt about to be made)."""
        if self.strategy == RetryStrategy.FIXED:
            delay = self.base_delay_seconds
        elif self.strategy == RetryStrategy.LINEAR:
            delay = self.base_delay_seconds * attempt_number
        else:  # EXPONENTIAL
            delay = self.base_delay_seconds * (2 ** (attempt_number - 1))
        return min(delay, self.max_delay_seconds)


class Queue(Base, UUIDPKMixin, TimestampMixin):
    __tablename__ = "queues"
    __table_args__ = (UniqueConstraint("project_id", "name", name="uq_queue_project_name"),)

    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # Higher number = higher priority when workers pick a queue to poll.
    priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # Max number of jobs from this queue that may run concurrently, cluster-wide.
    concurrency_limit: Mapped[int] = mapped_column(Integer, default=10, nullable=False)
    is_paused: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    retry_policy_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("retry_policies.id", ondelete="SET NULL"), nullable=True
    )

    project: Mapped["Project"] = relationship(back_populates="queues")
    retry_policy: Mapped["RetryPolicy | None"] = relationship()
    jobs: Mapped[list["Job"]] = relationship(back_populates="queue", cascade="all, delete-orphan")
