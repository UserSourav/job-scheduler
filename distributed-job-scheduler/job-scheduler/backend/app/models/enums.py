import enum


class JobStatus(str, enum.Enum):
    QUEUED = "queued"          # ready to be claimed (immediate jobs)
    SCHEDULED = "scheduled"    # has a future run_at, not yet due
    CLAIMED = "claimed"        # a worker has atomically claimed it
    RUNNING = "running"        # worker is actively executing it
    COMPLETED = "completed"    # terminal: success
    RETRYING = "retrying"      # failed, waiting for next retry attempt
    FAILED = "failed"          # terminal: failed, retries exhausted, not yet DLQ'd
    DEAD_LETTER = "dead_letter"  # terminal: moved to Dead Letter Queue
    CANCELLED = "cancelled"    # terminal: cancelled by a user


class JobType(str, enum.Enum):
    IMMEDIATE = "immediate"
    DELAYED = "delayed"
    SCHEDULED = "scheduled"
    RECURRING = "recurring"
    BATCH = "batch"


class RetryStrategy(str, enum.Enum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class WorkerStatus(str, enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    DRAINING = "draining"  # graceful shutdown in progress, not accepting new jobs


class ExecutionStatus(str, enum.Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"


class LogLevel(str, enum.Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class OrgRole(str, enum.Enum):
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
