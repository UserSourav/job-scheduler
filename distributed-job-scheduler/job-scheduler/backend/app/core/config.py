"""
Centralized application configuration.

All settings are read from environment variables (or a .env file) so the same
codebase can run in local dev, CI, and production without code changes.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- General ---
    APP_NAME: str = "Distributed Job Scheduler"
    ENV: str = "development"
    DEBUG: bool = True

    # --- Database ---
    # Example (Postgres): postgresql+asyncpg://user:pass@localhost:5432/jobscheduler
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobscheduler"

    # --- Auth ---
    SECRET_KEY: str = "change-this-secret-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24h

    # --- Worker / Scheduler tuning ---
    JOB_CLAIM_BATCH_SIZE: int = 5
    WORKER_POLL_INTERVAL_SECONDS: float = 1.0
    WORKER_HEARTBEAT_INTERVAL_SECONDS: float = 5.0
    WORKER_STALE_HEARTBEAT_SECONDS: int = 30  # after this, a claimed job is reclaimable
    SCHEDULER_POLL_INTERVAL_SECONDS: float = 2.0

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
