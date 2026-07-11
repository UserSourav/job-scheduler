from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api import auth, dashboard, jobs, projects, queues, retry_policies, workers
from app.core.config import settings
from app.core.database import Base, engine
import app.models  # noqa: F401  (ensures all models are registered on Base)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # For a real production rollout, use Alembic migrations instead of
    # create_all (see alembic/ and docs/DESIGN_DECISIONS.md). create_all is
    # used here so the project runs with zero migration setup out of the box.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(projects.router)
app.include_router(queues.router)
app.include_router(retry_policies.router)
app.include_router(jobs.router)
app.include_router(workers.router)
app.include_router(dashboard.router)


@app.get("/api/v1/health")
async def health():
    return {"status": "ok", "service": settings.APP_NAME}
