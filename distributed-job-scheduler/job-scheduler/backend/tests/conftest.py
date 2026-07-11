import asyncio
import os
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/jobscheduler_test"
)

from app.core.database import Base, engine  # noqa: E402
from app import models as _models  # noqa: E402,F401  (registers all models on Base)
from app.main import app  # noqa: E402


@pytest_asyncio.fixture(autouse=True, scope="function")
async def _reset_db():
    # Each pytest-asyncio test gets its own event loop by default; asyncpg
    # connections are bound to the loop they were created on, so the engine's
    # pool must be disposed first or later tests fail with
    # "Task <Task ...> attached to a different loop".
    await engine.dispose()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def auth_headers(client):
    email = f"user_{uuid.uuid4().hex[:8]}@test.com"
    await client.post(
        "/api/v1/auth/register",
        json={"email": email, "password": "password123", "full_name": "Test User"},
    )
    resp = await client.post("/api/v1/auth/login", json={"email": email, "password": "password123"})
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}
