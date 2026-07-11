import pytest

pytestmark = pytest.mark.asyncio


async def test_register_and_login(client):
    resp = await client.post(
        "/api/v1/auth/register",
        json={"email": "a@b.com", "password": "secret123", "full_name": "A B"},
    )
    assert resp.status_code == 201
    assert resp.json()["email"] == "a@b.com"

    resp = await client.post("/api/v1/auth/login", json={"email": "a@b.com", "password": "secret123"})
    assert resp.status_code == 200
    assert "access_token" in resp.json()


async def test_login_wrong_password(client):
    await client.post(
        "/api/v1/auth/register",
        json={"email": "c@d.com", "password": "secret123", "full_name": "C D"},
    )
    resp = await client.post("/api/v1/auth/login", json={"email": "c@d.com", "password": "wrong"})
    assert resp.status_code == 401


async def test_protected_endpoint_requires_token(client):
    resp = await client.get("/api/v1/organizations")
    assert resp.status_code == 401
