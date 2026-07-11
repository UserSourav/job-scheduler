import pytest

pytestmark = pytest.mark.asyncio


async def _setup_project(client, auth_headers):
    org = (await client.post(
        "/api/v1/organizations", json={"name": "Org", "slug": "org1"}, headers=auth_headers
    )).json()
    project = (await client.post(
        "/api/v1/projects",
        json={"organization_id": org["id"], "name": "Proj", "slug": "proj1"},
        headers=auth_headers,
    )).json()
    return org, project


async def test_create_org_project(client, auth_headers):
    org, project = await _setup_project(client, auth_headers)
    assert project["organization_id"] == org["id"]
    assert project["api_key"]


async def test_create_queue_with_retry_policy(client, auth_headers):
    _, project = await _setup_project(client, auth_headers)
    policy = (await client.post(
        "/api/v1/retry-policies",
        json={"name": "p", "strategy": "exponential", "max_retries": 3, "base_delay_seconds": 2, "max_delay_seconds": 60},
        headers=auth_headers,
    )).json()
    queue = (await client.post(
        "/api/v1/queues",
        json={
            "project_id": project["id"], "name": "q1", "priority": 1,
            "concurrency_limit": 2, "retry_policy_id": policy["id"],
        },
        headers=auth_headers,
    )).json()
    assert queue["name"] == "q1"
    assert queue["retry_policy_id"] == policy["id"]


async def test_queue_pause_resume(client, auth_headers):
    _, project = await _setup_project(client, auth_headers)
    queue = (await client.post(
        "/api/v1/queues", json={"project_id": project["id"], "name": "q2"}, headers=auth_headers
    )).json()
    resp = await client.post(f"/api/v1/queues/{queue['id']}/pause", headers=auth_headers)
    assert resp.json()["is_paused"] is True
    resp = await client.post(f"/api/v1/queues/{queue['id']}/resume", headers=auth_headers)
    assert resp.json()["is_paused"] is False
