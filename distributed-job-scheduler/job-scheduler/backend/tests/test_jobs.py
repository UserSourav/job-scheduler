import asyncio
import uuid

import pytest

pytestmark = pytest.mark.asyncio



async def _register_worker(client, queue_name):
    resp = await client.post(
        "/api/v1/workers/register",
        json={"hostname": "test-host", "queue_names": [queue_name], "concurrency": 20},
    )
    return resp.json()["id"]

async def _setup_queue(client, auth_headers, max_retries=2, concurrency=5):
    org = (await client.post(
        "/api/v1/organizations", json={"name": "Org", "slug": f"org-{uuid.uuid4().hex[:6]}"}, headers=auth_headers
    )).json()
    project = (await client.post(
        "/api/v1/projects",
        json={"organization_id": org["id"], "name": "Proj", "slug": f"proj-{uuid.uuid4().hex[:6]}"},
        headers=auth_headers,
    )).json()
    policy = (await client.post(
        "/api/v1/retry-policies",
        json={"name": "p", "strategy": "fixed", "max_retries": max_retries, "base_delay_seconds": 1, "max_delay_seconds": 5},
        headers=auth_headers,
    )).json()
    queue = (await client.post(
        "/api/v1/queues",
        json={
            "project_id": project["id"], "name": f"q-{uuid.uuid4().hex[:6]}",
            "concurrency_limit": concurrency, "retry_policy_id": policy["id"],
        },
        headers=auth_headers,
    )).json()
    return queue


async def test_create_immediate_job_is_queued(client, auth_headers):
    queue = await _setup_queue(client, auth_headers)
    job = (await client.post(
        "/api/v1/jobs",
        json={"queue_id": queue["id"], "name": "j1", "job_type": "immediate", "payload": {"x": 1}},
        headers=auth_headers,
    )).json()
    assert job["status"] == "queued"


async def test_delayed_job_requires_delay_seconds(client, auth_headers):
    queue = await _setup_queue(client, auth_headers)
    resp = await client.post(
        "/api/v1/jobs",
        json={"queue_id": queue["id"], "name": "j2", "job_type": "delayed", "payload": {}},
        headers=auth_headers,
    )
    assert resp.status_code == 422


async def test_full_lifecycle_success(client, auth_headers):
    queue = await _setup_queue(client, auth_headers)
    job = (await client.post(
        "/api/v1/jobs",
        json={"queue_id": queue["id"], "name": "j3", "job_type": "immediate", "payload": {}},
        headers=auth_headers,
    )).json()

    worker_id = await _register_worker(client, queue["name"])
    claimed = (await client.post(
        "/api/v1/jobs/claim",
        json={"worker_id": worker_id, "queue_names": [queue["name"]], "max_jobs": 5},
    )).json()
    assert len(claimed) == 1
    assert claimed[0]["status"] == "claimed"

    execution = (await client.post(
        f"/api/v1/jobs/{job['id']}/start", params={"worker_id": worker_id}
    )).json()
    assert execution["status"] == "running"

    completed = (await client.post(
        f"/api/v1/jobs/{job['id']}/complete",
        json={"execution_id": execution["id"], "result": {"ok": True}},
    )).json()
    assert completed["status"] == "completed"


async def test_retry_then_dead_letter(client, auth_headers):
    """max_retries=2 on the queue's policy: job should retry twice then DLQ."""
    queue = await _setup_queue(client, auth_headers, max_retries=2)
    job = (await client.post(
        "/api/v1/jobs",
        json={"queue_id": queue["id"], "name": "j4", "job_type": "immediate", "payload": {}},
        headers=auth_headers,
    )).json()
    worker_id = await _register_worker(client, queue["name"])

    for expected_status in ["retrying", "retrying", "dead_letter"]:
        claimed = (await client.post(
            "/api/v1/jobs/claim",
            json={"worker_id": worker_id, "queue_names": [queue["name"]], "max_jobs": 5},
        )).json()
        assert len(claimed) == 1, f"expected a claimable job before reaching {expected_status}"
        execution = (await client.post(
            f"/api/v1/jobs/{job['id']}/start", params={"worker_id": worker_id}
        )).json()
        result = (await client.post(
            f"/api/v1/jobs/{job['id']}/fail",
            json={"execution_id": execution["id"], "error": "boom", "retryable": True},
        )).json()
        assert result["status"] == expected_status
        if result["status"] == "retrying":
            # retry delay is 1s (fixed strategy); wait for it to become due
            await asyncio.sleep(1.1)

    dlq = (await client.get("/api/v1/dashboard/dead-letter-queue", headers=auth_headers)).json()
    assert any(j["id"] == job["id"] for j in dlq)


async def test_concurrent_claims_never_double_assign(client, auth_headers):
    """
    The core correctness property of the whole system: with N jobs and many
    concurrent claimants, every job is claimed exactly once, never twice.
    """
    queue = await _setup_queue(client, auth_headers, concurrency=50)
    job_ids = []
    for i in range(10):
        job = (await client.post(
            "/api/v1/jobs",
            json={"queue_id": queue["id"], "name": f"job-{i}", "job_type": "immediate", "payload": {}},
            headers=auth_headers,
        )).json()
        job_ids.append(job["id"])

    async def claim_once(worker_id):
        resp = await client.post(
            "/api/v1/jobs/claim",
            json={"worker_id": worker_id, "queue_names": [queue["name"]], "max_jobs": 3},
        )
        return resp.json()

    worker_ids = [await _register_worker(client, queue["name"]) for _ in range(8)]
    results = await asyncio.gather(*(claim_once(w) for w in worker_ids))

    claimed_ids = [job["id"] for batch in results for job in batch]
    assert len(claimed_ids) == len(set(claimed_ids)), "a job was claimed by more than one worker"
    assert len(claimed_ids) == 10, "all jobs should have been claimed exactly once across workers"


async def test_queue_concurrency_limit_is_respected(client, auth_headers):
    queue = await _setup_queue(client, auth_headers, concurrency=2)
    for i in range(5):
        await client.post(
            "/api/v1/jobs",
            json={"queue_id": queue["id"], "name": f"lim-{i}", "job_type": "immediate", "payload": {}},
            headers=auth_headers,
        )
    claimed = (await client.post(
        "/api/v1/jobs/claim",
        json={"worker_id": await _register_worker(client, queue["name"]), "queue_names": [queue["name"]], "max_jobs": 10},
    )).json()
    assert len(claimed) == 2, "should not exceed the queue's concurrency_limit"


async def test_paused_queue_yields_no_claims(client, auth_headers):
    queue = await _setup_queue(client, auth_headers)
    await client.post(
        "/api/v1/jobs",
        json={"queue_id": queue["id"], "name": "paused-job", "job_type": "immediate", "payload": {}},
        headers=auth_headers,
    )
    await client.post(f"/api/v1/queues/{queue['id']}/pause", headers=auth_headers)
    claimed = (await client.post(
        "/api/v1/jobs/claim",
        json={"worker_id": await _register_worker(client, queue["name"]), "queue_names": [queue["name"]], "max_jobs": 10},
    )).json()
    assert claimed == []


async def test_cancel_job(client, auth_headers):
    queue = await _setup_queue(client, auth_headers)
    job = (await client.post(
        "/api/v1/jobs",
        json={"queue_id": queue["id"], "name": "cancel-me", "job_type": "immediate", "payload": {}},
        headers=auth_headers,
    )).json()
    resp = await client.post(f"/api/v1/jobs/{job['id']}/cancel", headers=auth_headers)
    assert resp.json()["status"] == "cancelled"
    # cancelling twice should fail
    resp2 = await client.post(f"/api/v1/jobs/{job['id']}/cancel", headers=auth_headers)
    assert resp2.status_code == 400


async def test_batch_job_creation(client, auth_headers):
    queue = await _setup_queue(client, auth_headers)
    resp = await client.post(
        "/api/v1/jobs/batch",
        json={
            "queue_id": queue["id"],
            "jobs": [
                {"queue_id": queue["id"], "name": "b1", "job_type": "immediate", "payload": {}},
                {"queue_id": queue["id"], "name": "b2", "job_type": "immediate", "payload": {}},
            ],
        },
        headers=auth_headers,
    )
    jobs = resp.json()
    assert len(jobs) == 2
    assert jobs[0]["batch_id"] == jobs[1]["batch_id"]


async def test_idempotency_key_prevents_duplicates(client, auth_headers):
    queue = await _setup_queue(client, auth_headers)
    payload = {
        "queue_id": queue["id"], "name": "idem", "job_type": "immediate",
        "payload": {}, "idempotency_key": "same-key-123",
    }
    first = (await client.post("/api/v1/jobs", json=payload, headers=auth_headers)).json()
    second = (await client.post("/api/v1/jobs", json=payload, headers=auth_headers)).json()
    assert first["id"] == second["id"]
