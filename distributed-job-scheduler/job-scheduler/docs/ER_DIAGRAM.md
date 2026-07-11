# Database Design (ER Diagram)

```mermaid
erDiagram
    USERS ||--o{ ORGANIZATION_MEMBERS : "has"
    ORGANIZATIONS ||--o{ ORGANIZATION_MEMBERS : "has"
    ORGANIZATIONS ||--o{ PROJECTS : "owns"
    PROJECTS ||--o{ QUEUES : "owns"
    RETRY_POLICIES ||--o{ QUEUES : "default policy for"
    QUEUES ||--o{ JOBS : "contains"
    JOBS ||--o{ JOB_EXECUTIONS : "has attempts"
    JOBS ||--o{ JOB_LOGS : "has log entries"
    JOBS ||--o{ DEAD_LETTER_ENTRIES : "moved to (on final failure)"
    JOBS }o--o| JOBS : "recurring parent -> spawned child"
    WORKERS ||--o{ JOB_EXECUTIONS : "executed by"
    WORKERS ||--o{ WORKER_HEARTBEATS : "reports"
    WORKERS ||--o{ JOBS : "currently claimed by"

    USERS {
        uuid id PK
        string email UK
        string hashed_password
        string full_name
        bool is_active
    }

    ORGANIZATIONS {
        uuid id PK
        string name
        string slug UK
    }

    ORGANIZATION_MEMBERS {
        uuid id PK
        uuid organization_id FK
        uuid user_id FK
        enum role "owner/admin/member"
    }

    PROJECTS {
        uuid id PK
        uuid organization_id FK
        string name
        string slug
        string api_key UK
    }

    RETRY_POLICIES {
        uuid id PK
        string name
        enum strategy "fixed/linear/exponential"
        int max_retries
        int base_delay_seconds
        int max_delay_seconds
    }

    QUEUES {
        uuid id PK
        uuid project_id FK
        string name
        int priority
        int concurrency_limit
        bool is_paused
        uuid retry_policy_id FK
    }

    JOBS {
        uuid id PK
        uuid queue_id FK
        enum job_type "immediate/delayed/scheduled/recurring/batch"
        enum status "queued/scheduled/claimed/running/completed/retrying/failed/dead_letter/cancelled"
        string name
        json payload
        int priority
        timestamp run_at
        string cron_expression
        uuid parent_recurring_job_id FK
        uuid batch_id
        string idempotency_key
        int max_attempts
        int attempt_count
        uuid claimed_by_worker_id FK
        timestamp claimed_at
        timestamp started_at
        timestamp completed_at
        text last_error
    }

    JOB_EXECUTIONS {
        uuid id PK
        uuid job_id FK
        uuid worker_id FK
        int attempt_number
        enum status "running/succeeded/failed/timed_out"
        timestamp started_at
        timestamp finished_at
        int duration_ms
        json result
        text error
    }

    JOB_LOGS {
        uuid id PK
        uuid job_id FK
        uuid execution_id FK
        enum level "debug/info/warning/error"
        text message
    }

    DEAD_LETTER_ENTRIES {
        uuid id PK
        uuid job_id FK
        uuid queue_id FK
        text reason
        json original_payload
        int attempt_count
        timestamp moved_at
    }

    WORKERS {
        uuid id PK
        string hostname
        string_array queue_names
        enum status "online/offline/draining"
        int concurrency
        int active_job_count
        timestamp last_heartbeat_at
    }

    WORKER_HEARTBEATS {
        uuid id PK
        uuid worker_id FK
        int active_job_count
        float cpu_percent
        float memory_mb
    }
```

## Design notes

**Primary keys.** All tables use UUIDv4 primary keys instead of auto-increment
integers. This lets any component (API, worker, dashboard) generate an ID
client-side before the row exists, avoids leaking row counts, and makes
merging data across environments (e.g. replaying a DLQ entry from staging
into prod) safe since IDs can never collide.

**Foreign keys and cascade behavior.**
- `organization_members`, `projects`, `queues`, `jobs`, `job_executions`,
  `job_logs`, `dead_letter_entries` all cascade-delete with their parent
  (`ondelete="CASCADE"`) — deleting an organization cleans up everything
  under it, which matches "delete my account" / "delete my project" as a
  single, safe operation with no orphaned rows.
- `jobs.claimed_by_worker_id` and `job_executions.worker_id` use
  `ondelete="SET NULL"` instead of cascade — if a worker row is deleted
  (e.g. cleanup of old worker records), the historical fact that "this job
  was once claimed by worker X" shouldn't be destroyed, but the job/execution
  itself must survive independently of worker bookkeeping.
- `queues.retry_policy_id` is `SET NULL` for the same reason: deleting a
  retry policy shouldn't cascade-delete every queue that referenced it.

**Indexes.** The single most important index in the schema is the composite
`ix_jobs_claim_lookup` on `(queue_id, status, run_at)`. Every atomic-claim
query filters on exactly these three columns (`WHERE queue_id = ? AND status
IN (...) AND run_at <= now()`), so this index is what keeps claiming O(log n)
instead of a sequential scan as the `jobs` table grows into the millions of
rows. Secondary indexes exist on every foreign key (for join performance) and
on `idempotency_key` (for the duplicate-submission check on the write path).

**Normalization.** The schema is in 3NF: `retry_policies` is extracted into
its own table rather than duplicating strategy/backoff columns on every
`queue` row, because policies are meant to be reused across many queues
(e.g. one "aggressive-retry" policy shared by all payment-related queues).
`job_executions` is a separate table from `jobs` (1-to-many) rather than a
`retry_history` JSON blob on `jobs`, so that individual attempts can be
queried, filtered, and joined efficiently (e.g. "average duration of
successful executions in the last 24h").

**Why `job_logs` is separate from `job_executions`.** An execution row is
one attempt's structured outcome (status, duration, result/error) — cheap,
bounded, always exactly `attempt_count` rows per job. `job_logs` is an
unbounded, append-only event stream (arbitrary text messages at info/warn/
error level) that a real job handler could also write to mid-execution for
progress reporting. Keeping them separate means the hot `job_executions`
table (queried constantly for retry math and dashboards) never grows
unpredictably large the way a log stream would.

**Performance considerations at scale.** The two things worth calling out
for a production deployment beyond what's implemented here:
1. **Partitioning `jobs` / `job_logs` by month** once volume passes tens of
   millions of rows — completed/dead-lettered jobs older than N days are
   rarely queried and can move to cold storage.
2. **A covering index or materialized view for `dashboard/throughput`** —
   the current implementation runs a live `GROUP BY date_trunc('hour', ...)`
   query, which is fine at moderate volume but would benefit from a
   pre-aggregated rollup table if dashboards are polled frequently at scale.
