# Crobat design notes

## The one-table idea

Most schedulers keep the schedule in a database and the work in a
separate queue/broker. Every hand-off between two systems is a place to
lose or duplicate work: write the row, crash before publishing, and the
job never runs; publish, crash before marking it published, and it runs
twice. Crobat removes the hand-off. The MySQL `jobs` table *is* the
queue, and the scheduler's materialization is a single transaction:

1. lock due `tasks` rows (`ORDER BY id ... FOR UPDATE`),
2. `INSERT` one `pending` row per due task into `jobs`,
3. advance each task's `next_run_at` (or clear it for one-shot tasks),
4. commit.

Either the whole step happened or none of it did. The schedule and the
queue cannot disagree, because they are the same write.

## At-least-once + fencing

Executors are unreliable by assumption: they stall, crash, and lose
leases. So:

- A claim sets `worker_id`, `lease_expires_at`, and increments
  `attempt`. The incremented attempt is a **fencing token**.
- The Reconciler returns expired-lease jobs to `pending` (or `dead` if
  the attempt budget is spent). A rescued job is claimed again, with a
  higher attempt.
- Results are saved with a guarded update:
  `UPDATE ... WHERE status='running' AND worker_id=? AND attempt=?`.
  A stale executor that wakes up after losing its lease matches zero
  rows; its result is dropped.
- Because a call may therefore happen more than once, every delivery
  carries `Idempotency-Key: <job id>` so the receiving endpoint can
  dedupe, plus `X-Scheduled-At` and `X-Attempt` for observability.

## Fairness

Claiming orders by priority first, then interleaves tenants round-robin
(the SQL uses `ROW_NUMBER() OVER (PARTITION BY tenant_id ...)` and
takes rn 1 of every tenant, then rn 2, ...). One tenant bursting 30,000
jobs gets its share of each claim batch, not the whole batch.

## SSRF guard

Tenants choose the URLs; the executor is a server making requests on
their behalf — a textbook SSRF surface. The guard validates the scheme
(http/https), restricts ports to 80/443, resolves the host and rejects
it if *any* address is non-public (private, loopback, link-local —
including the cloud metadata address 169.254.169.254 — CGNAT, multicast,
reserved). Redirects are never followed. The dialer re-resolves at
connect time and dials the validated IP directly, closing the
DNS-rebinding gap between check and connect.

## Secrets

Headers named in a task's `secret_headers` are encrypted with
AES-256-GCM (`enc:v1:` prefix) before they are stored, copied into job
rows only as ciphertext, decrypted by the executor immediately before
the call, and never returned by the APIs (the Task Service renders them
as `[encrypted]`).

## History: hot table + Cassandra

Finished jobs would grow the hot queue table forever, so the Archiver
moves them to Cassandra: append-only `job_history`, partitioned by
(tenant, task, month), 30-day TTL. The write order is strict — history
first, delete second — and both the Cassandra write (primary key
includes job id) and the Job Service's merged read (dedupe by job id)
are idempotent, so a crash mid-archival is a no-op on retry. The Job
Service merges hot + archived rows, so history reads are correct at
every instant of the move.

## Time and cron

Cron expressions are evaluated in the task's IANA timezone with
robfig/cron: "0 9 * * *" in America/New_York means 9am local across DST
transitions. Advancement is computed from the *scheduled* time, not the
materialization time, so a late tick does not drift the schedule; if
the service was down long enough that the next computed run is already
past, it skips ahead instead of bursting missed runs.
