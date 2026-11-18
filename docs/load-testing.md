# Load testing Crobat

## The scenario

`loadtest/k6-schedule-30k.js` schedules **30,000 jobs for the same
second** — the worst case for a job scheduler, because it concentrates
all of the system's hard problems (claim contention, lock ordering,
fairness, duplicate delivery) into a single instant:

```bash
docker compose up --build -d
k6 run -e TOKEN=<tenant jwt> loadtest/k6-schedule-30k.js
```

Exact counts during/after a run:

```sql
SELECT status, COUNT(*) FROM jobs GROUP BY status;
SELECT COUNT(*) FROM tasks WHERE idempotency_key LIKE 'burst-%';
```

Success criteria: zero lost jobs (every created job reaches `succeeded`
or `dead`), no executor deadlocks in `SHOW ENGINE INNODB STATUS`, and
the burst drains without one tenant's jobs starving another's.

> **Honesty note.** This environment has no Docker and no k6, so the
> 30,000-job run has **not** been executed against this code and no
> measured drain times are claimed anywhere in this repo. What follows
> is design analysis: the two classic failure modes a burst like this
> exposes in "database as queue" schedulers, and the specific mechanisms
> in Crobat that address them. Treat any future numbers as real only
> when they come from an actual run of the script above.

## Failure mode 1: the hidden throughput cap

**How it happens.** If executors claim work by locking a shared row — a
"queue head" pointer, a single scheduler state row, or simply
`SELECT ... FOR UPDATE` without `SKIP LOCKED` on an unindexed scan —
claims serialize. Adding executors stops helping: throughput plateaus
at whatever one lock-holder at a time can do, and the plateau is
invisible in functional tests because everything still *works*, just
slowly. A 30,000-job burst turns that plateau into a minutes-long
drain, and jobs fire late.

**How Crobat addresses it.**

- Claims are **batched** (default 32 per poll) and use
  `SELECT ... FOR UPDATE SKIP LOCKED` over the
  `idx_jobs_claim (status, scheduled_at)` index: an executor never waits
  behind another executor's claim — it walks past locked rows to the
  next claimable ones.
- There is no shared counter or head row anywhere in the claim path;
  the only writes are to the claimed rows themselves.
- The scheduler **materializes 30 s ahead**, so when the burst second
  arrives the rows already exist and executors are already holding most
  of them on local timers — the burst is spread *before* it lands
  instead of being discovered during it.
- Priority is a stored `priority_rank` column, so priority ordering is
  an index order, not a sort of string values at claim time.

## Failure mode 2: the deadlock that runs a job twice

**How it happens.** Two executors claim overlapping candidate sets and
update them in different orders: executor A locks job 1 then job 2,
executor B locks job 2 then job 1 — InnoDB kills one transaction. The
subtle part is what happens next: if the dead transaction had already
made an HTTP call (or its claim commit raced a retry), the job can be
executed by both executors. The scheduler transaction has the same
shape: two scheduler replicas locking task rows in different orders
deadlock the same way.

**How Crobat addresses it.**

- **Consistent lock ordering everywhere.** The scheduler locks due
  tasks `ORDER BY id`; the claim path sorts candidate ids before its
  `UPDATE`. Transactions that always take locks in the same global
  order cannot form a wait cycle.
- **The fencing token makes a double claim harmless.** Claiming
  increments `jobs.attempt`. A result is saved only by the guarded
  update `... WHERE status='running' AND worker_id=? AND attempt=?`.
  If a lease is taken over (reconciler rescue, deadlock victim retried,
  stalled executor), the stale owner's save matches zero rows and is
  dropped — it cannot overwrite the new owner's result.
- **Delivery is at-least-once, deduped at the receiver.** A job whose
  call may have happened twice carries the same `Idempotency-Key`
  (= job id) on every attempt, plus `X-Scheduled-At` and `X-Attempt`,
  so tenant endpoints can dedupe — the standard contract for safe
  retries, stated in the README.

## What to measure on a real run

- Drain time for the 30 k burst (jobs/second) at 1, 2, 4, 8 executors —
  look for the plateau that failure mode 1 predicts if batching or
  indexing regresses.
- `Innodb_row_lock_waits` / deadlock count from
  `SHOW ENGINE INNODB STATUS` — expect lock *waits* near zero thanks to
  `SKIP LOCKED`, and zero deadlocks.
- Scheduler tick duration during materialization of large due batches.
- p99 lateness: `finished_at - scheduled_at` distribution for the
  burst, from the history table.
