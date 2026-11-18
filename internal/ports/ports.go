// Package ports defines the interfaces the application core depends on.
// Adapters (MySQL, Cassandra, in-memory, HTTP) implement them.
package ports

import (
	"context"
	"time"

	"github.com/Sravanthipanga/crobat/internal/domain"
)

// TaskStore persists task definitions.
type TaskStore interface {
	// CreateTask inserts a task. It is idempotent on
	// (TenantID, IdempotencyKey): a replayed POST returns the original task
	// with created=false instead of inserting a duplicate.
	CreateTask(ctx context.Context, task *domain.Task) (stored *domain.Task, created bool, err error)
	GetTask(ctx context.Context, tenantID, id string) (*domain.Task, error)
	UpdateTask(ctx context.Context, task *domain.Task) error
	ListTasks(ctx context.Context, tenantID string) ([]*domain.Task, error)
}

// SchedulerStore is the Scheduler's transactional view of the database.
type SchedulerStore interface {
	// MaterializeDue, in ONE transaction, inserts pending job rows for
	// every active task with next_run_at <= horizon (up to limit) and
	// advances each task's next_run_at. Because the job row and the
	// schedule advance commit together, the schedule and the queue can
	// never disagree — there is no broker to fall out of sync with.
	MaterializeDue(ctx context.Context, now, horizon time.Time, limit int) ([]*domain.Job, error)
}

// ResultOutcome is a guarded result write from an executor.
type ResultOutcome struct {
	JobID      string
	WorkerID   string           // must match the claiming worker
	Attempt    int              // fencing token captured at claim time
	Status     domain.JobStatus // succeeded | pending (retry) | dead
	HTTPStatus int
	Err        string
	RetryAt    time.Time // when Status == pending: next attempt time
	FinishedAt time.Time // when Status is terminal
}

// JobStore is the executors'/reconciler's view of the jobs table.
type JobStore interface {
	// ClaimJobs atomically claims up to limit pending jobs with
	// ScheduledAt <= now, for workerID: status -> running, attempt
	// incremented (the new fencing token), lease set to now+lease.
	// Claiming is priority-ordered with round-robin fair sharing across
	// tenants. The SQL adapter uses SELECT ... FOR UPDATE SKIP LOCKED so
	// executors never block on each other. Callers may pass a `now`
	// slightly in the future (the executor's claim-ahead window) to claim
	// jobs just before they fall due and wait out the remainder on a
	// local timer.
	ClaimJobs(ctx context.Context, workerID string, limit int, lease time.Duration, now time.Time) ([]*domain.Job, error)
	// SaveResult applies outcome only if the row is still running and
	// owned by (WorkerID, Attempt). It returns false (with
	// domain.ErrStaleClaim semantics) when the guard rejects the write.
	SaveResult(ctx context.Context, outcome ResultOutcome) (applied bool, err error)
	GetJob(ctx context.Context, tenantID, id string) (*domain.Job, error)
	ListJobs(ctx context.Context, tenantID string, status domain.JobStatus, limit int) ([]*domain.Job, error)
	// ListDLQ lists dead jobs for a tenant, newest first.
	ListDLQ(ctx context.Context, tenantID string, limit int) ([]*domain.Job, error)
	// RequeueJob manually retries a dead job: back to pending, attempts reset.
	RequeueJob(ctx context.Context, tenantID, id string, now time.Time) error
	// ExpireLeases rescues running jobs whose lease expired: back to
	// pending, or dead if the attempt budget is exhausted.
	ExpireLeases(ctx context.Context, now time.Time) (requeued, dead int, err error)
	// ListFinished returns terminal jobs awaiting archival, oldest first.
	ListFinished(ctx context.Context, limit int) ([]*domain.Job, error)
	// DeleteJob removes a job row. The Archiver calls it only AFTER the
	// history record is durably written.
	DeleteJob(ctx context.Context, id string) error
}

// HistoryStore is the append-only run archive (Cassandra in production,
// in-memory in tests).
type HistoryStore interface {
	Append(ctx context.Context, rec domain.HistoryRecord) error
	List(ctx context.Context, tenantID, taskID string, limit int) ([]domain.HistoryRecord, error)
}

// Caller performs the HTTP call for a claimed job. Implementations must
// send the Idempotency-Key, X-Scheduled-At and X-Attempt headers.
type Caller interface {
	Call(ctx context.Context, job *domain.Job, req domain.RequestSpec) (httpStatus int, err error)
}

// SecretBox encrypts and decrypts secret header values at rest.
type SecretBox interface {
	Encrypt(plaintext string) (string, error)
	Decrypt(ciphertext string) (string, error)
}
