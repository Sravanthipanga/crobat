// Package domain holds Crobat's core types. It has no dependencies on
// infrastructure: storage, HTTP and crypto all live in adapters behind the
// ports in internal/ports (hexagonal architecture).
package domain

import (
	"errors"
	"time"
)

// Priority levels for tasks and jobs. Lower Rank() is claimed first.
type Priority string

const (
	PriorityCritical Priority = "critical"
	PriorityHigh     Priority = "high"
	PriorityNormal   Priority = "normal"
	PriorityLow      Priority = "low"
)

// Rank orders priorities for claiming: critical first, low last.
func (p Priority) Rank() int {
	switch p {
	case PriorityCritical:
		return 0
	case PriorityHigh:
		return 1
	case PriorityNormal:
		return 2
	case PriorityLow:
		return 3
	}
	return 2
}

// Valid reports whether p is a known priority.
func (p Priority) Valid() bool {
	switch p {
	case PriorityCritical, PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

// ScheduleKind selects how a task's run times are computed.
type ScheduleKind string

const (
	ScheduleNow  ScheduleKind = "now"  // run once, immediately
	ScheduleAt   ScheduleKind = "at"   // run once, at At
	ScheduleCron ScheduleKind = "cron" // run on a cron expression in Timezone
)

// Schedule describes when a task fires.
type Schedule struct {
	Kind     ScheduleKind
	At       time.Time // ScheduleAt only
	CronExpr string    // ScheduleCron only, 5-field cron (robfig/cron parser)
	Timezone string    // IANA name; empty means UTC
}

// OneShot reports whether the schedule fires exactly once.
func (s Schedule) OneShot() bool { return s.Kind == ScheduleNow || s.Kind == ScheduleAt }

// RequestSpec is the HTTP call a job makes. In stored form, the values of
// headers named in SecretHeaders are encrypted (see adapters/crypto);
// everywhere else the spec is carried verbatim.
type RequestSpec struct {
	Method        string
	URL           string
	Headers       map[string]string
	Body          string
	SecretHeaders []string
}

// Clone deep-copies the spec.
func (r RequestSpec) Clone() RequestSpec {
	out := r
	if r.Headers != nil {
		out.Headers = make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			out.Headers[k] = v
		}
	}
	if r.SecretHeaders != nil {
		out.SecretHeaders = append([]string(nil), r.SecretHeaders...)
	}
	return out
}

// RetryPolicy controls retries after a failed attempt.
type RetryPolicy struct {
	MaxAttempts int           // total attempts including the first; >= 1
	BackoffBase time.Duration // delay after the first failure
	BackoffMax  time.Duration // cap for the exponential backoff
}

// TaskStatus is the lifecycle of a task definition.
type TaskStatus string

const (
	TaskActive  TaskStatus = "active"
	TaskPaused  TaskStatus = "paused"
	TaskDeleted TaskStatus = "deleted"
)

// Task is the configuration row: what to call, when, and how hard to try.
// Stored in the MySQL `tasks` table.
type Task struct {
	ID             string
	TenantID       string
	Name           string
	Schedule       Schedule
	Priority       Priority
	Request        RequestSpec // stored form: secrets encrypted
	Timeout        time.Duration
	Retry          RetryPolicy
	NextRunAt      *time.Time // nil once a one-shot task has been materialized
	Status         TaskStatus
	IdempotencyKey string // the Idempotency-Key header of the creating POST
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Clone deep-copies the task.
func (t *Task) Clone() *Task {
	out := *t
	out.Request = t.Request.Clone()
	if t.NextRunAt != nil {
		nr := *t.NextRunAt
		out.NextRunAt = &nr
	}
	return &out
}

// JobStatus is the lifecycle of a single scheduled run.
type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobRunning   JobStatus = "running"
	JobSucceeded JobStatus = "succeeded"
	JobDead      JobStatus = "dead" // attempts exhausted: the DLQ
)

// Job is one row of the MySQL `jobs` table — the hot table that IS the
// queue. ID doubles as the Idempotency-Key sent to tenant endpoints, so a
// retried delivery can be deduped by the receiver.
type Job struct {
	ID          string
	TaskID      string
	TenantID    string
	Priority    Priority
	Request     RequestSpec // copied from the task at materialization time
	MaxAttempts int
	BackoffBase time.Duration // copied from the task's retry policy
	BackoffMax  time.Duration
	Timeout     time.Duration
	ScheduledAt time.Time
	Status      JobStatus
	// Attempt is the fencing token: incremented on every claim. A result is
	// only saved when (WorkerID, Attempt) still match the row, so a stale
	// executor whose lease was taken over cannot overwrite the new owner.
	Attempt        int
	WorkerID       string
	LeaseExpiresAt *time.Time
	HTTPStatus     int
	LastError      string
	CreatedAt      time.Time
	FinishedAt     *time.Time
}

// Clone deep-copies the job.
func (j *Job) Clone() *Job {
	out := *j
	out.Request = j.Request.Clone()
	if j.LeaseExpiresAt != nil {
		le := *j.LeaseExpiresAt
		out.LeaseExpiresAt = &le
	}
	if j.FinishedAt != nil {
		f := *j.FinishedAt
		out.FinishedAt = &f
	}
	return &out
}

// Terminal reports whether the job will never run again.
func (j *Job) Terminal() bool { return j.Status == JobSucceeded || j.Status == JobDead }

// HistoryRecord is one archived run, written to Cassandra by the Archiver
// (append-only, 30-day TTL, partitioned by tenant · task · month).
type HistoryRecord struct {
	JobID       string
	TaskID      string
	TenantID    string
	ScheduledAt time.Time
	FinishedAt  time.Time
	Status      JobStatus
	Attempts    int
	HTTPStatus  int
	LastError   string
}

// Shared sentinel errors.
var (
	ErrNotFound   = errors.New("not found")
	ErrConflict   = errors.New("conflict")
	ErrValidation = errors.New("validation error")
	ErrStaleClaim = errors.New("stale claim: job is owned by another attempt")
)
