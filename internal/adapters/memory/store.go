// Package memory provides in-memory adapters for every store port. They
// are the semantic reference for the SQL adapters (same contracts, same
// fencing and fairness rules) and let the whole system be unit-tested
// without MySQL or Cassandra. A single mutex stands in for the database
// transaction in multi-step operations.
package memory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Sravanthipanga/crobat/internal/app/cronx"
	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

// NewID returns a random 128-bit hex id.
func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// Store implements ports.TaskStore, ports.SchedulerStore and ports.JobStore.
type Store struct {
	mu       sync.Mutex
	tasks    map[string]*domain.Task
	taskIdem map[string]string // tenantID + "\x00" + idempotencyKey -> task ID
	jobs     map[string]*domain.Job
}

var (
	_ ports.TaskStore      = (*Store)(nil)
	_ ports.SchedulerStore = (*Store)(nil)
	_ ports.JobStore       = (*Store)(nil)
)

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		tasks:    make(map[string]*domain.Task),
		taskIdem: make(map[string]string),
		jobs:     make(map[string]*domain.Job),
	}
}

// --- TaskStore ---

func (s *Store) CreateTask(_ context.Context, task *domain.Task) (*domain.Task, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := task.TenantID + "\x00" + task.IdempotencyKey
	if task.IdempotencyKey != "" {
		if existingID, ok := s.taskIdem[key]; ok {
			return s.tasks[existingID].Clone(), false, nil
		}
	}
	stored := task.Clone()
	if stored.ID == "" {
		stored.ID = NewID()
	}
	s.tasks[stored.ID] = stored
	if task.IdempotencyKey != "" {
		s.taskIdem[key] = stored.ID
	}
	return stored.Clone(), true, nil
}

func (s *Store) GetTask(_ context.Context, tenantID, id string) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok || t.TenantID != tenantID || t.Status == domain.TaskDeleted {
		return nil, domain.ErrNotFound
	}
	return t.Clone(), nil
}

func (s *Store) UpdateTask(_ context.Context, task *domain.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.tasks[task.ID]
	if !ok || existing.TenantID != task.TenantID {
		return domain.ErrNotFound
	}
	s.tasks[task.ID] = task.Clone()
	return nil
}

func (s *Store) ListTasks(_ context.Context, tenantID string) ([]*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.Task
	for _, t := range s.tasks {
		if t.TenantID == tenantID && t.Status != domain.TaskDeleted {
			out = append(out, t.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// --- SchedulerStore ---

// MaterializeDue mirrors the MySQL transaction: due tasks are visited in
// id order (the same consistent lock ordering the SQL adapter uses with
// ORDER BY id ... FOR UPDATE), a job row is inserted and next_run_at
// advanced, all atomically under the store lock.
func (s *Store) MaterializeDue(_ context.Context, now, horizon time.Time, limit int) ([]*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var due []*domain.Task
	for _, t := range s.tasks {
		if t.Status == domain.TaskActive && t.NextRunAt != nil && !t.NextRunAt.After(horizon) {
			due = append(due, t)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].ID < due[j].ID })

	var created []*domain.Job
	for _, t := range due {
		if limit > 0 && len(created) >= limit {
			break
		}
		scheduledFor := *t.NextRunAt
		job := &domain.Job{
			ID:          NewID(),
			TaskID:      t.ID,
			TenantID:    t.TenantID,
			Priority:    t.Priority,
			Request:     t.Request.Clone(),
			MaxAttempts: t.Retry.MaxAttempts,
			BackoffBase: t.Retry.BackoffBase,
			BackoffMax:  t.Retry.BackoffMax,
			Timeout:     t.Timeout,
			ScheduledAt: scheduledFor,
			Status:      domain.JobPending,
			CreatedAt:   now,
		}
		if job.MaxAttempts < 1 {
			job.MaxAttempts = 1
		}
		s.jobs[job.ID] = job
		created = append(created, job.Clone())

		if t.Schedule.OneShot() {
			t.NextRunAt = nil
		} else {
			next, err := cronx.Advance(t.Schedule, scheduledFor, now)
			if err != nil {
				return nil, err
			}
			t.NextRunAt = &next
		}
		t.UpdatedAt = now
	}
	return created, nil
}

// --- JobStore ---

// ClaimJobs mirrors the SQL claim: due pending jobs are ordered by
// priority, then interleaved round-robin across tenants (the SQL adapter
// gets the same effect with ROW_NUMBER() OVER (PARTITION BY tenant_id)),
// so a tenant with 30,000 queued jobs cannot starve everyone else.
func (s *Store) ClaimJobs(_ context.Context, workerID string, limit int, lease time.Duration, now time.Time) ([]*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 1
	}

	var candidates []*domain.Job
	for _, j := range s.jobs {
		if j.Status == domain.JobPending && !j.ScheduledAt.After(now) {
			candidates = append(candidates, j)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.Priority.Rank() != b.Priority.Rank() {
			return a.Priority.Rank() < b.Priority.Rank()
		}
		if !a.ScheduledAt.Equal(b.ScheduledAt) {
			return a.ScheduledAt.Before(b.ScheduledAt)
		}
		return a.ID < b.ID
	})
	ordered := interleaveByTenant(candidates)

	leaseExp := now.Add(lease)
	var claimed []*domain.Job
	for _, j := range ordered {
		if len(claimed) >= limit {
			break
		}
		j.Status = domain.JobRunning
		j.WorkerID = workerID
		j.Attempt++ // the new fencing token
		le := leaseExp
		j.LeaseExpiresAt = &le
		claimed = append(claimed, j.Clone())
	}
	return claimed, nil
}

// interleaveByTenant round-robins a priority-sorted list across tenants:
// first every tenant's best job, then every tenant's second-best, etc.
func interleaveByTenant(sorted []*domain.Job) []*domain.Job {
	byTenant := make(map[string][]*domain.Job)
	var tenantOrder []string
	for _, j := range sorted {
		if _, ok := byTenant[j.TenantID]; !ok {
			tenantOrder = append(tenantOrder, j.TenantID)
		}
		byTenant[j.TenantID] = append(byTenant[j.TenantID], j)
	}
	var out []*domain.Job
	for round := 0; ; round++ {
		added := false
		for _, tenant := range tenantOrder {
			if round < len(byTenant[tenant]) {
				out = append(out, byTenant[tenant][round])
				added = true
			}
		}
		if !added {
			return out
		}
	}
}

func (s *Store) SaveResult(_ context.Context, outcome ports.ResultOutcome) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[outcome.JobID]
	if !ok {
		return false, domain.ErrNotFound
	}
	// The fencing guard: only the current owner may write a result.
	if j.Status != domain.JobRunning || j.WorkerID != outcome.WorkerID || j.Attempt != outcome.Attempt {
		return false, nil
	}
	j.HTTPStatus = outcome.HTTPStatus
	j.LastError = outcome.Err
	j.WorkerID = ""
	j.LeaseExpiresAt = nil
	switch outcome.Status {
	case domain.JobSucceeded, domain.JobDead:
		j.Status = outcome.Status
		f := outcome.FinishedAt
		j.FinishedAt = &f
	case domain.JobPending: // retry later
		j.Status = domain.JobPending
		j.ScheduledAt = outcome.RetryAt
	default:
		return false, fmt.Errorf("%w: cannot save result with status %q", domain.ErrValidation, outcome.Status)
	}
	return true, nil
}

func (s *Store) GetJob(_ context.Context, tenantID, id string) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok || j.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return j.Clone(), nil
}

func (s *Store) ListJobs(_ context.Context, tenantID string, status domain.JobStatus, limit int) ([]*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.Job
	for _, j := range s.jobs {
		if j.TenantID != tenantID {
			continue
		}
		if status != "" && j.Status != status {
			continue
		}
		out = append(out, j.Clone())
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ScheduledAt.Equal(out[j].ScheduledAt) {
			return out[i].ScheduledAt.After(out[j].ScheduledAt)
		}
		return out[i].ID < out[j].ID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) ListDLQ(ctx context.Context, tenantID string, limit int) ([]*domain.Job, error) {
	return s.ListJobs(ctx, tenantID, domain.JobDead, limit)
}

func (s *Store) RequeueJob(_ context.Context, tenantID, id string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok || j.TenantID != tenantID {
		return domain.ErrNotFound
	}
	if j.Status != domain.JobDead {
		return fmt.Errorf("%w: only dead jobs can be requeued, job is %s", domain.ErrConflict, j.Status)
	}
	j.Status = domain.JobPending
	j.Attempt = 0
	j.ScheduledAt = now
	j.LastError = ""
	j.FinishedAt = nil
	return nil
}

func (s *Store) ExpireLeases(_ context.Context, now time.Time) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	requeued, dead := 0, 0
	for _, j := range s.jobs {
		if j.Status != domain.JobRunning || j.LeaseExpiresAt == nil || j.LeaseExpiresAt.After(now) {
			continue
		}
		j.WorkerID = ""
		j.LeaseExpiresAt = nil
		if j.Attempt >= j.MaxAttempts {
			j.Status = domain.JobDead
			f := now
			j.FinishedAt = &f
			j.LastError = appendErr(j.LastError, "lease expired with attempts exhausted")
			dead++
		} else {
			j.Status = domain.JobPending
			j.ScheduledAt = now
			requeued++
		}
	}
	return requeued, dead, nil
}

func appendErr(existing, msg string) string {
	if existing == "" {
		return msg
	}
	return existing + "; " + msg
}

func (s *Store) ListFinished(_ context.Context, limit int) ([]*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.Job
	for _, j := range s.jobs {
		if j.Terminal() {
			out = append(out, j.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool {
		fi, fj := time.Time{}, time.Time{}
		if out[i].FinishedAt != nil {
			fi = *out[i].FinishedAt
		}
		if out[j].FinishedAt != nil {
			fj = *out[j].FinishedAt
		}
		if !fi.Equal(fj) {
			return fi.Before(fj)
		}
		return out[i].ID < out[j].ID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) DeleteJob(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[id]; !ok {
		return domain.ErrNotFound
	}
	delete(s.jobs, id)
	return nil
}

// JobCount is a test/inspection helper.
func (s *Store) JobCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.jobs)
}

// History is the in-memory ports.HistoryStore.
type History struct {
	mu      sync.Mutex
	records []domain.HistoryRecord
	seen    map[string]bool
}

var _ ports.HistoryStore = (*History)(nil)

// NewHistory returns an empty history store.
func NewHistory() *History { return &History{seen: make(map[string]bool)} }

// Append is idempotent on JobID, mirroring the Cassandra write (same
// primary key overwrites) so an archiver retry never duplicates history.
func (h *History) Append(_ context.Context, rec domain.HistoryRecord) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.seen[rec.JobID] {
		return nil
	}
	h.seen[rec.JobID] = true
	h.records = append(h.records, rec)
	return nil
}

func (h *History) List(_ context.Context, tenantID, taskID string, limit int) ([]domain.HistoryRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []domain.HistoryRecord
	for _, r := range h.records {
		if r.TenantID == tenantID && r.TaskID == taskID {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FinishedAt.After(out[j].FinishedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
