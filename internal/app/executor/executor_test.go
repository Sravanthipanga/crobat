package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/memory"
	"github.com/Sravanthipanga/crobat/internal/domain"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

type fakeCaller struct {
	status int
	err    error
	calls  []*domain.Job
}

func (f *fakeCaller) Call(_ context.Context, job *domain.Job, _ domain.RequestSpec) (int, error) {
	f.calls = append(f.calls, job.Clone())
	return f.status, f.err
}

func seedJob(t *testing.T, s *memory.Store, key string, due time.Time, maxAttempts int) *domain.Job {
	t.Helper()
	_, _, err := s.CreateTask(context.Background(), &domain.Task{
		TenantID: "tenant-1", Name: key,
		Schedule:  domain.Schedule{Kind: domain.ScheduleAt, At: due},
		Priority:  domain.PriorityNormal,
		Request:   domain.RequestSpec{Method: "POST", URL: "https://example.com/hook"},
		Retry:     domain.RetryPolicy{MaxAttempts: maxAttempts, BackoffBase: time.Second, BackoffMax: time.Minute},
		NextRunAt: &due, Status: domain.TaskActive, IdempotencyKey: key,
		CreatedAt: base, UpdatedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := s.MaterializeDue(context.Background(), due, due, 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("materialize: %v jobs err %v", len(jobs), err)
	}
	return jobs[0]
}

func newExecutor(store *memory.Store, caller *fakeCaller) *Service {
	return &Service{
		Jobs: store, Caller: caller, WorkerID: "w1",
		Now:          func() time.Time { return base },
		Wait:         func(context.Context, time.Time) error { return nil },
		JitterSource: func() float64 { return 0 },
	}
}

func TestExecuteSuccess(t *testing.T) {
	store := memory.NewStore()
	job := seedJob(t, store, "k1", base, 3)
	caller := &fakeCaller{status: 200}
	svc := newExecutor(store, caller)
	saved, err := svc.RunOnce(context.Background())
	if err != nil || saved != 1 {
		t.Fatalf("saved=%d err=%v", saved, err)
	}
	if len(caller.calls) != 1 || caller.calls[0].Attempt != 1 {
		t.Fatalf("caller saw %+v", caller.calls)
	}
	stored, _ := store.GetJob(context.Background(), "tenant-1", job.ID)
	if stored.Status != domain.JobSucceeded || stored.HTTPStatus != 200 || stored.FinishedAt == nil {
		t.Fatalf("stored: %+v", stored)
	}
}

func TestExecuteFailureSchedulesRetry(t *testing.T) {
	store := memory.NewStore()
	job := seedJob(t, store, "k1", base, 3)
	caller := &fakeCaller{status: 500, err: errors.New("endpoint returned HTTP 500")}
	svc := newExecutor(store, caller)
	if _, err := svc.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, _ := store.GetJob(context.Background(), "tenant-1", job.ID)
	if stored.Status != domain.JobPending {
		t.Fatalf("status = %s, want pending (retry)", stored.Status)
	}
	// Backoff: base 1s, jitter 0 -> 500ms after `base`.
	want := base.Add(500 * time.Millisecond)
	if !stored.ScheduledAt.Equal(want) {
		t.Fatalf("retry scheduled at %s, want %s", stored.ScheduledAt, want)
	}
	if stored.Attempt != 1 {
		t.Fatalf("attempt = %d (attempts are counted at claim time)", stored.Attempt)
	}
}

func TestExecuteFailureExhaustedGoesToDLQ(t *testing.T) {
	store := memory.NewStore()
	job := seedJob(t, store, "k1", base, 1)
	caller := &fakeCaller{status: 500, err: errors.New("boom")}
	svc := newExecutor(store, caller)
	if _, err := svc.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, _ := store.GetJob(context.Background(), "tenant-1", job.ID)
	if stored.Status != domain.JobDead {
		t.Fatalf("status = %s, want dead", stored.Status)
	}
	dlq, err := store.ListDLQ(context.Background(), "tenant-1", 10)
	if err != nil || len(dlq) != 1 {
		t.Fatalf("dlq: %d jobs err %v", len(dlq), err)
	}
}

func TestExecutorWaitsForScheduledTime(t *testing.T) {
	store := memory.NewStore()
	due := base.Add(10 * time.Second)
	job := seedJob(t, store, "k1", due, 1)
	caller := &fakeCaller{status: 200}
	svc := newExecutor(store, caller)
	var waitedFor time.Time
	svc.Wait = func(_ context.Context, t time.Time) error { waitedFor = t; return nil }
	// ClaimAhead defaults to 30s, so the job (due in 10s) is claimed now
	// and the executor must wait until its scheduled time.
	if _, err := svc.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !waitedFor.Equal(due) {
		t.Fatalf("waited for %s, want %s", waitedFor, due)
	}
	stored, _ := store.GetJob(context.Background(), "tenant-1", job.ID)
	if stored.Status != domain.JobSucceeded {
		t.Fatalf("status = %s", stored.Status)
	}
}
