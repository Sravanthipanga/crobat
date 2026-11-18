package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/memory"
	"github.com/Sravanthipanga/crobat/internal/domain"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func seedTask(t *testing.T, s *memory.Store, key string, nextRun time.Time) {
	t.Helper()
	_, _, err := s.CreateTask(context.Background(), &domain.Task{
		TenantID:       "tenant-1",
		Name:           key,
		Schedule:       domain.Schedule{Kind: domain.ScheduleAt, At: nextRun},
		Priority:       domain.PriorityNormal,
		Request:        domain.RequestSpec{Method: "POST", URL: "https://example.com/hook"},
		Retry:          domain.RetryPolicy{MaxAttempts: 1},
		NextRunAt:      &nextRun,
		Status:         domain.TaskActive,
		IdempotencyKey: key,
		CreatedAt:      base,
		UpdatedAt:      base,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRunOnceMaterializesWithinLookaheadOnly(t *testing.T) {
	store := memory.NewStore()
	seedTask(t, store, "due-soon", base.Add(20*time.Second)) // inside 30s lookahead
	seedTask(t, store, "due-later", base.Add(10*time.Minute))
	svc := &Service{Store: store, Lookahead: 30 * time.Second, Now: func() time.Time { return base }}
	jobs, err := svc.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("materialized %d jobs, want 1", len(jobs))
	}
	if !jobs[0].ScheduledAt.Equal(base.Add(20 * time.Second)) {
		t.Fatalf("scheduled_at = %s", jobs[0].ScheduledAt)
	}
}

func TestRunOnceRespectsPausedTasks(t *testing.T) {
	store := memory.NewStore()
	task := &domain.Task{
		TenantID: "tenant-1", Name: "paused",
		Schedule: domain.Schedule{Kind: domain.ScheduleAt, At: base},
		Priority: domain.PriorityNormal,
		Request:  domain.RequestSpec{Method: "POST", URL: "https://example.com/hook"},
		Retry:    domain.RetryPolicy{MaxAttempts: 1},
		Status:   domain.TaskPaused, IdempotencyKey: "p1",
		CreatedAt: base, UpdatedAt: base,
	}
	next := base
	task.NextRunAt = &next
	if _, _, err := store.CreateTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	svc := &Service{Store: store, Now: func() time.Time { return base }}
	jobs, err := svc.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("paused task materialized %d jobs", len(jobs))
	}
}
