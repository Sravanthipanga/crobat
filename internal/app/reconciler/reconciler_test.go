package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/memory"
	"github.com/Sravanthipanga/crobat/internal/domain"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func TestRunOnceRescuesExpiredLeases(t *testing.T) {
	store := memory.NewStore()
	ctx := context.Background()
	_, _, err := store.CreateTask(ctx, &domain.Task{
		TenantID: "tenant-1", Name: "k",
		Schedule:  domain.Schedule{Kind: domain.ScheduleAt, At: base},
		Priority:  domain.PriorityNormal,
		Request:   domain.RequestSpec{Method: "POST", URL: "https://example.com/hook"},
		Retry:     domain.RetryPolicy{MaxAttempts: 3},
		NextRunAt: &base, Status: domain.TaskActive, IdempotencyKey: "k",
		CreatedAt: base, UpdatedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MaterializeDue(ctx, base, base, 10); err != nil {
		t.Fatal(err)
	}
	// An executor claims the job and dies: it never saves a result.
	if _, err := store.ClaimJobs(ctx, "dead-worker", 1, time.Minute, base); err != nil {
		t.Fatal(err)
	}
	svc := &Service{Jobs: store, Now: func() time.Time { return base.Add(5 * time.Minute) }}
	requeued, dead, err := svc.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 1 || dead != 0 {
		t.Fatalf("requeued=%d dead=%d", requeued, dead)
	}
	// The job is claimable again by a healthy executor.
	claimed, err := store.ClaimJobs(ctx, "healthy", 1, time.Minute, base.Add(5*time.Minute))
	if err != nil || len(claimed) != 1 || claimed[0].Attempt != 2 {
		t.Fatalf("reclaim: %+v err %v", claimed, err)
	}
}

func TestRunOnceLeavesLiveLeasesAlone(t *testing.T) {
	store := memory.NewStore()
	ctx := context.Background()
	_, _, _ = store.CreateTask(ctx, &domain.Task{
		TenantID: "tenant-1", Name: "k",
		Schedule:  domain.Schedule{Kind: domain.ScheduleAt, At: base},
		Priority:  domain.PriorityNormal,
		Request:   domain.RequestSpec{Method: "POST", URL: "https://example.com/hook"},
		Retry:     domain.RetryPolicy{MaxAttempts: 3},
		NextRunAt: &base, Status: domain.TaskActive, IdempotencyKey: "k",
		CreatedAt: base, UpdatedAt: base,
	})
	_, _ = store.MaterializeDue(ctx, base, base, 10)
	_, _ = store.ClaimJobs(ctx, "w1", 1, 10*time.Minute, base)
	svc := &Service{Jobs: store, Now: func() time.Time { return base.Add(time.Minute) }}
	requeued, dead, err := svc.RunOnce(ctx)
	if err != nil || requeued != 0 || dead != 0 {
		t.Fatalf("requeued=%d dead=%d err=%v", requeued, dead, err)
	}
}
