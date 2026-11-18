package jobsvc

import (
	"context"
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/memory"
	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func setup(t *testing.T) (*Service, *memory.Store, *memory.History, *domain.Task) {
	t.Helper()
	store := memory.NewStore()
	history := memory.NewHistory()
	ctx := context.Background()
	task, _, err := store.CreateTask(ctx, &domain.Task{
		TenantID: "tenant-1", Name: "k",
		Schedule:  domain.Schedule{Kind: domain.ScheduleCron, CronExpr: "0 * * * *"},
		Priority:  domain.PriorityNormal,
		Request:   domain.RequestSpec{Method: "POST", URL: "https://example.com/hook"},
		Retry:     domain.RetryPolicy{MaxAttempts: 1},
		NextRunAt: &base, Status: domain.TaskActive, IdempotencyKey: "k",
		CreatedAt: base, UpdatedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Service{Jobs: store, History: history}, store, history, task
}

func runOneJob(t *testing.T, store *memory.Store, at time.Time, status domain.JobStatus) *domain.Job {
	t.Helper()
	ctx := context.Background()
	jobs, err := store.MaterializeDue(ctx, at, at, 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("materialize at %s: %v jobs err %v", at, len(jobs), err)
	}
	claimed, _ := store.ClaimJobs(ctx, "w1", 1, time.Minute, at)
	applied, err := store.SaveResult(ctx, ports.ResultOutcome{
		JobID: jobs[0].ID, WorkerID: "w1", Attempt: claimed[0].Attempt,
		Status: status, HTTPStatus: 200, FinishedAt: at,
	})
	if err != nil || !applied {
		t.Fatalf("save: applied=%v err=%v", applied, err)
	}
	return jobs[0]
}

func TestHistoryMergesHotAndArchive(t *testing.T) {
	svc, store, history, task := setup(t)
	ctx := context.Background()
	old := runOneJob(t, store, base, domain.JobSucceeded)
	recent := runOneJob(t, store, base.Add(time.Hour), domain.JobDead)

	// The old job gets archived (history row + hot row deleted), the
	// recent one is still hot. History must show both, exactly once.
	if err := history.Append(ctx, domain.HistoryRecord{
		JobID: old.ID, TaskID: task.ID, TenantID: "tenant-1",
		ScheduledAt: old.ScheduledAt, FinishedAt: base,
		Status: domain.JobSucceeded, Attempts: 1, HTTPStatus: 200,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteJob(ctx, old.ID); err != nil {
		t.Fatal(err)
	}
	records, err := svc.TaskHistory(ctx, "tenant-1", task.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("history has %d records, want 2", len(records))
	}
	if records[0].JobID != recent.ID {
		t.Fatalf("newest first: got %s, want %s", records[0].JobID, recent.ID)
	}
}

func TestHistoryDedupesJobInBothStores(t *testing.T) {
	svc, store, history, task := setup(t)
	ctx := context.Background()
	job := runOneJob(t, store, base, domain.JobSucceeded)
	// Archiver wrote history but crashed before deleting the hot row:
	// the job is in BOTH stores and must appear exactly once.
	if err := history.Append(ctx, domain.HistoryRecord{
		JobID: job.ID, TaskID: task.ID, TenantID: "tenant-1",
		ScheduledAt: job.ScheduledAt, FinishedAt: base,
		Status: domain.JobSucceeded, Attempts: 1, HTTPStatus: 200,
	}); err != nil {
		t.Fatal(err)
	}
	records, err := svc.TaskHistory(ctx, "tenant-1", task.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("history has %d records, want exactly 1 (deduped)", len(records))
	}
}

func TestHistoryIgnoresUnfinishedJobs(t *testing.T) {
	svc, store, _, task := setup(t)
	ctx := context.Background()
	if _, err := store.MaterializeDue(ctx, base, base, 10); err != nil {
		t.Fatal(err)
	}
	records, err := svc.TaskHistory(ctx, "tenant-1", task.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("pending job leaked into history: %+v", records)
	}
}
