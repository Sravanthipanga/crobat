// Package e2e wires the whole system together on the in-memory adapters:
// task created -> scheduler materializes -> executor delivers ->
// archiver moves it to history -> job service reads it back, once.
package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/memory"
	"github.com/Sravanthipanga/crobat/internal/app/archiver"
	"github.com/Sravanthipanga/crobat/internal/app/executor"
	"github.com/Sravanthipanga/crobat/internal/app/jobsvc"
	"github.com/Sravanthipanga/crobat/internal/app/scheduler"
	"github.com/Sravanthipanga/crobat/internal/app/tasksvc"
	"github.com/Sravanthipanga/crobat/internal/domain"
)

type okCaller struct{ calls int }

func (c *okCaller) Call(_ context.Context, _ *domain.Job, _ domain.RequestSpec) (int, error) {
	c.calls++
	return 200, nil
}

func TestFullPipeline(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()
	store := memory.NewStore()
	history := memory.NewHistory()

	tasks := &tasksvc.Service{Tasks: store, Now: func() time.Time { return base }}
	task, _, err := tasks.Create(ctx, tasksvc.CreateInput{
		TenantID:       "tenant-1",
		IdempotencyKey: "create-1",
		Name:           "ping",
		Schedule:       domain.Schedule{Kind: domain.ScheduleAt, At: base.Add(time.Minute)},
		Priority:       domain.PriorityNormal,
		Request:        domain.RequestSpec{Method: "POST", URL: "https://example.com/hook"},
		Retry:          domain.RetryPolicy{MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}

	sched := &scheduler.Service{Store: store, Lookahead: 2 * time.Minute, Now: func() time.Time { return base }}
	jobs, err := sched.RunOnce(ctx)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("scheduler: %d jobs err %v", len(jobs), err)
	}

	caller := &okCaller{}
	exec := &executor.Service{
		Jobs: store, Caller: caller, WorkerID: "w1",
		Now:  func() time.Time { return base.Add(time.Minute) },
		Wait: func(context.Context, time.Time) error { return nil },
	}
	saved, err := exec.RunOnce(ctx)
	if err != nil || saved != 1 || caller.calls != 1 {
		t.Fatalf("executor: saved=%d calls=%d err=%v", saved, caller.calls, err)
	}

	arch := &archiver.Service{Jobs: store, History: history}
	n, err := arch.RunOnce(ctx)
	if err != nil || n != 1 {
		t.Fatalf("archiver: %d err %v", n, err)
	}
	if store.JobCount() != 0 {
		t.Fatal("hot table should be empty after archival")
	}

	jobsSvc := &jobsvc.Service{Jobs: store, History: history}
	records, err := jobsSvc.TaskHistory(ctx, "tenant-1", task.ID, 10)
	if err != nil || len(records) != 1 {
		t.Fatalf("history: %d records err %v", len(records), err)
	}
	if records[0].Status != domain.JobSucceeded || records[0].Attempts != 1 {
		t.Fatalf("record: %+v", records[0])
	}
}
