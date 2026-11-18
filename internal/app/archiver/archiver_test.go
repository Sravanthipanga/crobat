package archiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/memory"
	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func finishedJob(t *testing.T, store *memory.Store) *domain.Job {
	t.Helper()
	ctx := context.Background()
	_, _, err := store.CreateTask(ctx, &domain.Task{
		TenantID: "tenant-1", Name: "k",
		Schedule:  domain.Schedule{Kind: domain.ScheduleAt, At: base},
		Priority:  domain.PriorityNormal,
		Request:   domain.RequestSpec{Method: "POST", URL: "https://example.com/hook"},
		Retry:     domain.RetryPolicy{MaxAttempts: 1},
		NextRunAt: &base, Status: domain.TaskActive, IdempotencyKey: "k",
		CreatedAt: base, UpdatedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := store.MaterializeDue(ctx, base, base, 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("materialize: %v", err)
	}
	claimed, _ := store.ClaimJobs(ctx, "w1", 1, time.Minute, base)
	applied, err := store.SaveResult(ctx, ports.ResultOutcome{
		JobID: jobs[0].ID, WorkerID: "w1", Attempt: claimed[0].Attempt,
		Status: domain.JobSucceeded, HTTPStatus: 200, FinishedAt: base,
	})
	if err != nil || !applied {
		t.Fatalf("save: applied=%v err=%v", applied, err)
	}
	return jobs[0]
}

func TestRunOnceArchivesThenDeletes(t *testing.T) {
	store := memory.NewStore()
	history := memory.NewHistory()
	job := finishedJob(t, store)
	svc := &Service{Jobs: store, History: history}
	n, err := svc.RunOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("archived=%d err=%v", n, err)
	}
	if store.JobCount() != 0 {
		t.Fatal("hot row was not deleted after archival")
	}
	records, err := history.List(context.Background(), "tenant-1", job.TaskID, 10)
	if err != nil || len(records) != 1 {
		t.Fatalf("history: %d records err %v", len(records), err)
	}
	if records[0].JobID != job.ID || records[0].Status != domain.JobSucceeded || records[0].Attempts != 1 {
		t.Fatalf("record: %+v", records[0])
	}
}

type failingHistory struct{ err error }

func (f failingHistory) Append(context.Context, domain.HistoryRecord) error { return f.err }
func (f failingHistory) List(context.Context, string, string, int) ([]domain.HistoryRecord, error) {
	return nil, f.err
}

func TestRunOnceDoesNotDeleteWhenHistoryWriteFails(t *testing.T) {
	store := memory.NewStore()
	finishedJob(t, store)
	svc := &Service{Jobs: store, History: failingHistory{err: errors.New("cassandra down")}}
	if _, err := svc.RunOnce(context.Background()); err == nil {
		t.Fatal("expected error when history write fails")
	}
	if store.JobCount() != 1 {
		t.Fatal("hot row was deleted even though the history write failed — that would lose the record")
	}
}
