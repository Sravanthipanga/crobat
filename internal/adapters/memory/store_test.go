package memory

import (
	"context"
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func makeTask(tenant, key string, priority domain.Priority, nextRun time.Time, maxAttempts int) *domain.Task {
	return &domain.Task{
		TenantID:       tenant,
		Name:           "task-" + key,
		Schedule:       domain.Schedule{Kind: domain.ScheduleAt, At: nextRun},
		Priority:       priority,
		Request:        domain.RequestSpec{Method: "POST", URL: "https://example.com/hook"},
		Retry:          domain.RetryPolicy{MaxAttempts: maxAttempts, BackoffBase: time.Second, BackoffMax: time.Minute},
		NextRunAt:      &nextRun,
		Status:         domain.TaskActive,
		IdempotencyKey: key,
		CreatedAt:      base,
		UpdatedAt:      base,
	}
}

func seedJob(t *testing.T, s *Store, tenant, key string, priority domain.Priority, due time.Time, maxAttempts int) *domain.Job {
	t.Helper()
	task, _, err := s.CreateTask(context.Background(), makeTask(tenant, key, priority, due, maxAttempts))
	if err != nil {
		t.Fatal(err)
	}
	_ = task
	jobs, err := s.MaterializeDue(context.Background(), due, due, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 materialized job, got %d", len(jobs))
	}
	return jobs[0]
}

func TestMaterializeOneShotClearsNextRun(t *testing.T) {
	s := NewStore()
	task, _, _ := s.CreateTask(context.Background(), makeTask("t1", "k1", domain.PriorityNormal, base, 3))
	jobs, err := s.MaterializeDue(context.Background(), base, base.Add(30*time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].TaskID != task.ID || jobs[0].Status != domain.JobPending {
		t.Fatalf("unexpected jobs: %+v", jobs)
	}
	stored, err := s.GetTask(context.Background(), "t1", task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.NextRunAt != nil {
		t.Fatal("one-shot task next_run_at should be cleared after materializing")
	}
	// A second sweep must not create a duplicate job.
	again, err := s.MaterializeDue(context.Background(), base, base.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 || s.JobCount() != 1 {
		t.Fatalf("duplicate materialization: %d jobs", s.JobCount())
	}
}

func TestMaterializeCronAdvances(t *testing.T) {
	s := NewStore()
	task := makeTask("t1", "cron1", domain.PriorityNormal, base, 3)
	task.Schedule = domain.Schedule{Kind: domain.ScheduleCron, CronExpr: "*/15 * * * *"}
	created, _, err := s.CreateTask(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	task = created
	if _, err := s.MaterializeDue(context.Background(), base, base, 10); err != nil {
		t.Fatal(err)
	}
	stored, _ := s.GetTask(context.Background(), "t1", task.ID)
	want := base.Add(15 * time.Minute)
	if stored.NextRunAt == nil || !stored.NextRunAt.Equal(want) {
		t.Fatalf("next_run_at = %v, want %s", stored.NextRunAt, want)
	}
}

func TestClaimPriorityOrder(t *testing.T) {
	s := NewStore()
	seedJob(t, s, "t1", "low", domain.PriorityLow, base, 1)
	seedJob(t, s, "t1", "crit", domain.PriorityCritical, base, 1)
	seedJob(t, s, "t1", "norm", domain.PriorityNormal, base, 1)
	claimed, err := s.ClaimJobs(context.Background(), "w1", 3, time.Minute, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d", len(claimed))
	}
	if claimed[0].Priority != domain.PriorityCritical || claimed[2].Priority != domain.PriorityLow {
		t.Fatalf("order = %s, %s, %s", claimed[0].Priority, claimed[1].Priority, claimed[2].Priority)
	}
	for _, j := range claimed {
		if j.Status != domain.JobRunning || j.Attempt != 1 || j.WorkerID != "w1" || j.LeaseExpiresAt == nil {
			t.Fatalf("claim did not set running/attempt/lease: %+v", j)
		}
	}
}

func TestClaimFairSharingAcrossTenants(t *testing.T) {
	s := NewStore()
	// Tenant A floods the queue; tenant B has a single job. With a claim
	// limit of 4, B's job must be claimed in the first round-robin pass.
	for i := 0; i < 10; i++ {
		seedJob(t, s, "tenant-a", string(rune('a'+i)), domain.PriorityNormal, base, 1)
	}
	bJob := seedJob(t, s, "tenant-b", "only", domain.PriorityNormal, base, 1)
	claimed, err := s.ClaimJobs(context.Background(), "w1", 4, time.Minute, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 4 {
		t.Fatalf("claimed %d", len(claimed))
	}
	found := false
	for _, j := range claimed {
		if j.ID == bJob.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("tenant B's job was starved by tenant A's burst")
	}
	// Round-robin: at most ceil(limit/tenants)+... A should hold 3, B 1.
	aCount := 0
	for _, j := range claimed {
		if j.TenantID == "tenant-a" {
			aCount++
		}
	}
	if aCount != 3 {
		t.Fatalf("tenant A claimed %d of 4, want 3 (round-robin)", aCount)
	}
}

func TestClaimSkipsNotYetDueAndRunning(t *testing.T) {
	s := NewStore()
	seedJob(t, s, "t1", "future", domain.PriorityNormal, base.Add(time.Hour), 1)
	due := seedJob(t, s, "t1", "due", domain.PriorityNormal, base, 1)
	claimed, err := s.ClaimJobs(context.Background(), "w1", 10, time.Minute, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != due.ID {
		t.Fatalf("claimed %+v", claimed)
	}
	// Already running: a second claim finds nothing.
	claimed, err = s.ClaimJobs(context.Background(), "w2", 10, time.Minute, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("running job re-claimed: %+v", claimed)
	}
}

func TestFencingTokenRejectsStaleSave(t *testing.T) {
	s := NewStore()
	job := seedJob(t, s, "t1", "k", domain.PriorityNormal, base, 3)
	ctx := context.Background()

	// Worker 1 claims (attempt 1), then stalls past its lease.
	claimed, _ := s.ClaimJobs(ctx, "w1", 1, time.Minute, base)
	if claimed[0].Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", claimed[0].Attempt)
	}
	if _, _, err := s.ExpireLeases(ctx, base.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	// Worker 2 takes over (attempt 2 = the new fencing token).
	claimed2, _ := s.ClaimJobs(ctx, "w2", 1, time.Minute, base.Add(2*time.Minute))
	if len(claimed2) != 1 || claimed2[0].Attempt != 2 {
		t.Fatalf("takeover claim: %+v", claimed2)
	}
	// Worker 1 finally finishes and tries to save: must be rejected.
	applied, err := s.SaveResult(ctx, ports.ResultOutcome{
		JobID: job.ID, WorkerID: "w1", Attempt: 1,
		Status: domain.JobSucceeded, FinishedAt: base.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("stale executor's save was applied")
	}
	// Worker 2's save goes through.
	applied, err = s.SaveResult(ctx, ports.ResultOutcome{
		JobID: job.ID, WorkerID: "w2", Attempt: 2,
		Status: domain.JobSucceeded, HTTPStatus: 200, FinishedAt: base.Add(3 * time.Minute),
	})
	if err != nil || !applied {
		t.Fatalf("owner save: applied=%v err=%v", applied, err)
	}
	stored, _ := s.GetJob(ctx, "t1", job.ID)
	if stored.Status != domain.JobSucceeded || stored.HTTPStatus != 200 {
		t.Fatalf("stored job: %+v", stored)
	}
}

func TestExpireLeasesRequeuesAndDeadLetters(t *testing.T) {
	s := NewStore()
	retryable := seedJob(t, s, "t1", "retry", domain.PriorityNormal, base, 3)
	exhausted := seedJob(t, s, "t1", "once", domain.PriorityNormal, base, 1)
	ctx := context.Background()
	if _, err := s.ClaimJobs(ctx, "w1", 2, time.Minute, base); err != nil {
		t.Fatal(err)
	}
	requeued, dead, err := s.ExpireLeases(ctx, base.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 1 || dead != 1 {
		t.Fatalf("requeued=%d dead=%d", requeued, dead)
	}
	j1, _ := s.GetJob(ctx, "t1", retryable.ID)
	if j1.Status != domain.JobPending {
		t.Fatalf("retryable job status = %s", j1.Status)
	}
	j2, _ := s.GetJob(ctx, "t1", exhausted.ID)
	if j2.Status != domain.JobDead || j2.FinishedAt == nil {
		t.Fatalf("exhausted job: %+v", j2)
	}
}

func TestRequeueDeadJob(t *testing.T) {
	s := NewStore()
	job := seedJob(t, s, "t1", "k", domain.PriorityNormal, base, 1)
	ctx := context.Background()
	claimed, _ := s.ClaimJobs(ctx, "w1", 1, time.Minute, base)
	applied, err := s.SaveResult(ctx, ports.ResultOutcome{
		JobID: job.ID, WorkerID: "w1", Attempt: claimed[0].Attempt,
		Status: domain.JobDead, FinishedAt: base,
	})
	if err != nil || !applied {
		t.Fatalf("save dead: applied=%v err=%v", applied, err)
	}
	if err := s.RequeueJob(ctx, "t1", job.ID, base); err != nil {
		t.Fatal(err)
	}
	stored, _ := s.GetJob(ctx, "t1", job.ID)
	if stored.Status != domain.JobPending || stored.Attempt != 0 {
		t.Fatalf("requeued job: %+v", stored)
	}
	// Requeueing a non-dead job is a conflict.
	if err := s.RequeueJob(ctx, "t1", job.ID, base); err == nil {
		t.Fatal("expected conflict requeueing a pending job")
	}
}

func TestCreateTaskIdempotency(t *testing.T) {
	s := NewStore()
	ctx := context.Background()
	first, created1, err := s.CreateTask(ctx, makeTask("t1", "same-key", domain.PriorityNormal, base, 1))
	if err != nil || !created1 {
		t.Fatalf("first create: created=%v err=%v", created1, err)
	}
	second, created2, err := s.CreateTask(ctx, makeTask("t1", "same-key", domain.PriorityNormal, base, 1))
	if err != nil || created2 {
		t.Fatalf("replay create: created=%v err=%v", created2, err)
	}
	if first.ID != second.ID {
		t.Fatal("idempotent replay returned a different task")
	}
	// Same key, different tenant: a different task.
	_, created3, err := s.CreateTask(ctx, makeTask("t2", "same-key", domain.PriorityNormal, base, 1))
	if err != nil || !created3 {
		t.Fatalf("other tenant create: created=%v err=%v", created3, err)
	}
}

func TestTenantIsolation(t *testing.T) {
	s := NewStore()
	ctx := context.Background()
	task, _, _ := s.CreateTask(ctx, makeTask("t1", "k", domain.PriorityNormal, base, 1))
	if _, err := s.GetTask(ctx, "t2", task.ID); err == nil {
		t.Fatal("tenant t2 read tenant t1's task")
	}
}
