package tasksvc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/crypto"
	"github.com/Sravanthipanga/crobat/internal/adapters/memory"
	"github.com/Sravanthipanga/crobat/internal/domain"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newSvc(box *crypto.SecretBox) (*Service, *memory.Store) {
	store := memory.NewStore()
	return &Service{Tasks: store, Box: box, Now: func() time.Time { return base }}, store
}

func cronInput() CreateInput {
	return CreateInput{
		TenantID:       "tenant-1",
		IdempotencyKey: "key-1",
		Name:           "daily report",
		Schedule:       domain.Schedule{Kind: domain.ScheduleCron, CronExpr: "0 9 * * *", Timezone: "UTC"},
		Priority:       domain.PriorityHigh,
		Request:        domain.RequestSpec{Method: "POST", URL: "https://example.com/reports"},
		Retry:          domain.RetryPolicy{MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Minute},
	}
}

func TestCreateComputesNextRun(t *testing.T) {
	svc, _ := newSvc(nil)
	task, created, err := svc.Create(context.Background(), cronInput())
	if err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	want := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC) // base is Jan 1 12:00 UTC
	if task.NextRunAt == nil || !task.NextRunAt.Equal(want) {
		t.Fatalf("next_run_at = %v, want %s", task.NextRunAt, want)
	}
	if task.Status != domain.TaskActive {
		t.Fatalf("status = %s", task.Status)
	}
}

func TestCreateIdempotentReplay(t *testing.T) {
	svc, _ := newSvc(nil)
	ctx := context.Background()
	first, created1, err := svc.Create(ctx, cronInput())
	if err != nil || !created1 {
		t.Fatalf("first: created=%v err=%v", created1, err)
	}
	second, created2, err := svc.Create(ctx, cronInput())
	if err != nil || created2 {
		t.Fatalf("replay: created=%v err=%v", created2, err)
	}
	if first.ID != second.ID {
		t.Fatal("replay returned a different task id")
	}
	tasks, err := svc.List(ctx, "tenant-1")
	if err != nil || len(tasks) != 1 {
		t.Fatalf("list: %d tasks, err %v", len(tasks), err)
	}
}

func TestCreateRequiresIdempotencyKey(t *testing.T) {
	svc, _ := newSvc(nil)
	in := cronInput()
	in.IdempotencyKey = ""
	if _, _, err := svc.Create(context.Background(), in); err == nil {
		t.Fatal("expected validation error without Idempotency-Key")
	}
}

func TestCreateValidatesScheduleAndPriority(t *testing.T) {
	svc, _ := newSvc(nil)
	in := cronInput()
	in.Schedule.CronExpr = "bogus"
	if _, _, err := svc.Create(context.Background(), in); err == nil {
		t.Fatal("expected validation error for bad cron")
	}
	in = cronInput()
	in.Priority = "urgent-ish"
	if _, _, err := svc.Create(context.Background(), in); err == nil {
		t.Fatal("expected validation error for bad priority")
	}
}

func TestSecretsEncryptedAtRest(t *testing.T) {
	box, err := crypto.NewRandomSecretBox()
	if err != nil {
		t.Fatal(err)
	}
	svc, store := newSvc(box)
	in := cronInput()
	in.Request.Headers = map[string]string{"Authorization": "Bearer topsecret", "X-Plain": "visible"}
	in.Request.SecretHeaders = []string{"Authorization"}
	task, _, err := svc.Create(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.GetTask(context.Background(), "tenant-1", task.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := stored.Request.Headers["Authorization"]
	if !strings.HasPrefix(got, crypto.Prefix) {
		t.Fatalf("stored secret not encrypted: %q", got)
	}
	if strings.Contains(got, "topsecret") {
		t.Fatal("stored secret leaks plaintext")
	}
	if stored.Request.Headers["X-Plain"] != "visible" {
		t.Fatal("non-secret header was altered")
	}
	// The caller's input must not be mutated.
	if in.Request.Headers["Authorization"] != "Bearer topsecret" {
		t.Fatal("Create mutated the caller's request spec")
	}
}

func TestPauseResumeDelete(t *testing.T) {
	svc, _ := newSvc(nil)
	ctx := context.Background()
	task, _, err := svc.Create(ctx, cronInput())
	if err != nil {
		t.Fatal(err)
	}
	paused, err := svc.Pause(ctx, "tenant-1", task.ID)
	if err != nil || paused.Status != domain.TaskPaused {
		t.Fatalf("pause: %+v err %v", paused, err)
	}
	resumed, err := svc.Resume(ctx, "tenant-1", task.ID)
	if err != nil || resumed.Status != domain.TaskActive || resumed.NextRunAt == nil {
		t.Fatalf("resume: %+v err %v", resumed, err)
	}
	if err := svc.Delete(ctx, "tenant-1", task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(ctx, "tenant-1", task.ID); err == nil {
		t.Fatal("deleted task still readable")
	}
}
