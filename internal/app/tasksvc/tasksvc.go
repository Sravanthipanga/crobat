// Package tasksvc is the Task Service core: create/edit/pause/delete of
// task definitions. The HTTP adapter lives in adapters/httpapi; this
// package owns validation, secret encryption and idempotent creation.
package tasksvc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/crypto"
	"github.com/Sravanthipanga/crobat/internal/app/cronx"
	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

// Service implements the task use cases.
type Service struct {
	Tasks ports.TaskStore
	Box   *crypto.SecretBox // may be nil: secrets then stored as given (local dev only)
	Now   func() time.Time  // injectable clock
}

// CreateInput is a validated task-creation request.
type CreateInput struct {
	TenantID       string
	IdempotencyKey string
	Name           string
	Schedule       domain.Schedule
	Priority       domain.Priority
	Request        domain.RequestSpec
	Timeout        time.Duration
	Retry          domain.RetryPolicy
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Create validates the input, encrypts secrets, computes the first
// next_run_at and stores the task. Replaying the same Idempotency-Key for
// the tenant returns the original task (created=false) — a retried POST
// never creates a duplicate task.
func (s *Service) Create(ctx context.Context, in CreateInput) (task *domain.Task, created bool, err error) {
	if err := validateCreate(in); err != nil {
		return nil, false, err
	}
	storedReq := in.Request
	if s.Box != nil {
		storedReq, err = crypto.EncryptSecrets(s.Box, in.Request)
		if err != nil {
			return nil, false, err
		}
	}
	now := s.now().UTC()
	next, err := cronx.NextRun(in.Schedule, now)
	if err != nil {
		return nil, false, err
	}
	retryPolicy := in.Retry
	if retryPolicy.MaxAttempts < 1 {
		retryPolicy.MaxAttempts = 1
	}
	t := &domain.Task{
		TenantID:       in.TenantID,
		Name:           in.Name,
		Schedule:       in.Schedule,
		Priority:       in.Priority,
		Request:        storedReq,
		Timeout:        in.Timeout,
		Retry:          retryPolicy,
		NextRunAt:      &next,
		Status:         domain.TaskActive,
		IdempotencyKey: in.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	return s.Tasks.CreateTask(ctx, t)
}

func validateCreate(in CreateInput) error {
	if in.TenantID == "" {
		return fmt.Errorf("%w: tenant is required", domain.ErrValidation)
	}
	if in.IdempotencyKey == "" {
		return fmt.Errorf("%w: Idempotency-Key header is required", domain.ErrValidation)
	}
	if strings.TrimSpace(in.Name) == "" {
		return fmt.Errorf("%w: name is required", domain.ErrValidation)
	}
	if !in.Priority.Valid() {
		return fmt.Errorf("%w: unknown priority %q", domain.ErrValidation, in.Priority)
	}
	if err := cronx.Validate(in.Schedule); err != nil {
		return err
	}
	method := strings.ToUpper(in.Request.Method)
	if method == "" {
		return fmt.Errorf("%w: request method is required", domain.ErrValidation)
	}
	if in.Request.URL == "" {
		return fmt.Errorf("%w: request url is required", domain.ErrValidation)
	}
	return nil
}

// Get returns one task owned by the tenant.
func (s *Service) Get(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	return s.Tasks.GetTask(ctx, tenantID, id)
}

// List returns the tenant's tasks.
func (s *Service) List(ctx context.Context, tenantID string) ([]*domain.Task, error) {
	return s.Tasks.ListTasks(ctx, tenantID)
}

// Pause stops a task from materializing new jobs (in-flight jobs finish).
func (s *Service) Pause(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	return s.setStatus(ctx, tenantID, id, domain.TaskPaused)
}

// Resume reactivates a paused task. Its next run is recomputed from now
// so a long-paused cron task does not burst missed runs.
func (s *Service) Resume(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	t, err := s.Tasks.GetTask(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	t.Status = domain.TaskActive
	next, err := cronx.NextRun(t.Schedule, s.now().UTC())
	if err != nil {
		return nil, err
	}
	t.NextRunAt = &next
	t.UpdatedAt = s.now().UTC()
	if err := s.Tasks.UpdateTask(ctx, t); err != nil {
		return nil, err
	}
	return t, nil
}

// Delete soft-deletes a task: no new jobs, existing history is kept.
func (s *Service) Delete(ctx context.Context, tenantID, id string) error {
	_, err := s.setStatus(ctx, tenantID, id, domain.TaskDeleted)
	return err
}

func (s *Service) setStatus(ctx context.Context, tenantID, id string, status domain.TaskStatus) (*domain.Task, error) {
	t, err := s.Tasks.GetTask(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	t.Status = status
	t.UpdatedAt = s.now().UTC()
	if err := s.Tasks.UpdateTask(ctx, t); err != nil {
		return nil, err
	}
	return t, nil
}
