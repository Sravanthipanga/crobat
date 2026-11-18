// Package executor is the Executor service. Executors claim due jobs in
// batches, wait on their own timers until each job's scheduled time, make
// the HTTP call, and save the result through the fencing-token guard.
//
// Delivery is at-least-once: if an executor dies mid-call, its lease
// expires and the Reconciler hands the job to another executor, which
// calls again with the SAME Idempotency-Key (= job id), so the tenant
// endpoint can dedupe the repeat.
package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/Sravanthipanga/crobat/internal/app/retry"
	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

// Service executes claimed jobs.
type Service struct {
	Jobs      ports.JobStore
	Caller    ports.Caller
	WorkerID  string
	BatchSize int           // jobs claimed per poll; default 32
	Lease     time.Duration // claim lease; default 2 minutes
	// ClaimAhead is how far before their scheduled time jobs may be
	// claimed; the executor then waits out the remainder on its own
	// timer. Matches the scheduler's materialization lookahead (30s), so
	// a burst due in the same second is already spread across executors
	// when the second arrives. Must stay well below Lease.
	ClaimAhead time.Duration
	Now        func() time.Time
	// Wait blocks until t (or ctx is done). Injectable for tests.
	Wait func(ctx context.Context, t time.Time) error
	// JitterSource supplies backoff jitter in [0,1); nil = math/rand.
	JitterSource func() float64
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Service) wait(ctx context.Context, t time.Time) error {
	if s.Wait != nil {
		return s.Wait(ctx, t)
	}
	d := time.Until(t)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// RunOnce claims one batch and executes every job in it. It returns the
// number of jobs whose results were saved.
func (s *Service) RunOnce(ctx context.Context) (int, error) {
	batch := s.BatchSize
	if batch <= 0 {
		batch = 32
	}
	lease := s.Lease
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	claimAhead := s.ClaimAhead
	if claimAhead <= 0 {
		claimAhead = 30 * time.Second
	}
	jobs, err := s.Jobs.ClaimJobs(ctx, s.WorkerID, batch, lease, s.now().UTC().Add(claimAhead))
	if err != nil {
		return 0, err
	}
	saved := 0
	for _, job := range jobs {
		if err := ctx.Err(); err != nil {
			return saved, err
		}
		if err := s.execute(ctx, job); err == nil {
			saved++
		}
	}
	return saved, nil
}

// execute waits for the job's scheduled time, calls, and saves the
// outcome. A rejected (stale) save is not an error: it means the lease
// was taken over and the new owner owns the result now.
func (s *Service) execute(ctx context.Context, job *domain.Job) error {
	if err := s.wait(ctx, job.ScheduledAt); err != nil {
		return err // lease will expire and the reconciler will rescue the job
	}
	status, callErr := s.Caller.Call(ctx, job, job.Request)
	now := s.now().UTC()

	outcome := ports.ResultOutcome{
		JobID:      job.ID,
		WorkerID:   s.WorkerID,
		Attempt:    job.Attempt, // fencing token from claim time
		HTTPStatus: status,
		FinishedAt: now,
	}
	switch {
	case callErr == nil:
		outcome.Status = domain.JobSucceeded
	default:
		outcome.Err = callErr.Error()
		policy := domain.RetryPolicy{
			MaxAttempts: job.MaxAttempts,
			BackoffBase: job.BackoffBase, // copied from the task at materialization
			BackoffMax:  job.BackoffMax,
		}
		if retry.Exhausted(policy, job.Attempt) {
			outcome.Status = domain.JobDead
		} else {
			outcome.Status = domain.JobPending
			outcome.RetryAt = now.Add(retry.Backoff(policy, job.Attempt, s.JitterSource))
		}
	}

	applied, err := s.Jobs.SaveResult(ctx, outcome)
	if err != nil {
		return fmt.Errorf("save result for job %s: %w", job.ID, err)
	}
	if !applied {
		return domain.ErrStaleClaim
	}
	return nil
}

// Run polls until the context is cancelled.
func (s *Service) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			_, _ = s.RunOnce(ctx)
		}
	}
}
