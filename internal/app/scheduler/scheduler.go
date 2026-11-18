// Package scheduler is the Scheduler service: every tick it materializes
// jobs for tasks due in the next Lookahead window, in a single store
// transaction (INSERT jobs + advance next_run_at). Materializing slightly
// ahead of time lets executors claim jobs and wait on their own timers,
// so a burst of jobs due in the same second is spread across executors
// before the second arrives instead of being discovered during it.
package scheduler

import (
	"context"
	"time"

	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

// Service runs the materialization loop.
type Service struct {
	Store     ports.SchedulerStore
	Lookahead time.Duration // how far ahead to materialize; default 30s
	BatchSize int           // max tasks materialized per tick; default 1000
	Now       func() time.Time
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// RunOnce materializes one batch of due tasks and returns the jobs created.
func (s *Service) RunOnce(ctx context.Context) ([]*domain.Job, error) {
	lookahead := s.Lookahead
	if lookahead <= 0 {
		lookahead = 30 * time.Second
	}
	batch := s.BatchSize
	if batch <= 0 {
		batch = 1000
	}
	now := s.now().UTC()
	return s.Store.MaterializeDue(ctx, now, now.Add(lookahead), batch)
}

// Run ticks until the context is cancelled.
func (s *Service) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if _, err := s.RunOnce(ctx); err != nil {
				// A failed tick is retried on the next one; materialization
				// is idempotent per (task, next_run_at) advance.
				continue
			}
		}
	}
}
