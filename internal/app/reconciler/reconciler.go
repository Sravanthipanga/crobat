// Package reconciler is the Reconciler service: it rescues jobs whose
// executor died (or stalled) while holding them. A running job whose
// lease has expired goes back to pending for another executor, or to the
// DLQ if its attempt budget is exhausted. This is what makes executor
// crashes safe under at-least-once delivery.
package reconciler

import (
	"context"
	"time"

	"github.com/Sravanthipanga/crobat/internal/ports"
)

// Service runs the lease-expiry sweep.
type Service struct {
	Jobs ports.JobStore
	Now  func() time.Time
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// RunOnce performs one sweep and returns (requeued, dead) counts.
func (s *Service) RunOnce(ctx context.Context) (int, int, error) {
	return s.Jobs.ExpireLeases(ctx, s.now().UTC())
}

// Run sweeps until the context is cancelled.
func (s *Service) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			_, _, _ = s.RunOnce(ctx)
		}
	}
}
