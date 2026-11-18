// Package archiver is the Archiver service: it moves finished jobs out of
// the hot MySQL table into the append-only Cassandra history. The order
// matters — history is written FIRST and the MySQL row deleted only after
// the write succeeds — so a crash mid-archival duplicates nothing and
// loses nothing (the job is simply archived again next sweep; the
// Cassandra write is keyed by job id and the Job Service dedupes reads).
package archiver

import (
	"context"
	"time"

	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

// Service runs the archival sweep.
type Service struct {
	Jobs      ports.JobStore
	History   ports.HistoryStore
	BatchSize int // default 500
}

// RunOnce archives one batch of finished jobs and returns how many were
// fully archived (history written + hot row deleted).
func (s *Service) RunOnce(ctx context.Context) (int, error) {
	batch := s.BatchSize
	if batch <= 0 {
		batch = 500
	}
	finished, err := s.Jobs.ListFinished(ctx, batch)
	if err != nil {
		return 0, err
	}
	archived := 0
	for _, job := range finished {
		rec := domain.HistoryRecord{
			JobID:       job.ID,
			TaskID:      job.TaskID,
			TenantID:    job.TenantID,
			ScheduledAt: job.ScheduledAt,
			Status:      job.Status,
			Attempts:    job.Attempt,
			HTTPStatus:  job.HTTPStatus,
			LastError:   job.LastError,
		}
		if job.FinishedAt != nil {
			rec.FinishedAt = *job.FinishedAt
		}
		if err := s.History.Append(ctx, rec); err != nil {
			return archived, err // stop: do NOT delete what we could not archive
		}
		if err := s.Jobs.DeleteJob(ctx, job.ID); err != nil {
			return archived, err
		}
		archived++
	}
	return archived, nil
}

// Run sweeps until the context is cancelled.
func (s *Service) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Minute
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
