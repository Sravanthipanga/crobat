// Package jobsvc is the Job Service core: job status, history reads, DLQ
// listing and manual retry. History reads merge the hot MySQL jobs table
// with the Cassandra archive, deduped by job id — a job appears exactly
// once whether it is still hot, already archived, or (briefly) both.
package jobsvc

import (
	"context"
	"sort"
	"time"

	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

// Service implements the job use cases.
type Service struct {
	Jobs    ports.JobStore
	History ports.HistoryStore
	Now     func() time.Time
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// GetJob returns one job owned by the tenant.
func (s *Service) GetJob(ctx context.Context, tenantID, id string) (*domain.Job, error) {
	return s.Jobs.GetJob(ctx, tenantID, id)
}

// ListJobs lists a tenant's jobs, optionally filtered by status.
func (s *Service) ListJobs(ctx context.Context, tenantID string, status domain.JobStatus, limit int) ([]*domain.Job, error) {
	return s.Jobs.ListJobs(ctx, tenantID, status, limit)
}

// ListDLQ lists the tenant's dead-letter queue.
func (s *Service) ListDLQ(ctx context.Context, tenantID string, limit int) ([]*domain.Job, error) {
	return s.Jobs.ListDLQ(ctx, tenantID, limit)
}

// Retry manually requeues a dead job.
func (s *Service) Retry(ctx context.Context, tenantID, id string) error {
	return s.Jobs.RequeueJob(ctx, tenantID, id, s.now().UTC())
}

// TaskHistory returns the merged run history for a task: finished jobs still
// in the hot table plus archived records, deduped by job id, newest first.
func (s *Service) TaskHistory(ctx context.Context, tenantID, taskID string, limit int) ([]domain.HistoryRecord, error) {
	byID := make(map[string]domain.HistoryRecord)

	archived, err := s.History.List(ctx, tenantID, taskID, 0)
	if err != nil {
		return nil, err
	}
	for _, rec := range archived {
		byID[rec.JobID] = rec
	}

	hot, err := s.Jobs.ListJobs(ctx, tenantID, "", 0)
	if err != nil {
		return nil, err
	}
	for _, j := range hot {
		if j.TaskID != taskID || !j.Terminal() {
			continue
		}
		if _, dup := byID[j.ID]; dup {
			continue // archived copy wins; the hot row is about to be deleted
		}
		fin := j.ScheduledAt
		if j.FinishedAt != nil {
			fin = *j.FinishedAt
		}
		byID[j.ID] = domain.HistoryRecord{
			JobID:       j.ID,
			TaskID:      j.TaskID,
			TenantID:    j.TenantID,
			ScheduledAt: j.ScheduledAt,
			FinishedAt:  fin,
			Status:      j.Status,
			Attempts:    j.Attempt,
			HTTPStatus:  j.HTTPStatus,
			LastError:   j.LastError,
		}
	}

	out := make([]domain.HistoryRecord, 0, len(byID))
	for _, rec := range byID {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].FinishedAt.Equal(out[j].FinishedAt) {
			return out[i].FinishedAt.After(out[j].FinishedAt)
		}
		return out[i].JobID < out[j].JobID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
