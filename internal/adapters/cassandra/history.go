// Package cassandra implements ports.HistoryStore against Apache
// Cassandra via gocql.
//
// Data model (see migrations/cassandra.cql): table job_history with
// partition key (tenant_id, task_id, month) — exactly the access pattern
// of the Job Service ("history for this task") — clustered by
// finished_at DESC, job_id. Writes use a 30-day TTL: history is an
// operational debugging window, not an audit log, so it expires.
//
// This adapter needs a live Cassandra and is verified by compilation
// plus the integration tests behind the `integration` build tag. The
// in-memory History in adapters/memory implements the same contract and
// is fully unit-tested.
package cassandra

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/gocql/gocql"

	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

// HistoryTTLSeconds is the 30-day TTL applied to every history row.
const HistoryTTLSeconds = 30 * 24 * 60 * 60

// HistoryStore is a Cassandra-backed ports.HistoryStore.
type HistoryStore struct {
	session *gocql.Session
}

var _ ports.HistoryStore = (*HistoryStore)(nil)

// NewHistoryStore connects to a Cassandra cluster and keyspace.
func NewHistoryStore(hosts []string, keyspace string) (*HistoryStore, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("cassandra session: %w", err)
	}
	return &HistoryStore{session: session}, nil
}

// NewHistoryStoreFromSession wraps an existing session (integration tests).
func NewHistoryStoreFromSession(session *gocql.Session) *HistoryStore {
	return &HistoryStore{session: session}
}

// Close releases the session.
func (s *HistoryStore) Close() { s.session.Close() }

func monthOf(t time.Time) string { return t.UTC().Format("2006-01") }

// Append inserts one history row. The primary key includes job_id, so a
// retried append overwrites the same row — archival is idempotent.
func (s *HistoryStore) Append(ctx context.Context, rec domain.HistoryRecord) error {
	q := `INSERT INTO job_history
		(tenant_id, task_id, month, finished_at, job_id, scheduled_at, status, attempts, http_status, last_error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?`
	return s.session.Query(q,
		rec.TenantID, rec.TaskID, monthOf(rec.FinishedAt), rec.FinishedAt, rec.JobID,
		rec.ScheduledAt, string(rec.Status), rec.Attempts, rec.HTTPStatus, rec.LastError,
		HistoryTTLSeconds,
	).WithContext(ctx).Exec()
}

// List reads the current and previous month partitions for the task
// (history lookups are recent by nature, and rows expire after 30 days),
// merges them newest-first and applies the limit.
func (s *HistoryStore) List(ctx context.Context, tenantID, taskID string, limit int) ([]domain.HistoryRecord, error) {
	now := time.Now().UTC()
	months := []string{monthOf(now), monthOf(now.AddDate(0, -1, 0))}
	var out []domain.HistoryRecord
	for _, month := range months {
		q := `SELECT tenant_id, task_id, finished_at, job_id, scheduled_at, status, attempts, http_status, last_error
			FROM job_history WHERE tenant_id = ? AND task_id = ? AND month = ?`
		iter := s.session.Query(q, tenantID, taskID, month).WithContext(ctx).Iter()
		for {
			var rec domain.HistoryRecord
			var status string
			if !iter.Scan(&rec.TenantID, &rec.TaskID, &rec.FinishedAt, &rec.JobID,
				&rec.ScheduledAt, &status, &rec.Attempts, &rec.HTTPStatus, &rec.LastError) {
				break
			}
			rec.Status = domain.JobStatus(status)
			out = append(out, rec)
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FinishedAt.After(out[j].FinishedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
