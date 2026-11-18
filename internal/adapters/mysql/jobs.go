package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Sravanthipanga/crobat/internal/domain"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

const jobColumns = `id, task_id, tenant_id, priority,
	req_method, req_url, req_headers, req_body, secret_headers,
	max_attempts, backoff_base_ms, backoff_max_ms, timeout_ms,
	scheduled_at, status, attempt, worker_id, lease_expires_at,
	http_status, last_error, created_at, finished_at`

func scanJob(row interface{ Scan(...any) error }) (*domain.Job, error) {
	var (
		j          domain.Job
		headers    string
		secrets    string
		backoffB   int64
		backoffM   int64
		timeoutMs  int64
		status     string
		workerID   sql.NullString
		leaseExp   sql.NullTime
		httpStatus sql.NullInt64
		finishedAt sql.NullTime
	)
	err := row.Scan(
		&j.ID, &j.TaskID, &j.TenantID, &j.Priority,
		&j.Request.Method, &j.Request.URL, &headers, &j.Request.Body, &secrets,
		&j.MaxAttempts, &backoffB, &backoffM, &timeoutMs,
		&j.ScheduledAt, &status, &j.Attempt, &workerID, &leaseExp,
		&httpStatus, &j.LastError, &j.CreatedAt, &finishedAt,
	)
	if err != nil {
		return nil, err
	}
	j.Request.Headers, err = unmarshalHeaders(headers)
	if err != nil {
		return nil, err
	}
	j.Request.SecretHeaders, err = unmarshalStrings(secrets)
	if err != nil {
		return nil, err
	}
	j.BackoffBase = fromMs(backoffB)
	j.BackoffMax = fromMs(backoffM)
	j.Timeout = fromMs(timeoutMs)
	j.Status = domain.JobStatus(status)
	if workerID.Valid {
		j.WorkerID = workerID.String
	}
	if leaseExp.Valid {
		le := leaseExp.Time
		j.LeaseExpiresAt = &le
	}
	if httpStatus.Valid {
		j.HTTPStatus = int(httpStatus.Int64)
	}
	if finishedAt.Valid {
		f := finishedAt.Time
		j.FinishedAt = &f
	}
	return &j, nil
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// ClaimJobs claims due pending jobs for workerID.
//
// Fairness: the candidate set is ranked with ROW_NUMBER() OVER
// (PARTITION BY tenant_id ORDER BY priority, scheduled_at) and taken in
// round-robin order (rn 1 of every tenant, then rn 2, ...), so one
// tenant's burst cannot starve the others.
//
// Concurrency: candidates are selected FOR UPDATE SKIP LOCKED inside a
// transaction, so concurrent executors walk past rows another executor
// is claiming instead of blocking on them. Candidate ids are sorted
// before the UPDATE so concurrent claims lock rows in a consistent
// order — the classic fix for claim deadlocks.
func (s *Store) ClaimJobs(ctx context.Context, workerID string, limit int, lease time.Duration, now time.Time) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 1
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id FROM (
			SELECT id, priority_rank, scheduled_at,
			       ROW_NUMBER() OVER (PARTITION BY tenant_id
			                          ORDER BY priority_rank ASC, scheduled_at ASC, id ASC) AS rn
			FROM jobs
			WHERE status = 'pending' AND scheduled_at <= ?
		) ranked
		ORDER BY rn ASC, priority_rank ASC, scheduled_at ASC
		LIMIT ?
		FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(ids) == 0 {
		return nil, tx.Commit()
	}
	sort.Strings(ids) // consistent lock ordering across concurrent claims

	leaseExp := now.Add(lease)
	if _, err := tx.ExecContext(ctx,
		`UPDATE jobs SET status = 'running', worker_id = ?, attempt = attempt + 1, lease_expires_at = ?
		 WHERE id IN (`+placeholders(len(ids))+`) AND status = 'pending'`,
		append([]any{workerID, leaseExp}, stringsToAny(ids)...)...); err != nil {
		return nil, err
	}

	claimed, err := queryJobs(ctx, tx,
		`SELECT `+jobColumns+` FROM jobs WHERE id IN (`+placeholders(len(ids))+`) AND worker_id = ? AND status = 'running'`,
		append(stringsToAny(ids), workerID)...)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func stringsToAny(in []string) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func queryJobs(ctx context.Context, q querier, query string, args ...any) ([]*domain.Job, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// SaveResult is the fencing-token guarded update: the row must still be
// running AND owned by (worker_id, attempt) from claim time. Zero rows
// affected means a stale executor lost its lease — its result is dropped.
func (s *Store) SaveResult(ctx context.Context, outcome ports.ResultOutcome) (bool, error) {
	var (
		res sql.Result
		err error
	)
	switch outcome.Status {
	case domain.JobSucceeded, domain.JobDead:
		res, err = s.db.ExecContext(ctx, `
			UPDATE jobs SET status = ?, http_status = ?, last_error = ?, finished_at = ?,
			       worker_id = NULL, lease_expires_at = NULL
			WHERE id = ? AND status = 'running' AND worker_id = ? AND attempt = ?`,
			string(outcome.Status), outcome.HTTPStatus, outcome.Err, outcome.FinishedAt,
			outcome.JobID, outcome.WorkerID, outcome.Attempt)
	case domain.JobPending:
		res, err = s.db.ExecContext(ctx, `
			UPDATE jobs SET status = 'pending', http_status = ?, last_error = ?, scheduled_at = ?,
			       worker_id = NULL, lease_expires_at = NULL, finished_at = NULL
			WHERE id = ? AND status = 'running' AND worker_id = ? AND attempt = ?`,
			outcome.HTTPStatus, outcome.Err, outcome.RetryAt,
			outcome.JobID, outcome.WorkerID, outcome.Attempt)
	default:
		return false, fmt.Errorf("%w: cannot save result with status %q", domain.ErrValidation, outcome.Status)
	}
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *Store) GetJob(ctx context.Context, tenantID, id string) (*domain.Job, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE tenant_id = ? AND id = ?`, tenantID, id)
	j, err := scanJob(row)
	if err == sql.ErrNoRows {
		return nil, domain.ErrNotFound
	}
	return j, err
}

func (s *Store) ListJobs(ctx context.Context, tenantID string, status domain.JobStatus, limit int) ([]*domain.Job, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs WHERE tenant_id = ?`
	args := []any{tenantID}
	if status != "" {
		query += ` AND status = ?`
		args = append(args, string(status))
	}
	query += ` ORDER BY scheduled_at DESC, id ASC`
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	return queryJobs(ctx, s.db, query, args...)
}

func (s *Store) ListDLQ(ctx context.Context, tenantID string, limit int) ([]*domain.Job, error) {
	return s.ListJobs(ctx, tenantID, domain.JobDead, limit)
}

func (s *Store) RequeueJob(ctx context.Context, tenantID, id string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET status = 'pending', attempt = 0, scheduled_at = ?, last_error = '', finished_at = NULL
		WHERE tenant_id = ? AND id = ? AND status = 'dead'`, now, tenantID, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		// Distinguish "no such job" from "not dead".
		if _, err := s.GetJob(ctx, tenantID, id); err != nil {
			return err
		}
		return fmt.Errorf("%w: only dead jobs can be requeued", domain.ErrConflict)
	}
	return nil
}

// ExpireLeases performs the reconciler sweep as two guarded UPDATEs.
func (s *Store) ExpireLeases(ctx context.Context, now time.Time) (int, int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	deadRes, err := tx.ExecContext(ctx, `
		UPDATE jobs SET status = 'dead', finished_at = ?, worker_id = NULL, lease_expires_at = NULL,
		       last_error = CONCAT(last_error, '; lease expired with attempts exhausted')
		WHERE status = 'running' AND lease_expires_at < ? AND attempt >= max_attempts`, now, now)
	if err != nil {
		return 0, 0, err
	}
	requeueRes, err := tx.ExecContext(ctx, `
		UPDATE jobs SET status = 'pending', scheduled_at = ?, worker_id = NULL, lease_expires_at = NULL
		WHERE status = 'running' AND lease_expires_at < ? AND attempt < max_attempts`, now, now)
	if err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	dead, _ := deadRes.RowsAffected()
	requeued, _ := requeueRes.RowsAffected()
	return int(requeued), int(dead), nil
}

func (s *Store) ListFinished(ctx context.Context, limit int) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 500
	}
	return queryJobs(ctx, s.db,
		`SELECT `+jobColumns+` FROM jobs WHERE status IN ('succeeded','dead') ORDER BY finished_at ASC, id ASC LIMIT ?`, limit)
}

func (s *Store) DeleteJob(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return domain.ErrNotFound
	}
	return nil
}
