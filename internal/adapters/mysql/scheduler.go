package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/Sravanthipanga/crobat/internal/app/cronx"
	"github.com/Sravanthipanga/crobat/internal/domain"
)

// MaterializeDue is the scheduler transaction — the heart of the design.
//
// In ONE transaction it:
//  1. locks the due task rows (ORDER BY id: a consistent lock order, so
//     two scheduler replicas cannot deadlock each other),
//  2. INSERTs one pending job row per due task into the jobs table,
//  3. advances each task's next_run_at (or clears it for one-shot tasks).
//
// Because steps 2 and 3 commit atomically, the schedule and the queue can
// never disagree — unlike a design that writes a DB row and then
// publishes to a separate broker, where a crash between the two steps
// loses or duplicates the job forever.
func (s *Store) MaterializeDue(ctx context.Context, now, horizon time.Time, limit int) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 1000
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		`SELECT `+taskColumns+` FROM tasks
		 WHERE status = 'active' AND next_run_at IS NOT NULL AND next_run_at <= ?
		 ORDER BY id ASC
		 LIMIT ?
		 FOR UPDATE`, horizon, limit)
	if err != nil {
		return nil, err
	}
	var due []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		due = append(due, t)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	var created []*domain.Job
	for _, t := range due {
		scheduledFor := *t.NextRunAt
		job := &domain.Job{
			ID:          newID(),
			TaskID:      t.ID,
			TenantID:    t.TenantID,
			Priority:    t.Priority,
			Request:     t.Request.Clone(),
			MaxAttempts: t.Retry.MaxAttempts,
			BackoffBase: t.Retry.BackoffBase,
			BackoffMax:  t.Retry.BackoffMax,
			Timeout:     t.Timeout,
			ScheduledAt: scheduledFor,
			Status:      domain.JobPending,
			CreatedAt:   now,
		}
		if job.MaxAttempts < 1 {
			job.MaxAttempts = 1
		}
		if err := insertJob(ctx, tx, job); err != nil {
			return nil, err
		}
		created = append(created, job)

		var nextRun any
		if !t.Schedule.OneShot() {
			next, err := cronx.Advance(t.Schedule, scheduledFor, now)
			if err != nil {
				return nil, err
			}
			nextRun = next
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE tasks SET next_run_at = ?, updated_at = ? WHERE id = ?`,
			nextRun, now, t.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return created, nil
}

func insertJob(ctx context.Context, tx *sql.Tx, j *domain.Job) error {
	headers, err := marshalHeaders(j.Request.Headers)
	if err != nil {
		return err
	}
	secrets, err := marshalStrings(j.Request.SecretHeaders)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO jobs (`+jobColumnsInsert+`)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.TaskID, j.TenantID, string(j.Priority), j.Priority.Rank(),
		j.Request.Method, j.Request.URL, headers, j.Request.Body, secrets,
		j.MaxAttempts, ms(j.BackoffBase), ms(j.BackoffMax), ms(j.Timeout),
		j.ScheduledAt, string(j.Status), j.Attempt, j.CreatedAt)
	return err
}

const jobColumnsInsert = `id, task_id, tenant_id, priority, priority_rank,
	req_method, req_url, req_headers, req_body, secret_headers,
	max_attempts, backoff_base_ms, backoff_max_ms, timeout_ms,
	scheduled_at, status, attempt, created_at`
