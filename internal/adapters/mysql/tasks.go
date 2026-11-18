package mysql

import (
	"context"
	"database/sql"

	"github.com/Sravanthipanga/crobat/internal/domain"
)

const taskColumns = `id, tenant_id, name, schedule_kind, run_at, cron_expr, timezone,
	priority, req_method, req_url, req_headers, req_body, secret_headers,
	timeout_ms, max_attempts, backoff_base_ms, backoff_max_ms,
	next_run_at, status, idempotency_key, created_at, updated_at`

func scanTask(row interface{ Scan(...any) error }) (*domain.Task, error) {
	var (
		t         domain.Task
		kind      string
		runAt     sql.NullTime
		cronExpr  string
		tz        string
		headers   string
		secrets   string
		nextRunAt sql.NullTime
		status    string
		timeoutMs int64
		backoffB  int64
		backoffM  int64
	)
	err := row.Scan(
		&t.ID, &t.TenantID, &t.Name, &kind, &runAt, &cronExpr, &tz,
		&t.Priority, &t.Request.Method, &t.Request.URL, &headers, &t.Request.Body, &secrets,
		&timeoutMs, &t.Retry.MaxAttempts, &backoffB, &backoffM,
		&nextRunAt, &status, &t.IdempotencyKey, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	t.Schedule = domain.Schedule{Kind: domain.ScheduleKind(kind), CronExpr: cronExpr, Timezone: tz}
	if runAt.Valid {
		t.Schedule.At = runAt.Time
	}
	t.Request.Headers, err = unmarshalHeaders(headers)
	if err != nil {
		return nil, err
	}
	t.Request.SecretHeaders, err = unmarshalStrings(secrets)
	if err != nil {
		return nil, err
	}
	t.Timeout = fromMs(timeoutMs)
	t.Retry.BackoffBase = fromMs(backoffB)
	t.Retry.BackoffMax = fromMs(backoffM)
	if nextRunAt.Valid {
		nr := nextRunAt.Time
		t.NextRunAt = &nr
	}
	t.Status = domain.TaskStatus(status)
	return &t, nil
}

func taskArgs(t *domain.Task) ([]any, error) {
	headers, err := marshalHeaders(t.Request.Headers)
	if err != nil {
		return nil, err
	}
	secrets, err := marshalStrings(t.Request.SecretHeaders)
	if err != nil {
		return nil, err
	}
	var runAt any
	if t.Schedule.Kind == domain.ScheduleAt {
		runAt = t.Schedule.At
	}
	var nextRun any
	if t.NextRunAt != nil {
		nextRun = *t.NextRunAt
	}
	return []any{
		t.ID, t.TenantID, t.Name, string(t.Schedule.Kind), runAt, t.Schedule.CronExpr, t.Schedule.Timezone,
		string(t.Priority), t.Request.Method, t.Request.URL, headers, t.Request.Body, secrets,
		ms(t.Timeout), t.Retry.MaxAttempts, ms(t.Retry.BackoffBase), ms(t.Retry.BackoffMax),
		nextRun, string(t.Status), t.IdempotencyKey, t.CreatedAt, t.UpdatedAt,
	}, nil
}

// CreateTask inserts the task; a duplicate (tenant_id, idempotency_key)
// returns the original row with created=false.
func (s *Store) CreateTask(ctx context.Context, task *domain.Task) (*domain.Task, bool, error) {
	stored := task.Clone()
	if stored.ID == "" {
		stored.ID = newID()
	}
	args, err := taskArgs(stored)
	if err != nil {
		return nil, false, err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO tasks (`+
		`id, tenant_id, name, schedule_kind, run_at, cron_expr, timezone,
		 priority, req_method, req_url, req_headers, req_body, secret_headers,
		 timeout_ms, max_attempts, backoff_base_ms, backoff_max_ms,
		 next_run_at, status, idempotency_key, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, args...)
	if err != nil {
		if isDuplicateKey(err) && stored.IdempotencyKey != "" {
			existing, getErr := s.getTaskByIdempotencyKey(ctx, stored.TenantID, stored.IdempotencyKey)
			if getErr != nil {
				return nil, false, getErr
			}
			return existing, false, nil
		}
		return nil, false, err
	}
	return stored, true, nil
}

func (s *Store) getTaskByIdempotencyKey(ctx context.Context, tenantID, key string) (*domain.Task, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE tenant_id = ? AND idempotency_key = ?`, tenantID, key)
	t, err := scanTask(row)
	if err == sql.ErrNoRows {
		return nil, domain.ErrNotFound
	}
	return t, err
}

func (s *Store) GetTask(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE tenant_id = ? AND id = ? AND status <> 'deleted'`, tenantID, id)
	t, err := scanTask(row)
	if err == sql.ErrNoRows {
		return nil, domain.ErrNotFound
	}
	return t, err
}

func (s *Store) UpdateTask(ctx context.Context, task *domain.Task) error {
	args, err := taskArgs(task)
	if err != nil {
		return err
	}
	// id and tenant_id are the WHERE keys; drop them from the SET list by
	// re-binding: the statement below sets every mutable column.
	res, err := s.db.ExecContext(ctx, `UPDATE tasks SET
		name = ?, schedule_kind = ?, run_at = ?, cron_expr = ?, timezone = ?,
		priority = ?, req_method = ?, req_url = ?, req_headers = ?, req_body = ?, secret_headers = ?,
		timeout_ms = ?, max_attempts = ?, backoff_base_ms = ?, backoff_max_ms = ?,
		next_run_at = ?, status = ?, updated_at = ?
		WHERE tenant_id = ? AND id = ?`,
		args[2], args[3], args[4], args[5], args[6],
		args[7], args[8], args[9], args[10], args[11], args[12],
		args[13], args[14], args[15], args[16],
		args[17], args[18], args[21],
		task.TenantID, task.ID)
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

func (s *Store) ListTasks(ctx context.Context, tenantID string) ([]*domain.Task, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE tenant_id = ? AND status <> 'deleted' ORDER BY created_at ASC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
