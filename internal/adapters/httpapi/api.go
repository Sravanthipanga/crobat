// Package httpapi exposes the Task Service and Job Service as REST APIs.
// Every route requires a tenant JWT (see adapters/jwtauth); the tenant id
// comes from the token, never from the request, and scopes all data.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Sravanthipanga/crobat/internal/app/jobsvc"
	"github.com/Sravanthipanga/crobat/internal/app/tasksvc"
	"github.com/Sravanthipanga/crobat/internal/domain"
)

// TokenVerifier is satisfied by adapters/jwtauth.Verifier.
type TokenVerifier interface {
	Verify(token string) (tenantID string, err error)
}

type ctxKey string

const tenantKey ctxKey = "tenant"

// Server holds the API dependencies. TaskSvc may be nil on a Job Service
// process and vice versa; routes for a nil service are not registered.
type Server struct {
	Verifier TokenVerifier
	TaskSvc  *tasksvc.Service
	JobSvc   *jobsvc.Service
}

// Handler returns the full route tree.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	if s.TaskSvc != nil {
		mux.HandleFunc("POST /v1/tasks", s.createTask)
		mux.HandleFunc("GET /v1/tasks", s.listTasks)
		mux.HandleFunc("GET /v1/tasks/{id}", s.getTask)
		mux.HandleFunc("POST /v1/tasks/{id}/pause", s.pauseTask)
		mux.HandleFunc("POST /v1/tasks/{id}/resume", s.resumeTask)
		mux.HandleFunc("DELETE /v1/tasks/{id}", s.deleteTask)
	}
	if s.JobSvc != nil {
		mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
		mux.HandleFunc("GET /v1/jobs", s.listJobs)
		mux.HandleFunc("GET /v1/dlq", s.listDLQ)
		mux.HandleFunc("POST /v1/jobs/{id}/retry", s.retryJob)
		mux.HandleFunc("GET /v1/tasks/{id}/history", s.taskHistory)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return s.auth(mux)
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		authz := r.Header.Get("Authorization")
		if !strings.HasPrefix(authz, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		tenantID, err := s.Verifier.Verify(strings.TrimPrefix(authz, "Bearer "))
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		ctx := context.WithValue(r.Context(), tenantKey, tenantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func tenantOf(r *http.Request) string {
	v, _ := r.Context().Value(tenantKey).(string)
	return v
}

// --- wire types ---

type scheduleJSON struct {
	Kind     string    `json:"kind"`         // now | at | cron
	At       time.Time `json:"at,omitempty"` // for kind=at
	CronExpr string    `json:"cron_expr,omitempty"`
	Timezone string    `json:"timezone,omitempty"`
}

type requestJSON struct {
	Method        string            `json:"method"`
	URL           string            `json:"url"`
	Headers       map[string]string `json:"headers,omitempty"`
	Body          string            `json:"body,omitempty"`
	SecretHeaders []string          `json:"secret_headers,omitempty"`
}

type createTaskJSON struct {
	Name            string       `json:"name"`
	Schedule        scheduleJSON `json:"schedule"`
	Priority        string       `json:"priority"`
	Request         requestJSON  `json:"request"`
	TimeoutSeconds  int          `json:"timeout_seconds,omitempty"`
	MaxAttempts     int          `json:"max_attempts,omitempty"`
	BackoffBaseSecs int          `json:"backoff_base_seconds,omitempty"`
	BackoffMaxSecs  int          `json:"backoff_max_seconds,omitempty"`
}

type taskJSON struct {
	ID              string       `json:"id"`
	TenantID        string       `json:"tenant_id"`
	Name            string       `json:"name"`
	Schedule        scheduleJSON `json:"schedule"`
	Priority        string       `json:"priority"`
	Request         requestJSON  `json:"request"`
	TimeoutSeconds  int          `json:"timeout_seconds"`
	MaxAttempts     int          `json:"max_attempts"`
	BackoffBaseSecs int          `json:"backoff_base_seconds"`
	BackoffMaxSecs  int          `json:"backoff_max_seconds"`
	NextRunAt       *time.Time   `json:"next_run_at,omitempty"`
	Status          string       `json:"status"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
}

func taskToJSON(t *domain.Task) taskJSON {
	return taskJSON{
		ID:       t.ID,
		TenantID: t.TenantID,
		Name:     t.Name,
		Schedule: scheduleJSON{
			Kind: string(t.Schedule.Kind), At: t.Schedule.At,
			CronExpr: t.Schedule.CronExpr, Timezone: t.Schedule.Timezone,
		},
		Priority: string(t.Priority),
		Request: requestJSON{
			Method: t.Request.Method, URL: t.Request.URL,
			Headers: redactSecrets(t), Body: t.Request.Body,
			SecretHeaders: t.Request.SecretHeaders,
		},
		TimeoutSeconds:  int(t.Timeout.Seconds()),
		MaxAttempts:     t.Retry.MaxAttempts,
		BackoffBaseSecs: int(t.Retry.BackoffBase.Seconds()),
		BackoffMaxSecs:  int(t.Retry.BackoffMax.Seconds()),
		NextRunAt:       t.NextRunAt,
		Status:          string(t.Status),
		CreatedAt:       t.CreatedAt,
		UpdatedAt:       t.UpdatedAt,
	}
}

// redactSecrets never returns secret values over the API: stored
// ciphertext is replaced with a marker (plaintext is never stored, but
// the marker makes the contract explicit).
func redactSecrets(t *domain.Task) map[string]string {
	out := make(map[string]string, len(t.Request.Headers))
	secrets := make(map[string]bool, len(t.Request.SecretHeaders))
	for _, name := range t.Request.SecretHeaders {
		secrets[name] = true
	}
	for k, v := range t.Request.Headers {
		if secrets[k] {
			out[k] = "[encrypted]"
		} else {
			out[k] = v
		}
	}
	return out
}

type jobJSON struct {
	ID          string     `json:"id"`
	TaskID      string     `json:"task_id"`
	TenantID    string     `json:"tenant_id"`
	Priority    string     `json:"priority"`
	ScheduledAt time.Time  `json:"scheduled_at"`
	Status      string     `json:"status"`
	Attempt     int        `json:"attempt"`
	HTTPStatus  int        `json:"http_status,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

func jobToJSON(j *domain.Job) jobJSON {
	return jobJSON{
		ID: j.ID, TaskID: j.TaskID, TenantID: j.TenantID,
		Priority: string(j.Priority), ScheduledAt: j.ScheduledAt,
		Status: string(j.Status), Attempt: j.Attempt,
		HTTPStatus: j.HTTPStatus, LastError: j.LastError,
		FinishedAt: j.FinishedAt,
	}
}

type historyJSON struct {
	JobID       string    `json:"job_id"`
	TaskID      string    `json:"task_id"`
	ScheduledAt time.Time `json:"scheduled_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	HTTPStatus  int       `json:"http_status,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
}

// --- task handlers ---

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	var body createTaskJSON
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	priority := domain.Priority(body.Priority)
	if body.Priority == "" {
		priority = domain.PriorityNormal
	}
	task, created, err := s.TaskSvc.Create(r.Context(), tasksvc.CreateInput{
		TenantID:       tenantOf(r),
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Name:           body.Name,
		Schedule: domain.Schedule{
			Kind:     domain.ScheduleKind(body.Schedule.Kind),
			At:       body.Schedule.At,
			CronExpr: body.Schedule.CronExpr,
			Timezone: body.Schedule.Timezone,
		},
		Priority: priority,
		Request: domain.RequestSpec{
			Method:        body.Request.Method,
			URL:           body.Request.URL,
			Headers:       body.Request.Headers,
			Body:          body.Request.Body,
			SecretHeaders: body.Request.SecretHeaders,
		},
		Timeout: time.Duration(body.TimeoutSeconds) * time.Second,
		Retry: domain.RetryPolicy{
			MaxAttempts: body.MaxAttempts,
			BackoffBase: time.Duration(body.BackoffBaseSecs) * time.Second,
			BackoffMax:  time.Duration(body.BackoffMaxSecs) * time.Second,
		},
	})
	if err != nil {
		writeDomainError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, taskToJSON(task))
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.TaskSvc.List(r.Context(), tenantOf(r))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	out := make([]taskJSON, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, taskToJSON(t))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.TaskSvc.Get(r.Context(), tenantOf(r), r.PathValue("id"))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, taskToJSON(task))
}

func (s *Server) pauseTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.TaskSvc.Pause(r.Context(), tenantOf(r), r.PathValue("id"))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, taskToJSON(task))
}

func (s *Server) resumeTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.TaskSvc.Resume(r.Context(), tenantOf(r), r.PathValue("id"))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, taskToJSON(task))
}

func (s *Server) deleteTask(w http.ResponseWriter, r *http.Request) {
	if err := s.TaskSvc.Delete(r.Context(), tenantOf(r), r.PathValue("id")); err != nil {
		writeDomainError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- job handlers ---

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.JobSvc.GetJob(r.Context(), tenantOf(r), r.PathValue("id"))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobToJSON(job))
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.JobSvc.ListJobs(r.Context(), tenantOf(r),
		domain.JobStatus(r.URL.Query().Get("status")), limitParam(r))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobsToJSON(jobs))
}

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.JobSvc.ListDLQ(r.Context(), tenantOf(r), limitParam(r))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobsToJSON(jobs))
}

func (s *Server) retryJob(w http.ResponseWriter, r *http.Request) {
	if err := s.JobSvc.Retry(r.Context(), tenantOf(r), r.PathValue("id")); err != nil {
		writeDomainError(w, err)
		return
	}
	job, err := s.JobSvc.GetJob(r.Context(), tenantOf(r), r.PathValue("id"))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobToJSON(job))
}

func (s *Server) taskHistory(w http.ResponseWriter, r *http.Request) {
	records, err := s.JobSvc.TaskHistory(r.Context(), tenantOf(r), r.PathValue("id"), limitParam(r))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	out := make([]historyJSON, 0, len(records))
	for _, rec := range records {
		out = append(out, historyJSON{
			JobID: rec.JobID, TaskID: rec.TaskID,
			ScheduledAt: rec.ScheduledAt, FinishedAt: rec.FinishedAt,
			Status: string(rec.Status), Attempts: rec.Attempts,
			HTTPStatus: rec.HTTPStatus, LastError: rec.LastError,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func jobsToJSON(jobs []*domain.Job) []jobJSON {
	out := make([]jobJSON, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobToJSON(j))
	}
	return out
}

func limitParam(r *http.Request) int {
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 {
		return 100
	}
	if n > 1000 {
		return 1000
	}
	return n
}

// --- responses ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, domain.ErrValidation):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, domain.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}
