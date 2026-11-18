package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Sravanthipanga/crobat/internal/adapters/jwtauth"
	"github.com/Sravanthipanga/crobat/internal/adapters/memory"
	"github.com/Sravanthipanga/crobat/internal/app/jobsvc"
	"github.com/Sravanthipanga/crobat/internal/app/tasksvc"
)

func newTestServer(t *testing.T) (http.Handler, *jwtauth.Verifier) {
	t.Helper()
	store := memory.NewStore()
	verifier := jwtauth.NewVerifier("test-secret")
	server := &Server{
		Verifier: verifier,
		TaskSvc:  &tasksvc.Service{Tasks: store},
		JobSvc:   &jobsvc.Service{Jobs: store, History: memory.NewHistory()},
	}
	return server.Handler(), verifier
}

func tokenFor(t *testing.T, v *jwtauth.Verifier, tenant string) string {
	t.Helper()
	tok, err := v.Sign(tenant)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func doReq(t *testing.T, h http.Handler, method, path, token string, headers map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

const taskBody = `{
	"name": "daily report",
	"schedule": {"kind": "cron", "cron_expr": "0 9 * * *", "timezone": "America/New_York"},
	"priority": "high",
	"request": {"method": "POST", "url": "https://example.com/reports",
	            "headers": {"Authorization": "Bearer abc"}, "secret_headers": ["Authorization"]},
	"max_attempts": 3
}`

func TestCreateTaskRequiresAuth(t *testing.T) {
	h, _ := newTestServer(t)
	rec := doReq(t, h, "POST", "/v1/tasks", "", map[string]string{"Idempotency-Key": "k1"}, taskBody)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestCreateTaskRequiresIdempotencyKey(t *testing.T) {
	h, v := newTestServer(t)
	rec := doReq(t, h, "POST", "/v1/tasks", tokenFor(t, v, "tenant-1"), nil, taskBody)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (missing Idempotency-Key)", rec.Code)
	}
}

func TestCreateTaskAndIdempotentReplay(t *testing.T) {
	h, v := newTestServer(t)
	tok := tokenFor(t, v, "tenant-1")
	hdr := map[string]string{"Idempotency-Key": "order-123"}

	rec := doReq(t, h, "POST", "/v1/tasks", tok, hdr, taskBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body %s", rec.Code, rec.Body)
	}
	var created taskJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.NextRunAt == nil {
		t.Fatalf("created task: %+v", created)
	}
	// Secret values are never returned by the API.
	if created.Request.Headers["Authorization"] != "[encrypted]" {
		t.Fatalf("secret header leaked: %q", created.Request.Headers["Authorization"])
	}

	replay := doReq(t, h, "POST", "/v1/tasks", tok, hdr, taskBody)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d, want 200", replay.Code)
	}
	var replayed taskJSON
	if err := json.Unmarshal(replay.Body.Bytes(), &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.ID != created.ID {
		t.Fatal("idempotent replay created a second task")
	}
}

func TestTenantIsolationOverHTTP(t *testing.T) {
	h, v := newTestServer(t)
	tokA := tokenFor(t, v, "tenant-a")
	tokB := tokenFor(t, v, "tenant-b")
	rec := doReq(t, h, "POST", "/v1/tasks", tokA, map[string]string{"Idempotency-Key": "k"}, taskBody)
	var created taskJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	rec = doReq(t, h, "GET", "/v1/tasks/"+created.ID, tokB, nil, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant read status = %d, want 404", rec.Code)
	}
	rec = doReq(t, h, "GET", "/v1/tasks/"+created.ID, tokA, nil, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("owner read status = %d", rec.Code)
	}
}

func TestJobEndpointsAndHealth(t *testing.T) {
	h, v := newTestServer(t)
	tok := tokenFor(t, v, "tenant-1")
	rec := doReq(t, h, "GET", "/v1/dlq", tok, nil, "")
	if rec.Code != http.StatusOK || rec.Body.String() != "[]\n" {
		t.Fatalf("dlq: status %d body %q", rec.Code, rec.Body)
	}
	rec = doReq(t, h, "GET", "/v1/jobs/nonexistent", tok, nil, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing job status = %d", rec.Code)
	}
	rec = doReq(t, h, "GET", "/healthz", "", nil, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d", rec.Code)
	}
}

func TestBadTokenRejected(t *testing.T) {
	h, _ := newTestServer(t)
	rec := doReq(t, h, "GET", "/v1/tasks", "not-a-real-token", nil, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}
