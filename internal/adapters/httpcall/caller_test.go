package httpcall

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/crypto"
	"github.com/Sravanthipanga/crobat/internal/domain"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func testJob(spec domain.RequestSpec) *domain.Job {
	return &domain.Job{
		ID:          "job-42",
		TaskID:      "task-1",
		TenantID:    "tenant-1",
		Request:     spec,
		ScheduledAt: base,
		Attempt:     2,
		Timeout:     5 * time.Second,
	}
}

func TestCallSendsDeliveryHeadersAndDecryptsSecrets(t *testing.T) {
	var got http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	box, err := crypto.NewRandomSecretBox()
	if err != nil {
		t.Fatal(err)
	}
	enc, err := box.Encrypt("Bearer s3cret")
	if err != nil {
		t.Fatal(err)
	}
	spec := domain.RequestSpec{
		Method: "POST",
		URL:    server.URL + "/orders",
		Headers: map[string]string{
			"Authorization":   enc,
			"Idempotency-Key": "spoofed-value", // must be overridden by the caller
		},
		SecretHeaders: []string{"Authorization"},
	}
	caller := &Caller{Box: box, Client: server.Client()}
	status, err := caller.Call(context.Background(), testJob(spec), spec)
	if err != nil || status != 200 {
		t.Fatalf("status=%d err=%v", status, err)
	}
	if got.Get("Idempotency-Key") != "job-42" {
		t.Fatalf("Idempotency-Key = %q, want the job id", got.Get("Idempotency-Key"))
	}
	if got.Get("X-Scheduled-At") != "2026-01-01T12:00:00Z" {
		t.Fatalf("X-Scheduled-At = %q", got.Get("X-Scheduled-At"))
	}
	if got.Get("X-Attempt") != "2" {
		t.Fatalf("X-Attempt = %q", got.Get("X-Attempt"))
	}
	if got.Get("Authorization") != "Bearer s3cret" {
		t.Fatalf("Authorization = %q, want decrypted secret", got.Get("Authorization"))
	}
}

func TestCallNon2xxIsFailureWithStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	spec := domain.RequestSpec{Method: "GET", URL: server.URL}
	caller := &Caller{Client: server.Client()}
	status, err := caller.Call(context.Background(), testJob(spec), spec)
	if err == nil {
		t.Fatal("expected error for 503")
	}
	if status != 503 {
		t.Fatalf("status = %d, want 503 recorded", status)
	}
}

func TestCallRespectsJobTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	job := testJob(domain.RequestSpec{Method: "GET", URL: server.URL})
	job.Timeout = 50 * time.Millisecond
	caller := &Caller{Client: server.Client()}
	if _, err := caller.Call(context.Background(), job, job.Request); err == nil {
		t.Fatal("expected timeout error")
	}
}
