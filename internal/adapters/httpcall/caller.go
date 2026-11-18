// Package httpcall is the executor's ports.Caller: it performs the actual
// HTTP call for a job through the SSRF guard, with the delivery headers
// that make at-least-once safe:
//
//	Idempotency-Key: <job id>     — the receiver dedupes retries by this
//	X-Scheduled-At:  <RFC3339>    — when the job was due
//	X-Attempt:       <n>          — the fencing/attempt number
//
// Secret headers are decrypted just before the call; the stored job row
// only ever holds ciphertext.
package httpcall

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/crypto"
	"github.com/Sravanthipanga/crobat/internal/adapters/ssrf"
	"github.com/Sravanthipanga/crobat/internal/domain"
)

// Caller performs job HTTP calls.
type Caller struct {
	Guard *ssrf.Guard
	Box   *crypto.SecretBox // may be nil if no secrets are used
	// Client overrides the guard's client (tests).
	Client *http.Client
}

var _ interface {
	Call(context.Context, *domain.Job, domain.RequestSpec) (int, error)
} = (*Caller)(nil)

// Call makes the request. A non-2xx status is reported as an error (the
// executor will retry per policy or dead-letter); the status code is
// returned either way so it can be recorded on the job.
func (c *Caller) Call(ctx context.Context, job *domain.Job, spec domain.RequestSpec) (int, error) {
	reqSpec := spec
	if c.Box != nil {
		dec, err := crypto.DecryptSecrets(c.Box, spec)
		if err != nil {
			return 0, fmt.Errorf("decrypt secrets: %w", err)
		}
		reqSpec = dec
	}
	if c.Guard != nil {
		if err := c.Guard.ValidateURL(ctx, reqSpec.URL); err != nil {
			return 0, err
		}
	}

	timeout := job.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var body io.Reader
	if reqSpec.Body != "" {
		body = strings.NewReader(reqSpec.Body)
	}
	req, err := http.NewRequestWithContext(callCtx, strings.ToUpper(reqSpec.Method), reqSpec.URL, body)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	for k, v := range reqSpec.Headers {
		req.Header.Set(k, v)
	}
	// Delivery headers are set AFTER tenant headers so they cannot be
	// spoofed by task configuration.
	req.Header.Set("Idempotency-Key", job.ID)
	req.Header.Set("X-Scheduled-At", job.ScheduledAt.UTC().Format(time.RFC3339))
	req.Header.Set("X-Attempt", strconv.Itoa(job.Attempt))
	req.Header.Set("User-Agent", "Crobat/1.0 (+https://github.com/Sravanthipanga/crobat)")

	client := c.Client
	if client == nil && c.Guard != nil {
		client = c.Guard.HTTPClient()
	}
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http call: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, fmt.Errorf("endpoint returned HTTP %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}
