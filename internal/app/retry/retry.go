// Package retry computes retry delays: exponential backoff with full
// jitter, capped by the task's retry policy.
package retry

import (
	"math/rand"
	"time"

	"github.com/Sravanthipanga/crobat/internal/domain"
)

const (
	defaultBase = time.Second
	defaultMax  = 5 * time.Minute
)

// Backoff returns the delay before the next attempt, given that attempt
// `failedAttempt` (1-based) just failed. jitterSource returns a value in
// [0,1); nil uses math/rand. The delay is uniform in [d/2, d] where d is
// the capped exponential delay (full-jitter style), which spreads retries
// of simultaneously-failed jobs instead of letting them thunder back in
// lockstep.
func Backoff(p domain.RetryPolicy, failedAttempt int, jitterSource func() float64) time.Duration {
	base := p.BackoffBase
	if base <= 0 {
		base = defaultBase
	}
	max := p.BackoffMax
	if max <= 0 {
		max = defaultMax
	}
	if failedAttempt < 1 {
		failedAttempt = 1
	}
	d := base
	for i := 1; i < failedAttempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	j := 0.5
	if jitterSource != nil {
		j = jitterSource()
	} else {
		j = rand.Float64()
	}
	if j < 0 {
		j = 0
	}
	if j > 1 {
		j = 1
	}
	return d/2 + time.Duration(j*float64(d/2))
}

// Exhausted reports whether the attempt budget is spent.
func Exhausted(p domain.RetryPolicy, attemptsMade int) bool {
	max := p.MaxAttempts
	if max < 1 {
		max = 1
	}
	return attemptsMade >= max
}
