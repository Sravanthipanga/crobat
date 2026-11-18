package retry

import (
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/domain"
)

func fixedJitter(v float64) func() float64 { return func() float64 { return v } }

func TestBackoffGrowsExponentially(t *testing.T) {
	p := domain.RetryPolicy{MaxAttempts: 5, BackoffBase: time.Second, BackoffMax: time.Minute}
	// jitter 0.5 -> delay = d/2 + 0.5*(d/2) = 0.75d
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 750 * time.Millisecond},
		{2, 1500 * time.Millisecond},
		{3, 3 * time.Second},
		{4, 6 * time.Second},
	}
	for _, c := range cases {
		if got := Backoff(p, c.attempt, fixedJitter(0.5)); got != c.want {
			t.Errorf("attempt %d: got %s, want %s", c.attempt, got, c.want)
		}
	}
}

func TestBackoffIsCapped(t *testing.T) {
	p := domain.RetryPolicy{MaxAttempts: 100, BackoffBase: time.Second, BackoffMax: 3 * time.Second}
	for attempt := 1; attempt <= 20; attempt++ {
		got := Backoff(p, attempt, nil)
		if got > 3*time.Second {
			t.Fatalf("attempt %d: backoff %s exceeds cap", attempt, got)
		}
		if got < 500*time.Millisecond {
			t.Fatalf("attempt %d: backoff %s below half the base", attempt, got)
		}
	}
}

func TestBackoffJitterStaysInBand(t *testing.T) {
	p := domain.RetryPolicy{MaxAttempts: 5, BackoffBase: 2 * time.Second, BackoffMax: time.Minute}
	for i := 0; i < 200; i++ {
		got := Backoff(p, 1, nil) // d = 2s, band [1s, 2s]
		if got < time.Second || got > 2*time.Second {
			t.Fatalf("backoff %s outside [1s, 2s]", got)
		}
	}
}

func TestBackoffDefaults(t *testing.T) {
	got := Backoff(domain.RetryPolicy{}, 1, fixedJitter(0))
	if got != 500*time.Millisecond { // default base 1s, jitter 0 -> d/2
		t.Fatalf("got %s, want 500ms", got)
	}
}

func TestExhausted(t *testing.T) {
	p := domain.RetryPolicy{MaxAttempts: 3}
	if Exhausted(p, 2) {
		t.Fatal("2 of 3 attempts should not be exhausted")
	}
	if !Exhausted(p, 3) {
		t.Fatal("3 of 3 attempts should be exhausted")
	}
	if !Exhausted(domain.RetryPolicy{}, 1) {
		t.Fatal("zero policy means a single attempt")
	}
}
