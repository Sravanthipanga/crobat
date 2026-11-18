package domain

import "testing"

func TestPriorityRankOrdering(t *testing.T) {
	if !(PriorityCritical.Rank() < PriorityHigh.Rank() &&
		PriorityHigh.Rank() < PriorityNormal.Rank() &&
		PriorityNormal.Rank() < PriorityLow.Rank()) {
		t.Fatal("priority ranks are not strictly ordered critical < high < normal < low")
	}
	for _, p := range []Priority{PriorityCritical, PriorityHigh, PriorityNormal, PriorityLow} {
		if !p.Valid() {
			t.Fatalf("%s should be valid", p)
		}
	}
	if Priority("urgent").Valid() {
		t.Fatal("unknown priority reported valid")
	}
}

func TestJobTerminal(t *testing.T) {
	for _, s := range []JobStatus{JobSucceeded, JobDead} {
		if !(&Job{Status: s}).Terminal() {
			t.Fatalf("%s should be terminal", s)
		}
	}
	for _, s := range []JobStatus{JobPending, JobRunning} {
		if (&Job{Status: s}).Terminal() {
			t.Fatalf("%s should not be terminal", s)
		}
	}
}

func TestRequestSpecCloneIsDeep(t *testing.T) {
	orig := RequestSpec{Method: "POST", Headers: map[string]string{"A": "1"}, SecretHeaders: []string{"A"}}
	clone := orig.Clone()
	clone.Headers["A"] = "mutated"
	clone.SecretHeaders[0] = "B"
	if orig.Headers["A"] != "1" || orig.SecretHeaders[0] != "A" {
		t.Fatal("Clone is not a deep copy")
	}
}
