package cronx

import (
	"testing"
	"time"

	"github.com/Sravanthipanga/crobat/internal/domain"
)

func cronSchedule(expr, tz string) domain.Schedule {
	return domain.Schedule{Kind: domain.ScheduleCron, CronExpr: expr, Timezone: tz}
}

func TestNextRunEvery15Minutes(t *testing.T) {
	after := time.Date(2026, 1, 1, 0, 7, 0, 0, time.UTC)
	next, err := NextRun(cronSchedule("*/15 * * * *", ""), after)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 1, 1, 0, 15, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextRunTimezoneSpringForward(t *testing.T) {
	// 9am in America/New_York: on 2026-03-08 clocks spring forward, so
	// 9am local is 13:00 UTC (EDT), not 14:00 UTC (EST).
	after := time.Date(2026, 3, 7, 15, 0, 0, 0, time.UTC) // 10am EST, past Mar 7's run
	next, err := NextRun(cronSchedule("0 9 * * *", "America/New_York"), after)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 3, 8, 13, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextRunTimezoneFallBack(t *testing.T) {
	// On 2026-11-01 clocks fall back: 9am local is 14:00 UTC (EST).
	after := time.Date(2026, 10, 31, 15, 0, 0, 0, time.UTC) // 11am EDT, past Oct 31's run
	next, err := NextRun(cronSchedule("0 9 * * *", "America/New_York"), after)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextRunTimezoneHalfHourOffset(t *testing.T) {
	// 08:30 in Asia/Kolkata (UTC+5:30) is 03:00 UTC.
	after := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next, err := NextRun(cronSchedule("30 8 * * *", "Asia/Kolkata"), after)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestNextRunAtAndNow(t *testing.T) {
	at := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	got, err := NextRun(domain.Schedule{Kind: domain.ScheduleAt, At: at}, time.Now())
	if err != nil || !got.Equal(at) {
		t.Fatalf("at: got %s err %v", got, err)
	}
	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	got, err = NextRun(domain.Schedule{Kind: domain.ScheduleNow}, now)
	if err != nil || !got.Equal(now) {
		t.Fatalf("now: got %s err %v", got, err)
	}
}

func TestValidateRejectsBadCronAndTimezone(t *testing.T) {
	if err := Validate(cronSchedule("not a cron", "")); err == nil {
		t.Fatal("expected error for bad cron expression")
	}
	if err := Validate(cronSchedule("0 9 * * *", "Not/AZone")); err == nil {
		t.Fatal("expected error for bad timezone")
	}
	if err := Validate(domain.Schedule{Kind: domain.ScheduleAt}); err == nil {
		t.Fatal("expected error for 'at' without a time")
	}
}

func TestAdvanceSkipsMissedRuns(t *testing.T) {
	// Scheduler was down for 10 minutes: advancing an every-minute cron
	// must land in the future, not burst one job per missed minute.
	s := cronSchedule("* * * * *", "")
	now := time.Date(2026, 1, 1, 0, 10, 30, 0, time.UTC)
	scheduledFor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next, err := Advance(s, scheduledFor, now)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 1, 1, 0, 11, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestAdvanceFromScheduledTimeNoDrift(t *testing.T) {
	s := cronSchedule("*/15 * * * *", "")
	scheduledFor := time.Date(2026, 1, 1, 0, 15, 0, 0, time.UTC)
	now := time.Date(2026, 1, 1, 0, 15, 2, 0, time.UTC) // materialized 2s late
	next, err := Advance(s, scheduledFor, now)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}
