// Package cronx computes run times for task schedules. Cron evaluation is
// timezone-aware: the expression is evaluated in the task's IANA timezone,
// so "0 9 * * *" in America/New_York fires at 9am local even across DST
// transitions.
package cronx

import (
	"fmt"
	"time"
	_ "time/tzdata" // embed tzdata so IANA lookups work in scratch images

	"github.com/robfig/cron/v3"

	"github.com/Sravanthipanga/crobat/internal/domain"
)

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// Location resolves a schedule timezone (empty = UTC).
func Location(tz string) (*time.Location, error) {
	if tz == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("%w: unknown timezone %q", domain.ErrValidation, tz)
	}
	return loc, nil
}

// Validate checks that a schedule is well-formed.
func Validate(s domain.Schedule) error {
	switch s.Kind {
	case domain.ScheduleNow:
		return nil
	case domain.ScheduleAt:
		if s.At.IsZero() {
			return fmt.Errorf("%w: 'at' schedule requires a time", domain.ErrValidation)
		}
		return nil
	case domain.ScheduleCron:
		if _, err := Location(s.Timezone); err != nil {
			return err
		}
		if _, err := parser.Parse(s.CronExpr); err != nil {
			return fmt.Errorf("%w: bad cron expression %q: %v", domain.ErrValidation, s.CronExpr, err)
		}
		return nil
	}
	return fmt.Errorf("%w: unknown schedule kind %q", domain.ErrValidation, s.Kind)
}

// NextRun returns the next fire time strictly after `after`, in UTC.
// For one-shot schedules it returns the single fire time regardless.
func NextRun(s domain.Schedule, after time.Time) (time.Time, error) {
	if err := Validate(s); err != nil {
		return time.Time{}, err
	}
	switch s.Kind {
	case domain.ScheduleNow:
		return after.UTC(), nil
	case domain.ScheduleAt:
		return s.At.UTC(), nil
	case domain.ScheduleCron:
		loc, _ := Location(s.Timezone)
		sched, _ := parser.Parse(s.CronExpr)
		return sched.Next(after.In(loc)).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("%w: unknown schedule kind %q", domain.ErrValidation, s.Kind)
}

// Advance computes the next_run_at to store after a job scheduled for
// `scheduledFor` has been materialized at time `now`. Cron tasks advance
// from their scheduled time (no drift); if the service was down and the
// computed next run is already past, it skips ahead from `now` rather than
// bursting one job per missed run.
func Advance(s domain.Schedule, scheduledFor, now time.Time) (time.Time, error) {
	next, err := NextRun(s, scheduledFor)
	if err != nil {
		return time.Time{}, err
	}
	if s.Kind == domain.ScheduleCron && !next.After(now) {
		next, err = NextRun(s, now)
		if err != nil {
			return time.Time{}, err
		}
	}
	return next, nil
}
