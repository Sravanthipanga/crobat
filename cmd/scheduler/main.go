// Scheduler: materializes due tasks into job rows every tick.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sravanthipanga/crobat/internal/app/scheduler"
	"github.com/Sravanthipanga/crobat/internal/platform"
)

func main() {
	stores, err := platform.BuildStores()
	if err != nil {
		log.Fatalf("stores: %v", err)
	}
	defer stores.Close()

	svc := &scheduler.Service{
		Store:     stores.Scheduler,
		Lookahead: platform.EnvDuration("CROBAT_SCHEDULER_LOOKAHEAD", 30*time.Second),
		BatchSize: platform.EnvInt("CROBAT_SCHEDULER_BATCH", 1000),
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	interval := platform.EnvDuration("CROBAT_SCHEDULER_INTERVAL", 5*time.Second)
	log.Printf("scheduler running every %s", interval)
	if err := svc.Run(ctx, interval); err != nil && ctx.Err() == nil {
		log.Fatalf("scheduler: %v", err)
	}
}
