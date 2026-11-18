// Archiver: moves finished jobs from the hot MySQL table to Cassandra
// history — history write first, hot-row delete second.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sravanthipanga/crobat/internal/app/archiver"
	"github.com/Sravanthipanga/crobat/internal/platform"
)

func main() {
	stores, err := platform.BuildStores()
	if err != nil {
		log.Fatalf("stores: %v", err)
	}
	defer stores.Close()

	svc := &archiver.Service{
		Jobs:      stores.Jobs,
		History:   stores.History,
		BatchSize: platform.EnvInt("CROBAT_ARCHIVER_BATCH", 500),
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	interval := platform.EnvDuration("CROBAT_ARCHIVER_INTERVAL", time.Minute)
	log.Printf("archiver running every %s", interval)
	if err := svc.Run(ctx, interval); err != nil && ctx.Err() == nil {
		log.Fatalf("archiver: %v", err)
	}
}
