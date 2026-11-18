// Reconciler: rescues jobs whose executor's lease expired.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sravanthipanga/crobat/internal/app/reconciler"
	"github.com/Sravanthipanga/crobat/internal/platform"
)

func main() {
	stores, err := platform.BuildStores()
	if err != nil {
		log.Fatalf("stores: %v", err)
	}
	defer stores.Close()

	svc := &reconciler.Service{Jobs: stores.Jobs}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	interval := platform.EnvDuration("CROBAT_RECONCILER_INTERVAL", 30*time.Second)
	log.Printf("reconciler running every %s", interval)
	if err := svc.Run(ctx, interval); err != nil && ctx.Err() == nil {
		log.Fatalf("reconciler: %v", err)
	}
}
