// Executor: claims due jobs, waits for their scheduled time, calls the
// tenant endpoint, and saves results through the fencing-token guard.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/httpcall"
	"github.com/Sravanthipanga/crobat/internal/adapters/ssrf"
	"github.com/Sravanthipanga/crobat/internal/app/executor"
	"github.com/Sravanthipanga/crobat/internal/platform"
)

func main() {
	stores, err := platform.BuildStores()
	if err != nil {
		log.Fatalf("stores: %v", err)
	}
	defer stores.Close()

	svc := &executor.Service{
		Jobs:      stores.Jobs,
		Caller:    &httpcall.Caller{Guard: ssrf.New(), Box: platform.SecretBox()},
		WorkerID:  platform.WorkerID("executor"),
		BatchSize: platform.EnvInt("CROBAT_EXECUTOR_BATCH", 32),
		Lease:     platform.EnvDuration("CROBAT_EXECUTOR_LEASE", 2*time.Minute),
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("executor %s running", svc.WorkerID)
	if err := svc.Run(ctx, platform.EnvDuration("CROBAT_EXECUTOR_INTERVAL", time.Second)); err != nil && ctx.Err() == nil {
		log.Fatalf("executor: %v", err)
	}
}
