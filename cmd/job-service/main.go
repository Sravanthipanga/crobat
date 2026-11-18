// Job Service: the read API for jobs, history and the DLQ, plus manual retry.
package main

import (
	"log"
	"net/http"

	"github.com/Sravanthipanga/crobat/internal/adapters/httpapi"
	"github.com/Sravanthipanga/crobat/internal/adapters/jwtauth"
	"github.com/Sravanthipanga/crobat/internal/app/jobsvc"
	"github.com/Sravanthipanga/crobat/internal/platform"
)

func main() {
	stores, err := platform.BuildStores()
	if err != nil {
		log.Fatalf("stores: %v", err)
	}
	defer stores.Close()

	svc := &jobsvc.Service{Jobs: stores.Jobs, History: stores.History}
	server := &httpapi.Server{
		Verifier: jwtauth.NewVerifier(platform.Env("CROBAT_JWT_SECRET", "")),
		JobSvc:   svc,
	}
	addr := platform.Env("CROBAT_ADDR", ":8082")
	log.Printf("job-service listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}
