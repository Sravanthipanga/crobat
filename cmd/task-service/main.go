// Task Service: the write API for task definitions.
package main

import (
	"log"
	"net/http"

	"github.com/Sravanthipanga/crobat/internal/adapters/httpapi"
	"github.com/Sravanthipanga/crobat/internal/adapters/jwtauth"
	"github.com/Sravanthipanga/crobat/internal/app/tasksvc"
	"github.com/Sravanthipanga/crobat/internal/platform"
)

func main() {
	stores, err := platform.BuildStores()
	if err != nil {
		log.Fatalf("stores: %v", err)
	}
	defer stores.Close()

	svc := &tasksvc.Service{Tasks: stores.Tasks, Box: platform.SecretBox()}
	server := &httpapi.Server{
		Verifier: jwtauth.NewVerifier(platform.Env("CROBAT_JWT_SECRET", "")),
		TaskSvc:  svc,
	}
	addr := platform.Env("CROBAT_ADDR", ":8081")
	log.Printf("task-service listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}
