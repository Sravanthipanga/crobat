GO ?= go

.PHONY: build vet test test-integration compose-up compose-down tidy
build:
	$(GO) build ./...
vet:
	$(GO) vet ./...
test:
	$(GO) test ./...
# Integration tests need live MySQL/Cassandra (e.g. `make compose-up` first)
# and are excluded from the default run via the `integration` build tag.
test-integration:
	$(GO) test -tags integration ./...
tidy:
	$(GO) mod tidy
compose-up:
	docker compose up --build -d
compose-down:
	docker compose down -v
