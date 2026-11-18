# Builds all six Crobat services into one image; docker-compose selects
# the binary per service via `command:`.
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/...

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/ /usr/local/bin/
# Default command; compose overrides per service.
CMD ["task-service"]
