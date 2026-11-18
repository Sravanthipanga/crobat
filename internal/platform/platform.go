// Package platform wires configuration and adapters for the service
// binaries. Configuration is environment-driven:
//
//	CROBAT_STORE          "mysql" (default in compose) or "memory"
//	CROBAT_MYSQL_DSN      MySQL DSN (parseTime=true&loc=UTC)
//	CROBAT_CASSANDRA_HOSTS comma-separated Cassandra hosts; empty = in-memory history
//	CROBAT_CASSANDRA_KEYSPACE default "crobat"
//	CROBAT_JWT_SECRET     HS256 secret for tenant tokens
//	CROBAT_SECRET_KEY     base64 32-byte AES key for secret headers
//	CROBAT_ADDR           listen address for API services
package platform

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Sravanthipanga/crobat/internal/adapters/cassandra"
	"github.com/Sravanthipanga/crobat/internal/adapters/crypto"
	"github.com/Sravanthipanga/crobat/internal/adapters/memory"
	mysqladapter "github.com/Sravanthipanga/crobat/internal/adapters/mysql"
	"github.com/Sravanthipanga/crobat/internal/ports"
)

// Env returns an env var or a default.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvDuration parses a duration env var (e.g. "5s") or returns def.
func EnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// EnvInt parses an int env var or returns def.
func EnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// Stores bundles the store adapters a service needs.
type Stores struct {
	Tasks     ports.TaskStore
	Scheduler ports.SchedulerStore
	Jobs      ports.JobStore
	History   ports.HistoryStore
	closers   []func()
}

// Close releases underlying connections.
func (s *Stores) Close() {
	for _, c := range s.closers {
		c()
	}
}

// BuildStores constructs the configured adapters.
func BuildStores() (*Stores, error) {
	out := &Stores{}

	switch Env("CROBAT_STORE", "memory") {
	case "memory":
		store := memory.NewStore()
		out.Tasks, out.Scheduler, out.Jobs = store, store, store
	case "mysql":
		dsn := os.Getenv("CROBAT_MYSQL_DSN")
		if dsn == "" {
			return nil, fmt.Errorf("CROBAT_MYSQL_DSN is required when CROBAT_STORE=mysql")
		}
		store, err := mysqladapter.New(dsn)
		if err != nil {
			return nil, err
		}
		out.Tasks, out.Scheduler, out.Jobs = store, store, store
		out.closers = append(out.closers, func() { _ = store.Close() })
	default:
		return nil, fmt.Errorf("unknown CROBAT_STORE %q", os.Getenv("CROBAT_STORE"))
	}

	if hosts := os.Getenv("CROBAT_CASSANDRA_HOSTS"); hosts != "" {
		history, err := cassandra.NewHistoryStore(
			strings.Split(hosts, ","), Env("CROBAT_CASSANDRA_KEYSPACE", "crobat"))
		if err != nil {
			return nil, err
		}
		out.History = history
		out.closers = append(out.closers, history.Close)
	} else {
		out.History = memory.NewHistory()
	}
	return out, nil
}

// SecretBox builds the secret box from CROBAT_SECRET_KEY. It returns nil
// (with a loud warning) when unconfigured: secrets are then stored
// unencrypted, which is acceptable only for local development.
func SecretBox() *crypto.SecretBox {
	key := os.Getenv("CROBAT_SECRET_KEY")
	if key == "" {
		log.Print("WARNING: CROBAT_SECRET_KEY not set; secret headers will be stored UNENCRYPTED (local dev only)")
		return nil
	}
	box, err := crypto.NewSecretBox(key)
	if err != nil {
		log.Fatalf("CROBAT_SECRET_KEY: %v", err)
	}
	return box
}

// WorkerID returns a stable-ish executor identity for this process.
func WorkerID(prefix string) string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%s-%d", prefix, host, os.Getpid())
}
