// Package mysql implements the store ports against MySQL 8.
//
// The jobs table IS the queue: there is no broker. The scheduler inserts
// job rows and advances next_run_at in one transaction; executors claim
// rows with SELECT ... FOR UPDATE SKIP LOCKED; results are written with
// a fencing-token guarded UPDATE. The in-memory adapter in
// adapters/memory implements the identical contracts and is what the
// unit tests run against; this adapter is verified by compilation and by
// the integration tests behind the `integration` build tag (they need a
// live MySQL, e.g. via docker compose).
package mysql

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

// Store implements ports.TaskStore, ports.SchedulerStore and ports.JobStore.
type Store struct {
	db *sql.DB
}

// New opens (and pings) a MySQL connection pool. The DSN should carry
// parseTime=true and loc=UTC, e.g.
// "crobat:crobat@tcp(mysql:3306)/crobat?parseTime=true&loc=UTC".
func New(dsn string) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("mysql ping: %w", err)
	}
	return &Store{db: db}, nil
}

// NewFromDB wraps an existing pool (used by integration tests).
func NewFromDB(db *sql.DB) *Store { return &Store{db: db} }

// Close closes the pool.
func (s *Store) Close() error { return s.db.Close() }

func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func isDuplicateKey(err error) bool {
	var myErr *mysqlDriver.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number == 1062
	}
	return false
}

func marshalHeaders(h map[string]string) (string, error) {
	if h == nil {
		h = map[string]string{}
	}
	b, err := json.Marshal(h)
	return string(b), err
}

func unmarshalHeaders(s string) (map[string]string, error) {
	out := map[string]string{}
	if s == "" {
		return out, nil
	}
	err := json.Unmarshal([]byte(s), &out)
	return out, err
}

func marshalStrings(v []string) (string, error) {
	if v == nil {
		v = []string{}
	}
	b, err := json.Marshal(v)
	return string(b), err
}

func unmarshalStrings(s string) ([]string, error) {
	out := []string{}
	if s == "" {
		return out, nil
	}
	err := json.Unmarshal([]byte(s), &out)
	return out, err
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

func fromMs(v int64) time.Duration { return time.Duration(v) * time.Millisecond }
