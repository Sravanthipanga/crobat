//go:build integration

// Integration tests for the MySQL adapter. They require a live MySQL with
// migrations/mysql.sql applied and are excluded from the default
// `go test ./...` run:
//
//	CROBAT_TEST_MYSQL_DSN='crobat:crobat@tcp(127.0.0.1:3306)/crobat?parseTime=true&loc=UTC' \
//	  go test -tags integration ./internal/adapters/mysql/
package mysql

import (
	"database/sql"
	"os"
	"testing"
)

func TestIntegrationOpenAndPing(t *testing.T) {
	dsn := os.Getenv("CROBAT_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("CROBAT_TEST_MYSQL_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewFromDB(db)
	if err := store.db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}
