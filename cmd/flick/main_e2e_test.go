//go:build e2e

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/codetesla51/flick"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestMain applies the embedded migrations so the E2E CLI + dashboard tests
// work against a fresh database (CI spins up a bare postgres service).
// Runs only with -tags e2e; skipped entirely when FLICK_DSN is unset.
func TestMain(m *testing.M) {
	if testDSN() == "" {
		fmt.Println("FLICK_DSN not set; skipping e2e tests")
		os.Exit(0)
	}
	db, err := sql.Open("pgx", testDSN())
	if err != nil {
		log.Fatalf("migrations: open db: %v", err)
	}
	// Serialize concurrent TestMain migrations: go test runs package binaries
	// in parallel and both packages migrate the same database.
	lock, err := db.Conn(context.Background())
	if err != nil {
		log.Fatalf("migrations: lock conn: %v", err)
	}
	if _, err := lock.ExecContext(context.Background(), "SELECT pg_advisory_lock(20260819)"); err != nil {
		log.Fatalf("migrations: lock: %v", err)
	}
	if err := flick.Migrate(db); err != nil {
		lock.ExecContext(context.Background(), "SELECT pg_advisory_unlock(20260819)")
		log.Fatalf("migrations: %v", err)
	}
	lock.ExecContext(context.Background(), "SELECT pg_advisory_unlock(20260819)")
	lock.Close()
	db.Close()
	os.Exit(m.Run())
}

func TestCLISetListDeleteE2E(t *testing.T) {
	const key = "cli_e2e_test"
	pool := setupTestDB(t, key)
	ctx := context.Background()

	// CLI commands use resolveDSN() which checks the dsn global.
	dsn = testDSN()
	t.Cleanup(func() { dsn = "" })

	// set
	out, err := runCLI("set", key,
		"--state", "ENABLED",
		"--default-variant", "on",
		"--variants", `{"on":true,"off":false}`,
		"--targeting", `{"country":["NG"]}`,
		"--metadata", `{"owner":"cli"}`)
	if err != nil {
		t.Fatalf("set: %v\n%s", err, out)
	}
	if !strings.Contains(out, "set "+key) {
		t.Errorf("set output = %q, want confirmation", out)
	}

	// flag persisted with all fields
	var state, targeting, metadata string
	if err := pool.QueryRow(ctx,
		`SELECT state, targeting::text, metadata::text FROM flags WHERE key=$1`, key,
	).Scan(&state, &targeting, &metadata); err != nil {
		t.Fatalf("flags row: %v", err)
	}
	if state != "ENABLED" {
		t.Errorf("state = %q, want ENABLED", state)
	}
	if !strings.Contains(targeting, "NG") || !strings.Contains(metadata, "cli") {
		t.Errorf("targeting/metadata not persisted: %q %q", targeting, metadata)
	}

	// list shows it
	out, err = runCLI("list")
	if err != nil {
		t.Fatalf("list: %v\n%s", err, out)
	}
	if !strings.Contains(out, key) {
		t.Errorf("list output missing %q:\n%s", key, out)
	}

	// get shows it with details
	out, err = runCLI("get", key)
	if err != nil {
		t.Fatalf("get: %v\n%s", err, out)
	}
	if !strings.Contains(out, key) || !strings.Contains(out, "variants:") {
		t.Errorf("get output missing details for %q:\n%s", key, out)
	}

	// get on unknown key is an error
	if _, err := runCLI("get", "no-such-flag"); err == nil {
		t.Errorf("get unknown key: want error, got nil")
	}

	// delete
	out, err = runCLI("delete", key)
	if err != nil {
		t.Fatalf("delete: %v\n%s", err, out)
	}
	if !strings.Contains(out, "deleted "+key) {
		t.Errorf("delete output = %q, want confirmation", out)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM flags WHERE key=$1`, key).Scan(&n); err != nil {
		t.Fatalf("count flags: %v", err)
	}
	if n != 0 {
		t.Errorf("flags rows = %d, want 0 after delete", n)
	}

	// deleting again is an error
	if _, err := runCLI("delete", key); err == nil {
		t.Errorf("second delete: want error for unknown key, got nil")
	}

	// invalid state rejected
	if _, err := runCLI("set", key, "--state", "MAYBE", "--default-variant", "on",
		"--variants", `{"on":true}`); err == nil {
		t.Errorf("set with invalid state: want error, got nil")
	}
	// invalid JSON rejected
	if _, err := runCLI("set", key, "--default-variant", "on",
		"--variants", `not-json`); err == nil {
		t.Errorf("set with invalid variants JSON: want error, got nil")
	}
}
