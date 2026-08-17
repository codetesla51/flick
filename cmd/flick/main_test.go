package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// runCLI executes the root command in-process with the given args and
// returns captured stdout+stderr.
func runCLI(args ...string) (string, error) {
	rootCmd.SetArgs(args)
	var buf strings.Builder
	rootCmd.SetOut(&buf)
	rootCmd.SetErr(&buf)
	err := rootCmd.Execute()
	return buf.String(), err
}

func TestCLIHelpShowsAllCommands(t *testing.T) {
	out, err := runCLI("--help")
	if err != nil {
		t.Fatalf("--help: %v", err)
	}
	for _, c := range []string{"init", "serve", "set", "list", "delete", "version"} {
		if !strings.Contains(out, c) {
			t.Errorf("help output missing %q:\n%s", c, out)
		}
	}
}

func TestCLISetListDeleteE2E(t *testing.T) {
	dsn := os.Getenv("FLICK_DSN")
	if dsn == "" {
		dsn = "postgres://us:2@localhost:5432/flick?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	const key = "cli_e2e_test"
	cleanup := func() {
		pool.Exec(ctx, `DELETE FROM outbox WHERE topic='flags' AND payload->>'key'=$1`, key)
		pool.Exec(ctx, `DELETE FROM flags WHERE key=$1`, key)
	}
	cleanup()
	t.Cleanup(cleanup)

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
