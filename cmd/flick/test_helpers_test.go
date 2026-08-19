//go:build e2e

package main

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testDSN returns the DSN from the FLICK_DSN env var (empty when unset).
func testDSN() string { return os.Getenv("FLICK_DSN") }

// requireDB returns the FLICK_DSN env var, skipping the test when unset so
// e2e runs degrade gracefully on machines without a database.
func requireDB(t *testing.T) string {
	t.Helper()
	dsn := testDSN()
	if dsn == "" {
		t.Skip("FLICK_DSN not set; skipping e2e test")
	}
	return dsn
}

// setupTestDB creates a pool and registers cleanup to remove the given key
// from the flags table.
func setupTestDB(t *testing.T, key string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), requireDB(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	cleanupFlag(t, pool, key)
	t.Cleanup(func() {
		cleanupFlag(t, pool, key)
		pool.Close()
	})
	return pool
}

// cleanupFlag removes a flag.
func cleanupFlag(t *testing.T, pool *pgxpool.Pool, key string) {
	t.Helper()
	ctx := context.Background()
	pool.Exec(ctx, `DELETE FROM flags WHERE key=$1`, key)
}
