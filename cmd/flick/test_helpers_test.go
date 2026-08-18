package main

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testDSN returns the DSN for integration tests.
func testDSN() string {
	if dsn := os.Getenv("FLICK_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://us:2@localhost:5432/flick?sslmode=disable"
}

// setupTestDB creates a pool and registers cleanup to remove the given key
// from both outbox and flags tables.
func setupTestDB(t *testing.T, key string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN())
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

// cleanupFlag removes a flag and its outbox events.
func cleanupFlag(t *testing.T, pool *pgxpool.Pool, key string) {
	t.Helper()
	ctx := context.Background()
	pool.Exec(ctx, `DELETE FROM outbox WHERE topic='flags' AND payload->>'key'=$1`, key)
	pool.Exec(ctx, `DELETE FROM flags WHERE key=$1`, key)
}
