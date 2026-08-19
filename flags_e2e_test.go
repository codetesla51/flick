//go:build e2e

package flick

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestMain applies the embedded migrations so the E2E tests work against a
// fresh database (CI spins up a bare postgres service container). Runs only
// with -tags e2e; skipped entirely when FLICK_DSN is unset.
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
	if err := Migrate(db); err != nil {
		lock.ExecContext(context.Background(), "SELECT pg_advisory_unlock(20260819)")
		log.Fatalf("migrations: %v", err)
	}
	lock.ExecContext(context.Background(), "SELECT pg_advisory_unlock(20260819)")
	lock.Close()
	db.Close()

	// The notify-layer tests listen on a live LISTEN/NOTIFY consumer, so
	// they must not share the main database: events from every other test
	// would leak into (and flood) their hub. Give them a scratch database.
	notifyTestDSN = setupNotifyTestDB()
	os.Exit(m.Run())
}

// notifyTestDSN points at a dedicated scratch database used only by the
// notify-layer e2e tests.
var notifyTestDSN string

// setupNotifyTestDB drops and recreates a scratch database for the notify
// tests, applies the migrations, and returns its DSN (fatal on failure).
func setupNotifyTestDB() string {
	dsn := testDSN()

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("notify test db: open: %v", err)
	}
	defer admin.Close()
	// WITH (FORCE) terminates any backend left over from a previous run.
	if _, err := admin.Exec(`DROP DATABASE IF EXISTS flick_notify_e2e WITH (FORCE)`); err != nil {
		log.Fatalf("notify test db: drop: %v", err)
	}
	if _, err := admin.Exec(`CREATE DATABASE flick_notify_e2e`); err != nil {
		log.Fatalf("notify test db: create: %v", err)
	}

	// pgx's ConnString() ignores overridden fields (returns the original DSN),
	// so rewrite the URL's database path directly.
	u, err := url.Parse(dsn)
	if err != nil {
		log.Fatalf("notify test db: parse dsn: %v", err)
	}
	u.Path = "/flick_notify_e2e"
	n := u.String()

	db, err := sql.Open("pgx", n)
	if err != nil {
		log.Fatalf("notify test db: open scratch: %v", err)
	}
	defer db.Close()
	if err := Migrate(db); err != nil {
		log.Fatalf("notify test db: migrate: %v", err)
	}
	return n
}

func TestSetFlagE2E(t *testing.T) {
	const key = "setflag_e2e_test"
	pool := setupTestDB(t, key)
	ctx := context.Background()

	if err := SetFlag(ctx, pool, key, "ENABLED", "red",
		json.RawMessage(`{"red":25,"blue":75}`),
		json.RawMessage(`{"country":["NG"]}`),
		json.RawMessage(`{"owner":"e2e"}`),
	); err != nil {
		t.Fatalf("SetFlag: %v", err)
	}

	// flags row upserted
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM flags WHERE key=$1`, key).Scan(&state); err != nil {
		t.Fatalf("flags row: %v", err)
	}
	if state != "ENABLED" {
		t.Errorf("state = %q, want ENABLED", state)
	}

	// outbox event enqueued with matching payload
	var payload []byte
	if err := pool.QueryRow(ctx,
		`SELECT payload FROM outbox WHERE topic='flags' AND payload->>'key'=$1`, key,
	).Scan(&payload); err != nil {
		t.Fatalf("outbox row: %v", err)
	}
	var evt map[string]any
	if err := json.Unmarshal(payload, &evt); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if evt["key"] != key || evt["defaultVariant"] != "red" || evt["state"] != "ENABLED" {
		t.Errorf("event payload mismatch: %s", payload)
	}

	// upsert path: second SetFlag updates flags and enqueues another event
	if err := SetFlag(ctx, pool, key, "DISABLED", "blue",
		json.RawMessage(`{"red":25,"blue":75}`),
		json.RawMessage(`{}`),
		json.RawMessage(`{}`),
	); err != nil {
		t.Fatalf("SetFlag upsert: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE topic='flags' AND payload->>'key'=$1`, key).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if n != 2 {
		t.Errorf("outbox events = %d, want 2", n)
	}
}

func TestDeleteFlagE2E(t *testing.T) {
	const key = "deleteflag_e2e_test"
	pool := setupTestDB(t, key)
	ctx := context.Background()

	// create the flag first
	if err := SetFlag(ctx, pool, key, "ENABLED", "red",
		json.RawMessage(`{"red":25,"blue":75}`),
		json.RawMessage(`{}`),
		json.RawMessage(`{"owner":"e2e"}`),
	); err != nil {
		t.Fatalf("SetFlag: %v", err)
	}

	// delete it
	if err := DeleteFlag(ctx, pool, key); err != nil {
		t.Fatalf("DeleteFlag: %v", err)
	}

	// flags row gone
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM flags WHERE key=$1`, key).Scan(&n); err != nil {
		t.Fatalf("count flags: %v", err)
	}
	if n != 0 {
		t.Errorf("flags rows = %d, want 0 after delete", n)
	}

	// exactly one outbox event, with deleted:true
	var payload []byte
	if err := pool.QueryRow(ctx,
		`SELECT payload FROM outbox WHERE topic='flags' AND payload->>'key'=$1 AND payload->>'deleted'='true'`, key,
	).Scan(&payload); err != nil {
		t.Fatalf("outbox delete event: %v", err)
	}
	var evt map[string]any
	if err := json.Unmarshal(payload, &evt); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if evt["key"] != key || evt["deleted"] != true {
		t.Errorf("delete event payload mismatch: %s", payload)
	}

	// deleting an absent key emits nothing
	if err := DeleteFlag(ctx, pool, key); err != nil {
		t.Fatalf("DeleteFlag absent: %v", err)
	}
	total := 0
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE topic='flags' AND payload->>'key'=$1 AND payload->>'deleted'='true'`, key,
	).Scan(&total); err != nil {
		t.Fatalf("count delete events: %v", err)
	}
	if total != 1 {
		t.Errorf("delete events = %d, want 1 (absent-key delete must be a no-op)", total)
	}
	if n != 0 {
		t.Errorf("flags rows = %d, want 0", n)
	}
}
