//go:build e2e

package flick

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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
	if err := Migrate(db); err != nil {
		log.Fatalf("migrations: %v", err)
	}
	db.Close()
	os.Exit(m.Run())
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
