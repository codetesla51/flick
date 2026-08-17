package flick

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSetFlagE2E(t *testing.T) {
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

	const key = "setflag_e2e_test"
	if _, err := pool.Exec(ctx, `DELETE FROM outbox WHERE topic='flags' AND payload->>'key'=$1`, key); err != nil {
		t.Fatalf("cleanup outbox: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM flags WHERE key=$1`, key); err != nil {
		t.Fatalf("cleanup flags: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM outbox WHERE topic='flags' AND payload->>'key'=$1`, key)
		pool.Exec(ctx, `DELETE FROM flags WHERE key=$1`, key)
	})

	err = SetFlag(ctx, pool, key, "ENABLED", "red",
		json.RawMessage(`{"red":25,"blue":75}`),
		json.RawMessage(`{"country":["NG"]}`),
		json.RawMessage(`{"owner":"e2e"}`),
	)
	if err != nil {
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