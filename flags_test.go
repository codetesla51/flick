package flick

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
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

func TestTranslateFlag(t *testing.T) {
	tests := []struct {
		name string
		row  flagEvent
		want flagdFlag
	}{
		{
			name: "full flag with variants and targeting",
			row: flagEvent{
				Key:            "my-flag",
				State:          "ENABLED",
				DefaultVariant: "red",
				Variants:       json.RawMessage(`{"red":25,"blue":75}`),
				Targeting:      json.RawMessage(`{"country":["NG"]}`),
			},
			want: flagdFlag{
				State:          "ENABLED",
				DefaultVariant: "red",
				Variants:       map[string]any{"red": float64(25), "blue": float64(75)},
				Targeting:      map[string]any{"country": []any{"NG"}},
			},
		},
		{
			name: "empty variants and targeting",
			row: flagEvent{
				Key:            "empty-flag",
				State:          "DISABLED",
				DefaultVariant: "off",
				Variants:       json.RawMessage(`{}`),
				Targeting:      json.RawMessage(`{}`),
			},
			want: flagdFlag{
				State:          "DISABLED",
				DefaultVariant: "off",
				Variants:       map[string]any{},
				Targeting:      map[string]any{},
			},
		},
		{
			name: "malformed variants falls back to empty",
			row: flagEvent{
				Key:            "broken",
				State:          "ENABLED",
				DefaultVariant: "a",
				Variants:       json.RawMessage(`not-json`),
				Targeting:      json.RawMessage(`{}`),
			},
			want: flagdFlag{
				State:          "ENABLED",
				DefaultVariant: "a",
				Variants:       map[string]any{},
				Targeting:      map[string]any{},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := TranslateFlag(tc.row)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("TranslateFlag() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestBuildSnapshotJSONShape(t *testing.T) {
	rows := []flagEvent{
		{
			Key:            "banner",
			State:          "ENABLED",
			DefaultVariant: "on",
			Variants:       json.RawMessage(`{"on":true,"off":false}`),
			Targeting:      json.RawMessage(`{}`), // empty targeting must be omitted
		},
		{
			Key:            "homepage-redesign",
			State:          "ENABLED",
			DefaultVariant: "v1",
			Variants:       json.RawMessage(`{"v1":0.8,"v2":0.2}`),
			Targeting:      json.RawMessage(`{"country":["US"]}`),
		},
	}

	snap, err := BuildSnapshot(rows)
	if err != nil {
		t.Fatalf("BuildSnapshot: %v", err)
	}

	var doc map[string]any
	if err := json.Unmarshal([]byte(snap), &doc); err != nil {
		t.Fatalf("snapshot is not valid JSON: %v", err)
	}

	flags, ok := doc["flags"].(map[string]any)
	if !ok {
		t.Fatalf("snapshot missing \"flags\" object: %s", snap)
	}
	if len(flags) != 2 {
		t.Errorf("flags count = %d, want 2", len(flags))
	}

	banner := flags["banner"].(map[string]any)
	if banner["state"] != "ENABLED" || banner["defaultVariant"] != "on" {
		t.Errorf("banner flag mismatch: %v", banner)
	}
	if _, omitted := banner["targeting"]; omitted {
		t.Errorf("empty targeting should be omitted, got %v", banner["targeting"])
	}

	redesign := flags["homepage-redesign"].(map[string]any)
	if redesign["targeting"] == nil {
		t.Errorf("non-empty targeting must be present, got %v", redesign)
	}
}

// outboxPayload decodes a raw outbox payload the same way phylax does
// (json.Unmarshal into map[string]any).
func outboxPayload(t *testing.T, raw string) map[string]any {
	t.Helper()
	var p map[string]any
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return p
}

func TestApplyDelta(t *testing.T) {
	current := map[string]flagdFlag{
		"existing": {
			State:          "ENABLED",
			DefaultVariant: "a",
			Variants:       map[string]any{"a": 1, "b": 2},
			Targeting:      map[string]any{},
		},
	}

	t.Run("add new flag", func(t *testing.T) {
		got, err := ApplyDelta(current, outboxPayload(t, `{"key":"banner","state":"ENABLED","defaultVariant":"on","variants":{"on":true,"off":false},"targeting":{}}`))
		if err != nil {
			t.Fatalf("ApplyDelta add: %v", err)
		}
		banner, ok := got["banner"]
		if !ok {
			t.Fatalf("banner not added: %v", got)
		}
		if banner.State != "ENABLED" || banner.DefaultVariant != "on" {
			t.Errorf("banner = %+v, want ENABLED/on", banner)
		}
		if banner.Variants["on"] != true {
			t.Errorf("banner variants = %v, want on:true", banner.Variants)
		}
		if len(got) != 2 {
			t.Errorf("len = %d, want 2", len(got))
		}
	})

	t.Run("update existing flag", func(t *testing.T) {
		got, err := ApplyDelta(current, outboxPayload(t, `{"key":"existing","state":"DISABLED","defaultVariant":"b","variants":{"a":1,"b":2},"targeting":{}}`))
		if err != nil {
			t.Fatalf("ApplyDelta update: %v", err)
		}
		if got["existing"].State != "DISABLED" || got["existing"].DefaultVariant != "b" {
			t.Errorf("existing = %+v, want DISABLED/b", got["existing"])
		}
		if len(got) != 2 { // banner from previous test must be preserved
			t.Errorf("len = %d, want 2 (banner preserved)", len(got))
		}
	})

	t.Run("delete flag", func(t *testing.T) {
		got, err := ApplyDelta(current, map[string]any{"key": "existing", "deleted": true})
		if err != nil {
			t.Fatalf("ApplyDelta delete: %v", err)
		}
		if _, ok := got["existing"]; ok {
			t.Errorf("existing still present after delete: %v", got)
		}
		if len(got) != 1 {
			t.Errorf("len = %d, want 1 (only banner left)", len(got))
		}
	})

	t.Run("malformed payload returns error instead of panicking", func(t *testing.T) {
		bad := []map[string]any{
			{"key": 123}, // key not a string
			{"key": "x"}, // missing state/defaultVariant/variants/targeting
			{"key": "x", "state": "ENABLED", "defaultVariant": "a", "variants": nil, "targeting": map[string]any{}},
			{"key": "x", "state": 5, "defaultVariant": "a", "variants": map[string]any{}, "targeting": map[string]any{}},
		}
		for _, p := range bad {
			if _, err := ApplyDelta(current, p); err == nil {
				t.Errorf("ApplyDelta(%v) = nil error, want error", p)
			}
		}
	})
}
