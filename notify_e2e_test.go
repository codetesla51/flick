//go:build e2e

package flick

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// setupNotifyPool creates a pool on the scratch notify-test database and
// registers cleanup for the given key. Both the layer and the writes must
// use the scratch database so the live consumer only ever hears this
// test's own events.
func setupNotifyPool(t *testing.T, key string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), notifyTestDSN)
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

// startLayer runs a NotifyLayer on the scratch notify-test database and
// returns a cleanup that cancels it and waits for the goroutine to fully
// stop — a still-running layer would otherwise steal the next test's
// notifications. It blocks until the layer's LISTEN has completed, so live
// deliveries are never raced against a not-yet-listening session.
func startLayer(t *testing.T, hub *Hub) *NotifyLayer {
	t.Helper()
	layer := NewNotifyLayer(notifyTestDSN, hub)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = layer.Start(ctx)
	}()
	select {
	case <-layer.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("notify layer did not become ready within 10s")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("notify layer did not stop within 5s")
		}
	})
	return layer
}

// awaitEvent reads hub deliveries until one matches pred, ignoring events
// from other sources. Fails the test if none shows up within 10s.
func awaitEvent(t *testing.T, ch <-chan map[string]any, pred func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case got := <-ch:
			if pred(got) {
				return got
			}
		case <-deadline:
			t.Fatalf("no matching delivery within 10s")
			return nil
		}
	}
}

// awaitKey reads hub deliveries until one for key arrives.
func awaitKey(t *testing.T, ch <-chan map[string]any, key string) map[string]any {
	t.Helper()
	return awaitEvent(t, ch, func(e map[string]any) bool { return e["key"] == key })
}

func TestNotifyLayerDeliversLiveEvent(t *testing.T) {
	ctx := context.Background()
	const key = "notify_live_e2e_test"
	pool := setupNotifyPool(t, key)

	hub := NewHub()
	subID, ch := hub.Subscribe()
	defer hub.Unsubscribe(subID)

	_ = startLayer(t, hub)

	if err := SetFlag(ctx, pool, key, "ENABLED", "on",
		json.RawMessage(`{"on":true,"off":false}`),
		json.RawMessage(`{}`),
		json.RawMessage(`{"owner":"notify"}`)); err != nil {
		t.Fatalf("SetFlag: %v", err)
	}

	got := awaitKey(t, ch, key)
	if got["state"] != "ENABLED" {
		t.Errorf("delivered state = %v, want ENABLED", got["state"])
	}
	if got["deleted"] == true {
		t.Errorf("delivered event marked deleted: %v", got)
	}
}

func TestNotifyLayerDeliversDelete(t *testing.T) {
	ctx := context.Background()
	const key = "notify_delete_e2e_test"
	pool := setupNotifyPool(t, key)
	if err := SetFlag(ctx, pool, key, "ENABLED", "on",
		json.RawMessage(`{"on":true}`), json.RawMessage(`{}`), json.RawMessage(`{}`)); err != nil {
		t.Fatalf("SetFlag: %v", err)
	}

	hub := NewHub()
	subID, ch := hub.Subscribe()
	defer hub.Unsubscribe(subID)

	_ = startLayer(t, hub)

	if err := DeleteFlag(ctx, pool, key); err != nil {
		t.Fatalf("DeleteFlag: %v", err)
	}

	got := awaitEvent(t, ch, func(e map[string]any) bool {
		return e["key"] == key && e["deleted"] == true
	})
	if got["deleted"] != true {
		t.Errorf("delete event not marked deleted: %v", got)
	}
}

func TestNotifyLayerMetricsCountDeliveries(t *testing.T) {
	ctx := context.Background()
	const key = "notify_metrics_e2e_test"
	pool := setupNotifyPool(t, key)

	hub := NewHub()
	subID, ch := hub.Subscribe()
	defer hub.Unsubscribe(subID)

	layer := startLayer(t, hub)

	if err := SetFlag(ctx, pool, key, "ENABLED", "on",
		json.RawMessage(`{"on":true}`), json.RawMessage(`{}`), json.RawMessage(`{}`)); err != nil {
		t.Fatalf("SetFlag: %v", err)
	}
	awaitKey(t, ch, key)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if snap := layer.MetricsSnapshot(); snap.Delivered >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("metrics never showed a delivery: %+v", layer.MetricsSnapshot())
}

// TestNotifyLayerDeleteAbsentKeyEmitsNothing: deleting an absent key must
// not reach consumers at all — DeleteFlag skips the notification when no
// row was deleted.
func TestNotifyLayerDeleteAbsentKeyEmitsNothing(t *testing.T) {
	ctx := context.Background()
	const key = "notify_absent_delete_e2e_test"
	pool := setupNotifyPool(t, key)

	hub := NewHub()
	subID, ch := hub.Subscribe()
	defer hub.Unsubscribe(subID)

	startLayer(t, hub)

	if err := DeleteFlag(ctx, pool, key); err != nil {
		t.Fatalf("DeleteFlag absent: %v", err)
	}

	select {
	case got := <-ch:
		t.Fatalf("unexpected delivery for absent-key delete: %v", got)
	case <-time.After(750 * time.Millisecond):
		// nothing delivered — correct
	}
}
