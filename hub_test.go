package flick

import (
	"testing"
	"time"
)

func TestHubPublishSubscribe(t *testing.T) {
	h := NewHub()

	id, ch := h.Subscribe()
	defer h.Unsubscribe(id)

	h.Publish(map[string]any{"key": "a", "state": "ENABLED"})
	h.Publish(map[string]any{"key": "b", "state": "DISABLED"})

	first := <-ch
	if first["key"] != "a" {
		t.Errorf("first delta = %v, want key=a (order preserved)", first)
	}
	second := <-ch
	if second["key"] != "b" {
		t.Errorf("second delta = %v, want key=b", second)
	}
}

func TestHubBuffersWhileSubscriberIsBusy(t *testing.T) {
	h := NewHub()

	// Subscribe first (as SyncFlags does), then let deltas pile up while the
	// subscriber is busy building a snapshot.
	id, ch := h.Subscribe()
	defer h.Unsubscribe(id)

	for i := 0; i < 10; i++ {
		h.Publish(map[string]any{"key": "flag", "state": "ENABLED"})
	}

	// Nothing dropped: all 10 arrived, in order.
	for i := 0; i < 10; i++ {
		if _, ok := <-ch; !ok {
			t.Fatalf("channel closed after %d deltas, want 10", i)
		}
	}
}

func TestHubUnsubscribeStopsDelivery(t *testing.T) {
	h := NewHub()

	id, ch := h.Subscribe()
	h.Unsubscribe(id)
	// Unsubscribe closes the channel; a subsequent read reports closed.
	if _, ok := <-ch; ok {
		t.Error("channel still open after Unsubscribe")
	}

	// Publish must not panic or deliver to the removed subscriber.
	h.Publish(map[string]any{"key": "x"})
}

func TestHubDropOnFullDoesNotBlock(t *testing.T) {
	h := NewHub()

	id, _ := h.Subscribe()
	defer h.Unsubscribe(id)

	// Fill the buffer beyond capacity without reading; Publish must return
	// (never block the delivery path).
	done := make(chan struct{})
	go func() {
		for i := 0; i < hubBufferSize*3; i++ {
			h.Publish(map[string]any{"key": "x"})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked with a full subscriber buffer")
	}
}
