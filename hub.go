package flick

import (
	"log"
	"sync"
)

const hubBufferSize = 256

// Hub fans out flag deltas from the outbox consumer to SyncFlags subscribers.
// Publishing never blocks: a subscriber whose buffer is full has the delta
// dropped (and logged), so a slow consumer can never stall the WAL stream.
type Hub struct {
	mu     sync.Mutex
	nextID int
	subs   map[int]chan map[string]any
}

// NewHub returns an empty Hub ready to publish and subscribe.
func NewHub() *Hub {
	return &Hub{subs: make(map[int]chan map[string]any)}
}

// Subscribe registers a subscriber and returns its id plus a buffered channel
// of deltas. Call Unsubscribe with the id when done.
func (h *Hub) Subscribe() (int, chan map[string]any) {
	c := make(chan map[string]any, hubBufferSize)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	h.subs[h.nextID] = c
	return h.nextID, c
}

// Unsubscribe removes the subscriber and closes its channel. Safe to call
// once; Publish never sends to a removed channel.
func (h *Hub) Unsubscribe(id int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.subs[id]; ok {
		delete(h.subs, id)
		close(c)
	}
}

// Publish delivers a delta to every subscriber, dropping (and logging) for
// any subscriber whose buffer is full.
func (h *Hub) Publish(payload map[string]any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, c := range h.subs {
		select {
		case c <- payload:
		default:
			log.Printf("hub: dropping delta for subscriber %d (buffer full)", id)
		}
	}
}
