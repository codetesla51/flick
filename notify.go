package flick

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// notifyChannel is the Postgres LISTEN/NOTIFY channel over which flag
// changes are signalled. SetFlag/DeleteFlag fire pg_notify('flick_flags',
// payload) inside the write transaction — pg_notify is delivered only when
// the transaction commits, so the flag write and its push stay atomic.
// The payload carries just the flag key (plus a deleted marker for deletes);
// the consumer re-reads the flags row by key, which also sidesteps the
// 8 KB NOTIFY payload limit.
const notifyChannel = "flick_flags"

const (
	// notifyEventBuffer is the per-console-subscriber event queue depth.
	notifyEventBuffer = 64
	// notifyRecentEvents is how many recent events are replayed to newly
	// connected console clients so the stream strip has context.
	notifyRecentEvents = 50
	// notifyReconnectDelay is the wait between notification-connection
	// retries.
	notifyReconnectDelay = time.Second
	// notifyAppName identifies the layer's connection in pg_stat_activity.
	notifyAppName = "flick-notify"
)

// NotifyEvent is one delivered flag change, as seen by console subscribers.
type NotifyEvent struct {
	Payload map[string]any `json:"payload"`
	Time    time.Time      `json:"time,omitempty"`
}

// NotifyMetrics mirrors the shape the console /metrics/stream endpoint
// expects.
type NotifyMetrics struct {
	Delivered   int64 `json:"delivered"`
	Changes     int64 `json:"changes"`
	Subscribers int   `json:"subscribers"`
	Dropped     int64 `json:"dropped"`
	Failed      int64 `json:"failed"`
}

// NotifyLayer streams flag changes to the hub (and the console feed) over
// Postgres LISTEN/NOTIFY.
//
// Writes keep their existing shape: SetFlag/DeleteFlag commit the flags
// write and a pg_notify('flick_flags', key) in one transaction. The layer
// listens on the channel, re-reads the flags row by key, and publishes the
// flag delta to hub — the same payload shape ApplyDelta/SyncFlags expect.
//
// Live NOTIFY delivery is at-most-once: a notification sent while the layer
// is disconnected is lost. This is fine because flagd resyncs a full
// snapshot whenever a client (re)connects, which bounds staleness
// regardless of transport.
type NotifyLayer struct {
	dsn string
	hub *Hub

	ready     chan struct{} // closed once the first LISTEN completes
	onceReady sync.Once

	mu      sync.Mutex
	subs    map[int]chan NotifyEvent
	nextSub int
	recent  []NotifyEvent

	delivered atomic.Int64 // events published to hub
	dropped   atomic.Int64 // console subscribers with full buffers
	failed    atomic.Int64 // connection / decode errors
}

// NewNotifyLayer returns a layer that streams flag changes to hub over
// LISTEN/NOTIFY. It connects lazily in Start.
func NewNotifyLayer(dsn string, hub *Hub) *NotifyLayer {
	return &NotifyLayer{dsn: dsn, hub: hub, subs: make(map[int]chan NotifyEvent), ready: make(chan struct{})}
}

// Ready is closed once the layer has completed its first LISTEN, so callers
// (tests, probes) can wait until live delivery is actually armed.
func (n *NotifyLayer) Ready() <-chan struct{} {
	return n.ready
}

// Start runs the layer until ctx is cancelled, reconnecting with backoff if
// the notification connection drops. It returns only when ctx is done.
func (n *NotifyLayer) Start(ctx context.Context) error {
	for {
		err := n.runOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		n.failed.Add(1)
		log.Printf("notify: stream error: %v; reconnecting in %s", err, notifyReconnectDelay)
		select {
		case <-time.After(notifyReconnectDelay):
		case <-ctx.Done():
			return nil
		}
	}
}

// runOnce connects, LISTENs, then consumes notifications until the
// connection drops.
func (n *NotifyLayer) runOnce(ctx context.Context) error {
	cfg, err := pgx.ParseConfig(n.dsn)
	if err != nil {
		return err
	}
	cfg.RuntimeParams["application_name"] = notifyAppName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	log.Printf("notify: connected (pid %d), listening on %s", conn.PgConn().PID(), notifyChannel)
	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return err
	}
	n.onceReady.Do(func() { close(n.ready) })

	for {
		notif, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if err := n.handleNotify(ctx, conn, notif.Payload); err != nil {
			n.failed.Add(1)
			log.Printf("notify: handling %q: %v", notif.Payload, err)
		}
	}
}

// handleNotify resolves a notification (a flag key, plus a deleted marker
// for deletes) to a flag delta and delivers it. Set notifications re-read
// the flags row by key so the delivered payload always reflects committed
// state; a set notification whose row is already gone (changed then deleted
// before we read it) is delivered as a delete.
func (n *NotifyLayer) handleNotify(ctx context.Context, conn *pgx.Conn, raw string) error {
	var m struct {
		Key     string `json:"key"`
		Deleted bool   `json:"deleted"`
	}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return fmt.Errorf("bad notification payload %q: %w", raw, err)
	}
	if m.Key == "" {
		return fmt.Errorf("notification payload missing key: %q", raw)
	}

	if m.Deleted {
		n.deliver(NotifyEvent{
			Payload: map[string]any{"key": m.Key, "deleted": true},
			Time:    time.Now().UTC(),
		})
		return nil
	}

	var (
		state, defaultVariant string
		variants, targeting   []byte
	)
	err := conn.QueryRow(ctx,
		`SELECT state, default_variant, variants::text, targeting::text FROM flags WHERE key = $1`,
		m.Key,
	).Scan(&state, &defaultVariant, &variants, &targeting)
	if errors.Is(err, pgx.ErrNoRows) {
		// Changed then deleted before we re-read it — converge on delete.
		n.deliver(NotifyEvent{
			Payload: map[string]any{"key": m.Key, "deleted": true},
			Time:    time.Now().UTC(),
		})
		return nil
	}
	if err != nil {
		return err
	}

	var variantsMap, targetingMap map[string]any
	_ = json.Unmarshal(variants, &variantsMap)
	_ = json.Unmarshal(targeting, &targetingMap)
	if variantsMap == nil {
		variantsMap = map[string]any{}
	}
	if targetingMap == nil {
		targetingMap = map[string]any{}
	}

	n.deliver(NotifyEvent{
		Payload: map[string]any{
			"key":            m.Key,
			"state":          state,
			"defaultVariant": defaultVariant,
			"variants":       variantsMap,
			"targeting":      targetingMap,
		},
		Time: time.Now().UTC(),
	})
	return nil
}

// deliver publishes an event to the hub (flagd sync) and the console feed.
func (n *NotifyLayer) deliver(e NotifyEvent) {
	n.delivered.Add(1)
	n.hub.Publish(e.Payload)

	n.mu.Lock()
	defer n.mu.Unlock()
	n.recent = append(n.recent, e)
	if len(n.recent) > notifyRecentEvents {
		n.recent = n.recent[len(n.recent)-notifyRecentEvents:]
	}
	for _, c := range n.subs {
		select {
		case c <- e:
		default:
			n.dropped.Add(1)
		}
	}
}

// SubscribeEvents registers a console event subscriber. The channel is
// pre-loaded with recent history and receives new events until
// UnsubscribeEvents is called.
func (n *NotifyLayer) SubscribeEvents() (int, <-chan NotifyEvent) {
	c := make(chan NotifyEvent, notifyEventBuffer)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nextSub++
	n.subs[n.nextSub] = c
	for _, e := range n.recent {
		select {
		case c <- e:
		default:
		}
	}
	return n.nextSub, c
}

// UnsubscribeEvents removes a console event subscriber and closes its
// channel. Safe to call once per SubscribeEvents id.
func (n *NotifyLayer) UnsubscribeEvents(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if c, ok := n.subs[id]; ok {
		delete(n.subs, id)
		close(c)
	}
}

// MetricsSnapshot returns the layer's live counters.
func (n *NotifyLayer) MetricsSnapshot() NotifyMetrics {
	return NotifyMetrics{
		Delivered: n.delivered.Load(),
		Changes:   n.delivered.Load(),
		Dropped:   n.dropped.Load(),
		Failed:    n.failed.Load(),
	}
}
