package flick

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// notifyChannel is the Postgres LISTEN/NOTIFY channel over which outbox
// events are signalled. The outbox_flags_notify trigger fires
// pg_notify('flick_flags', id) when a flags outbox row commits; the consumer
// re-reads the row by id, so the 8 KB NOTIFY payload limit never applies.
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

// NotifyEvent is one delivered outbox event, as seen by console subscribers.
type NotifyEvent struct {
	ID      int64          `json:"id"`
	Topic   string         `json:"topic"`
	Payload map[string]any `json:"payload"`
	Time    time.Time      `json:"time,omitempty"`
}

// NotifyMetrics mirrors the shape the console /metrics/stream endpoint
// expects, plus a replayed counter.
type NotifyMetrics struct {
	OutboxDelivered  int64 `json:"outbox_delivered"`
	ChangesProcessed int64 `json:"changes_processed"`
	Subscribers      int   `json:"subscribers"`
	ChangesDropped   int64 `json:"changes_dropped"`
	OutboxInflight   int64 `json:"outbox_inflight"`
	OutboxFailed     int64 `json:"outbox_failed"`
	Replayed         int64 `json:"replayed"`
}

// NotifyLayer streams outbox events to the hub (and the console feed) over
// Postgres LISTEN/NOTIFY.
//
// Writes keep their existing shape: SetFlag/DeleteFlag insert an outbox row
// in the same transaction as the flags change, and a trigger fires
// pg_notify('flick_flags', id) once that transaction commits. The layer
// listens on the channel, re-reads each row by id, and publishes the flag
// delta to hub — the same flagEvent shape the logical-replication consumer
// delivered, so ApplyDelta/SyncFlags are untouched.
//
// Live NOTIFY delivery is at-most-once: a notification sent while the layer
// is disconnected is lost. Start() therefore first replays outbox rows that
// were never delivered (delivered_at IS NULL) and marks them delivered, so
// changes made while flick was down are still pushed to flagd on startup.
// flagd additionally resyncs a full snapshot whenever a client (re)connects,
// which bounds staleness regardless of transport.
type NotifyLayer struct {
	dsn string
	hub *Hub

	mu      sync.Mutex
	subs    map[int]chan NotifyEvent
	nextSub int
	recent  []NotifyEvent

	delivered atomic.Int64 // events published to hub
	replayed  atomic.Int64 // events replayed from pending outbox rows
	dropped   atomic.Int64 // console subscribers with full buffers
	failed    atomic.Int64 // connection / decode errors
}

// NewNotifyLayer returns a layer that streams outbox events to hub over
// LISTEN/NOTIFY. It connects lazily in Start.
func NewNotifyLayer(dsn string, hub *Hub) *NotifyLayer {
	return &NotifyLayer{dsn: dsn, hub: hub, subs: make(map[int]chan NotifyEvent)}
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

// runOnce connects, LISTENs, replays pending rows, then consumes
// notifications until the connection drops.
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

	// LISTEN before replaying: after this point any committed row is either
	// delivered live by its notification or picked up by the replay below
	// (or both, in the tiny overlap — publishing the same delta twice is
	// idempotent for flagd, so duplicates are safe and misses are not).
	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return err
	}

	if err := n.replayPending(ctx, conn); err != nil {
		log.Printf("notify: replay pending: %v", err)
	}

	for {
		notif, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if err := n.handleNotify(ctx, conn, notif.Payload); err != nil {
			n.failed.Add(1)
			log.Printf("notify: handling row %s: %v", notif.Payload, err)
		}
	}
}

// replayPending publishes outbox rows that were never delivered and marks
// them delivered, so changes made while flick was down reach flagd on boot.
func (n *NotifyLayer) replayPending(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, `
		SELECT id, payload FROM outbox
		WHERE topic = 'flags' AND delivered_at IS NULL
		ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		events []NotifyEvent
		ids    []int64
	)
	for rows.Next() {
		var (
			e       NotifyEvent
			payload []byte
		)
		if err := rows.Scan(&e.ID, &payload); err != nil {
			return err
		}
		if err := json.Unmarshal(payload, &e.Payload); err != nil {
			log.Printf("notify: replay row %d: bad payload: %v", e.ID, err)
			continue
		}
		e.Topic = "flags"
		e.Time = time.Now().UTC()
		events = append(events, e)
		ids = append(ids, e.ID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, e := range events {
		n.deliver(e)
	}
	if len(ids) > 0 {
		if _, err := conn.Exec(ctx,
			`UPDATE outbox SET delivered_at = now() WHERE id = ANY($1) AND delivered_at IS NULL`, ids); err != nil {
			return err
		}
		n.replayed.Add(int64(len(ids)))
	}
	return nil
}

// handleNotify resolves a notification payload (an outbox row id) to its
// event, delivers it, and marks the row delivered.
func (n *NotifyLayer) handleNotify(ctx context.Context, conn *pgx.Conn, idStr string) error {
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return fmt.Errorf("bad notification payload %q: %w", idStr, err)
	}
	var payload []byte
	err = conn.QueryRow(ctx, `SELECT payload FROM outbox WHERE id = $1`, id).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("row %d deleted before delivery", id)
	}
	if err != nil {
		return err
	}
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return fmt.Errorf("row %d: bad payload: %w", id, err)
	}
	n.deliver(NotifyEvent{ID: id, Topic: "flags", Payload: m, Time: time.Now().UTC()})

	// Mark consumed so the row never replays and the console's pending
	// count stays accurate. Best-effort: delivery already happened.
	if _, err := conn.Exec(ctx,
		`UPDATE outbox SET delivered_at = now() WHERE id = $1 AND delivered_at IS NULL`, id); err != nil {
		log.Printf("notify: mark row %d delivered: %v", id, err)
	}
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
		OutboxDelivered:  n.delivered.Load(),
		ChangesProcessed: n.delivered.Load(),
		ChangesDropped:   n.dropped.Load(),
		OutboxFailed:     n.failed.Load(),
		Replayed:         n.replayed.Load(),
	}
}
