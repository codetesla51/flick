package flick

import (
	"context"
	"log"

	"github.com/codetesla51/phylax"
)

// NewOutbox builds a phylax CDC client wired to the outbox table: every
// delivered row is logged, acked (at-least-once), and flag deltas are
// published to hub. The client does not connect until Start is called;
// MetricsSnapshot() reports live counters at any point.
//
//	cdc, err := flick.NewOutbox(dsn, hub)
//	cdc.OnOutboxDelivery(...)   // optional extra consumers
//	err = cdc.Start(ctx)        // run until ctx is cancelled
func NewOutbox(dsn string, hub *Hub) (*phylax.CDC, error) {
	cdc, err := phylax.New(phylax.Config{
		DSN:             dsn,
		Tables:          []string{"outbox"},
		OutboxTable:     "outbox",
		SlotName:        "flick_slot",
		PublicationName: "flick_pub",
	})
	if err != nil {
		return nil, err
	}

	cdc.OnOutboxDelivery(func(ctx context.Context, row *phylax.OutboxRow) error {
		log.Printf("outbox deliver: id=%d topic=%q payload=%v", row.ID, row.Topic, row.Payload)
		if row.Topic == "flags" {
			hub.Publish(row.Payload)
		}
		return nil
	})

	return cdc, nil
}

// RunOutbox builds an outbox CDC client and runs it until ctx is cancelled.
// It is the one-shot form of NewOutbox + cdc.Start.
func RunOutbox(ctx context.Context, dsn string, hub *Hub) error {
	cdc, err := NewOutbox(dsn, hub)
	if err != nil {
		return err
	}
	return cdc.Start(ctx)
}
