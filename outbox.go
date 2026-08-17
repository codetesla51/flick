package flick

import (
	"context"
	"log"

	"github.com/codetesla51/phylax"
)

// RunOutbox wires phylax to the outbox table, consumes delivered rows until
// ctx is cancelled, and publishes flag deltas to hub. Delivery is logged and
// acked; the hub fan-out is drop-on-full by design.
func RunOutbox(ctx context.Context, dsn string, hub *Hub) error {
	cdc, err := phylax.New(phylax.Config{
		DSN:             dsn,
		Tables:          []string{"outbox"},
		OutboxTable:     "outbox",
		SlotName:        "flick_slot",
		PublicationName: "flick_pub",
	})
	if err != nil {
		return err
	}

	cdc.OnOutboxDelivery(func(ctx context.Context, row *phylax.OutboxRow) error {
		log.Printf("outbox deliver: id=%d topic=%q payload=%v", row.ID, row.Topic, row.Payload)
		if row.Topic == "flags" {
			hub.Publish(row.Payload)
		}
		return nil
	})

	return cdc.Start(ctx)
}
