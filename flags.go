package main

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

// flagEvent is the payload enqueued to the outbox on every flag write.
type flagEvent struct {
	Key            string          `json:"key"`
	State          string          `json:"state"`
	DefaultVariant string          `json:"defaultVariant"`
	Variants       json.RawMessage `json:"variants"`
	Targeting      json.RawMessage `json:"targeting"`
	Metadata       json.RawMessage `json:"metadata"`
}

// SetFlag upserts a feature flag and enqueues a 'flags' outbox event,
// both atomically in a single transaction.
func SetFlag(ctx context.Context, pool *pgxpool.Pool, key, state, defaultVariant string, variants, targeting, metadata json.RawMessage) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO flags (key, state, default_variant, variants, targeting, metadata, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (key) DO UPDATE SET
			state          = $2,
			default_variant = $3,
			variants       = $4,
			targeting      = $5,
			metadata       = $6,
			updated_at     = now()
	`, key, state, defaultVariant, variants, targeting, metadata); err != nil {
		return err
	}

	payload, err := json.Marshal(flagEvent{
		Key:            key,
		State:          state,
		DefaultVariant: defaultVariant,
		Variants:       variants,
		Targeting:      targeting,
		Metadata:       metadata,
	})
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox (topic, payload)
		VALUES ('flags', $1)
	`, json.RawMessage(payload)); err != nil {
		return err
	}

	return tx.Commit(ctx)
}