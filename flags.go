package flick

import (
	"context"
	"encoding/json"
	"fmt"

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

type flagdFlag struct {
	State          string         `json:"state"`
	DefaultVariant string         `json:"defaultVariant"`
	Variants       map[string]any `json:"variants"`
	Targeting      map[string]any `json:"targeting,omitempty"`
}

func TranslateFlag(row flagEvent) flagdFlag {
	var variants map[string]any
	if err := json.Unmarshal(row.Variants, &variants); err != nil {
		variants = map[string]any{}
	}

	var targeting map[string]any
	if err := json.Unmarshal(row.Targeting, &targeting); err != nil {
		targeting = map[string]any{}
	}

	return flagdFlag{
		State:          row.State,
		DefaultVariant: row.DefaultVariant,
		Variants:       variants,
		Targeting:      targeting,
	}
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

// BuildSnapshot builds a snapshot of all flags in the format expected by flagd.
func BuildSnapshot(rows []flagEvent) (string, error) {
	flags := make(map[string]flagdFlag)
	for _, row := range rows {
		flags[row.Key] = TranslateFlag(row)
	}

	doc := make(map[string]any)
	doc["flags"] = flags
	b, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func ApplyDelta(current map[string]flagdFlag, payload map[string]any) (map[string]flagdFlag, error) {

	key, ok := payload["key"].(string)
	if !ok {
		return current, fmt.Errorf("payload missing or invalid 'key'")
	}

	if deleted, ok := payload["deleted"].(bool); ok && deleted {
		delete(current, key)
		return current, nil
	}

	state, stateOK := payload["state"].(string)
	defaultVariant, variantOK := payload["defaultVariant"].(string)
	variants, variantsOK := payload["variants"].(map[string]any)
	targeting, targetingOK := payload["targeting"].(map[string]any)
	if !stateOK || !variantOK || !variantsOK || !targetingOK {
		return current, fmt.Errorf("payload for key %q missing or invalid fields: %v", key, payload)
	}

	current[key] = flagdFlag{
		State:          state,
		DefaultVariant: defaultVariant,
		Variants:       variants,
		Targeting:      targeting,
	}
	return current, nil
}