package flick

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// flagEvent is the row model for a flag as stored in the flags table.
type flagEvent struct {
	Key            string          `json:"key"`
	State          string          `json:"state"`
	DefaultVariant string          `json:"defaultVariant"`
	Variants       json.RawMessage `json:"variants"`
	Targeting      json.RawMessage `json:"targeting"`
	Metadata       json.RawMessage `json:"metadata"`
	Deleted        bool            `json:"deleted,omitempty"`
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

// SetFlag upserts a feature flag and notifies consumers (pg_notify) in a
// single transaction. The notification is delivered only when the
// transaction commits, so a rolled-back write never notifies — the flag
// write and its push stay atomic.
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

	// The listener re-reads the flags row by key, so the notification only
	// needs to carry the key — well under the 8 KB NOTIFY payload limit.
	notifyPayload, err := json.Marshal(map[string]any{"key": key})
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, notifyPayload); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// DeleteFlag removes a feature flag and notifies consumers (pg_notify) in a
// single transaction. Deleting an absent key is a no-op: nothing is deleted
// and no notification is sent.
func DeleteFlag(ctx context.Context, pool *pgxpool.Pool, key string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `DELETE FROM flags WHERE key = $1`, key)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}

	notifyPayload, err := json.Marshal(map[string]any{"key": key, "deleted": true})
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, notifyPayload); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// BuildSnapshot builds a snapshot of all flags in the format expected by flagd.
func BuildSnapshot(rows []flagEvent) (string, error) {
	flags := make(map[string]flagdFlag, len(rows))
	for _, row := range rows {
		flags[row.Key] = TranslateFlag(row)
	}
	return marshalFlags(flags)
}

// flagEventsToState translates flag rows into the in-memory state used by
// both FetchAllFlags and the SyncFlags streaming loop.
func flagEventsToState(rows []flagEvent) map[string]flagdFlag {
	flags := make(map[string]flagdFlag, len(rows))
	for _, row := range rows {
		flags[row.Key] = TranslateFlag(row)
	}
	return flags
}

// marshalFlags renders the flag state as a flagd-compatible {"flags": ...} doc.
func marshalFlags(flags map[string]flagdFlag) (string, error) {
	b, err := json.Marshal(map[string]any{"flags": flags})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func ApplyDelta(current map[string]flagdFlag, payload map[string]any) (map[string]flagdFlag, error) {

	key, keyOk := payload["key"].(string)

	if deleted, ok := payload["deleted"].(bool); ok && deleted {
		delete(current, key)
		return current, nil
	}

	state, stateOK := payload["state"].(string)
	defaultVariant, variantOK := payload["defaultVariant"].(string)
	variants, variantsOK := payload["variants"].(map[string]any)
	targeting, targetingOK := payload["targeting"].(map[string]any)
	if !stateOK || !variantOK || !variantsOK || !targetingOK || !keyOk {
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
