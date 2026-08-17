-- +goose Up
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_pending ON outbox (topic, id) WHERE delivered_at IS NULL;

-- +goose Down
DROP INDEX idx_outbox_pending;
DROP TABLE outbox;