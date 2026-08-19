-- +goose Up
-- v0.4: the outbox is gone. SetFlag/DeleteFlag now notify consumers
-- directly with pg_notify('flick_flags', ...) in the same transaction as
-- the flags write (pg_notify is delivered only on commit, so atomicity is
-- preserved), and the notify layer re-reads the flags row by key. The
-- outbox table, its notify trigger, and the replay machinery are removed.
DROP TRIGGER IF EXISTS outbox_flags_notify ON outbox;
DROP FUNCTION IF EXISTS flick_outbox_notify();
DROP TABLE IF EXISTS outbox;

-- +goose Down
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_pending ON outbox (topic, id) WHERE delivered_at IS NULL;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION flick_outbox_notify() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('flick_flags', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER outbox_flags_notify
AFTER INSERT ON outbox
FOR EACH ROW
WHEN (NEW.topic = 'flags')
EXECUTE FUNCTION flick_outbox_notify();
