-- +goose Up
-- Notify listeners whenever a flags event lands in the outbox, so flick
-- serve can push changes to flagd over Postgres LISTEN/NOTIFY instead of
-- logical replication. The notification carries only the row id: payloads
-- can exceed the 8 KB NOTIFY limit, and the consumer re-reads the row by
-- id (which also preserves exact commit order).
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

-- +goose Down
DROP TRIGGER IF EXISTS outbox_flags_notify ON outbox;
DROP FUNCTION IF EXISTS flick_outbox_notify();
