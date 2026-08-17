-- +goose Up
CREATE TABLE flags (
  key             TEXT PRIMARY KEY,
  state           TEXT NOT NULL DEFAULT 'ENABLED'
                    CHECK (state IN ('ENABLED', 'DISABLED')),
  default_variant TEXT NOT NULL,
  variants        JSONB NOT NULL,
  targeting       JSONB NOT NULL DEFAULT '{}',
  metadata        JSONB NOT NULL DEFAULT '{}',
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (variants ? default_variant)
);

-- +goose Down
DROP TABLE flags;