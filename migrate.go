package flick

import (
	"database/sql"
	"embed"

	"github.com/pressly/goose/v3"
)

//go:embed db/goose_migrations/*.sql
var embedMigrations embed.FS

// Migrate applies the embedded goose migrations to db.
func Migrate(db *sql.DB) error {
	goose.SetBaseFS(embedMigrations)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.Up(db, "db/goose_migrations")
}
