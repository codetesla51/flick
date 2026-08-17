package main

import (
	"database/sql"
	"fmt"

	"github.com/codetesla51/flick"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Set up the database (migrations + replication check)",
	Long: `Set up the database.

Applies the embedded goose migrations (flags + outbox tables) and verifies
the database is ready for logical replication streaming. The replication
slot and publication are created automatically by flick serve on first run.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		dsn := resolveDSN()

		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return fmt.Errorf("open db: %w", err)
		}
		defer db.Close()
		if err := db.Ping(); err != nil {
			return fmt.Errorf("ping db: %w", err)
		}

		if err := flick.Migrate(db); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		fmt.Println("migrations: up to date")

		var walLevel string
		if err := db.QueryRowContext(cmd.Context(), `SHOW wal_level`).Scan(&walLevel); err != nil {
			return fmt.Errorf("check wal_level: %w", err)
		}
		if walLevel != "logical" {
			return fmt.Errorf("wal_level = %q, but logical replication streaming requires 'logical'\n  fix: run ALTER SYSTEM SET wal_level = 'logical' (as superuser), then restart Postgres", walLevel)
		}
		fmt.Println("wal_level: logical")

		fmt.Println("ready: run `flick serve` to start the sync server")
		return nil
	},
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the flick version",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("flick %s\n", version)
	},
}
