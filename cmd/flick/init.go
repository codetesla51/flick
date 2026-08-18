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
	Short: "Set up the database (migrations + replication probe)",
	Long: `Set up the database.

Applies the embedded goose migrations (flags + outbox tables), verifies the
database is ready for logical replication, and runs an end-to-end probe:
it creates the flick_slot slot and flick_pub publication (the same ones
flick serve uses), writes a probe row to the outbox table, and confirms the
stream delivers it — so you know replication actually works before running
flick serve.`,
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
		var pendingRestart bool
		if err := db.QueryRowContext(cmd.Context(),
			`SELECT setting, pending_restart FROM pg_settings WHERE name = 'wal_level'`,
		).Scan(&walLevel, &pendingRestart); err != nil {
			return fmt.Errorf("check wal_level: %w", err)
		}
		if walLevel != "logical" {
			if pendingRestart {
				return fmt.Errorf("wal_level = %q — 'logical' is configured but Postgres hasn't restarted\n  fix: restart Postgres to apply the pending wal_level change", walLevel)
			}
			return fmt.Errorf("wal_level = %q, but logical replication streaming requires 'logical'\n  fix: run ALTER SYSTEM SET wal_level = 'logical' (as superuser), then restart Postgres", walLevel)
		}
		fmt.Println("wal_level: logical")

		if err := runReplicationProbe(cmd.Context(), dsn); err != nil {
			return err
		}

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
