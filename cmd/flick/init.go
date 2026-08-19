package main

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/codetesla51/flick"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Set up the database (migrations + notify probe)",
	Long: `Set up the database.

Applies the embedded goose migrations (flags + outbox tables), verifies the
outbox notify trigger is installed, and runs a live LISTEN/NOTIFY probe: it
listens on a test channel from one connection, sends a notification from a
second, and confirms delivery — so you know push notifications actually work
before running flick serve.`,
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

		var hasTrigger bool
		if err := db.QueryRowContext(cmd.Context(),
			`SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'outbox_flags_notify')`,
		).Scan(&hasTrigger); err != nil {
			return fmt.Errorf("check notify trigger: %w", err)
		}
		if !hasTrigger {
			return errors.New("outbox_flags_notify trigger missing — the migrate step above should have created it; check migration output")
		}
		fmt.Println("outbox notify trigger: present")

		if err := runNotifyProbe(cmd.Context(), dsn); err != nil {
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
