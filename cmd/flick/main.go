package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// version is baked in at build time via
//
//	go build -ldflags "-X main.version=v0.1.0" ./cmd/flick
var version = "dev (HEAD)"

var rootCmd = &cobra.Command{
	Use:   "flick",
	Short: "Postgres-native feature flags, synced to flagd",
	Long: `flick is a feature-flag service backed by Postgres.

Flags live in the flags table; every change (create, update, delete) is
written to the outbox table in the same transaction. A trigger fires a
Postgres LISTEN/NOTIFY signal; the flick sync server consumes it and pushes
the flag to connected flagd instances over gRPC
(flagd.sync.v1.FlagSyncService) — push, no polling.

Commands:
  flick init     set up the database (migrations + notify probe)
  flick serve    run the flagd sync gRPC server
  flick set      create or update a flag
  flick get      show a single flag
  flick list     list all flags
  flick delete   delete a flag
  flick export   export all flags as JSON
  flick import   import flags from JSON (stdin)
  flick version  print the version

Every database command accepts --dsn, falling back to the FLICK_DSN
environment variable.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

var dsn string

func init() {
	rootCmd.PersistentFlags().StringVar(&dsn, "dsn", "",
		"Postgres DSN (default: FLICK_DSN env, then postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable)")
	rootCmd.AddCommand(serveCmd, setCmd, getCmd, listCmd, deleteCmd, initCmd, versionCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

// resolveDSN picks the DSN from --dsn, FLICK_DSN, or the local default.
func resolveDSN() string {
	if dsn != "" {
		return dsn
	}
	if v := os.Getenv("FLICK_DSN"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
}

// resolveAddr picks a listen address from flag value, env, or fallback.
func resolveAddr(flag, env, fallback string) string {
	if flag != "" {
		return flag
	}
	if v := os.Getenv(env); v != "" {
		return v
	}
	return fallback
}
