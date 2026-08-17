package main

import (
	"encoding/json"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/codetesla51/flick"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
)

var (
	flagState          string
	flagDefaultVariant string
	flagVariants       string
	flagTargeting      string
	flagMetadata       string
)

func init() {
	setCmd.Flags().StringVar(&flagState, "state", "ENABLED", "ENABLED or DISABLED")
	setCmd.Flags().StringVar(&flagDefaultVariant, "default-variant", "", "variant used when no targeting matches (required)")
	setCmd.Flags().StringVar(&flagVariants, "variants", "", "JSON object of variant -> value (required)")
	setCmd.Flags().StringVar(&flagTargeting, "targeting", "{}", "JSON targeting rules")
	setCmd.Flags().StringVar(&flagMetadata, "metadata", "{}", "JSON metadata")
}

var setCmd = &cobra.Command{
	Use:   "set <key>",
	Short: "Create or update a flag",
	Long: `Create or update a flag.

Writes the flag and its outbox change event in one transaction — flagd
clients see the change within milliseconds.

  --state            ENABLED or DISABLED (default ENABLED)
  --default-variant  the variant used when no targeting matches (required)
  --variants         JSON object of variant -> value (required)
  --targeting        JSON targeting rules (default {})
  --metadata         JSON metadata (default {})`,
	Example: `  flick set show-banner --default-variant on --variants '{"on":true,"off":false}'
  flick set api-v2 --state DISABLED --default-variant off --variants '{"on":true,"off":false}'
  flick set api-v2 --default-variant off --variants '{"on":true,"off":false}' \
      --targeting '{"country":["NG"]}' --metadata '{"owner":"platform"}'`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]

		if flagState != "ENABLED" && flagState != "DISABLED" {
			return fmt.Errorf("--state must be ENABLED or DISABLED, got %q", flagState)
		}
		if flagDefaultVariant == "" {
			return fmt.Errorf("--default-variant is required")
		}
		for name, raw := range map[string]string{
			"--variants":  flagVariants,
			"--targeting": flagTargeting,
			"--metadata":  flagMetadata,
		} {
			if !json.Valid([]byte(raw)) {
				return fmt.Errorf("%s must be valid JSON, got %q", name, raw)
			}
		}

		pool, err := pgxpool.New(cmd.Context(), resolveDSN())
		if err != nil {
			return err
		}
		defer pool.Close()

		if err := flick.SetFlag(cmd.Context(), pool, key, flagState, flagDefaultVariant,
			json.RawMessage(flagVariants), json.RawMessage(flagTargeting), json.RawMessage(flagMetadata)); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "set %s (%s, default %q)\n", key, flagState, flagDefaultVariant)
		return nil
	},
}

var getCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Show a single flag",
	Long:  `Show all fields of one flag: state, default variant, variants, targeting, metadata, last update.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		pool, err := pgxpool.New(cmd.Context(), resolveDSN())
		if err != nil {
			return err
		}
		defer pool.Close()

		var state, def string
		var variants, targeting, metadata []byte
		var updated time.Time
		err = pool.QueryRow(cmd.Context(), `
			SELECT state, default_variant, variants, targeting, metadata, updated_at
			FROM flags WHERE key=$1`, key).Scan(&state, &def, &variants, &targeting, &metadata, &updated)
		if err != nil {
			if err.Error() == "no rows in result set" {
				return fmt.Errorf("no flag named %q", key)
			}
			return err
		}

		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		fmt.Fprintf(w, "key:\t%s\n", key)
		fmt.Fprintf(w, "state:\t%s\n", state)
		fmt.Fprintf(w, "default:\t%s\n", def)
		fmt.Fprintf(w, "variants:\t%s\n", variants)
		fmt.Fprintf(w, "targeting:\t%s\n", targeting)
		fmt.Fprintf(w, "metadata:\t%s\n", metadata)
		fmt.Fprintf(w, "updated:\t%s\n", updated.Format("2006-01-02 15:04:05"))
		return w.Flush()
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all flags",
	Long:  `List every flag: key, state, default variant, variants, last update, and any pending outbox events.`,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		pool, err := pgxpool.New(cmd.Context(), resolveDSN())
		if err != nil {
			return err
		}
		defer pool.Close()

		rows, err := pool.Query(cmd.Context(), `
			SELECT key, state, default_variant, variants, updated_at
			FROM flags ORDER BY key`)
		if err != nil {
			return err
		}
		defer rows.Close()

		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "KEY\tSTATE\tDEFAULT\tVARIANTS\tUPDATED")
		for rows.Next() {
			var (
				key, state, def string
				variants        []byte
				updated         time.Time
			)
			if err := rows.Scan(&key, &state, &def, &variants, &updated); err != nil {
				return err
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", key, state, def, variants, updated.Format("2006-01-02 15:04:05"))
		}
		if err := rows.Err(); err != nil {
			return err
		}
		w.Flush()

		var pending int
		if err := pool.QueryRow(cmd.Context(),
			`SELECT count(*) FROM outbox WHERE delivered_at IS NULL`).Scan(&pending); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "\n%d pending outbox event(s)\n", pending)
		return nil
	},
}

var deleteCmd = &cobra.Command{
	Use:   "delete <key>",
	Short: "Delete a flag",
	Long: `Delete a flag.

Removes the flag row and emits an outbox delete event in one transaction,
so live flagd clients drop the flag within milliseconds. Deleting an
unknown key is an error (nothing to delete).`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		pool, err := pgxpool.New(cmd.Context(), resolveDSN())
		if err != nil {
			return err
		}
		defer pool.Close()

		var exists bool
		if err := pool.QueryRow(cmd.Context(),
			`SELECT EXISTS (SELECT 1 FROM flags WHERE key=$1)`, key).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("no flag named %q", key)
		}

		if err := flick.DeleteFlag(cmd.Context(), pool, key); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "deleted %s\n", key)
		return nil
	},
}
