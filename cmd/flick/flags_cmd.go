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

// typeLabel returns a human-readable description of what JSON type a string parses to.
func typeLabel(raw string) string {
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return "(unparseable)"
	}
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case float64:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	default:
		return "an unexpected type"
	}
}

// validFlagdOps is the set of recognized JsonLogic / flagd targeting operators.
var validFlagdOps = map[string]bool{
	"if": true, "else": true, "and": true, "or": true, "not": true,
	">": true, "<": true, ">=": true, "<=": true, "==": true, "!=": true,
	"in": true, "nin": true, "exists": true, "cat": true, "substring": true,
	"var": true, "missing": true, "missing_some": true,
}

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
      --targeting '{"if":[{"in":[{"var":"env"},["staging"]]},"on"]}'`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]

		// --- state validation ---
		if flagState != "ENABLED" && flagState != "DISABLED" {
			return fmt.Errorf("--state must be ENABLED or DISABLED, got %q", flagState)
		}

		// --- default-variant required ---
		if flagDefaultVariant == "" {
			return fmt.Errorf("--default-variant is required")
		}

		// --- variants validation ---
		if !json.Valid([]byte(flagVariants)) {
			return fmt.Errorf("--variants must be valid JSON, got: %s", flagVariants)
		}
		var variantsMap map[string]any
		if err := json.Unmarshal([]byte(flagVariants), &variantsMap); err != nil {
			return fmt.Errorf("--variants must be a JSON object {\"on\":true,\"off\":false}, not %s", typeLabel(flagVariants))
		}
		if len(variantsMap) == 0 {
			return fmt.Errorf("--variants must have at least one variant")
		}
		if _, ok := variantsMap[flagDefaultVariant]; !ok {
			keys := make([]string, 0, len(variantsMap))
			for k := range variantsMap {
				keys = append(keys, k)
			}
			return fmt.Errorf("--default-variant %q not found in variants (available: %v)", flagDefaultVariant, keys)
		}

		// --- targeting validation ---
		if !json.Valid([]byte(flagTargeting)) {
			return fmt.Errorf("--targeting must be valid JSON, got: %s", flagTargeting)
		}
		var targetingRaw any
		if err := json.Unmarshal([]byte(flagTargeting), &targetingRaw); err != nil {
			return fmt.Errorf("--targeting must be a JSON object, not %s", typeLabel(flagTargeting))
		}
		if targetingRaw != nil {
			if _, ok := targetingRaw.(map[string]any); !ok {
				return fmt.Errorf("--targeting must be a JSON object {\"if\":[...]}, not %s", typeLabel(flagTargeting))
			}
		}

		// --- targeting operator warning ---
		if tm, ok := targetingRaw.(map[string]any); ok {
			for k := range tm {
				if !validFlagdOps[k] {
					fmt.Fprintf(cmd.ErrOrStderr(), "warning: %q is not a flagd operator — flagd uses JsonLogic rules, not attribute maps\n", k)
					fmt.Fprintf(cmd.ErrOrStderr(), "  correct: --targeting '{\"if\":[{\"in\":[{\"var\":\"country\"},[\"NG\"]]},\"on\"]}'\n")
					fmt.Fprintf(cmd.ErrOrStderr(), "  not:     --targeting '{\"country\":[\"NG\"]}'\n")
					break
				}
			}
		}

		// --- metadata validation ---
		if !json.Valid([]byte(flagMetadata)) {
			return fmt.Errorf("--metadata must be valid JSON, got: %s", flagMetadata)
		}
		var metadataRaw any
		if err := json.Unmarshal([]byte(flagMetadata), &metadataRaw); err != nil {
			return fmt.Errorf("--metadata must be a JSON object, not %s", typeLabel(flagMetadata))
		}
		if metadataRaw != nil {
			if _, ok := metadataRaw.(map[string]any); !ok {
				return fmt.Errorf("--metadata must be a JSON object, not %s", typeLabel(flagMetadata))
			}
		}

		// --- persist ---
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

		err = pool.QueryRow(cmd.Context(), `
			SELECT state, default_variant, variants, targeting, metadata, updated_at
			FROM flags WHERE key=$1`, key).Scan(&state, &def, &variants, &targeting, &metadata, &time.Time{})
		if err != nil {
			return fmt.Errorf("flag %q not found", key)
		}

		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "key:\t%s\n", key)
		fmt.Fprintf(w, "state:\t%s\n", state)
		fmt.Fprintf(w, "default_variant:\t%s\n", def)
		fmt.Fprintf(w, "variants:\t%s\n", variants)
		fmt.Fprintf(w, "targeting:\t%s\n", targeting)
		fmt.Fprintf(w, "metadata:\t%s\n", metadata)
		w.Flush()
		return nil
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all flags",
	Long:  `List every flag: key, state, default variant, variants, last update, and any pending outbox events.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		pool, err := pgxpool.New(cmd.Context(), resolveDSN())
		if err != nil {
			return err
		}
		defer pool.Close()

		rows, err := pool.Query(cmd.Context(), `
			SELECT f.key, f.state, f.default_variant, f.variants, f.updated_at,
			       o.seq IS NOT NULL AS pending
			FROM flags f
			LEFT JOIN outbox o ON o.flag_key = f.key
			ORDER BY f.key`)
		if err != nil {
			return err
		}
		defer rows.Close()

		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "KEY\tSTATE\tDEFAULT\tVARIANTS\tUPDATED\tPENDING\n")
		for rows.Next() {
			var k, state, def string
			var variants []byte
			var updatedAt time.Time
			var pending bool
			if err := rows.Scan(&k, &state, &def, &variants, &updatedAt, &pending); err != nil {
				return err
			}
			p := ""
			if pending {
				p = "*"
			}
			// Compact variants for display.
			var vm map[string]any
			_ = json.Unmarshal(variants, &vm)
			vs := fmt.Sprintf("%v", vm)
			if len(vs) > 40 {
				vs = vs[:37] + "..."
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", k, state, def, vs, updatedAt.Format("15:04:05"), p)
		}
		w.Flush()
		return rows.Err()
	},
}

var deleteCmd = &cobra.Command{
	Use:   "delete <key>",
	Short: "Delete a flag",
	Long:  `Delete a flag and enqueue a delete event in the outbox.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		pool, err := pgxpool.New(cmd.Context(), resolveDSN())
		if err != nil {
			return err
		}
		defer pool.Close()

		if err := flick.DeleteFlag(cmd.Context(), pool, key); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "deleted %s\n", key)
		return nil
	},
}

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export all flags as JSON",
	Long:  `Export all flags as a JSON array. Useful for backups or migration.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		pool, err := pgxpool.New(cmd.Context(), resolveDSN())
		if err != nil {
			return err
		}
		defer pool.Close()

		rows, err := pool.Query(cmd.Context(), `
			SELECT key, state, default_variant, variants, targeting, metadata
			FROM flags ORDER BY key`)
		if err != nil {
			return err
		}
		defer rows.Close()

		var flags []map[string]any
		for rows.Next() {
			var k, state, def string
			var variants, targeting, metadata json.RawMessage
			if err := rows.Scan(&k, &state, &def, &variants, &targeting, &metadata); err != nil {
				return err
			}
			flags = append(flags, map[string]any{
				"key":             k,
				"state":           state,
				"default_variant": def,
				"variants":        json.RawMessage(variants),
				"targeting":       json.RawMessage(targeting),
				"metadata":        json.RawMessage(metadata),
			})
		}
		if err := rows.Err(); err != nil {
			return err
		}

		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(flags)
	},
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import flags from JSON (stdin)",
	Long: `Import flags from a JSON array (as produced by "flick export").
Reads from stdin. Each flag must have "key", "state", "default_variant",
"variants", "targeting", and optionally "metadata".`,
	RunE: func(cmd *cobra.Command, args []string) error {
		var flags []struct {
			Key            string          `json:"key"`
			State          string          `json:"state"`
			DefaultVariant string          `json:"default_variant"`
			Variants       json.RawMessage `json:"variants"`
			Targeting      json.RawMessage `json:"targeting"`
			Metadata       json.RawMessage `json:"metadata"`
		}
		if err := json.NewDecoder(cmd.InOrStdin()).Decode(&flags); err != nil {
			return fmt.Errorf("invalid JSON input: %w", err)
		}

		pool, err := pgxpool.New(cmd.Context(), resolveDSN())
		if err != nil {
			return err
		}
		defer pool.Close()

		for _, f := range flags {
			if f.Metadata == nil {
				f.Metadata = json.RawMessage(`{}`)
			}
			if err := flick.SetFlag(cmd.Context(), pool, f.Key, f.State, f.DefaultVariant,
				f.Variants, f.Targeting, f.Metadata); err != nil {
				return fmt.Errorf("importing %s: %w", f.Key, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "imported %s (%s, default %q)\n", f.Key, f.State, f.DefaultVariant)
		}
		return nil
	},
}

func init() {
	initCmd.Flags().Bool("with-flags", false, "also seed 3 example flags (maintenance-mode, dark-mode-rollout, banner-message)")

	rootCmd.AddCommand(setCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(exportCmd)
	rootCmd.AddCommand(importCmd)
}
