package main

import (
	"strings"
	"testing"

	"github.com/codetesla51/flick"
)

// runCLI executes the root command in-process with the given args and
// returns captured stdout+stderr.
func runCLI(args ...string) (string, error) {
	rootCmd.SetArgs(args)
	var buf strings.Builder
	rootCmd.SetOut(&buf)
	rootCmd.SetErr(&buf)
	err := rootCmd.Execute()
	return buf.String(), err
}

func TestCLIHelpShowsAllCommands(t *testing.T) {
	out, err := runCLI("--help")
	if err != nil {
		t.Fatalf("--help: %v", err)
	}
	for _, c := range []string{"init", "serve", "set", "get", "list", "delete", "version"} {
		if !strings.Contains(out, c) {
			t.Errorf("help output missing %q:\n%s", c, out)
		}
	}
}

func TestNewOutboxWiresCDC(t *testing.T) {
	// NewOutbox only constructs the client; the DSN is never dialed here.
	cdc, err := flick.NewOutbox("postgres://u:p@localhost:5432/db?sslmode=disable", flick.NewHub())
	if err != nil {
		t.Fatalf("NewOutbox: %v", err)
	}
	if cdc == nil {
		t.Fatal("NewOutbox returned nil cdc")
	}
	// MetricsSnapshot must be safe before Start (all zeros, no panic).
	if snap := cdc.MetricsSnapshot(); snap.Subscribers != 0 {
		t.Errorf("pre-start subscribers = %d, want 0", snap.Subscribers)
	}
}
