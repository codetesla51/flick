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

func TestNewNotifyLayerWiresHub(t *testing.T) {
	// NewNotifyLayer only constructs the layer; the DSN is never dialed here.
	hub := flick.NewHub()
	layer := flick.NewNotifyLayer("postgres://u:p@localhost:5432/db?sslmode=disable", hub)
	if layer == nil {
		t.Fatal("NewNotifyLayer returned nil layer")
	}
	// MetricsSnapshot must be safe before Start (all zeros, no panic).
	if snap := layer.MetricsSnapshot(); snap.OutboxDelivered != 0 || snap.Replayed != 0 {
		t.Errorf("pre-start metrics = %+v, want zeros", snap)
	}
	// subscribe/unsubscribe round-trip without a database
	id, ch := layer.SubscribeEvents()
	if id == 0 || ch == nil {
		t.Fatalf("SubscribeEvents = %d, %v", id, ch)
	}
	layer.UnsubscribeEvents(id)
	if hub.SubscriberCount() != 0 {
		t.Errorf("hub subscriber count = %d, want 0", hub.SubscriberCount())
	}
}
