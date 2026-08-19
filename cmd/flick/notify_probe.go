package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	probeChannel = "flick_probe"
	probeTimeout = 10 * time.Second
)

// runNotifyProbe verifies LISTEN/NOTIFY works against the configured
// database: it listens on a test channel from one connection, sends a
// notification from a second, and confirms delivery. Two connections are
// required because a session does not receive its own notifications.
func runNotifyProbe(ctx context.Context, dsn string) error {
	listener, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("listener connection: %w", err)
	}
	defer listener.Close(context.Background())

	if _, err := listener.Exec(ctx, "LISTEN "+probeChannel); err != nil {
		return fmt.Errorf("LISTEN %s: %w", probeChannel, err)
	}

	sender, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("sender connection: %w", err)
	}
	defer sender.Close(context.Background())

	if _, err := sender.Exec(ctx, "SELECT pg_notify($1, 'ping')", probeChannel); err != nil {
		return fmt.Errorf("pg_notify: %w", err)
	}

	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	notif, err := listener.WaitForNotification(probeCtx)
	if err != nil {
		return fmt.Errorf("no notification received within %s: %w\n  fix: check the DB user can LISTEN/NOTIFY and nothing (firewall, pooler) sits between flick and Postgres", probeTimeout, err)
	}
	if notif.Payload != "ping" {
		return fmt.Errorf("unexpected notification payload %q, want %q", notif.Payload, "ping")
	}
	fmt.Println("notify probe: LISTEN/NOTIFY round-trip ok")
	return nil
}
