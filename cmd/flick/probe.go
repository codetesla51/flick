package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/codetesla51/phylax"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The probe reuses the exact slot and publication names flick serve uses,
// so init verifies the real setup rather than a throwaway one.
const (
	probeSlot    = "flick_slot"
	probePub     = "flick_pub"
	probeTopic   = "probe"
	probeTimeout = 20 * time.Second
	probeTick    = 250 * time.Millisecond
)

// replicationDSN appends the replication=database parameter required for a
// logical replication connection.
func replicationDSN(dsn string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "replication=database"
}

// runReplicationProbe verifies end-to-end that outbox logical replication
// works against the configured database. Every step fails with an
// actionable message:
//
//  1. opens a replication connection and performs the replication handshake,
//  2. ensures the flick_slot slot and flick_pub publication exist,
//  3. starts a stream and inserts a probe row into the outbox table,
//  4. confirms the probe row is delivered (or says exactly what broke).
func runReplicationProbe(ctx context.Context, dsn string) error {
	// -- 1. Admin connection (for slot/pub management and probe inserts) --
	admin, err := phylax.OpenAdminConnection(ctx, dsn)
	if err != nil {
		return fmt.Errorf("admin connection: %w", err)
	}
	defer admin.Close(ctx)

	var outboxTable string
	if err := admin.QueryRow(ctx, "SELECT to_regclass('public.outbox')::text").Scan(&outboxTable); err != nil {
		return fmt.Errorf("checking outbox table: %w", err)
	}
	if outboxTable == "" || outboxTable == "NULL" {
		return errors.New("outbox table does not exist — the migrate step above should have created it; check migration output")
	}

	// -- 2. Replication connection + handshake --
	repl, err := phylax.OpenReplicationConnection(ctx, replicationDSN(dsn))
	if err != nil {
		return fmt.Errorf("replication connection failed: %w\n  fix: the DB user needs the REPLICATION privilege, and pg_hba.conf must permit replication connections", err)
	}
	defer repl.Close(context.Background())

	sysIdent, err := phylax.IdentifySystem(ctx, repl)
	if err != nil {
		return fmt.Errorf("replication handshake (IDENTIFY_SYSTEM) failed: %w", err)
	}
	fmt.Printf("replication connection: ok (system %s, wal %s)\n", sysIdent.SystemID, sysIdent.XLogPos)

	// -- 3. Slot + publication (idempotent; same names serve uses). Slots
	// are cluster-wide but bound to one database, so a slot created for
	// another flick database cannot be reused here.
	var slotDB string
	err = admin.QueryRow(ctx,
		"SELECT database::text FROM pg_replication_slots WHERE slot_name = $1",
		probeSlot,
	).Scan(&slotDB)
	switch {
	case err == pgx.ErrNoRows:
		// slot does not exist yet — create it below
	case err != nil:
		return fmt.Errorf("checking replication slot %q: %w", probeSlot, err)
	case slotDB != currentDatabase(ctx, admin):
		return fmt.Errorf("replication slot %q exists but belongs to database %q (this one is %q)\n  fix: replication slots are per-database — use a different slot name for this database, or drop the old slot", probeSlot, slotDB, currentDatabase(ctx, admin))
	}

	created, err := phylax.EnsureReplicationSlot(ctx, repl, admin, probeSlot)
	if err != nil {
		return fmt.Errorf("replication slot %q: %w\n  fix: the DB user needs the REPLICATION privilege to create slots", probeSlot, err)
	}
	if created {
		fmt.Printf("replication slot %q: created\n", probeSlot)
	} else {
		fmt.Printf("replication slot %q: already exists\n", probeSlot)
	}

	created, err = phylax.EnsurePublication(ctx, admin, probePub, []string{"outbox"})
	if err != nil {
		return fmt.Errorf("publication %q: %w\n  fix: the DB user needs CREATE on the database to create publications", probePub, err)
	}
	if created {
		fmt.Printf("publication %q: created\n", probePub)
	} else {
		fmt.Printf("publication %q: already exists\n", probePub)
	}

	// -- 4. Stream probe: start a CDC stream, insert a probe row, confirm
	// it arrives. Rows are inserted on a tick until one is delivered — the
	// stream only sees rows written after it attaches, so early inserts are
	// expected to be missed and later ones prove delivery.
	cdc, err := phylax.New(phylax.Config{
		DSN:             dsn,
		Tables:          []string{"outbox"},
		SlotName:        probeSlot,
		PublicationName: probePub,
		OutboxTable:     "outbox",
	})
	if err != nil {
		return fmt.Errorf("building stream client: %w", err)
	}

	delivered := make(chan int64, 1)
	cdc.OnOutboxDelivery(func(_ context.Context, row *phylax.OutboxRow) error {
		if row.Topic == probeTopic {
			select {
			case delivered <- row.ID:
			default:
			}
		}
		return nil
	})

	streamErr := make(chan error, 1)
	probeCtx, stopProbe := context.WithCancel(ctx)
	defer stopProbe()
	go func() { streamErr <- cdc.Start(probeCtx) }()

	start := time.Now()
	timeout := time.After(probeTimeout)
	probeTicker := time.NewTicker(probeTick)
	defer probeTicker.Stop()

	var deliveredID int64
	for deliveredID == 0 {
		select {
		case deliveredID = <-delivered:
			// probe row arrived — replication works
		case err := <-streamErr:
			if err != nil {
				return probeStreamError(admin, err)
			}
			return errors.New("stream stopped before the probe was delivered")
		case <-timeout:
			return fmt.Errorf("stream probe timed out after %s: inserted probe rows into the outbox table but none were delivered\n  fix: check the Postgres server log, and confirm nothing else holds %q (a stuck consumer can stall the slot)", probeTimeout, probeSlot)
		case <-probeTicker.C:
			var id int64
			if err := admin.QueryRow(ctx,
				"INSERT INTO outbox (topic, payload) VALUES ($1, '{\"probe\":true}'::jsonb) RETURNING id",
				probeTopic,
			).Scan(&id); err != nil {
				return fmt.Errorf("inserting probe row into outbox: %w", err)
			}
		}
	}

	elapsed := time.Since(start).Round(time.Millisecond)
	// Give the consumer a moment to ack the delivered row before the stream
	// shuts down — otherwise phylax logs a scary "failed to ack: context
	// canceled" line right after a successful probe. The probe row is
	// deleted in cleanup either way.
	time.Sleep(500 * time.Millisecond)
	stopProbe() // stop the stream; Start returns nil on ctx cancellation

	// -- 5. Cleanup: remove probe rows (both delivered and any that were
	// written before the stream attached). Deletes on outbox are not
	// delivered, so this is safe.
	if _, err := admin.Exec(ctx, "DELETE FROM outbox WHERE topic = $1", probeTopic); err != nil {
		return fmt.Errorf("cleaning up probe rows: %w", err)
	}

	fmt.Printf("stream probe: outbox row #%d delivered in %s — replication works\n", deliveredID, elapsed)
	return nil
}

// currentDatabase returns the name of the database the admin connection is
// attached to.
func currentDatabase(ctx context.Context, admin *pgx.Conn) string {
	var name string
	if err := admin.QueryRow(ctx, "SELECT current_database()").Scan(&name); err != nil {
		return "?"
	}
	return name
}

// probeStreamError turns a stream failure into an actionable message. The
// most common case is SQLSTATE 55006: the flick_slot slot is already active
// because a `flick serve` (or another consumer) is streaming on it.
func probeStreamError(admin *pgx.Conn, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "55006" {
		// Find who holds the slot for a more helpful message.
		holder := "another process"
		var pid int
		var appName, clientAddr string
		if e := admin.QueryRow(context.Background(),
			`SELECT r.pid, COALESCE(r.application_name,''), COALESCE(r.client_addr::text,'local')`+
				` FROM pg_stat_replication r JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = $1`,
			probeSlot,
		).Scan(&pid, &appName, &clientAddr); e == nil {
			if appName == "" {
				holder = fmt.Sprintf("PID %d from %s", pid, clientAddr)
			} else {
				holder = fmt.Sprintf("PID %d (%s from %s)", pid, appName, clientAddr)
			}
		}
		return fmt.Errorf("stream probe failed: replication slot %q is already active — %s is consuming it\n  fix: that is most likely a running `flick serve`; stop it, then re-run `flick init`", probeSlot, holder)
	}
	return fmt.Errorf("stream failed before the probe was delivered: %w", err)
}
