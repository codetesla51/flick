package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/codetesla51/flick"
	syncv1 "github.com/codetesla51/flick/gen/flagd/sync/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var syncAddr string

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run the flagd sync gRPC server",
	Long: `Run the flagd sync gRPC server.

Starts the outbox logical-replication consumer and serves
flagd.sync.v1.FlagSyncService (FetchAllFlags + SyncFlags), so flagd can use
this address as a "grpc" sync source.

Example:
  flagd start --sources '[{"uri":"localhost:8015","provider":"grpc"}]'`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runServe(resolveDSN(), syncAddr)
	},
}

func init() {
	serveCmd.Flags().StringVar(&syncAddr, "addr", "",
		"sync gRPC listen address (default: FLICK_SYNC_ADDR env, then :8015)")
}

func runServe(dsn, addr string) error {
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping pool: %w", err)
	}

	errCh := make(chan error, 1)
	hub := flick.NewHub()
	go func() {
		errCh <- flick.RunOutbox(ctx, dsn, hub)
	}()

	if addr == "" {
		addr = os.Getenv("FLICK_SYNC_ADDR")
		if addr == "" {
			addr = ":8015"
		}
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("sync listen: %w", err)
	}
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	syncv1.RegisterFlagSyncServiceServer(grpcServer, flick.NewSyncService(pool, hub))
	go func() {
		log.Printf("sync gRPC server listening on %s", addr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("sync server: %v", err)
		}
	}()
	defer grpcServer.GracefulStop()

	log.Println("migrations applied; app db pool ready; outbox consumer started")
	select {
	case err := <-errCh:
		return fmt.Errorf("outbox consumer: %w", err)
	case <-ctx.Done():
		log.Println("shutting down")
		return nil
	}
}
