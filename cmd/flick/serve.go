package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
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

var (
	syncAddr    string
	metricsAddr string
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run the flagd sync gRPC server",
	Long: `Run the flagd sync gRPC server.

Starts the outbox LISTEN/NOTIFY consumer and serves
flagd.sync.v1.FlagSyncService (FetchAllFlags + SyncFlags), so flagd can use
this address as a "grpc" sync source. Also serves live metrics and the
console on the metrics address.

Example:
  flagd start --sources '[{"uri":"localhost:8015","provider":"grpc"}]'`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runServe(resolveDSN(), syncAddr, metricsAddr)
	},
}

func init() {
	serveCmd.Flags().StringVar(&syncAddr, "addr", "",
		"sync gRPC listen address (default: FLICK_SYNC_ADDR env, then :8015)")
	serveCmd.Flags().StringVar(&metricsAddr, "metrics-addr", "",
		"metrics/events/console listen address (default: FLICK_METRICS_ADDR env, then :8016)")
}

func runServe(dsn, addr, metrics string) error {
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
	layer := flick.NewNotifyLayer(dsn, hub)
	go func() {
		if err := layer.Start(ctx); err != nil {
			errCh <- err
		}
	}()

	metrics = resolveAddr(metrics, "FLICK_METRICS_ADDR", ":8016")
	dashSrv := &http.Server{Addr: metrics, Handler: newDashboardMux(pool, layer, hub)}
	go func() {
		log.Printf("console on %s (dashboard, /api/flags, /events, /metrics/stream)", metrics)
		if err := dashSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("console server: %v", err)
		}
	}()
	defer dashSrv.Shutdown(context.Background())

	addr = resolveAddr(addr, "FLICK_SYNC_ADDR", ":8015")
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

	log.Println("migrations applied; app db pool ready; notify stream started")
	select {
	case err := <-errCh:
		return fmt.Errorf("notify stream: %w", err)
	case <-ctx.Done():
		log.Println("shutting down")
		return nil
	}
}
