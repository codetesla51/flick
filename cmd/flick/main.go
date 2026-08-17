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
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dsn := os.Getenv("FLICK_DSN")
	if dsn == "" {
		dsn = "postgres://us:2@localhost:5432/flick?sslmode=disable"
	}

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

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping pool: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	hub := flick.NewHub()
	go func() {
		errCh <- flick.RunOutbox(ctx, dsn, hub)
	}()

	syncAddr := os.Getenv("FLICK_SYNC_ADDR")
	if syncAddr == "" {
		syncAddr = ":8015"
	}
	lis, err := net.Listen("tcp", syncAddr)
	if err != nil {
		return fmt.Errorf("sync listen: %w", err)
	}
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	syncv1.RegisterFlagSyncServiceServer(grpcServer, flick.NewSyncService(pool, hub))
	go func() {
		log.Printf("sync gRPC server listening on %s", syncAddr)
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
