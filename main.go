package main

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed db/goose_migrations/*.sql
var embedMigrations embed.FS

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

	goose.SetBaseFS(embedMigrations)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("goose dialect: %w", err)
	}

	if err := goose.Up(db, "db/goose_migrations"); err != nil {
		return fmt.Errorf("goose up: %w", err)
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

	log.Println("migrations applied; app db pool ready")
	return nil
}