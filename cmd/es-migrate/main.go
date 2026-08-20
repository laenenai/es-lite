// Command es-migrate applies es-lite's schema migrations once and exits — for
// a Kubernetes init container, a deploy step, or a leader (ADR 0010). It uses
// the same backend env as es-lited: BACKEND (postgres|sqlite), PG_DSN,
// SQLITE_DSN. Idempotent and safe to run concurrently (Postgres serializes on
// an advisory lock).
package main

import (
	"context"
	"log"
	"os"

	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/sqlite"
)

func main() {
	ctx := context.Background()
	backend := os.Getenv("BACKEND")
	if backend == "" {
		backend = "postgres"
	}

	switch backend {
	case "postgres":
		dsn := os.Getenv("PG_DSN")
		if dsn == "" {
			log.Fatal("es-migrate: PG_DSN is required for BACKEND=postgres")
		}
		store, err := postgres.Open(ctx, dsn, postgres.WithoutAutoMigrate())
		if err != nil {
			log.Fatalf("es-migrate: open postgres: %v", err)
		}
		defer store.Close()
		if err := store.Migrate(ctx); err != nil {
			log.Fatalf("es-migrate: %v", err)
		}

	case "sqlite":
		dsn := os.Getenv("SQLITE_DSN")
		if dsn == "" {
			dsn = "file:eslite.db"
		}
		store, err := sqlite.Open(ctx, dsn, sqlite.WithoutAutoMigrate())
		if err != nil {
			log.Fatalf("es-migrate: open sqlite: %v", err)
		}
		defer store.Close()
		if err := store.Migrate(ctx); err != nil {
			log.Fatalf("es-migrate: %v", err)
		}

	default:
		log.Fatalf("es-migrate: unknown BACKEND %q (want postgres|sqlite)", backend)
	}
	log.Printf("es-migrate: %s schema up to date", backend)
}
