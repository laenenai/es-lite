// Command es-lited runs es-lite as a NATS eventstore service (ADR 0009): it
// serves an es.Store over NATS request/reply, backed by Postgres. It holds no
// keys and never decrypts — clients encrypt payloads client-side, so the
// service is zero-knowledge about payload content.
//
// Config via env: PG_DSN (required), NATS_URL (default nats://127.0.0.1:4222).
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"

	"github.com/laenenai/es-lite/natsstore"
	"github.com/laenenai/es-lite/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pgDSN := os.Getenv("PG_DSN")
	if pgDSN == "" {
		log.Fatal("es-lited: PG_DSN is required")
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	// No keystore on the server: it stores only ciphertext (zero-knowledge).
	store, err := postgres.Open(ctx, pgDSN, nil)
	if err != nil {
		log.Fatalf("es-lited: open postgres: %v", err)
	}
	defer store.Close()

	nc, err := nats.Connect(natsURL, nats.Name("es-lited"))
	if err != nil {
		log.Fatalf("es-lited: connect nats: %v", err)
	}
	defer nc.Close()

	srv := natsstore.NewServer(store.Workspace)
	log.Printf("es-lited: serving es.Store over NATS at %s (prefix %s), backend postgres",
		natsURL, natsstore.DefaultPrefix)
	if err := srv.Serve(ctx, nc); err != nil && ctx.Err() == nil {
		log.Fatalf("es-lited: serve: %v", err)
	}
	log.Print("es-lited: shut down")
}
