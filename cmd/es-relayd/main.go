// Command es-relayd is the delivery relay (ADR 0003): it drains the Postgres
// event log and publishes each event to JetStream. It is zero-knowledge — it
// holds no keys, so it publishes CIPHERTEXT payloads; crypto-capable
// projections decrypt them. Idempotent publish (Nats-Msg-Id = global_position)
// makes it safe to run N-way or fail over, but run it as a singleton
// (replicas: 1) or leader-elected to avoid wasted work.
//
// Config (env): PG_DSN (required), NATS_URL, ES_STREAM (default ES_EVENTS),
// ES_BATCH (default 200), HEALTH_ADDR (default :8080).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/natsjs"
	"github.com/laenenai/es-lite/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pgDSN := os.Getenv("PG_DSN")
	if pgDSN == "" {
		log.Fatal("es-relayd: PG_DSN is required")
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	streamName := os.Getenv("ES_STREAM")
	if streamName == "" {
		streamName = "ES_EVENTS"
	}
	batch := 200
	if v, err := strconv.Atoi(os.Getenv("ES_BATCH")); err == nil && v > 0 {
		batch = v
	}

	// No keystore: the relay only moves ciphertext (zero-knowledge, ADR 0003).
	// WithoutAutoMigrate — es-migrate owns schema.
	store, err := postgres.Open(ctx, pgDSN, nil, postgres.WithoutAutoMigrate())
	if err != nil {
		log.Fatalf("es-relayd: open postgres: %v", err)
	}
	defer store.Close()

	nc, err := nats.Connect(natsURL, nats.Name("es-relayd"))
	if err != nil {
		log.Fatalf("es-relayd: connect nats: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("es-relayd: jetstream: %v", err)
	}
	if _, err := natsjs.EnsureStream(ctx, js, natsjs.StreamConfig{Name: streamName}); err != nil {
		log.Fatalf("es-relayd: ensure stream: %v", err)
	}

	startHealth(nc)

	pub := natsjs.NewPublisher(js, nil) // evt.<ws>.<aggregate>.<event> subjects
	relay := delivery.NewRelay(store, pub.Handle, delivery.RelayConfig{
		BatchSize:    batch,
		PollInterval: time.Second,
	})
	log.Printf("es-relayd: draining postgres -> jetstream stream %q (batch %d)", streamName, batch)
	if err := relay.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("es-relayd: relay: %v", err)
	}
	log.Print("es-relayd: shut down")
}

func startHealth(nc *nats.Conn) {
	addr := os.Getenv("HEALTH_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if nc.IsConnected() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("es-relayd: health server: %v", err)
		}
	}()
	log.Printf("es-relayd: health probe on %s/healthz", addr)
}
