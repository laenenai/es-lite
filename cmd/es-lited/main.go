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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	// Kubelet liveness/readiness. Mesh-native health is the NATS Micro
	// $SRV.PING the service registers; this HTTP probe is for kubelet, which
	// wants a process-local check — readiness for a NATS-only service is
	// "connected to NATS".
	healthAddr := os.Getenv("HEALTH_ADDR")
	if healthAddr == "" {
		healthAddr = ":8080"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if nc.IsConnected() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("nats disconnected"))
	})
	healthSrv := &http.Server{Addr: healthAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("es-lited: health server: %v", err)
		}
	}()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = healthSrv.Shutdown(shutCtx)
	}()
	log.Printf("es-lited: health probe on %s/healthz", healthAddr)

	// Subject prefix — also the shard/region routing knob (ADR 0009): run
	// one es-lited per shard on its own prefix (e.g. svc.eslite.eu), and
	// clients target the matching prefix.
	prefix := os.Getenv("NATS_SUBJECT_PREFIX")
	if prefix == "" {
		prefix = natsstore.DefaultPrefix
	}
	srv := natsstore.NewServer(store.Workspace, natsstore.WithServerPrefix(prefix))
	log.Printf("es-lited: serving es.Store over NATS at %s (prefix %s), backend postgres",
		natsURL, prefix)
	if err := srv.Serve(ctx, nc); err != nil && ctx.Err() == nil {
		log.Fatalf("es-lited: serve: %v", err)
	}
	log.Print("es-lited: shut down")
}
