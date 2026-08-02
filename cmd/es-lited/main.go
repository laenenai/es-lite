// Command es-lited runs es-lite as a NATS eventstore service (ADR 0009): it
// serves an es.Store over NATS request/reply. It holds no keys and never
// decrypts — clients encrypt payloads client-side, so the service is
// zero-knowledge about payload content.
//
// Config (env):
//
//	BACKEND             postgres (default) | sqlite
//	PG_DSN              Postgres DSN (required for BACKEND=postgres)
//	SQLITE_DSN          SQLite DSN (BACKEND=sqlite; default file:eslite.db)
//	NATS_URL            NATS endpoint (default nats://127.0.0.1:4222)
//	NATS_SUBJECT_PREFIX subject prefix / shard knob (default svc.eslite)
//	HEALTH_ADDR         kubelet health probe addr (default :8080)
//
// BACKEND=sqlite is single-workspace (SQLite has no workspace_id/RLS) — for
// single-tenant or edge deployments. Multi-workspace uses Postgres.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/natsstore"
	"github.com/laenenai/es-lite/obs"
	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/sqlite"
)

// version is stamped into telemetry; override at build with -ldflags.
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Optional OTLP -> OpenObserve (no-op unless OTEL_EXPORTER_OTLP_ENDPOINT set).
	shutdownObs, err := obs.Setup(ctx, "es-lited", version)
	if err != nil {
		log.Fatalf("es-lited: observability: %v", err)
	}
	defer func() { _ = shutdownObs(context.Background()) }()

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	// Select the storage backend. The server passes no keystore either way:
	// it stores only ciphertext (zero-knowledge).
	backend := os.Getenv("BACKEND")
	if backend == "" {
		backend = "postgres"
	}
	scope, closeStore, err := openBackend(ctx, backend)
	if err != nil {
		log.Fatalf("es-lited: open %s backend: %v", backend, err)
	}
	defer closeStore()

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

	// Subject prefix carries the region from day 1 (architecture ADR 0005 §B):
	// subjects are <prefix>.<ws>.<method>, so the prefix is svc.eslite.<region>.
	// It is also the shard/region routing knob (ADR 0009) — run one es-lited
	// per region on its own prefix; clients target the matching prefix.
	prefix := os.Getenv("NATS_SUBJECT_PREFIX")
	if prefix == "" {
		prefix = natsstore.DefaultPrefix + ".local" // default local region
	}
	srv := natsstore.NewServer(scope,
		natsstore.WithServerPrefix(prefix),
		natsstore.WithMiddleware(natsstore.ObservabilityMiddleware("es-lited")))
	log.Printf("es-lited: serving es.Store over NATS at %s (prefix %s), backend %s",
		natsURL, prefix, backend)
	if err := srv.Serve(ctx, nc); err != nil && ctx.Err() == nil {
		log.Fatalf("es-lited: serve: %v", err)
	}
	log.Print("es-lited: shut down")
}

// openBackend opens the selected storage backend and returns a workspace
// Scoper plus a close function. Postgres is multi-workspace (RLS +
// partitioning); SQLite is single-workspace (the Scoper ignores the workspace
// argument), for single-tenant / edge deployments.
func openBackend(ctx context.Context, backend string) (natsstore.Scoper, func(), error) {
	// AUTO_MIGRATE=false: assume the schema is already migrated (a separate
	// es-migrate init container ran it) — so replicas never touch DDL on boot
	// (ADR 0010). Default true keeps dev/single-node zero-config.
	autoMigrate := os.Getenv("AUTO_MIGRATE") != "false"

	switch backend {
	case "postgres":
		dsn := os.Getenv("PG_DSN")
		if dsn == "" {
			return nil, nil, fmt.Errorf("PG_DSN is required for BACKEND=postgres")
		}
		var opts []postgres.Option
		if !autoMigrate {
			opts = append(opts, postgres.WithoutAutoMigrate())
		}
		store, err := postgres.Open(ctx, dsn, nil, opts...)
		if err != nil {
			return nil, nil, err
		}
		return store.Workspace, func() { store.Close() }, nil

	case "sqlite":
		dsn := os.Getenv("SQLITE_DSN")
		if dsn == "" {
			dsn = "file:eslite.db"
		}
		var opts []sqlite.Option
		if !autoMigrate {
			opts = append(opts, sqlite.WithoutAutoMigrate())
		}
		store, err := sqlite.Open(ctx, dsn, opts...)
		if err != nil {
			return nil, nil, err
		}
		// Single-workspace: ignore the workspace argument.
		return func(string) es.Store { return store }, func() { store.Close() }, nil

	default:
		return nil, nil, fmt.Errorf("unknown BACKEND %q (want postgres|sqlite)", backend)
	}
}
