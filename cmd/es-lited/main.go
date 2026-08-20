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
//	RELAY               true runs the delivery relay in-process under NATS KV
//	                    leader election (Postgres only); default off
//	ES_STREAM           relay target JetStream stream (default ES_EVENTS)
//	ES_BATCH            relay drain batch size (default 200)
//	ES_ENSURE_STREAM    false assumes an ops-provisioned stream (default true
//	                    auto-creates it, R1 / infinite retention)
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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/laenenai/natskit"

	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/leader"
	"github.com/laenenai/es-lite/natsjs"
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

	// Select the storage backend. The server holds no keys either way: it
	// stores whatever opaque bytes clients send (zero-knowledge, ADR 0025).
	backend := os.Getenv("BACKEND")
	if backend == "" {
		backend = "postgres"
	}
	scope, drainer, closeStore, err := openBackend(ctx, backend)
	if err != nil {
		log.Fatalf("es-lited: open %s backend: %v", backend, err)
	}
	defer closeStore()

	// Via natskit so TLS (NATS_CA / client cert) is applied uniformly (architecture ADR 0013).
	nc, err := natskit.Connect("es-lited", natsURL, os.Getenv("NATS_CREDS"))
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
	// RELAY=true folds the delivery relay (ADR 0003) into the normal process
	// instead of a dedicated es-relayd singleton: every replica campaigns for a
	// NATS KV lease and only the elected leader drains Postgres -> JetStream, so
	// the relay fails over automatically without a separate Deployment. Requires
	// a gap-safe Drainer — Postgres only; SQLite uses the in-process Poller.
	if os.Getenv("RELAY") == "true" {
		if drainer == nil {
			log.Printf("es-lited: RELAY=true ignored: backend %s has no gap-safe drainer", backend)
		} else {
			go runElectedRelay(ctx, nc, drainer, prefix)
		}
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

// runElectedRelay campaigns for the relay lease and, while leader, drains the
// Postgres log to JetStream. The election key is scoped to the subject prefix
// so each region/shard elects its own relay leader independently. Blocks until
// ctx is cancelled.
func runElectedRelay(ctx context.Context, nc *nats.Conn, drainer delivery.Drainer, prefix string) {
	js, err := jetstream.New(nc)
	if err != nil {
		log.Printf("es-lited: relay: jetstream: %v", err)
		return
	}
	streamName := os.Getenv("ES_STREAM")
	if streamName == "" {
		streamName = "ES_EVENTS"
	}
	// ES_ENSURE_STREAM=false: assume the JetStream stream is provisioned by ops
	// (with the right replicas/retention) — es-lited never touches topology.
	// Default true keeps dev/single-node zero-config; note auto-create defaults
	// to R1 / infinite retention, which is not what you want in prod.
	if os.Getenv("ES_ENSURE_STREAM") != "false" {
		if _, err := natsjs.EnsureStream(ctx, js, natsjs.StreamConfig{Name: streamName}); err != nil {
			log.Printf("es-lited: relay: ensure stream: %v", err)
			return
		}
	}
	batch := 200
	if v, err := strconv.Atoi(os.Getenv("ES_BATCH")); err == nil && v > 0 {
		batch = v
	}

	pub := natsjs.NewPublisher(js, nil) // evt.<ws>.<aggregate>.<event> subjects
	relayed, _ := otel.Meter("es-lited").Int64Counter("eslite.relayed",
		metric.WithDescription("events relayed to jetstream"))
	publish := func(ctx context.Context, evs []es.Envelope) error {
		if err := pub.Handle(ctx, evs); err != nil {
			return err
		}
		relayed.Add(ctx, int64(len(evs)))
		return nil
	}
	relay := delivery.NewRelay(drainer, publish, delivery.RelayConfig{
		BatchSize:    batch,
		PollInterval: time.Second,
	})

	// Election key is per-prefix (region/shard) so relays don't contend across
	// regions. KV keys allow dots, but sanitize any stray chars just in case.
	key := "relay." + strings.Map(kvKeyRune, prefix)
	id := instanceID()
	log.Printf("es-lited: relay enabled, campaigning for leadership (key %s, id %s)", key, id)
	err = leader.Run(ctx, js, leader.Config{Key: key, ID: id}, func(leaderCtx context.Context) {
		log.Print("es-lited: became relay leader, draining postgres -> jetstream")
		if err := relay.Run(leaderCtx); err != nil && leaderCtx.Err() == nil {
			log.Printf("es-lited: relay: %v", err)
		}
		log.Print("es-lited: relinquished relay leadership")
	})
	if err != nil && ctx.Err() == nil {
		log.Printf("es-lited: relay election: %v", err)
	}
}

// instanceID is a best-effort unique id for this process (host-pid).
func instanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}

// kvKeyRune maps a rune to a JetStream KV-key-safe rune (-/_=.a-zA-Z0-9),
// replacing anything else with '_'.
func kvKeyRune(r rune) rune {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return r
	case r == '-' || r == '_' || r == '=' || r == '.' || r == '/':
		return r
	default:
		return '_'
	}
}

// openBackend opens the selected storage backend and returns a workspace
// Scoper plus a close function. Postgres is multi-workspace (RLS +
// partitioning); SQLite is single-workspace (the Scoper ignores the workspace
// argument), for single-tenant / edge deployments.
func openBackend(ctx context.Context, backend string) (natsstore.Scoper, delivery.Drainer, func(), error) {
	// AUTO_MIGRATE=false: assume the schema is already migrated (a separate
	// es-migrate init container ran it) — so replicas never touch DDL on boot
	// (ADR 0010). Default true keeps dev/single-node zero-config.
	autoMigrate := os.Getenv("AUTO_MIGRATE") != "false"

	switch backend {
	case "postgres":
		dsn := os.Getenv("PG_DSN")
		if dsn == "" {
			return nil, nil, nil, fmt.Errorf("PG_DSN is required for BACKEND=postgres")
		}
		var opts []postgres.Option
		if !autoMigrate {
			opts = append(opts, postgres.WithoutAutoMigrate())
		}
		store, err := postgres.Open(ctx, dsn, opts...)
		if err != nil {
			return nil, nil, nil, err
		}
		// Postgres drains gap-safe (FOR UPDATE SKIP LOCKED), so it can back the
		// in-process relay under leader election (RELAY=true).
		return store.Workspace, store, func() { store.Close() }, nil

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
			return nil, nil, nil, err
		}
		// Single-workspace: ignore the workspace argument. No Drainer — SQLite is
		// single-node; use the in-process Poller, not the relay.
		return func(string) es.Store { return store }, nil, func() { store.Close() }, nil

	default:
		return nil, nil, nil, fmt.Errorf("unknown BACKEND %q (want postgres|sqlite)", backend)
	}
}
