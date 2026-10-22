package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/postgres"
)

// Integration tests for OpenWithPool. Skip unless PG_DSN is set, matching
// postgres_test.go:
//
//	PG_DSN='postgres://postgres:pw@127.0.0.1:5433/eslite?sslmode=disable' go test ./postgres/...

// callerPool builds a pool the test owns, the way a caller supplying its own
// pool would: from parsed config it can adjust, rather than pgxpool.New's
// defaults.
func callerPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	// An application_name is the simplest observable proof that the caller's
	// own pool configuration survived — Open cannot set one.
	cfg.ConnConfig.RuntimeParams["application_name"] = "eslite-openwithpool-test"
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestOpenWithPoolRejectsNilPool(t *testing.T) {
	if _, err := postgres.OpenWithPool(context.Background(), nil); err == nil {
		t.Fatal("OpenWithPool(nil) returned no error, want one")
	}
}

func TestOpenWithPoolAppliesCallerPoolConfig(t *testing.T) {
	ctx := context.Background()
	pool := callerPool(t)

	s, err := postgres.OpenWithPool(ctx, pool)
	if err != nil {
		t.Fatalf("open with pool: %v", err)
	}
	t.Cleanup(s.Close)
	if _, err := s.Pool().Exec(ctx, `TRUNCATE events, checkpoints, unique_claims`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// The store must be using the caller's pool, not one of its own: the
	// application_name set in callerPool is visible only on that pool's
	// connections.
	var appName string
	if err := s.Pool().QueryRow(ctx, `SHOW application_name`).Scan(&appName); err != nil {
		t.Fatalf("show application_name: %v", err)
	}
	if appName != "eslite-openwithpool-test" {
		t.Fatalf("application_name=%q, want %q — the store is not using the caller's pool",
			appName, "eslite-openwithpool-test")
	}

	// And it must work end to end through that pool.
	rt := aggregate.NewRuntime(s.Workspace("ws_owp"), counter.Decider, counterv1.EventCodec{})
	sid, err := es.NewStreamID("counter", "owp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 10, Initial: 3}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	state, version, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state.GetCount() != 3 || version != 1 {
		t.Fatalf("count=%d version=%d, want 3/1", state.GetCount(), version)
	}
}

// TestOpenWithPoolCloseLeavesCallerPoolOpen is the point of the whole feature:
// a pool the caller supplied stays the caller's, so the caller can keep using
// it for other work after the Store is done with it. If Close closed it, every
// other user of that pool would break.
func TestOpenWithPoolCloseLeavesCallerPoolOpen(t *testing.T) {
	ctx := context.Background()
	pool := callerPool(t)

	s, err := postgres.OpenWithPool(ctx, pool)
	if err != nil {
		t.Fatalf("open with pool: %v", err)
	}

	s.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("caller pool unusable after Store.Close: %v — Close must not close a borrowed pool", err)
	}
}

// TestOpenClosesItsOwnPool is the counterpart: a pool Open created is Open's to
// release, so Close must close it.
func TestOpenClosesItsOwnPool(t *testing.T) {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	ctx := context.Background()

	s, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	pool := s.Pool()

	s.Close()

	if err := pool.Ping(ctx); err == nil {
		t.Fatal("pool still usable after Store.Close, want it closed — Open owns the pool it creates")
	}
}
