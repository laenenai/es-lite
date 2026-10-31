package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/migrate"
)

// TestPopulatedStoreChainsFromMigration stages a store that already holds
// rows under schema 0001, applies 0002, appends, and checks the ADR 0029 §5
// behaviour: old rows stay unhashed, the first new row chains to
// UnchainedHeadHash(sid, head), VerifyStream reports ChainedFrom, and the
// migration version is 2. It is an internal test so it can apply one
// migration at a time.
func TestPopulatedStoreChainsFromMigration(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", withPragmas("file:"+t.Name()+"?mode=memory&cache=shared"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	migs, err := migrate.Load(migrationsFS, "migrations")
	if err != nil || len(migs) < 2 || migs[0].Version != 1 || migs[1].Version != 2 {
		t.Fatalf("migrations = %+v err=%v, want 0001 and 0002", migs, err)
	}
	// Schema 0001 only, recorded in schema_migrations as Migrate would.
	if _, err := db.ExecContext(ctx, `CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, migs[0].SQL); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (1)`); err != nil {
		t.Fatal(err)
	}

	// Two pre-chain rows, as the old Append wrote them (no hash columns).
	sid, _ := es.NewStreamID("thing", "old")
	old := es.TruncateMicros(time.Now().Add(-time.Hour))
	for v := 1; v <= 2; v++ {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO events (event_id, stream_type, stream_id, version, type_url, schema_version, occurred_at, recorded_at,
			   correlation_id, causation_id, command_id, actor_type, actor_id, actor_principal, payload)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			newV7().String(), sid.Type, sid.Canonical(), v, "thing.v1.Happened", 1, formatTS(old), formatTS(old),
			"", "", "", "", "", "", []byte{byte(v)}); err != nil {
			t.Fatalf("seed v%d: %v", v, err)
		}
	}

	// Now the upgrade: Migrate applies 0002 only.
	s, err := NewWithDB(ctx, db)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	var version int
	if err := db.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != 2 {
		t.Fatalf("schema version = %d err=%v, want 2", version, err)
	}
	since, err := s.ChainedSince(ctx)
	if err != nil || since.IsZero() {
		t.Fatalf("ChainedSince = %v err=%v", since, err)
	}

	// The first post-migration append chains to the unchained head.
	res, err := s.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: 2,
		Events: []es.EventData{{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte{3}}}})
	if err != nil {
		t.Fatalf("append after migration: %v", err)
	}
	want := es.UnchainedHeadHash(sid, 2)
	if string(res.Envelopes[0].PrevHash) != string(want[:]) {
		t.Fatal("first chained event does not link to UnchainedHeadHash(sid, 2)")
	}

	rows, err := s.ReadStream(ctx, sid, 0, 0)
	if err != nil || len(rows) != 3 {
		t.Fatalf("read: %d rows err=%v", len(rows), err)
	}
	if rows[0].Hash != nil || rows[1].Hash != nil || rows[2].Hash == nil {
		t.Fatal("pre-0002 rows must stay unhashed and the new row must be hashed")
	}

	rep, err := es.VerifyStreamWith(ctx, s, sid, es.VerifyOptions{ChainedSince: since})
	if err != nil || !rep.Intact() {
		t.Fatalf("verify: err=%v rep=%+v", err, rep)
	}
	if rep.ChainedFrom != 3 || rep.Head != 3 {
		t.Fatalf("ChainedFrom=%d Head=%d, want 3/3", rep.ChainedFrom, rep.Head)
	}
	if _, err := es.VerifyStreamWith(ctx, s, sid, es.VerifyOptions{Strict: true}); !errors.Is(err, es.ErrChainBroken) {
		t.Fatalf("strict verify on a populated store err = %v, want ErrChainBroken", err)
	}
	// An anchor over a partly unchained store still binds the heads.
	a, err := s.Anchor(ctx, 0)
	if err != nil || a.StreamCount != 1 {
		t.Fatalf("anchor: %+v err=%v", a, err)
	}
	if err := s.CheckAnchor(ctx, a); err != nil {
		t.Fatalf("check anchor: %v", err)
	}
}
