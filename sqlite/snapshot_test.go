package sqlite_test

// Integration tests for the SQLite snapshot.Cache adapter (ADR 0042 §3.2):
// the round trip preserves every field — including recorded_at's
// nanoseconds, which the events table's fixed-width layout keeps exactly —
// absent is ok=false with no error, Save is last-writer-wins by design
// (no monotonic guard: a stale row costs at most n extra folded events,
// and a guard would permanently wedge a stream after a log reset), Purge
// empties the database's rows, and a store opened before its migrations
// (WithoutAutoMigrate) fails Load and Save observably instead of silently
// reading as empty: the runtime discards these errors, so silence would
// degrade to permanent full folds with no signal.
//
// One SQLite database is one workspace, so unlike the Postgres adapter
// there is no workspace parameter and no isolation test: every cache over
// one Store serves the same single workspace.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/snapshot"
	"github.com/laenenai/es-lite/sqlite"
)

// snapStream returns a canonical stream id for the snapshot tests.
func snapStream(t *testing.T, id string) string {
	t.Helper()
	sid, err := es.NewStreamID("counter", id)
	if err != nil {
		t.Fatal(err)
	}
	return sid.Canonical()
}

func TestSQLiteSnapshotCacheRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	c := sqlite.NewSnapshotCache(s)
	sid := snapStream(t, "main")

	// The fixed-width layout stores all nine nanosecond digits, so the
	// round trip is exact — no truncation, unlike Postgres's micros.
	fixed := time.Date(2026, 11, 20, 10, 0, 0, 123456789, time.UTC)
	want := snapshot.Snapshot{Version: 7, FoldVersion: 2, RecordedAt: fixed, State: []byte{1, 2, 3}}
	if err := c.Save(ctx, sid, want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := c.Load(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v, want ok", ok, err)
	}
	if got.Version != want.Version || got.FoldVersion != want.FoldVersion || !bytes.Equal(got.State, want.State) {
		t.Fatalf("load = (v%d fold%d %q), want (v%d fold%d %q)",
			got.Version, got.FoldVersion, got.State, want.Version, want.FoldVersion, want.State)
	}
	if !got.RecordedAt.Equal(fixed) {
		t.Fatalf("recorded_at = %v, want %v (the fixed-width layout must preserve nanoseconds)", got.RecordedAt, fixed)
	}
}

func TestSQLiteSnapshotCacheAbsent(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	c := sqlite.NewSnapshotCache(s)

	got, ok, err := c.Load(ctx, snapStream(t, "absent"))
	if ok || err != nil {
		t.Fatalf("absent load: ok=%v err=%v, want false/nil", ok, err)
	}
	if got.Version != 0 || got.FoldVersion != 0 || !got.RecordedAt.IsZero() || len(got.State) != 0 {
		t.Fatalf("absent load returned %+v, want the zero snapshot", got)
	}
}

// TestSQLiteSnapshotCacheOverwrite pins the deliberate no-guard behaviour
// (ADR 0042 §3.2): the second Save replaces the row even at a LOWER
// version. A monotonic guard would permanently wedge a stream after a log
// reset, because the stale row could never be replaced from a lower
// version.
func TestSQLiteSnapshotCacheOverwrite(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	c := sqlite.NewSnapshotCache(s)
	sid := snapStream(t, "main")

	v7 := snapshot.Snapshot{Version: 7, FoldVersion: 2, RecordedAt: time.Now().UTC(), State: []byte("seven")}
	if err := c.Save(ctx, sid, v7); err != nil {
		t.Fatalf("save v7: %v", err)
	}
	v5 := snapshot.Snapshot{Version: 5, FoldVersion: 2, RecordedAt: time.Now().UTC(), State: []byte("five")}
	if err := c.Save(ctx, sid, v5); err != nil {
		t.Fatalf("save v5: %v", err)
	}

	got, ok, err := c.Load(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v, want ok", ok, err)
	}
	if got.Version != 5 || string(got.State) != "five" {
		t.Fatalf("load = (v%d %q), want (v5 \"five\") — last-writer-wins must let the lower version through", got.Version, got.State)
	}
}

func TestSQLiteSnapshotCachePurge(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	c := sqlite.NewSnapshotCache(s)

	for _, sid := range []string{snapStream(t, "one"), snapStream(t, "two")} {
		snap := snapshot.Snapshot{Version: 1, FoldVersion: 1, RecordedAt: time.Now().UTC(), State: []byte(sid)}
		if err := c.Save(ctx, sid, snap); err != nil {
			t.Fatalf("save %s: %v", sid, err)
		}
	}
	if err := c.Purge(ctx); err != nil {
		t.Fatalf("purge: %v", err)
	}
	for _, sid := range []string{snapStream(t, "one"), snapStream(t, "two")} {
		if _, ok, err := c.Load(ctx, sid); ok || err != nil {
			t.Fatalf("load %s after purge: ok=%v err=%v, want absent", sid, ok, err)
		}
	}
}

// TestSQLiteSnapshotCacheMissingTableErrors pins the WithoutAutoMigrate
// contract (ADR 0042 §7): a store opened before its migrations fails Load
// and Save observably. The runtime discards these errors, so a silent
// ok=false/nil would degrade the stream to permanent full folds with no
// signal at all.
func TestSQLiteSnapshotCacheMissingTableErrors(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared", sqlite.WithoutAutoMigrate())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	c := sqlite.NewSnapshotCache(s)
	sid := snapStream(t, "main")

	if _, ok, err := c.Load(ctx, sid); err == nil {
		t.Fatalf("load on a missing table: ok=%v err=nil, want an error, not a silent miss", ok)
	}
	if err := c.Save(ctx, sid, snapshot.Snapshot{Version: 1, FoldVersion: 1, RecordedAt: time.Now().UTC(), State: []byte("x")}); err == nil {
		t.Fatal("save on a missing table returned nil error, want an error")
	}
}
