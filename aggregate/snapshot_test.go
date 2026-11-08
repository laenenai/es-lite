package aggregate_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/snapshot"
)

// testCache is the repository's first snapshot.Cache implementation —
// test-only: one snapshot per canonical stream id in a plain map, guarded
// by a mutex.
type testCache struct {
	mu    sync.Mutex
	snaps map[string]snapshot.Snapshot
}

func newTestCache() *testCache {
	return &testCache{snaps: make(map[string]snapshot.Snapshot)}
}

func (c *testCache) Load(_ context.Context, streamID string) (snapshot.Snapshot, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap, ok := c.snaps[streamID]
	return snap, ok, nil
}

func (c *testCache) Save(_ context.Context, streamID string, snap snapshot.Snapshot) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snaps[streamID] = snap
	return nil
}

// counterStateCodec round-trips the counter's proto state for the
// snapshot cache (es.StateCodec).
type counterStateCodec struct{}

func (counterStateCodec) Encode(s *counterv1.Counter) ([]byte, error) { return proto.Marshal(s) }

func (counterStateCodec) Decode(b []byte) (*counterv1.Counter, error) {
	var s counterv1.Counter
	if err := proto.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// recordingStore wraps a Store and records the `from` of every ReadStream
// call, so a test can tell a seeded tail fold (from > 0) from a cold full
// fold (from == 0) — and a cache-only read (no calls at all).
type recordingStore struct {
	es.Store
	froms []uint64
}

func (s *recordingStore) ReadStream(ctx context.Context, sid es.StreamID, from, to uint64) ([]es.Envelope, error) {
	s.froms = append(s.froms, from)
	return s.Store.ReadStream(ctx, sid, from, to)
}

func (s *recordingStore) readFromZero() bool {
	for _, f := range s.froms {
		if f == 0 {
			return true
		}
	}
	return false
}

// countingStore wraps a Store and counts every envelope its ReadStream
// returns, so a test can bound a load's read cost: a seeded load's reads
// are the lineage probe plus the tail fold, nothing else.
type countingStore struct {
	es.Store
	rows int
}

func (s *countingStore) ReadStream(ctx context.Context, sid es.StreamID, from, to uint64) ([]es.Envelope, error) {
	envs, err := s.Store.ReadStream(ctx, sid, from, to)
	s.rows += len(envs)
	return envs, err
}

// newSnapshotRuntime wires a counter runtime with the test cache over
// store (fold version 1).
func newSnapshotRuntime(t *testing.T, store es.Store) (*testCache, *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]) {
	t.Helper()
	cache := newTestCache()
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, counterStateCodec{}, 1)
	return cache, rt
}

func mustHandle(t *testing.T, rt *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event], ctx context.Context, sid es.StreamID, cmd counterv1.Command) aggregate.Result[*counterv1.Counter, counterv1.Event] {
	t.Helper()
	res, err := rt.Handle(ctx, sid, cmd, es.Meta{})
	if err != nil {
		t.Fatalf("handle %T: %v", cmd, err)
	}
	return res
}

// TestLoadDiscardsSnapshotAheadOfLog pins F24: a snapshot can outlive the
// log it summarizes. After a reset or truncation the log no longer holds
// the seed's version, and load — which folds only the tail past the seed —
// would return the phantom cached state as if it were the stream's truth.
// The guard probes the envelope at the seed's version (one bounded read)
// and discards the seed whenever the log cannot show it.
func TestLoadDiscardsSnapshotAheadOfLog(t *testing.T) {
	ctx := context.Background()

	t.Run("stale seed ahead of a truncated log", func(t *testing.T) {
		store := openStore(t)
		_, rt := newSnapshotRuntime(t, store)
		sid, _ := es.NewStreamID("counter", "truncated")

		mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
		for i := 0; i < 4; i++ {
			mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
		}
		// The cache now holds the write-through snapshot at v5 (count 45).

		// Reset the log out-of-band to its first two events, the way an
		// operator's truncate does. The log now knows only v1–v2 (count
		// 15); the cache still claims v5.
		if _, err := store.DB().ExecContext(ctx,
			`DELETE FROM events WHERE stream_id = ? AND version > 2`, sid.Canonical(),
		); err != nil {
			t.Fatalf("stage log reset: %v", err)
		}

		st, v, err := rt.Load(ctx, sid)
		if err != nil {
			t.Fatalf("load over a truncated log: %v", err)
		}
		if v != 2 || st.GetCount() != 15 {
			t.Fatalf("load = (count %d, v%d), want the log's (count 15, v2) — not the cached v5 state", st.GetCount(), v)
		}
	})

	t.Run("seed at v1 over a fully reset log", func(t *testing.T) {
		store := openStore(t)
		_, rt := newSnapshotRuntime(t, store)
		sid, _ := es.NewStreamID("counter", "reset")

		mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
		// The cache holds v1. Wipe the whole stream: the probe at from==1
		// reads (0, 1) from the beginning of a stream that no longer
		// exists, which the store reports as es.ErrStreamNotFound — the
		// guard must read that as "no lineage" too, not as a failure.
		if _, err := store.DB().ExecContext(ctx,
			`DELETE FROM events WHERE stream_id = ?`, sid.Canonical(),
		); err != nil {
			t.Fatalf("stage full reset: %v", err)
		}

		st, v, err := rt.Load(ctx, sid)
		if err != nil {
			t.Fatalf("load over a fully reset log: %v", err)
		}
		if v != 0 || st.GetCount() != 0 || st.GetInitialized() {
			t.Fatalf("load = (count %d, initialized %v, v%d), want Initial() at v0", st.GetCount(), st.GetInitialized(), v)
		}
	})

	t.Run("seed at v1 over a head-truncated log", func(t *testing.T) {
		store := openStore(t)
		_, rt := newSnapshotRuntime(t, store)
		sid, _ := es.NewStreamID("counter", "head-truncated")

		mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
		// Append v2 through a snapshot-less runtime so the cache keeps
		// holding v1, then delete v1 out-of-band. The stream still exists
		// (v2 survives), so the probe's (0, 1) read returns (nil, nil) —
		// an empty range on an existing stream, the ReadStream contract's
		// other "no rows" shape.
		raw := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})
		if _, err := raw.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 10}), es.Meta{}); err != nil {
			t.Fatalf("raw handle of v2: %v", err)
		}
		if _, err := store.DB().ExecContext(ctx,
			`DELETE FROM events WHERE stream_id = ? AND version = 1`, sid.Canonical(),
		); err != nil {
			t.Fatalf("stage head truncation: %v", err)
		}

		st, v, err := rt.Load(ctx, sid)
		if err != nil {
			t.Fatalf("load over a head-truncated log: %v", err)
		}
		if v != 2 || st.GetCount() != 10 || st.GetInitialized() {
			t.Fatalf("load = (count %d, initialized %v, v%d), want the survivor log's fold (count 10, v2) — not the seeded v1 state", st.GetCount(), st.GetInitialized(), v)
		}
	})
}

// TestSnapshotsHappyPath pins the snapshot path end to end: Handle writes
// its folded state through to the cache, a later load seeds from it and
// folds only the tail past the seed, and the answer equals a full fold.
func TestSnapshotsHappyPath(t *testing.T) {
	ctx := context.Background()
	rs := &recordingStore{Store: openStore(t)}
	cache, rt := newSnapshotRuntime(t, rs)
	sid, _ := es.NewStreamID("counter", "main")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})

	// Write-through: after Handle the cache holds the folded state at the
	// stream's current version.
	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok {
		t.Fatalf("cache after two handles: ok=%v err=%v, want a write-through snapshot", ok, err)
	}
	if snap.Version != 2 || snap.FoldVersion != 1 {
		t.Fatalf("cached snapshot: version=%d foldVersion=%d, want 2/1", snap.Version, snap.FoldVersion)
	}
	seeded, err := counterStateCodec{}.Decode(snap.State)
	if err != nil || seeded.GetCount() != 15 {
		t.Fatalf("cached state: count=%d err=%v, want 15", seeded.GetCount(), err)
	}

	// From here the cache is warm: every store read must start above v0 —
	// a tail fold off the seed, never a cold full fold.
	rs.froms = nil

	res := mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
	if res.ToVersion != 3 || res.State.GetCount() != 25 {
		t.Fatalf("seeded handle: count=%d toVersion=%d, want 25 at v3", res.State.GetCount(), res.ToVersion)
	}

	st, v, err := rt.Load(ctx, sid)
	if err != nil || v != 3 || st.GetCount() != 25 {
		t.Fatalf("seeded load: count=%d v=%d err=%v, want 25 at v3", st.GetCount(), v, err)
	}
	if rs.readFromZero() {
		t.Fatal("a warm-cache read folded from v0 — the seed was not used")
	}

	snap, ok, _ = cache.Load(ctx, sid.Canonical())
	if !ok || snap.Version != 3 {
		t.Fatalf("cache after the seeded handle: ok=%v version=%d, want v3 written through", ok, snap.Version)
	}
}

// TestLoadStale pins the bounded-staleness read (ADR 0007): within maxAge
// the cache answers alone (fresh=true, zero store reads), and an aged-out
// snapshot falls back to a strong Load (fresh=false) that still returns
// the log's state.
func TestLoadStale(t *testing.T) {
	ctx := context.Background()
	rs := &recordingStore{Store: openStore(t)}
	cache, rt := newSnapshotRuntime(t, rs)
	sid, _ := es.NewStreamID("counter", "main")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})

	rs.froms = nil
	st, v, fresh, err := rt.LoadStale(ctx, sid, 0)
	if err != nil || !fresh || v != 2 || st.GetCount() != 15 {
		t.Fatalf("LoadStale(any age): count=%d v=%d fresh=%v err=%v, want the cache's 15 at v2", st.GetCount(), v, fresh, err)
	}
	if len(rs.froms) != 0 {
		t.Fatalf("LoadStale made %d store reads on the fresh path, want the cache to answer alone", len(rs.froms))
	}

	// Age the cached snapshot out of a 1h window: LoadStale must fall
	// back to a strong Load rather than serve two-hour-old state.
	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok {
		t.Fatalf("cache before re-aging: ok=%v err=%v", ok, err)
	}
	snap.RecordedAt = time.Now().UTC().Add(-2 * time.Hour)
	if err := cache.Save(ctx, sid.Canonical(), snap); err != nil {
		t.Fatalf("re-age snapshot: %v", err)
	}

	st, v, fresh, err = rt.LoadStale(ctx, sid, time.Hour)
	if err != nil || fresh || v != 2 || st.GetCount() != 15 {
		t.Fatalf("LoadStale(aged out): count=%d v=%d fresh=%v err=%v, want fresh=false with the log's 15 at v2", st.GetCount(), v, fresh, err)
	}
}

// TestSnapshotFoldVersionMismatchIgnored pins the fold-version guard: a
// snapshot written by an older runtime generation (another fold version)
// is never a seed — its state encodes the old fold's semantics — and the
// stream is refolded from the log instead.
func TestSnapshotFoldVersionMismatchIgnored(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	cache, rt := newSnapshotRuntime(t, store)
	sid, _ := es.NewStreamID("counter", "main")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})

	// Stage what the old generation left behind: a fold-1 snapshot whose
	// state no later fold may trust (here: a count no honest fold of this
	// log produces).
	poison, err := counterStateCodec{}.Encode(&counterv1.Counter{Initialized: true, Min: 0, Max: 100, Count: 999})
	if err != nil {
		t.Fatalf("encode poison state: %v", err)
	}
	if err := cache.Save(ctx, sid.Canonical(), snapshot.Snapshot{
		Version:     1,
		FoldVersion: 1,
		RecordedAt:  time.Now().UTC(),
		State:       poison,
	}); err != nil {
		t.Fatalf("stage poison snapshot: %v", err)
	}

	// A new generation (fold version 2) over the same log and cache.
	rt2 := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, counterStateCodec{}, 2)
	st, v, err := rt2.Load(ctx, sid)
	if err != nil || v != 1 || st.GetCount() != 5 {
		t.Fatalf("load over a mismatched fold: count=%d v=%d err=%v, want a full fold to 5 at v1 — not the cached 999", st.GetCount(), v, err)
	}
}

// TestLoadAsOfVersionExcludesFutureSnapshot pins the upToVersion==0 rule:
// time travel never seeds from a snapshot, because a snapshot newer than
// the target would swallow the requested past — seeding from v2 to
// reconstruct v1 would misreport both.
func TestLoadAsOfVersionExcludesFutureSnapshot(t *testing.T) {
	ctx := context.Background()
	_, rt := newSnapshotRuntime(t, openStore(t))
	sid, _ := es.NewStreamID("counter", "main")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
	// The cache now holds v2 (count 15).

	st, err := rt.LoadAsOfVersion(ctx, sid, 1)
	if err != nil {
		t.Fatalf("as-of v1 with a warm cache: %v", err)
	}
	if st.GetCount() != 5 {
		t.Fatalf("as-of v1 count = %d, want 5 from the v1 fold — not the cached v2 state (15)", st.GetCount())
	}
}

// TestLoadAsOfVersionIsTailBoundedWithSnapshots pins the other half of the
// seed rule (F21): a version-bounded load with a snapshot at or below the
// bound seeds from it and folds only the (snapshot, version] tail — O(tail)
// store rows — instead of re-reading the whole stream, which is O(stream
// length) per event for a FETCH_MODE_STATE_AS_OF projection. The Task 25
// lineage guard still probes the seed's version, so the counting store
// sees probe + tail rows: 1 + 2 here, never the 12 of a full re-read.
func TestLoadAsOfVersionIsTailBoundedWithSnapshots(t *testing.T) {
	ctx := context.Background()
	cs := &countingStore{Store: openStore(t)}
	cache, rt := newSnapshotRuntime(t, cs)
	sid, _ := es.NewStreamID("counter", "main")

	// A v12 stream: v1 Init (count 5), v2–v12 Increment (10 each). The
	// snapshot runtime handles v1–v10 and writes the state through at v10;
	// a snapshot-less runtime appends v11–v12 so the cache keeps v10.
	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 5})
	for i := 0; i < 9; i++ {
		mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
	}
	raw := aggregate.NewRuntime(cs, counter.Decider, counterv1.EventCodec{})
	for i := 0; i < 2; i++ {
		mustHandle(t, raw, ctx, sid, counterv1.Command(&counterv1.Increment{By: 10}))
	}

	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok || snap.Version != 10 {
		t.Fatalf("staged snapshot: ok=%v version=%d err=%v, want the write-through v10", ok, snap.Version, err)
	}

	cs.rows = 0
	st, err := rt.LoadAsOfVersion(ctx, sid, 12)
	if err != nil {
		t.Fatalf("as-of v12 with a v10 snapshot: %v", err)
	}
	if st.GetCount() != 115 {
		t.Fatalf("as-of v12 count = %d, want the full v12 fold's 115 (5 + 11×10)", st.GetCount())
	}
	// One lineage-probe row (the envelope at v10) plus the two tail rows
	// (v11, v12) — 3 rows, not the 12 a seedless load would re-read.
	if cs.rows != 3 {
		t.Fatalf("as-of v12 read %d rows, want the lineage probe + tail = 1 + 2 = 3 — not the whole 12-row stream", cs.rows)
	}
}

// TestLoadAsOfVersionDiscardsPhantomSnapshot pins the Task 25 lineage guard
// on the version-bounded path that became seedable above: when the seed's
// version no longer exists in the log (a reset or truncation outlived the
// snapshot), the probe discards the seed and the load re-reads the stream
// from the beginning — the answer is the surviving log's fold, never the
// phantom cached state.
func TestLoadAsOfVersionDiscardsPhantomSnapshot(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	_, rt := newSnapshotRuntime(t, store)
	sid, _ := es.NewStreamID("counter", "phantom")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 5})
	for i := 0; i < 4; i++ {
		mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
	}
	// The cache holds v5 (count 45). Append v6–v8 snapshot-less so the
	// cache stays at v5, then delete v5 out-of-band, the way an operator's
	// truncate does: the log's truth is v1–v4 + v6–v8 (count 65), but the
	// cache still claims v5 — a phantom a seeded bounded load must not
	// present (it would answer 45 + 30 = 75).
	raw := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})
	for i := 0; i < 3; i++ {
		mustHandle(t, raw, ctx, sid, counterv1.Command(&counterv1.Increment{By: 10}))
	}
	if _, err := store.DB().ExecContext(ctx,
		`DELETE FROM events WHERE stream_id = ? AND version = 5`, sid.Canonical(),
	); err != nil {
		t.Fatalf("stage v5 truncation: %v", err)
	}

	st, err := rt.LoadAsOfVersion(ctx, sid, 8)
	if err != nil {
		t.Fatalf("as-of v8 over a log that lost the seed's v5: %v", err)
	}
	if st.GetCount() != 65 {
		t.Fatalf("as-of v8 count = %d, want the surviving log's 65 — not the phantom seeded fold (75)", st.GetCount())
	}
}
