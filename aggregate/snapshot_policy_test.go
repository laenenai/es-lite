package aggregate_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/snapshot"
)

// countingCodec wraps counterStateCodec and counts the Encode and Decode
// calls, so a test can prove the write policy runs before the state
// marshal: a not-due append must not even Encode.
type countingCodec struct {
	encodes int
	decodes int
}

func (c *countingCodec) Encode(s *counterv1.Counter) ([]byte, error) {
	c.encodes++
	return counterStateCodec{}.Encode(s)
}

func (c *countingCodec) Decode(b []byte) (*counterv1.Counter, error) {
	c.decodes++
	return counterStateCodec{}.Decode(b)
}

// flakyCache stages a lost snapshot write: it wraps *testCache (embedded by
// pointer — testCache holds a mutex, never copy it) and fails exactly one
// Save — the first to arrive — then forwards everything.
type flakyCache struct {
	*testCache
	failNext bool
}

func (c *flakyCache) Save(ctx context.Context, streamID string, snap snapshot.Snapshot) error {
	if c.failNext {
		c.failNext = false
		return errors.New("snapshot cache: staged Save failure")
	}
	return c.testCache.Save(ctx, streamID, snap)
}

// noopEventCodec is the event codec for a decider that never produces
// events: both methods panic, because a no-op command must neither append
// nor fold a single event.
type noopEventCodec struct{}

func (noopEventCodec) Encode(struct{}) (es.EncodedEvent, error) {
	panic("event Encode must not be called for a no-op command")
}

func (noopEventCodec) Decode(es.EncodedEvent) (struct{}, error) {
	panic("event Decode must not be called for a no-op command")
}

// noopStateCodec serializes the struct{} state and counts Encode calls, so
// a test can prove a no-op command never marshals state for a snapshot.
type noopStateCodec struct{ encodes int }

func (c *noopStateCodec) Encode(struct{}) ([]byte, error) {
	c.encodes++
	return nil, nil
}

func (c *noopStateCodec) Decode([]byte) (struct{}, error) { return struct{}{}, nil }

// TestSnapshotEveryDefaultWriteThrough pins the default: without
// WithSnapshotEvery the cache keeps today's behaviour — every successful
// append writes the folded state through.
func TestSnapshotEveryDefaultWriteThrough(t *testing.T) {
	ctx := context.Background()
	cache, rt := newSnapshotRuntime(t, openStore(t))
	sid, _ := es.NewStreamID("counter", "default")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})

	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok {
		t.Fatalf("cache after one Init, policy unset: ok=%v err=%v, want the write-through snapshot", ok, err)
	}
	if snap.Version != 1 {
		t.Fatalf("cached version = %d, want 1 (an unset policy writes every append)", snap.Version)
	}
}

// TestSnapshotEveryZeroDisablesWrites pins n == 0: a configured cache that
// never receives an automatic write — a read-only cache.
func TestSnapshotEveryZeroDisablesWrites(t *testing.T) {
	ctx := context.Background()
	cache, rt := newSnapshotRuntime(t, openStore(t))
	rt.WithSnapshotEvery(0)
	sid, _ := es.NewStreamID("counter", "zero")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	for i := 0; i < 3; i++ {
		mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1})
	}

	if _, ok, err := cache.Load(ctx, sid.Canonical()); err != nil || ok {
		t.Fatalf("cache after four appends with WithSnapshotEvery(0): ok=%v err=%v, want ok=false (writes disabled)", ok, err)
	}
}

// TestSnapshotEveryWritesAtTheGap pins the schedule: with n=3 the writes
// land at v3 and v6, and the appends between them leave the cached row
// alone — each measured on the gap since its own load's seed.
func TestSnapshotEveryWritesAtTheGap(t *testing.T) {
	ctx := context.Background()
	cache, rt := newSnapshotRuntime(t, openStore(t))
	rt.WithSnapshotEvery(3)
	sid, _ := es.NewStreamID("counter", "gap")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 5}) // v1
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1})                    // v2
	if _, ok, _ := cache.Load(ctx, sid.Canonical()); ok {
		t.Fatal("cache holds a snapshot below the gap (v1–v2 with n=3)")
	}

	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1}) // v3 — at the gap
	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok || snap.Version != 3 {
		t.Fatalf("cache at v3: ok=%v version=%d err=%v, want the snapshot written at the gap", ok, snap.Version, err)
	}

	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1}) // v4 — seeds at 3
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1}) // v5
	snap, ok, _ = cache.Load(ctx, sid.Canonical())
	if !ok || snap.Version != 3 {
		t.Fatalf("cache after v4–v5: ok=%v version=%d, want the v3 snapshot kept (the gap was not reached)", ok, snap.Version)
	}

	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1}) // v6 — gap from the v3 seed
	snap, ok, _ = cache.Load(ctx, sid.Canonical())
	if !ok || snap.Version != 6 {
		t.Fatalf("cache after v6: ok=%v version=%d, want the snapshot rewritten at the gap (6)", ok, snap.Version)
	}
}

// TestSnapshotEveryEncodesOnlyWhenDue pins the ordering the policy exists
// for: the gate runs BEFORE stateCodec.Encode, so a not-due append never
// pays the state marshal. Without the policy, one append is one encode.
func TestSnapshotEveryEncodesOnlyWhenDue(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)

	// Unset policy: the write-through encodes the folded state on every
	// successful append — one Init, one encode.
	cache := newTestCache()
	codec := &countingCodec{}
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, codec, 1)
	sid, _ := es.NewStreamID("counter", "encode-default")
	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	if codec.encodes != 1 {
		t.Fatalf("unset policy: encodes=%d after one append, want 1 (the write-through encodes every append)", codec.encodes)
	}

	// n=3: v1 and v2 are below the gap and must not even encode; v3
	// encodes exactly once.
	cache = newTestCache()
	codec = &countingCodec{}
	rt = aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, codec, 1).
		WithSnapshotEvery(3)
	sid, _ = es.NewStreamID("counter", "encode-gated")
	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	if codec.encodes != 0 {
		t.Fatalf("n=3: encodes=%d after the v1 append, want 0 — the policy must gate before the encode", codec.encodes)
	}
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1})
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1})
	if codec.encodes != 1 {
		t.Fatalf("n=3: encodes=%d at v3, want exactly the one write at the gap", codec.encodes)
	}
}

// TestSnapshotEveryRetriesALostWrite pins the healing: the runtime discards
// Save's error, so a failed write never fails the command — and because
// the next load finds no seed, the very next append is due again and
// rewrites the row. Handle must never fail because of the cache.
func TestSnapshotEveryRetriesALostWrite(t *testing.T) {
	ctx := context.Background()
	cache := &flakyCache{testCache: newTestCache(), failNext: true}
	rt := aggregate.NewRuntime(openStore(t), counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, counterStateCodec{}, 1).
		WithSnapshotEvery(3)
	sid, _ := es.NewStreamID("counter", "lost")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 5}) // v1
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1})                    // v2
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1})                    // v3 — the Save fails
	if _, ok, _ := cache.Load(ctx, sid.Canonical()); ok {
		t.Fatal("cache holds a snapshot after the staged Save failure at v3")
	}

	// The v4 load finds no seed, so the append is measured from 0:
	// 4 >= 0+3 is due, and the lost v3 write is retried at v4.
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1})
	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok || snap.Version != 4 {
		t.Fatalf("cache after v4: ok=%v version=%d err=%v, want the lost v3 write retried at 4", ok, snap.Version, err)
	}
}

// TestSnapshotEveryHealsADiscardedSeed pins the reset healing: a log
// restored below the cached version makes the lineage guard discard the
// seed, so the next append full-folds from 0 and — with seed 0 — is due at
// to >= n, replacing the phantom row without operator action.
func TestSnapshotEveryHealsADiscardedSeed(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	cache, rt := newSnapshotRuntime(t, store)
	sid, _ := es.NewStreamID("counter", "heal")

	// Build to v5 under the default write-through, so the cache holds v5
	// (count 9).
	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 5})
	for i := 0; i < 4; i++ {
		mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1})
	}

	// Reset the log out-of-band to v1–v2 (count 6), the way an operator's
	// truncate does: the cache still claims v5 — a phantom.
	if _, err := store.DB().ExecContext(ctx,
		`DELETE FROM events WHERE stream_id = ? AND version > 2`, sid.Canonical(),
	); err != nil {
		t.Fatalf("stage log reset: %v", err)
	}

	// One Increment under the policy: the load discards the phantom seed
	// and full-folds to the log's v2 (count 6), the append lands at v3,
	// and 3 >= 0+3 rewrites the row.
	rt.WithSnapshotEvery(3)
	res := mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 1})
	if res.ToVersion != 3 || res.State.GetCount() != 7 {
		t.Fatalf("increment after the reset: count=%d toVersion=%d, want the refolded 7 at v3", res.State.GetCount(), res.ToVersion)
	}

	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok || snap.Version != 3 {
		t.Fatalf("cache after healing: ok=%v version=%d err=%v, want the phantom v5 replaced by 3", ok, snap.Version, err)
	}
	healed, err := counterStateCodec{}.Decode(snap.State)
	if err != nil || healed.GetCount() != 7 {
		t.Fatalf("healed snapshot state: count=%d err=%v, want the refolded 7 — not the phantom v5 state", healed.GetCount(), err)
	}
}

// TestSnapshotEveryWithNoCacheIsInert pins that the knob alone is harmless:
// a runtime with the policy but no cache handles commands without panic.
func TestSnapshotEveryWithNoCacheIsInert(t *testing.T) {
	ctx := context.Background()
	rt := aggregate.NewRuntime(openStore(t), counter.Decider, counterv1.EventCodec{}).
		WithSnapshotEvery(3)
	sid, _ := es.NewStreamID("counter", "nocache")

	res := mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	if res.ToVersion != 1 {
		t.Fatalf("init without a cache: toVersion=%d, want 1", res.ToVersion)
	}
}

// TestNoOpCommandWritesNoSnapshot pins that a command producing neither
// events nor constraints never writes: Handle returns equal versions and
// the append — the only thing the write policy measures — never happens,
// so the state is never encoded and the cache stays empty. That holds even
// with the policy unset, because the no-op returns before the write site.
func TestNoOpCommandWritesNoSnapshot(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache()
	codec := &noopStateCodec{}
	rt := aggregate.NewRuntime(openStore(t),
		es.Decider[struct{}, struct{}, struct{}]{
			Initial: func() struct{} { return struct{}{} },
			Decide:  func(struct{}, struct{}) ([]struct{}, []es.ConstraintOp, error) { return nil, nil, nil },
			Evolve:  func(s struct{}, _ struct{}) struct{} { return s },
		},
		noopEventCodec{},
	).WithSnapshots(cache, codec, 1)
	sid, _ := es.NewStreamID("noop", "x")

	res, err := rt.Handle(ctx, sid, struct{}{}, es.Meta{})
	if err != nil {
		t.Fatalf("handle the no-op command: %v", err)
	}
	if res.FromVersion != res.ToVersion {
		t.Fatalf("no-op handle: from=%d to=%d, want equal versions", res.FromVersion, res.ToVersion)
	}
	if codec.encodes != 0 {
		t.Fatalf("no-op handle encoded state %d time(s), want 0", codec.encodes)
	}
	if _, ok, _ := cache.Load(ctx, sid.Canonical()); ok {
		t.Fatal("cache holds a snapshot after a no-op command")
	}
}

// TestWarmSnapshotWritesAtHead pins the explicit warm-up (ADR 0042 §3.1):
// a stream the policy would never write automatically — five appends, all
// below n=100 — gets a snapshot at its head when the caller asks for one.
// The warm writes the head fold's state, not a stale one.
func TestWarmSnapshotWritesAtHead(t *testing.T) {
	ctx := context.Background()
	cache, rt := newSnapshotRuntime(t, openStore(t))
	rt.WithSnapshotEvery(100)
	sid, _ := es.NewStreamID("counter", "warm")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 5})
	for i := 0; i < 4; i++ {
		mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
	}
	if _, ok, _ := cache.Load(ctx, sid.Canonical()); ok {
		t.Fatal("cache holds a snapshot before the warm (the n=100 policy must not have written)")
	}

	version, err := rt.WarmSnapshot(ctx, sid)
	if err != nil {
		t.Fatalf("WarmSnapshot: %v", err)
	}
	if version != 5 {
		t.Fatalf("WarmSnapshot version = %d, want 5 (the stream's head)", version)
	}
	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok || snap.Version != 5 {
		t.Fatalf("cache after the warm: ok=%v version=%d err=%v, want the head snapshot at v5", ok, snap.Version, err)
	}
	warm, err := counterStateCodec{}.Decode(snap.State)
	if err != nil || warm.GetCount() != 45 {
		t.Fatalf("warmed state: count=%d err=%v, want the head fold's 45", warm.GetCount(), err)
	}
}

// TestWarmSnapshotWithZeroPolicyStillWrites pins that the warm-up
// deliberately bypasses the threshold: even with automatic writes disabled
// (n == 0, a read-only cache) an explicit warm writes the head snapshot —
// the caller is asking for a write, so the policy does not apply.
func TestWarmSnapshotWithZeroPolicyStillWrites(t *testing.T) {
	ctx := context.Background()
	cache, rt := newSnapshotRuntime(t, openStore(t))
	rt.WithSnapshotEvery(0)
	sid, _ := es.NewStreamID("counter", "warm-zero")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 5})
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
	if _, ok, _ := cache.Load(ctx, sid.Canonical()); ok {
		t.Fatal("cache holds a snapshot before the warm (n=0 disables automatic writes)")
	}

	version, err := rt.WarmSnapshot(ctx, sid)
	if err != nil {
		t.Fatalf("WarmSnapshot: %v", err)
	}
	if version != 3 {
		t.Fatalf("WarmSnapshot version = %d, want 3", version)
	}
	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok || snap.Version != 3 {
		t.Fatalf("cache after the warm: ok=%v version=%d err=%v, want the n=0 policy bypassed with a v3 snapshot", ok, snap.Version, err)
	}
}

// TestWarmSnapshotWithoutCacheErrors pins the guard: a runtime with no
// snapshot cache configured cannot warm — it returns an error naming the
// method instead of panicking on the nil cache.
func TestWarmSnapshotWithoutCacheErrors(t *testing.T) {
	ctx := context.Background()
	rt := aggregate.NewRuntime(openStore(t), counter.Decider, counterv1.EventCodec{})
	sid, _ := es.NewStreamID("counter", "warm-nocache")

	version, err := rt.WarmSnapshot(ctx, sid)
	if err == nil {
		t.Fatal("WarmSnapshot without a cache returned nil error, want an error")
	}
	if version != 0 {
		t.Fatalf("WarmSnapshot without a cache returned version %d, want 0", version)
	}
	if !strings.Contains(err.Error(), "WarmSnapshot") {
		t.Fatalf("error %q does not name WarmSnapshot", err)
	}
}

// TestWarmSnapshotSurfacesSaveError pins the difference from Handle: the
// append path treats a cache failure as best-effort and discards Save's
// error, but an explicit warm IS the caller asking for the write — the
// failure must come back, not vanish.
func TestWarmSnapshotSurfacesSaveError(t *testing.T) {
	ctx := context.Background()
	cache := &flakyCache{testCache: newTestCache(), failNext: true}
	rt := aggregate.NewRuntime(openStore(t), counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, counterStateCodec{}, 1).
		WithSnapshotEvery(0) // keep Handle off the cache so the staged failure survives for the warm
	sid, _ := es.NewStreamID("counter", "warm-fail")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})

	version, err := rt.WarmSnapshot(ctx, sid)
	if err == nil {
		t.Fatal("WarmSnapshot returned nil error after the staged Save failure, want the error surfaced")
	}
	if version != 0 {
		t.Fatalf("WarmSnapshot returned version %d with the Save error, want 0", version)
	}
	if !strings.Contains(err.Error(), "staged Save failure") {
		t.Fatalf("error %q is not the staged Save failure", err)
	}
}

// TestWarmSnapshotThenLoadSeeds pins the point of the warm-up: after an
// explicit warm, a load seeds from the head snapshot and reads at most one
// envelope — the lineage probe — instead of re-folding the whole stream,
// and it still returns the stream's full state.
func TestWarmSnapshotThenLoadSeeds(t *testing.T) {
	ctx := context.Background()
	cs := &countingStore{Store: openStore(t)}
	_, rt := newSnapshotRuntime(t, cs)
	rt.WithSnapshotEvery(0) // the warm below is the stream's only snapshot write
	sid, _ := es.NewStreamID("counter", "warm-seed")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 5})
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
	mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})

	if _, err := rt.WarmSnapshot(ctx, sid); err != nil {
		t.Fatalf("WarmSnapshot: %v", err)
	}

	cs.rows = 0
	st, v, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("load after the warm: %v", err)
	}
	if v != 3 || st.GetCount() != 25 {
		t.Fatalf("load after the warm = (count %d, v%d), want the full state (25 at v3)", st.GetCount(), v)
	}
	if cs.rows > 1 {
		t.Fatalf("load after the warm read %d envelopes, want at most the 1 lineage-probe row off the head snapshot", cs.rows)
	}
}
