package aggregate_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/sqlite"
)

// stageSixCommands drives six appends through rt — Init (count 5) then five
// Increment{By:10} (count 55), versions 1..6 — and returns the sixth
// handle's result for state comparison.
func stageSixCommands(t *testing.T, rt *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event], ctx context.Context, sid es.StreamID) aggregate.Result[*counterv1.Counter, counterv1.Event] {
	t.Helper()
	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	var res aggregate.Result[*counterv1.Counter, counterv1.Event]
	for i := 0; i < 5; i++ {
		res = mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
	}
	return res
}

// TestSnapshotAdapterSeedsALoad pins ADR 0042 §7's integration claim over
// the real SQLite adapter (not the test cache): a runtime whose cache is
// sqlite.NewSnapshotCache persists its write-through snapshot, and a second
// runtime over the same store and adapter seeds from that row — the load
// reads exactly one envelope (the lineage probe at the seed's version) and
// never re-folds the stream.
func TestSnapshotAdapterSeedsALoad(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	cache := sqlite.NewSnapshotCache(store)
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, counterStateCodec{}, 1).
		WithSnapshotEvery(3)
	sid, _ := es.NewStreamID("counter", "adapter")

	// Six appends under the gap policy (n=3): with no snapshot the seed is
	// 0, so writes land at v3 and v6 — the adapter's row holds the head.
	res := stageSixCommands(t, rt, ctx, sid)
	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok {
		t.Fatalf("adapter cache after six appends: ok=%v err=%v, want a snapshot at the head", ok, err)
	}
	if snap.Version != 6 {
		t.Fatalf("adapter cache version = %d, want 6 (writes land at v3 and v6)", snap.Version)
	}

	// A second runtime over the same log and the same adapter: the load
	// must seed from the persisted row. The counting store bounds the
	// read cost: the lineage probe (the envelope at v6, read as 5..6)
	// plus a tail fold that is empty — one envelope, never the six-row
	// stream.
	cs := &countingStore{Store: store}
	rt2 := aggregate.NewRuntime(cs, counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, counterStateCodec{}, 1)
	st, v, err := rt2.Load(ctx, sid)
	if err != nil {
		t.Fatalf("seeded load over the adapter: %v", err)
	}
	if v != 6 || st.GetCount() != 55 {
		t.Fatalf("seeded load: count=%d v=%d, want the six commands' 55 at v6", st.GetCount(), v)
	}
	if !proto.Equal(st, res.State) {
		t.Fatalf("seeded load state ≠ the last handle's state: got count %d, want %d", st.GetCount(), res.State.GetCount())
	}
	if cs.rows != 1 {
		t.Fatalf("seeded load read %d rows, want exactly the lineage probe's 1 (the tail past v6 is empty)", cs.rows)
	}
}

// TestSnapshotAdapterFoldVersionMismatch pins the fold-version half of §7
// over the real adapter: a row written under fold version 1 is invisible to
// a fold-2 runtime — no seed, a full six-envelope fold — and the fold-2
// runtime's next append rewrites the row under its own fold version (seed 0,
// so the gap rule is due at v7), so one generation's cached state never
// outlives a fold change.
func TestSnapshotAdapterFoldVersionMismatch(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	cache := sqlite.NewSnapshotCache(store)
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, counterStateCodec{}, 1).
		WithSnapshotEvery(3)
	sid, _ := es.NewStreamID("counter", "mismatch")
	stageSixCommands(t, rt, ctx, sid)

	// The row says fold 1; a fold-2 runtime refuses the seed and folds the
	// whole stream from the log — the correct state at full-fold cost.
	cs := &countingStore{Store: store}
	rt2 := aggregate.NewRuntime(cs, counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, counterStateCodec{}, 2).
		WithSnapshotEvery(3)
	st, v, err := rt2.Load(ctx, sid)
	if err != nil {
		t.Fatalf("load over a mismatched fold: %v", err)
	}
	if v != 6 || st.GetCount() != 55 {
		t.Fatalf("mismatched-fold load: count=%d v=%d, want the full fold's 55 at v6", st.GetCount(), v)
	}
	if cs.rows != 6 {
		t.Fatalf("mismatched-fold load read %d rows, want the full six-envelope fold (no seed)", cs.rows)
	}

	// One append on the fold-2 runtime: the seed is 0, so 7−0 ≥ 3 is due
	// and the row is rewritten with version 7 under fold version 2.
	mustHandle(t, rt2, ctx, sid, &counterv1.Increment{By: 10})
	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok {
		t.Fatalf("adapter cache after the fold-2 append: ok=%v err=%v", ok, err)
	}
	if snap.Version != 7 || snap.FoldVersion != 2 {
		t.Fatalf("rewritten snapshot: version=%d foldVersion=%d, want 7/2 (seed 0, 7 ≥ 3 is due)", snap.Version, snap.FoldVersion)
	}
}

// TestWarmSnapshotArchivedStream pins WarmSnapshot's reason to exist (ADR
// 0042 §3.1): the write policy runs only on the append path, so an archived
// stream that never appends again would keep full-folding forever. The
// explicit warm folds it once and writes the head through the real adapter
// despite a threshold the stream never reaches, and every later reader then
// pays one probe envelope instead of the full fold.
func TestWarmSnapshotArchivedStream(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	cache := sqlite.NewSnapshotCache(store)
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, counterStateCodec{}, 1).
		WithSnapshotEvery(100)
	sid, _ := es.NewStreamID("counter", "archived")

	mustHandle(t, rt, ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	for i := 0; i < 4; i++ {
		mustHandle(t, rt, ctx, sid, &counterv1.Increment{By: 10})
	}
	// v5, count 45 — and the policy never wrote: the gap 5−0 < 100.
	if _, ok, err := cache.Load(ctx, sid.Canonical()); ok || err != nil {
		t.Fatalf("cache before warming: ok=%v err=%v, want absence (5 is under the threshold 100)", ok, err)
	}

	got, err := rt.WarmSnapshot(ctx, sid)
	if err != nil {
		t.Fatalf("warm snapshot: %v", err)
	}
	if got != 5 {
		t.Fatalf("WarmSnapshot = %d, want the stream head 5", got)
	}
	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok || snap.Version != 5 {
		t.Fatalf("cache after warming: ok=%v version=%d err=%v, want the head v5 written through the adapter", ok, snap.Version, err)
	}

	// The archived stream is now seeded for every later reader: one
	// envelope (the probe at v5), not a full fold — the seed came from the
	// adapter's row, not the log.
	cs := &countingStore{Store: store}
	rt2 := aggregate.NewRuntime(cs, counter.Decider, counterv1.EventCodec{}).
		WithSnapshots(cache, counterStateCodec{}, 1)
	st, v, err := rt2.Load(ctx, sid)
	if err != nil || v != 5 || st.GetCount() != 45 {
		t.Fatalf("post-warm load: count=%d v=%d err=%v, want the warmed 45 at v5", st.GetCount(), v, err)
	}
	if cs.rows != 1 {
		t.Fatalf("post-warm load read %d rows, want the lineage probe's 1 — the seed came from the adapter", cs.rows)
	}
}
