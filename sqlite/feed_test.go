package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/projection"
	"github.com/laenenai/es-lite/sqlite"
)

func openFeedStore(t *testing.T) *sqlite.Store {
	t.Helper()
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared"
	s, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// openFeedStoreAt opens a file-backed store so a test can close and reopen it,
// proving durability rather than in-process reuse.
func openFeedStoreAt(t *testing.T, path string) *sqlite.Store {
	t.Helper()
	s, err := sqlite.Open(context.Background(), "file:"+path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	return s
}

// appendFeedEvent appends one event to sid at the given expected version and
// returns the committed envelope.
func appendFeedEvent(t *testing.T, s *sqlite.Store, sid es.StreamID, version uint64, typeURL string, payload []byte) es.Envelope {
	t.Helper()
	res, err := s.Append(context.Background(), es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events: []es.EventData{{
			TypeURL:       typeURL,
			SchemaVersion: 1,
			Payload:       payload,
		}},
	})
	if err != nil {
		t.Fatalf("append %s at v%d: %v", typeURL, version+1, err)
	}
	return res.Envelopes[0]
}

func mustStreamID(t *testing.T, typ, id string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID(typ, id)
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	return sid
}

// ---------------------------------------------------------------------------
// Log: Head, ReadAllBounded
// ---------------------------------------------------------------------------

func TestHeadEmptyAndAfterAppends(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)

	head, err := s.Head(ctx)
	if err != nil {
		t.Fatalf("head empty: %v", err)
	}
	if head != 0 {
		t.Fatalf("head empty = %d, want 0", head)
	}

	// Interleave two streams so the log head (global position) and the
	// highest per-stream version diverge: a version-based Head regression
	// would return 2 here instead of 3.
	a := mustStreamID(t, "thing", "head-a")
	b := mustStreamID(t, "thing", "head-b")
	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: a,
		Events:   []es.EventData{{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte("one")}},
	}); err != nil {
		t.Fatalf("append a1: %v", err)
	}
	if _, err := s.Append(ctx, es.AppendParams{
		StreamID: b,
		Events:   []es.EventData{{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte("two")}},
	}); err != nil {
		t.Fatalf("append b1: %v", err)
	}
	res, err := s.Append(ctx, es.AppendParams{
		StreamID:        a,
		ExpectedVersion: 1,
		Events:          []es.EventData{{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte("three")}},
	})
	if err != nil {
		t.Fatalf("append a2: %v", err)
	}
	head, err = s.Head(ctx)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if want := res.Envelopes[0].GlobalPosition; head != want || head != 3 {
		t.Fatalf("head = %d, want %d (global position 3, not the stream version 2)", head, want)
	}
}

func TestReadAllBoundedFiltersOrdersAndLimits(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	sid := mustStreamID(t, "thing", "bounded")

	const (
		consumed = "thing.v1.Consumed"
		ignored  = "thing.v1.Ignored"
	)
	var appended []es.Envelope
	for i, typ := range []string{consumed, ignored, consumed, ignored, consumed} {
		appended = append(appended, appendFeedEvent(t, s, sid, uint64(i), typ, []byte("x")))
	}

	got, truncated, err := s.ReadAllBounded(ctx, 0, 10, 0, []string{consumed})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if truncated {
		t.Fatal("unbounded read reported truncated")
	}
	if len(got) != 3 {
		t.Fatalf("got %d events, want 3 (type-filtered)", len(got))
	}
	for i, env := range got {
		want := appended[i*2]
		if env.GlobalPosition != want.GlobalPosition {
			t.Fatalf("event %d position = %d, want %d (global order)", i, env.GlobalPosition, want.GlobalPosition)
		}
	}

	got, _, err = s.ReadAllBounded(ctx, 0, 2, 0, []string{consumed})
	if err != nil {
		t.Fatalf("read limit: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("limit read returned %d events, want 2", len(got))
	}

	got, _, err = s.ReadAllBounded(ctx, appended[2].GlobalPosition, 10, 0, []string{consumed})
	if err != nil {
		t.Fatalf("read from: %v", err)
	}
	if len(got) != 1 || got[0].GlobalPosition != appended[4].GlobalPosition {
		t.Fatalf("read from position = %+v, want only the last consumed event", got)
	}
}

func TestReadAllBoundedEmptyTypes(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	sid := mustStreamID(t, "thing", "empty-types")
	appendFeedEvent(t, s, sid, 0, "thing.v1.Happened", []byte("x"))

	got, truncated, err := s.ReadAllBounded(ctx, 0, 10, 0, nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != nil || truncated {
		t.Fatalf("empty types = (%v, %v), want (nil, false)", got, truncated)
	}
}

func TestReadAllBoundedByteBudget(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	sid := mustStreamID(t, "thing", "budget")
	const typ = "thing.v1.Happened"

	var appended []es.Envelope
	for i := 0; i < 4; i++ {
		appended = append(appended, appendFeedEvent(t, s, sid, uint64(i), typ, []byte("abcd"))) // 4 bytes each
	}

	// Budget 10: 4 + 4 fit, the third row (would be 12) stops the page.
	got, truncated, err := s.ReadAllBounded(ctx, 0, 10, 10, []string{typ})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 || !truncated {
		t.Fatalf("budget page = %d events truncated=%v, want 2/true", len(got), truncated)
	}
	if got[0].GlobalPosition != appended[0].GlobalPosition || got[1].GlobalPosition != appended[1].GlobalPosition {
		t.Fatalf("budget page positions = %d,%d, want %d,%d",
			got[0].GlobalPosition, got[1].GlobalPosition,
			appended[0].GlobalPosition, appended[1].GlobalPosition)
	}

	// Resuming from the last returned position makes progress.
	got, _, err = s.ReadAllBounded(ctx, got[1].GlobalPosition, 10, 10, []string{typ})
	if err != nil {
		t.Fatalf("read resume: %v", err)
	}
	if len(got) != 2 || got[0].GlobalPosition != appended[2].GlobalPosition {
		t.Fatalf("resume page = %+v, want the next two events", got)
	}

	// maxBytes <= 0 disables the budget.
	got, truncated, err = s.ReadAllBounded(ctx, 0, 10, -1, []string{typ})
	if err != nil {
		t.Fatalf("read unbounded: %v", err)
	}
	if len(got) != 4 || truncated {
		t.Fatalf("unbounded page = %d events truncated=%v, want 4/false", len(got), truncated)
	}
}

func TestReadAllBoundedExactBudgetFit(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	sid := mustStreamID(t, "thing", "exact-budget")
	const typ = "thing.v1.Happened"

	appendFeedEvent(t, s, sid, 0, typ, []byte("abcd")) // 4 bytes
	appendFeedEvent(t, s, sid, 1, typ, []byte("efgh")) // 4 bytes

	// The page totals exactly the budget and no rows remain: it must not be
	// reported truncated. A >= comparison would stop after the first row.
	got, truncated, err := s.ReadAllBounded(ctx, 0, 10, 8, []string{typ})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 || truncated {
		t.Fatalf("exact fit = %d events truncated=%v, want 2/false", len(got), truncated)
	}
}

func TestReadAllBoundedDefaultLimit(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	sid := mustStreamID(t, "thing", "default-limit")
	const typ = "thing.v1.Happened"

	events := make([]es.EventData, 101)
	for i := range events {
		events[i] = es.EventData{TypeURL: typ, SchemaVersion: 1, Payload: []byte("x")}
	}
	if _, err := s.Append(ctx, es.AppendParams{StreamID: sid, Events: events}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// limit <= 0 selects the adapter default of 100, not "all".
	got, _, err := s.ReadAllBounded(ctx, 0, 0, 0, []string{typ})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 100 {
		t.Fatalf("default limit = %d events, want 100", len(got))
	}
}

func TestReadAllBoundedFirstOversizedEventIsIncluded(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	sid := mustStreamID(t, "thing", "oversized")
	const typ = "thing.v1.Happened"

	appendFeedEvent(t, s, sid, 0, typ, []byte("12345")) // 5 bytes
	appendFeedEvent(t, s, sid, 1, typ, []byte("67890"))

	got, truncated, err := s.ReadAllBounded(ctx, 0, 10, 3, []string{typ})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d events, want the first oversized event only", len(got))
	}
	if !truncated {
		t.Fatal("oversized first event did not report truncated")
	}
}

// ---------------------------------------------------------------------------
// Concurrency guard: the single-connection invariant
// ---------------------------------------------------------------------------

// TestConcurrentAppendsKeepPositionsGapFree is the SQLite safety guard (spec
// §5.10.1.4): the backend's gap-free, commit-ordered delivery rests entirely
// on Open's SetMaxOpenConns(1) serializing every writer against the single
// connection. Eight goroutines appending single events to distinct streams
// must produce exactly positions 1..400 — contiguous, no gaps — and an
// interleaved ReadAll reader must observe every page as a contiguous run
// starting at its cursor: never gp N+1 before gp N. If the single-connection
// invariant ever loosens, commit order and position order diverge and this
// test catches it.
func TestConcurrentAppendsKeepPositionsGapFree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openFeedStore(t)

	const (
		writers  = 8
		perWrite = 50
		total    = writers * perWrite
	)
	const typ = "thing.v1.Happened"

	// One stream per writer, built on the test goroutine: t.Fatalf must
	// not run on a writer goroutine.
	streams := make([]es.StreamID, writers)
	for g := range streams {
		streams[g] = mustStreamID(t, "thing", fmt.Sprintf("gapfree-%d", g))
	}

	var (
		mu       sync.Mutex
		writePos = make([]uint64, 0, total)
		writeErr error
	)
	var wg sync.WaitGroup
	for g := range writers {
		wg.Add(1)
		go func(sid es.StreamID) {
			defer wg.Done()
			for i := 0; i < perWrite; i++ {
				res, err := s.Append(ctx, es.AppendParams{
					StreamID:        sid,
					ExpectedVersion: uint64(i),
					Events: []es.EventData{{
						TypeURL:       typ,
						SchemaVersion: 1,
						Payload:       []byte("x"),
					}},
				})
				if err != nil {
					mu.Lock()
					if writeErr == nil {
						writeErr = fmt.Errorf("append %s v%d: %w", sid.Canonical(), i+1, err)
					}
					mu.Unlock()
					return
				}
				mu.Lock()
				writePos = append(writePos, res.Envelopes[0].GlobalPosition)
				mu.Unlock()
			}
		}(streams[g])
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	// Interleaved reader (the test goroutine): every observed page must be
	// a contiguous run starting at cursor+1 — observing gp N+1 before gp N
	// is exactly what the single-connection invariant must rule out.
	writersDone := false
	cursor := uint64(0)
	for cursor < total {
		page, err := s.ReadAll(ctx, cursor, 25)
		if err != nil {
			t.Fatalf("read all from %d: %v", cursor, err)
		}
		for i, e := range page {
			if want := cursor + uint64(i) + 1; e.GlobalPosition != want {
				t.Fatalf("reader observed gp %d at offset %d past cursor %d, want %d (gp N+1 before gp N)",
					e.GlobalPosition, i, cursor, want)
			}
		}
		if len(page) > 0 {
			cursor = page[len(page)-1].GlobalPosition
		} else if writersDone {
			break // every append has returned; nothing more can appear
		}
		select {
		case <-done:
			writersDone = true
		case <-time.After(time.Millisecond):
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if writeErr != nil {
		t.Fatalf("concurrent append: %v", writeErr)
	}
	if len(writePos) != total {
		t.Fatalf("%d of %d appends completed", len(writePos), total)
	}
	slices.Sort(writePos)
	for i, pos := range writePos {
		if pos != uint64(i+1) {
			t.Fatalf("appended positions are not exactly 1..%d: got %d at sorted index %d", total, pos, i)
		}
	}
	if cursor != total {
		t.Fatalf("reader reached %d of %d positions", cursor, total)
	}
}

// ---------------------------------------------------------------------------
// CheckpointStore
// ---------------------------------------------------------------------------

func TestCheckpointLoadAbsentSaveAndNoRewind(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	cp := sqlite.NewCheckpointStore(s)

	pos, err := cp.Load(ctx, "ws", "p", "live")
	if err != nil {
		t.Fatalf("load absent: %v", err)
	}
	if pos != 0 {
		t.Fatalf("absent load = %d, want 0", pos)
	}
	if _, found, err := cp.Checkpoint(ctx, "ws", "p", "live"); err != nil || found {
		t.Fatalf("absent checkpoint = found=%v err=%v, want false/nil", found, err)
	}

	if err := cp.Save(ctx, "ws", "p", "live", 5); err != nil {
		t.Fatalf("save: %v", err)
	}
	if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
		t.Fatalf("load = %d, want 5", pos)
	}

	// Save never rewinds.
	if err := cp.Save(ctx, "ws", "p", "live", 3); err != nil {
		t.Fatalf("save lower: %v", err)
	}
	if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
		t.Fatalf("load after lower save = %d, want 5 (monotonic)", pos)
	}

	// A different instance is a different row.
	if err := cp.Save(ctx, "ws", "p", "other", 2); err != nil {
		t.Fatalf("save other: %v", err)
	}
	if pos, _ = cp.Load(ctx, "ws", "p", "other"); pos != 2 {
		t.Fatalf("other instance = %d, want 2", pos)
	}
	if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
		t.Fatalf("live instance = %d, want 5", pos)
	}
}

func TestCheckpointResetDeleteAndTimestamp(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	cp := sqlite.NewCheckpointStore(s)

	before := time.Now().Add(-time.Second)
	if err := cp.Save(ctx, "ws", "p", "live", 7); err != nil {
		t.Fatalf("save: %v", err)
	}
	checkpoint, found, err := cp.Checkpoint(ctx, "ws", "p", "live")
	if err != nil || !found {
		t.Fatalf("checkpoint = found=%v err=%v, want true/nil", found, err)
	}
	if checkpoint.Position != 7 {
		t.Fatalf("checkpoint position = %d, want 7", checkpoint.Position)
	}
	if checkpoint.UpdatedAt.Before(before) || checkpoint.UpdatedAt.After(time.Now().Add(time.Second)) {
		t.Fatalf("checkpoint updated_at = %v, want a recent time", checkpoint.UpdatedAt)
	}

	// Reset overwrites, backwards included.
	if err := cp.Reset(ctx, "ws", "p", "live", 1); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if pos, _ := cp.Load(ctx, "ws", "p", "live"); pos != 1 {
		t.Fatalf("load after reset = %d, want 1", pos)
	}
	if checkpoint, _, _ = cp.Checkpoint(ctx, "ws", "p", "live"); checkpoint.Position != 1 {
		t.Fatalf("checkpoint after reset = %d, want 1", checkpoint.Position)
	}

	// Delete removes exactly that row.
	if err := cp.Save(ctx, "ws", "p", "other", 4); err != nil {
		t.Fatalf("save other: %v", err)
	}
	if err := cp.Delete(ctx, "ws", "p", "live"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, found, err := cp.Checkpoint(ctx, "ws", "p", "live"); err != nil || found {
		t.Fatalf("checkpoint after delete = found=%v err=%v, want false/nil", found, err)
	}
	if pos, _ := cp.Load(ctx, "ws", "p", "other"); pos != 4 {
		t.Fatalf("other instance after delete = %d, want 4", pos)
	}
	// Deleting again is a no-op.
	if err := cp.Delete(ctx, "ws", "p", "live"); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
}

// ---------------------------------------------------------------------------
// LeaseStore
// ---------------------------------------------------------------------------

func TestLeaseExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	leases := sqlite.NewLeaseStore(s)
	key := "lease-one-winner"

	ok, err := leases.Acquire(ctx, key, "owner-a", "consumer-a", time.Minute)
	if err != nil {
		t.Fatalf("acquire owner-a: %v", err)
	}
	if !ok {
		t.Fatal("owner-a lost a free lease")
	}

	ok, err = leases.Acquire(ctx, key, "owner-b", "consumer-b", time.Minute)
	if err != nil {
		t.Fatalf("acquire owner-b: %v", err)
	}
	if ok {
		t.Fatal("owner-b took a lease owner-a still holds; want exactly one winner")
	}

	lease, held, err := leases.Holder(ctx, key)
	if err != nil {
		t.Fatalf("holder: %v", err)
	}
	if !held || lease.Owner != "owner-a" || lease.Consumer != "consumer-a" {
		t.Fatalf("holder = %+v held=%v, want owner-a/consumer-a", lease, held)
	}
	if !lease.ExpiresAt.After(time.Now()) {
		t.Fatalf("holder ExpiresAt %v is not in the future", lease.ExpiresAt)
	}
}

func TestLeaseReacquireBySameOwnerUpdatesConsumer(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	leases := sqlite.NewLeaseStore(s)
	key := "lease-reacquire"

	if ok, err := leases.Acquire(ctx, key, "owner-a", "pod-a", time.Minute); err != nil || !ok {
		t.Fatalf("first acquire = %v err=%v, want true/nil", ok, err)
	}
	// A reconnect by the same owner must succeed and refresh the consumer
	// identity Status reports.
	if ok, err := leases.Acquire(ctx, key, "owner-a", "pod-b", time.Minute); err != nil || !ok {
		t.Fatalf("same-owner acquire = %v err=%v, want true/nil", ok, err)
	}
	lease, held, err := leases.Holder(ctx, key)
	if err != nil || !held {
		t.Fatalf("holder = found=%v err=%v, want held", held, err)
	}
	if lease.Owner != "owner-a" || lease.Consumer != "pod-b" {
		t.Fatalf("holder = %+v, want owner-a/pod-b", lease)
	}
}

func TestLeaseExpiryLetsAnotherOwnerWin(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	leases := sqlite.NewLeaseStore(s)
	key := "lease-expiry"

	if ok, err := leases.Acquire(ctx, key, "owner-a", "consumer-a", 40*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire owner-a = %v err=%v", ok, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		ok, err := leases.Acquire(ctx, key, "owner-b", "consumer-b", time.Minute)
		if err != nil {
			t.Fatalf("acquire owner-b: %v", err)
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owner-b never acquired the lease after it expired")
		}
		time.Sleep(5 * time.Millisecond)
	}

	lease, held, err := leases.Holder(ctx, key)
	if err != nil || !held {
		t.Fatalf("holder = found=%v err=%v, want the new owner", held, err)
	}
	if lease.Owner != "owner-b" {
		t.Fatalf("holder owner = %q, want owner-b", lease.Owner)
	}
}

func TestLeaseRenewByOwnerAndNonOwner(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	leases := sqlite.NewLeaseStore(s)
	key := "lease-renew"

	if ok, err := leases.Acquire(ctx, key, "owner-a", "consumer-a", 40*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire owner-a = %v err=%v", ok, err)
	}
	if ok, err := leases.Renew(ctx, key, "owner-b", time.Minute); err != nil {
		t.Fatalf("renew owner-b: %v", err)
	} else if ok {
		t.Fatal("non-owner renewed the lease")
	}
	if ok, err := leases.Renew(ctx, key, "owner-a", time.Minute); err != nil {
		t.Fatalf("renew owner-a: %v", err)
	} else if !ok {
		t.Fatal("owner failed to renew its own lease")
	}

	// Past the original TTL, the renewed lease is still held.
	time.Sleep(80 * time.Millisecond)
	lease, held, err := leases.Holder(ctx, key)
	if err != nil || !held {
		t.Fatalf("holder after renew = found=%v err=%v, want held", held, err)
	}
	if lease.Owner != "owner-a" {
		t.Fatalf("holder owner = %q, want owner-a", lease.Owner)
	}
}

func TestLeaseReleaseByOwnerAndNonOwner(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	leases := sqlite.NewLeaseStore(s)
	key := "lease-release"

	if ok, err := leases.Acquire(ctx, key, "owner-a", "consumer-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire = %v err=%v", ok, err)
	}

	// A non-owner's release is a no-op, not an error.
	if err := leases.Release(ctx, key, "owner-b"); err != nil {
		t.Fatalf("release owner-b: %v", err)
	}
	if _, held, err := leases.Holder(ctx, key); err != nil || !held {
		t.Fatalf("holder after non-owner release = held=%v err=%v, want still held", held, err)
	}

	if err := leases.Release(ctx, key, "owner-a"); err != nil {
		t.Fatalf("release owner-a: %v", err)
	}
	if _, held, err := leases.Holder(ctx, key); err != nil || held {
		t.Fatalf("holder after owner release = held=%v err=%v, want free", held, err)
	}
}

func TestLeaseHolderOnlyUnexpired(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	leases := sqlite.NewLeaseStore(s)
	key := "lease-holder-expiry"

	if ok, err := leases.Acquire(ctx, key, "owner-a", "consumer-a", 40*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire = %v err=%v", ok, err)
	}
	if _, held, err := leases.Holder(ctx, key); err != nil || !held {
		t.Fatalf("holder before expiry = held=%v err=%v, want held", held, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		_, held, err := leases.Holder(ctx, key)
		if err != nil {
			t.Fatalf("holder: %v", err)
		}
		if !held {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Holder kept reporting an expired lease")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// End to end: Store + feed.Server
// ---------------------------------------------------------------------------

// cancelSink records events and cancels its subscription once want events
// have arrived.
type cancelSink struct {
	mu     sync.Mutex
	events []es.Envelope
	want   int
	cancel context.CancelFunc
}

func (c *cancelSink) Send(_ context.Context, env es.Envelope) error {
	c.mu.Lock()
	c.events = append(c.events, env)
	n := len(c.events)
	c.mu.Unlock()
	if n >= c.want {
		c.cancel()
	}
	return nil
}

func (c *cancelSink) got() []es.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]es.Envelope(nil), c.events...)
}

func TestFeedEndToEndResumesFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "feed.db")
	s := openFeedStoreAt(t, path)
	sid := mustStreamID(t, "counter", "e2e")

	const (
		consumed = "counter.v1.Incremented"
		ignored  = "counter.v1.Decremented"
	)
	res, err := s.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{
			{TypeURL: consumed, SchemaVersion: 1, Payload: []byte("a1")},
			{TypeURL: ignored, SchemaVersion: 1, Payload: []byte("b1")},
			{TypeURL: consumed, SchemaVersion: 1, Payload: []byte("a2")},
		},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	newServer := func(s *sqlite.Store) *feed.Server {
		srv, err := feed.NewServer(feed.Config{
			Logs:        sqlite.NewLogs(s),
			Checkpoints: sqlite.NewCheckpointStore(s),
			Leases:      sqlite.NewLeaseStore(s),
			Poll:        10 * time.Millisecond,
			Batch:       10,
		})
		if err != nil {
			t.Fatalf("new server: %v", err)
		}
		return srv
	}

	req := feed.Request{
		Scope:      "ws",
		Projection: "p",
		Instance:   "live",
		Consumer:   "test",
		Definition: projection.Definition{Name: "p", Consumes: []string{consumed}},
	}

	// First process: collect the two consumed events, acknowledge, then close
	// the store so the resume crosses a real reopen rather than reusing state.
	srv := newServer(s)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	sink1 := &cancelSink{want: 2, cancel: cancel1}
	if err := srv.Subscribe(ctx1, req, sink1); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe 1: %v", err)
	}

	first := sink1.got()
	if len(first) != 2 {
		t.Fatalf("first subscription got %d events, want 2: %+v", len(first), first)
	}
	if first[0].GlobalPosition != res.Envelopes[0].GlobalPosition ||
		first[1].GlobalPosition != res.Envelopes[2].GlobalPosition {
		t.Fatalf("first subscription positions = %d,%d, want %d,%d (in order)",
			first[0].GlobalPosition, first[1].GlobalPosition,
			res.Envelopes[0].GlobalPosition, res.Envelopes[2].GlobalPosition)
	}

	acked := first[len(first)-1].GlobalPosition
	if err := srv.Checkpoint(ctx, req.Scope, req.Projection, req.Instance, acked); err != nil {
		t.Fatalf("checkpoint at %d: %v", acked, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	// Second process: reopen the same file, append one event, and prove the
	// checkpoint survived.
	s2 := openFeedStoreAt(t, path)
	t.Cleanup(func() { _ = s2.Close() })
	next := appendFeedEvent(t, s2, sid, 3, consumed, []byte("a3"))
	srv2 := newServer(s2)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	sink2 := &cancelSink{want: 1, cancel: cancel2}
	if err := srv2.Subscribe(ctx2, req, sink2); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe 2: %v", err)
	}

	second := sink2.got()
	if len(second) != 1 {
		t.Fatalf("resumed subscription got %d events, want 1: %+v", len(second), second)
	}
	if second[0].GlobalPosition != next.GlobalPosition {
		t.Fatalf("resumed event position = %d, want %d (after checkpoint %d)",
			second[0].GlobalPosition, next.GlobalPosition, acked)
	}
}
