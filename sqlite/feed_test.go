package sqlite_test

import (
	"bytes"
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

// openFeedStoreCapped opens a fresh in-memory store with a 64-byte payload
// cap: small enough that a test payload towers over it. It is the fixture
// for the poison-row contract — the store cap the adapters must honor on
// the read side, not just at append.
func openFeedStoreCapped(t *testing.T) *sqlite.Store {
	t.Helper()
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared"
	s, err := sqlite.Open(context.Background(), dsn, sqlite.WithMaxPayloadBytes(64))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// insertFeedRowDirect inserts one events row with raw SQL, bypassing the
// append-time cap — after Task 5 that is the only way a stored payload can
// exceed the store's cap (a lowered cap, direct SQL or corruption). It
// returns the row's global position. The timestamp literal matches the
// fixed-width layout the store writes (tsLayout) so a materialized row
// still parses.
func insertFeedRowDirect(t *testing.T, s *sqlite.Store, sid es.StreamID, version uint64, typeURL string, payload []byte) uint64 {
	t.Helper()
	var gp uint64
	err := s.DB().QueryRowContext(context.Background(),
		`INSERT INTO events (
			event_id, stream_type, stream_id, version,
			type_url, schema_version, occurred_at, recorded_at,
			correlation_id, causation_id, command_id,
			actor_type, actor_id, actor_principal, payload
		) VALUES (?,?,?,?, ?,?,?, ?,?,?, ?,?,?,?, ?)
		RETURNING global_position`,
		"11111111-1111-7111-8111-111111111111", "thing", sid.Canonical(), version,
		typeURL, 1, "2026-01-01T00:00:00.000000000Z", "2026-01-01T00:00:00.000000000Z",
		"", "", "",
		"thing", "poison", "thing:poison", payload,
	).Scan(&gp)
	if err != nil {
		t.Fatalf("direct insert: %v", err)
	}
	return gp
}

// TestOversizedFirstRowIsNotMaterialized pins the read-side poison contract
// (production review focus 3): a stored payload over the store's cap is
// never materialized into a page — the adapter must refuse to scan its
// bytes. The over-cap row enters via direct SQL because the append cap now
// refuses it. When it is the FIRST row of a page, ReadAllBounded returns
// *feed.EventTooLargeError naming position, type URL, size and limit, with
// no envelopes. When it is a LATER row, the page stops before it with
// truncated=true, and the next page makes it the first row and surfaces
// the error.
func TestOversizedFirstRowIsNotMaterialized(t *testing.T) {
	const typ = "thing.v1.Happened"
	oversized := bytes.Repeat([]byte("p"), 4096)

	t.Run("first row over cap is refused, not materialized", func(t *testing.T) {
		ctx := context.Background()
		s := openFeedStoreCapped(t)

		gp := insertFeedRowDirect(t, s, mustStreamID(t, "thing", "poison-first"), 1, typ, oversized)
		// A normal row follows the poison row.
		appendFeedEvent(t, s, mustStreamID(t, "thing", "poison-first-ok"), 0, typ, []byte("ok"))

		got, _, err := s.ReadAllBounded(ctx, 0, 10, 0, []string{typ})
		var tooLarge *feed.EventTooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("ReadAllBounded error = %v (%T), want *feed.EventTooLargeError", err, err)
		}
		if tooLarge.Position != gp || tooLarge.TypeURL != typ ||
			tooLarge.Size != len(oversized) || tooLarge.Limit != 64 {
			t.Fatalf("error fields = (pos %d, type %q, size %d, limit %d), want (pos %d, type %q, size %d, limit 64)",
				tooLarge.Position, tooLarge.TypeURL, tooLarge.Size, tooLarge.Limit, gp, typ, len(oversized))
		}
		if len(got) != 0 {
			t.Fatalf("over-cap first row was materialized: %d envelopes, want none", len(got))
		}
	})

	t.Run("later row over cap stops the page before it", func(t *testing.T) {
		ctx := context.Background()
		s := openFeedStoreCapped(t)

		first := appendFeedEvent(t, s, mustStreamID(t, "thing", "poison-later"), 0, typ, []byte("ok"))
		gp := insertFeedRowDirect(t, s, mustStreamID(t, "thing", "poison-later-b"), 1, typ, oversized)
		after := appendFeedEvent(t, s, mustStreamID(t, "thing", "poison-later-after"), 0, typ, []byte("after"))
		if !(first.GlobalPosition < gp && gp < after.GlobalPosition) {
			t.Fatalf("insert order: first=%d poison=%d after=%d, want the poison row between the normal rows",
				first.GlobalPosition, gp, after.GlobalPosition)
		}

		// The page delivers only the rows before the poison row and stops
		// before it: truncated=true is certain — the stopping row was seen.
		got, truncated, err := s.ReadAllBounded(ctx, 0, 10, 0, []string{typ})
		if err != nil {
			t.Fatalf("ReadAllBounded: %v", err)
		}
		if len(got) != 1 || got[0].GlobalPosition != first.GlobalPosition {
			t.Fatalf("page = %d envelopes at %v, want only the event before the poison row (pos %d)",
				len(got), feedPositions(got), first.GlobalPosition)
		}
		if !truncated {
			t.Fatal("page stopped before an over-cap row without truncated=true")
		}

		// The next page makes the poison row the first row: the typed error,
		// again with no envelopes materialized.
		got, _, err = s.ReadAllBounded(ctx, first.GlobalPosition, 10, 0, []string{typ})
		var tooLarge *feed.EventTooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("next page error = %v (%T), want *feed.EventTooLargeError", err, err)
		}
		if tooLarge.Position != gp || tooLarge.TypeURL != typ ||
			tooLarge.Size != len(oversized) || tooLarge.Limit != 64 {
			t.Fatalf("error fields = (pos %d, type %q, size %d, limit %d), want (pos %d, type %q, size %d, limit 64)",
				tooLarge.Position, tooLarge.TypeURL, tooLarge.Size, tooLarge.Limit, gp, typ, len(oversized))
		}
		if len(got) != 0 {
			t.Fatalf("over-cap first row was materialized: %d envelopes, want none", len(got))
		}
	})
}

// feedPositions collects a page's global positions for failure messages.
func feedPositions(envs []es.Envelope) []uint64 {
	out := make([]uint64, len(envs))
	for i, e := range envs {
		out[i] = e.GlobalPosition
	}
	return out
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

	if err := cp.Save(ctx, "ws", "p", "live", "", 5); err != nil {
		t.Fatalf("save: %v", err)
	}
	if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
		t.Fatalf("load = %d, want 5", pos)
	}

	// Save never rewinds.
	if err := cp.Save(ctx, "ws", "p", "live", "", 3); err != nil {
		t.Fatalf("save lower: %v", err)
	}
	if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
		t.Fatalf("load after lower save = %d, want 5 (monotonic)", pos)
	}

	// A different instance is a different row.
	if err := cp.Save(ctx, "ws", "p", "other", "", 2); err != nil {
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
	if err := cp.Save(ctx, "ws", "p", "live", "", 7); err != nil {
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
	if err := cp.Reset(ctx, "ws", "p", "live", "", 1); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if pos, _ := cp.Load(ctx, "ws", "p", "live"); pos != 1 {
		t.Fatalf("load after reset = %d, want 1", pos)
	}
	if checkpoint, _, _ = cp.Checkpoint(ctx, "ws", "p", "live"); checkpoint.Position != 1 {
		t.Fatalf("checkpoint after reset = %d, want 1", checkpoint.Position)
	}

	// Delete removes exactly that row.
	if err := cp.Save(ctx, "ws", "p", "other", "", 4); err != nil {
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

// The definition fingerprint round-trips through Checkpoint, and an empty
// definition never clears a recorded one: Save and Reset both preserve
// the stored hash when none is supplied — an ack that carries no
// identity, and a position-only advance like Skip, must not re-identify
// a checkpoint — while a non-empty definition becomes the checkpoint's
// new identity (a reset under a changed definition).
func TestCheckpointDefinitionHashRecordedAndPreserved(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	cp := sqlite.NewCheckpointStore(s)

	h1 := projection.Definition{Name: "p", Consumes: []string{"a.v1.A"}}.Hash()
	h2 := projection.Definition{Name: "p", Consumes: []string{"a.v1.A", "b.v1.B"}}.Hash()

	// A save with no definition records no fingerprint.
	if err := cp.Save(ctx, "ws", "p", "live", "", 5); err != nil {
		t.Fatalf("save without definition: %v", err)
	}
	got, found, err := cp.Checkpoint(ctx, "ws", "p", "live")
	if err != nil || !found || got.Position != 5 || got.DefinitionHash != "" {
		t.Fatalf("checkpoint = %+v found=%v err=%v, want position 5 and no hash", got, found, err)
	}

	// A definition-carrying save records the fingerprint.
	if err := cp.Save(ctx, "ws", "p", "live", h1, 6); err != nil {
		t.Fatalf("save with definition: %v", err)
	}
	if got, _, _ := cp.Checkpoint(ctx, "ws", "p", "live"); got.DefinitionHash != h1 || got.Position != 6 {
		t.Fatalf("checkpoint = %+v, want position 6 under hash %q", got, h1)
	}

	// An empty definition preserves the recorded hash on Save ...
	if err := cp.Save(ctx, "ws", "p", "live", "", 7); err != nil {
		t.Fatalf("save without definition: %v", err)
	}
	if got, _, _ := cp.Checkpoint(ctx, "ws", "p", "live"); got.Position != 7 || got.DefinitionHash != h1 {
		t.Fatalf("checkpoint = %+v, want position 7 with the hash preserved", got)
	}

	// ... and on Reset, which may rewind but still must not clear it.
	if err := cp.Reset(ctx, "ws", "p", "live", "", 1); err != nil {
		t.Fatalf("reset without definition: %v", err)
	}
	if got, _, _ := cp.Checkpoint(ctx, "ws", "p", "live"); got.Position != 1 || got.DefinitionHash != h1 {
		t.Fatalf("checkpoint = %+v, want position 1 with the hash preserved", got)
	}

	// A reset under a new definition re-identifies the checkpoint.
	if err := cp.Reset(ctx, "ws", "p", "live", h2, 0); err != nil {
		t.Fatalf("reset with definition: %v", err)
	}
	if got, _, _ := cp.Checkpoint(ctx, "ws", "p", "live"); got.Position != 0 || got.DefinitionHash != h2 {
		t.Fatalf("checkpoint = %+v, want position 0 under the new hash", got)
	}
}

// ---------------------------------------------------------------------------
// DeadLetterStore
// ---------------------------------------------------------------------------

// TestDeadLetterRecordInsertOrReplace pins the store contract: one pointer
// row per skipped position, replaced (not duplicated) on a repeat skip of
// the same key; the row is enriched with the skipped event's type and size
// when the caller — the feed core — cannot know them (it selects metadata
// only, never the payload bytes); a caller-supplied type passes through
// untouched; and a position with no event row at all still records — Skip
// does not require the position to exist.
func TestDeadLetterRecordInsertOrReplace(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	dls := sqlite.NewDeadLetterStore(s)
	const typ = "thing.v1.Happened"

	// A normal event row the enrichment can name: 5 payload bytes.
	env := appendFeedEvent(t, s, mustStreamID(t, "thing", "dl"), 0, typ, []byte("12345"))
	at := time.Unix(1_700_000_000, 0).UTC()
	later := at.Add(2 * time.Hour)
	const tsLayout = "2006-01-02T15:04:05.000000000Z" // the fixed layout the adapter writes

	type row struct {
		typeURL   string
		size      int
		reason    string
		actor     string
		createdAt string
	}
	read := func(position uint64) row {
		t.Helper()
		var r row
		err := s.DB().QueryRowContext(ctx,
			`SELECT type_url, size_bytes, reason, actor, created_at FROM feed_dead_letters
			 WHERE scope = ? AND projection = ? AND instance = ? AND position = ?`,
			"ws", "p", "live", position,
		).Scan(&r.typeURL, &r.size, &r.reason, &r.actor, &r.createdAt)
		if err != nil {
			t.Fatalf("read dead letter at %d: %v", position, err)
		}
		return r
	}
	count := func() int {
		t.Helper()
		var n int
		if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM feed_dead_letters`).Scan(&n); err != nil {
			t.Fatalf("count dead letters: %v", err)
		}
		return n
	}

	// The core delivers a pointer — no type, no size. The store names the
	// event from the log it shares a database with.
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: "ws", Projection: "p", Instance: "live",
		Position: env.GlobalPosition, Reason: "poison: over cap", Actor: "oncall",
		CreatedAt: at,
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if r := read(env.GlobalPosition); r.typeURL != typ || r.size != 5 ||
		r.reason != "poison: over cap" || r.actor != "oncall" ||
		r.createdAt != at.Format(tsLayout) {
		t.Fatalf("recorded row = %+v, want the event named (%q, 5 bytes) with the audit fields", r, typ)
	}

	// A repeat skip of the same position replaces the row: exactly one
	// row, the latest reason, actor and timestamp.
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: "ws", Projection: "p", Instance: "live",
		Position: env.GlobalPosition, Reason: "triaged: cap raised", Actor: "oncall-2",
		CreatedAt: later,
	}); err != nil {
		t.Fatalf("re-record: %v", err)
	}
	if n := count(); n != 1 {
		t.Fatalf("rows after the repeat = %d, want 1 (replaced, never duplicated)", n)
	}
	if r := read(env.GlobalPosition); r.reason != "triaged: cap raised" || r.actor != "oncall-2" ||
		r.createdAt != later.Format(tsLayout) || r.typeURL != typ || r.size != 5 {
		t.Fatalf("replaced row = %+v, want the latest audit fields over the same named event", r)
	}

	// A caller-supplied type and size pass through: the enrichment keeps a
	// complete dead letter as delivered.
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: "ws", Projection: "p", Instance: "live",
		Position: env.GlobalPosition, TypeURL: "custom.v1.Typed", SizeBytes: 99,
		Reason: "named by the caller", Actor: "oncall-3", CreatedAt: later,
	}); err != nil {
		t.Fatalf("record with a type: %v", err)
	}
	if r := read(env.GlobalPosition); r.typeURL != "custom.v1.Typed" || r.size != 99 {
		t.Fatalf("row with a caller-supplied type = %+v, want the caller's type/size kept", r)
	}

	// A position with no event row records the empty type and zero size:
	// the pointer still lands, Skip does not require the position to exist.
	const absent = 1 << 40
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: "ws", Projection: "p", Instance: "live",
		Position: absent, Reason: "skipped past a restored log", Actor: "oncall",
		CreatedAt: at,
	}); err != nil {
		t.Fatalf("record an absent position: %v", err)
	}
	if r := read(absent); r.typeURL != "" || r.size != 0 {
		t.Fatalf("absent-position row = %+v, want the empty type and zero size", r)
	}
	if n := count(); n != 2 {
		t.Fatalf("rows = %d, want 2 (the replaced key plus the absent position)", n)
	}
}

// ---------------------------------------------------------------------------
// LeaseStore
// ---------------------------------------------------------------------------

func TestLeaseExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	leases := sqlite.NewLeaseStore(s)
	const scope, projection, instance = "ws", "p", "live"

	ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", time.Minute)
	if err != nil {
		t.Fatalf("acquire owner-a: %v", err)
	}
	if !ok {
		t.Fatal("owner-a lost a free lease")
	}

	ok, err = leases.Acquire(ctx, scope, projection, instance, "owner-b", "consumer-b", time.Minute)
	if err != nil {
		t.Fatalf("acquire owner-b: %v", err)
	}
	if ok {
		t.Fatal("owner-b took a lease owner-a still holds; want exactly one winner")
	}

	lease, held, err := leases.Holder(ctx, scope, projection, instance)
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
	const scope, projection, instance = "ws", "p", "live"

	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "pod-a", time.Minute); err != nil || !ok {
		t.Fatalf("first acquire = %v err=%v, want true/nil", ok, err)
	}
	// A reconnect by the same owner must succeed and refresh the consumer
	// identity Status reports.
	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "pod-b", time.Minute); err != nil || !ok {
		t.Fatalf("same-owner acquire = %v err=%v, want true/nil", ok, err)
	}
	lease, held, err := leases.Holder(ctx, scope, projection, instance)
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
	const scope, projection, instance = "ws", "p", "live"

	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", 40*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire owner-a = %v err=%v", ok, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-b", "consumer-b", time.Minute)
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

	lease, held, err := leases.Holder(ctx, scope, projection, instance)
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
	const scope, projection, instance = "ws", "p", "live"

	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", 40*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire owner-a = %v err=%v", ok, err)
	}
	if ok, err := leases.Renew(ctx, scope, projection, instance, "owner-b", time.Minute); err != nil {
		t.Fatalf("renew owner-b: %v", err)
	} else if ok {
		t.Fatal("non-owner renewed the lease")
	}
	if ok, err := leases.Renew(ctx, scope, projection, instance, "owner-a", time.Minute); err != nil {
		t.Fatalf("renew owner-a: %v", err)
	} else if !ok {
		t.Fatal("owner failed to renew its own lease")
	}

	// Past the original TTL, the renewed lease is still held.
	time.Sleep(80 * time.Millisecond)
	lease, held, err := leases.Holder(ctx, scope, projection, instance)
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
	const scope, projection, instance = "ws", "p", "live"

	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire = %v err=%v", ok, err)
	}

	// A non-owner's release is a no-op, not an error.
	if err := leases.Release(ctx, scope, projection, instance, "owner-b"); err != nil {
		t.Fatalf("release owner-b: %v", err)
	}
	if _, held, err := leases.Holder(ctx, scope, projection, instance); err != nil || !held {
		t.Fatalf("holder after non-owner release = held=%v err=%v, want still held", held, err)
	}

	if err := leases.Release(ctx, scope, projection, instance, "owner-a"); err != nil {
		t.Fatalf("release owner-a: %v", err)
	}
	if _, held, err := leases.Holder(ctx, scope, projection, instance); err != nil || held {
		t.Fatalf("holder after owner release = held=%v err=%v, want free", held, err)
	}
}

func TestLeaseHolderOnlyUnexpired(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	leases := sqlite.NewLeaseStore(s)
	const scope, projection, instance = "ws", "p", "live"

	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", 40*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire = %v err=%v", ok, err)
	}
	if _, held, err := leases.Holder(ctx, scope, projection, instance); err != nil || !held {
		t.Fatalf("holder before expiry = held=%v err=%v, want held", held, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		_, held, err := leases.Holder(ctx, scope, projection, instance)
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

// The adapter composes its storage key from the (scope, projection,
// instance) triple. Distinct triples must stay distinct rows even when a
// part contains the separator-like "/" — ("ws","a/b","c") and ("ws","a",
// "b/c") are different subscriptions, never one lease. This pins the
// collision-freedom the feed core's validateKey guarantees (no control
// bytes in any part) for every adapter that joins parts into one key.
func TestLeaseDistinctTriplesNeverCollide(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	leases := sqlite.NewLeaseStore(s)

	if ok, err := leases.Acquire(ctx, "ws", "a/b", "c", "owner-a", "consumer-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire = %v err=%v, want true/nil", ok, err)
	}
	// The segment-shifted triple is a different subscription: another owner
	// can hold it alongside the first.
	if ok, err := leases.Acquire(ctx, "ws", "a", "b/c", "owner-b", "consumer-b", time.Minute); err != nil || !ok {
		t.Fatalf("acquire of the shifted triple = %v err=%v, want true (distinct triples never collide)", ok, err)
	}
	// The original triple is still fenced by owner-a.
	if ok, err := leases.Acquire(ctx, "ws", "a/b", "c", "owner-b", "consumer-b", time.Minute); err != nil || ok {
		t.Fatalf("acquire of the held triple = %v err=%v, want false (still held by owner-a)", ok, err)
	}
}

// TestLeaseStoreKeyCompositionIsPinned pins the persisted storage contract:
// feed_leases.key holds the NUL-joined triple, byte-exactly. Lease rows are
// durable state an upgrade must keep reading — a composition change
// (different separator, different part order) would pass the collision test
// above yet orphan every existing row, splitting the lease domain and
// admitting duplicate subscribers. This is a regression pin, green-on-arrival
// by design: it pins current storage behavior so a future composition change
// fails loudly here. It reads the stored row directly, after
// insertFeedRowDirect's raw-SQL pattern, because the public API never
// exposes the composed key.
func TestLeaseStoreKeyCompositionIsPinned(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	leases := sqlite.NewLeaseStore(s)

	// "/" inside a part is the byte most likely to betray a composition
	// change (a naive separator swap); the surrounding parts make every
	// byte position assertable.
	if ok, err := leases.Acquire(ctx, "ws", "a/b", "c", "owner-a", "consumer-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire = %v err=%v, want true/nil", ok, err)
	}

	var key string
	if err := s.DB().QueryRowContext(ctx,
		`SELECT key FROM feed_leases WHERE owner = ?`, "owner-a",
	).Scan(&key); err != nil {
		t.Fatalf("read stored lease row: %v", err)
	}
	if want := "ws\x00a/b\x00c"; key != want {
		t.Fatalf("stored feed_leases.key = %q, want %q (the NUL-joined triple, byte-exact)", key, want)
	}
}

// ---------------------------------------------------------------------------
// End to end: Store + feed.Server
// ---------------------------------------------------------------------------

// cancelSink records events, acks the final delivered position through ack
// (while the stream still holds its lease — acknowledgements are fenced on
// it), and cancels its subscription once want events have arrived.
type cancelSink struct {
	mu     sync.Mutex
	events []es.Envelope
	want   int
	cancel context.CancelFunc
	ack    func(ctx context.Context, position uint64) error
}

func (c *cancelSink) Send(ctx context.Context, env es.Envelope) error {
	c.mu.Lock()
	c.events = append(c.events, env)
	n := len(c.events)
	c.mu.Unlock()
	if n >= c.want {
		if c.ack != nil {
			if err := c.ack(ctx, env.GlobalPosition); err != nil {
				return fmt.Errorf("ack at %d: %w", env.GlobalPosition, err)
			}
		}
		c.cancel()
	}
	return nil
}

func (c *cancelSink) got() []es.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]es.Envelope(nil), c.events...)
}

// ackingSink records events and acknowledges every delivered position
// through ack while the stream still holds its lease — acknowledgements
// are fenced on the lease, so this is the only moment an ack can land.
type ackingSink struct {
	mu     sync.Mutex
	events []es.Envelope
	ack    func(ctx context.Context, position uint64) error
}

func (a *ackingSink) Send(ctx context.Context, env es.Envelope) error {
	a.mu.Lock()
	a.events = append(a.events, env)
	a.mu.Unlock()
	if a.ack != nil {
		if err := a.ack(ctx, env.GlobalPosition); err != nil {
			return fmt.Errorf("ack at %d: %w", env.GlobalPosition, err)
		}
	}
	return nil
}

func (a *ackingSink) got() []es.Envelope {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]es.Envelope(nil), a.events...)
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
		// Caller-supplied owner: the fenced ack below must name it, and a
		// minted owner (empty Request.Owner) is opaque to the consumer.
		Owner:      "e2e-owner",
		Definition: projection.Definition{Name: "p", Consumes: []string{consumed}},
	}

	// First process: collect the two consumed events, acknowledge, then close
	// the store so the resume crosses a real reopen rather than reusing state.
	// The ack happens inside Send, while the stream still holds its lease —
	// Checkpoint rejects anything else.
	srv := newServer(s)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	sink1 := &cancelSink{want: 2, cancel: cancel1, ack: func(ctx context.Context, pos uint64) error {
		return srv.Checkpoint(ctx, feed.Ack{
			Scope: req.Scope, Projection: req.Projection, Instance: req.Instance,
			Owner: req.Owner, Position: pos,
			// The ack records the subscription's fingerprint, so the
			// resumed subscription below crosses the reopen with a
			// recorded identity that must still match the definition.
			Definition: req.Definition.Hash(),
		})
	}}
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

	// The sink acked the last delivered position while the stream held its
	// lease; the cursor is already durable here.
	acked := first[len(first)-1].GlobalPosition
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

// TestSkipResumesPastPoison walks the whole poison runbook end to end on a
// real store: the over-cap row (inserted past the append cap by raw SQL,
// the only way such a row can exist) kills a live subscription with
// *feed.EventTooLargeError; the operator deliberately skips past it; the
// dead letter lands in feed_dead_letters as a complete pointer — the
// store names the event's type and size from the log without ever scanning
// its payload — and the resubscription delivers exactly the events after
// the poison position, never the poison itself. The skipped subscription's
// definition fingerprint survives the skip, so the resubscription serves
// under the identity the pre-poison ack recorded.
func TestSkipResumesPastPoison(t *testing.T) {
	ctx := context.Background()
	s := openFeedStoreCapped(t) // store cap 64: a 4096-byte row is poison
	const typ = "thing.v1.Happened"

	// Global order: before < poison < after. The poison row enters via
	// raw SQL because the append cap (Task 5) refuses it.
	before := appendFeedEvent(t, s, mustStreamID(t, "thing", "poison-e2e-a"), 0, typ, []byte("ok"))
	poisonGP := insertFeedRowDirect(t, s, mustStreamID(t, "thing", "poison-e2e"), 1, typ, bytes.Repeat([]byte("p"), 4096))
	after1 := appendFeedEvent(t, s, mustStreamID(t, "thing", "poison-e2e-b"), 0, typ, []byte("after-1"))
	after2 := appendFeedEvent(t, s, mustStreamID(t, "thing", "poison-e2e-b"), 1, typ, []byte("after-2"))
	if !(before.GlobalPosition < poisonGP && poisonGP < after1.GlobalPosition && after1.GlobalPosition < after2.GlobalPosition) {
		t.Fatalf("insert order: before=%d poison=%d after=%d,%d, want the poison between the normal rows",
			before.GlobalPosition, poisonGP, after1.GlobalPosition, after2.GlobalPosition)
	}

	srv, err := feed.NewServer(feed.Config{
		Logs:        sqlite.NewLogs(s),
		Checkpoints: sqlite.NewCheckpointStore(s),
		Leases:      sqlite.NewLeaseStore(s),
		DeadLetters: sqlite.NewDeadLetterStore(s),
		Poll:        10 * time.Millisecond,
		Batch:       10,
		LeaseTTL:    time.Minute,
		RenewEvery:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	req := feed.Request{
		Scope:      "ws",
		Projection: "p",
		Instance:   "live",
		Consumer:   "test",
		// Caller-supplied owner: the fenced acks below must name it.
		Owner:      "skip-e2e-owner",
		Definition: projection.Definition{Name: "p", Consumes: []string{typ}},
	}

	// The live subscription delivers the event before the poison, acks it
	// under the definition, then dies on the poison row — never
	// materialized (Task 6), never delivered.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	sink := &ackingSink{ack: func(ctx context.Context, pos uint64) error {
		return srv.Checkpoint(ctx, feed.Ack{
			Scope: req.Scope, Projection: req.Projection, Instance: req.Instance,
			Owner: req.Owner, Position: pos, Definition: req.Definition.Hash(),
		})
	}}
	err = srv.Subscribe(ctx1, req, sink)
	var tooLarge *feed.EventTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("subscribe error = %v (%T), want *feed.EventTooLargeError", err, err)
	}
	if tooLarge.Position != poisonGP || tooLarge.TypeURL != typ ||
		tooLarge.Size != 4096 || tooLarge.Limit != 64 {
		t.Fatalf("poison error = %+v, want position %d of type %q, 4096 bytes over the 64-byte cap",
			tooLarge, poisonGP, typ)
	}
	if got := sink.got(); len(got) != 1 || got[0].GlobalPosition != before.GlobalPosition {
		t.Fatalf("delivered before the poison = %v, want only the event at %d (the poison never delivered)",
			feedPositions(got), before.GlobalPosition)
	}

	// The operator skips past the poison. No force: the poison error killed
	// the stream and released its lease, so the fence is free.
	skipAt := time.Now()
	reason := "poison: 4096-byte payload over the 64-byte store cap"
	if err := srv.Skip(ctx, feed.SkipRequest{
		Scope: req.Scope, Projection: req.Projection, Instance: req.Instance,
		Position: poisonGP, Actor: "oncall@example.com", Reason: reason,
	}); err != nil {
		t.Fatalf("Skip: %v", err)
	}

	// The dead letter landed in the real table — a complete pointer: the
	// store named the event's type and size from the log (metadata only),
	// and the row carries the operator's reason and actor.
	var (
		typeURL   string
		size      int
		dlReason  string
		dlActor   string
		createdAt string
	)
	if err := s.DB().QueryRowContext(ctx,
		`SELECT type_url, size_bytes, reason, actor, created_at FROM feed_dead_letters
		 WHERE scope = ? AND projection = ? AND instance = ? AND position = ?`,
		req.Scope, req.Projection, req.Instance, poisonGP,
	).Scan(&typeURL, &size, &dlReason, &dlActor, &createdAt); err != nil {
		t.Fatalf("read the landed dead letter: %v", err)
	}
	if typeURL != typ || size != 4096 || dlReason != reason || dlActor != "oncall@example.com" {
		t.Fatalf("dead letter row = (%q, %d, %q, %q), want the poison named with the audit fields",
			typeURL, size, dlReason, dlActor)
	}
	at, err := time.Parse("2006-01-02T15:04:05.000000000Z", createdAt)
	if err != nil {
		t.Fatalf("dead letter created_at %q does not parse: %v", createdAt, err)
	}
	if at.Before(skipAt.Add(-time.Second)) || at.After(time.Now().Add(time.Second)) {
		t.Fatalf("dead letter created_at = %v, want around the skip at %v", at, skipAt)
	}

	// The checkpoint advanced to the poison position with the ack's
	// definition fingerprint preserved — a skip is a position advance,
	// never a redefinition.
	cp, found, err := sqlite.NewCheckpointStore(s).Checkpoint(ctx, req.Scope, req.Projection, req.Instance)
	if err != nil || !found {
		t.Fatalf("checkpoint after the skip = found %v, err %v", found, err)
	}
	if cp.Position != poisonGP {
		t.Fatalf("checkpoint = %d after the skip, want the poison position %d", cp.Position, poisonGP)
	}
	if cp.DefinitionHash != req.Definition.Hash() {
		t.Fatalf("definition hash = %q after the skip, want the acked %q preserved", cp.DefinitionHash, req.Definition.Hash())
	}

	// The resubscription delivers exactly the events after the poison —
	// never the poison itself.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	sink2 := &cancelSink{want: 2, cancel: cancel2}
	if err := srv.Subscribe(ctx2, req, sink2); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("resubscribe: %v", err)
	}
	got := sink2.got()
	want := []uint64{after1.GlobalPosition, after2.GlobalPosition}
	if !slices.Equal(feedPositions(got), want) {
		t.Fatalf("resumed positions = %v, want %v (the events after the poison, never the poison itself)",
			feedPositions(got), want)
	}
	if slices.Contains(feedPositions(got), poisonGP) {
		t.Fatalf("the poison event at %d was delivered after the skip", poisonGP)
	}
}
