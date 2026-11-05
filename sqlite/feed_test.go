package sqlite_test

import (
	"context"
	"database/sql"
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
	"github.com/laenenai/es-lite/feedtest"
	"github.com/laenenai/es-lite/projection"
	"github.com/laenenai/es-lite/sqlite"
)

// The SQLite feed adapters' tests. The port contracts — the checkpoint,
// lease, log, dead-letter and end-to-end behaviors both backends must
// honor — now live in the shared conformance suite (feedtest, spec
// §5.6), wired at the bottom of this file; what stays here is
// SQLite-specific:
//
//   - the single-connection gap-free safety guard (spec §5.10.1.4),
//   - the persisted lease-key composition pin (an upgrade must keep
//     reading existing rows),
//   - the caller-supplied half of the deliberate CreatedAt divergence
//     (the suite asserts presence/reasonableness only),
//   - the close/reopen durability the file-backed store proves across a
//     real process-shaped restart.

// tsLayout is the fixed-width timestamp layout the store writes
// (store.tsLayout): 9-digit nanoseconds, always UTC.
const tsLayout = "2006-01-02T15:04:05.000000000Z"

func openFeedStore(t testing.TB) *sqlite.Store {
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
func appendFeedEvent(t testing.TB, s *sqlite.Store, sid es.StreamID, version uint64, typeURL string, payload []byte) es.Envelope {
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

func mustStreamID(t testing.TB, typ, id string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID(typ, id)
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	return sid
}

// openFeedStoreCapped opens a fresh in-memory store with a 64-byte payload
// cap: small enough that a test payload towers over it. It is the fixture
// for the poison-row contract — the store cap the adapters must honor on
// the read side, not just at append.
func openFeedStoreCapped(t testing.TB) *sqlite.Store {
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
func insertFeedRowDirect(t testing.TB, s *sqlite.Store, sid es.StreamID, version uint64, typeURL string, payload []byte) uint64 {
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
		"11111111-1111-7111-8111-111111111111", sid.Type, sid.Canonical(), version,
		typeURL, 1, "2026-01-01T00:00:00.000000000Z", "2026-01-01T00:00:00.000000000Z",
		"", "", "",
		sid.Type, "poison", "conformance:poison", payload,
	).Scan(&gp)
	if err != nil {
		t.Fatalf("direct insert: %v", err)
	}
	return gp
}

// ---------------------------------------------------------------------------
// The shared conformance suite (spec §5.6)
// ---------------------------------------------------------------------------

// newSqliteFeedStore wires SQLite into the shared feed conformance suite
// (feedtest): every port over one in-memory store — one database per test
// name, capped at 64 payload bytes so the poison contract is exercisable —
// plus the seeding hooks. SQLite is single-workspace by design, so the
// scope arguments are carried for parity with multi-tenant backends and
// ignored, and the fixture leaves MultiWorkspace false: the suite's
// cross-workspace section does not apply to a one-database workspace.
func newSqliteFeedStore(ctx context.Context, t testing.TB) (feedtest.Store, func()) {
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared"
	s, err := sqlite.Open(ctx, dsn, sqlite.WithMaxPayloadBytes(64))
	if err != nil {
		t.Fatalf("sqlite: conformance store: open: %v", err)
	}
	return feedtest.Store{
		Logs:            sqlite.NewLogs(s),
		Checkpoints:     sqlite.NewCheckpointStore(s),
		Leases:          sqlite.NewLeaseStore(s),
		DeadLetters:     sqlite.NewDeadLetterStore(s),
		MaxPayloadBytes: 64,
		Append: func(ctx context.Context, t testing.TB, scope string, sid es.StreamID, version uint64, typeURL string, payload []byte) es.Envelope {
			return appendFeedEvent(t, s, sid, version, typeURL, payload)
		},
		PlantOverCap: func(ctx context.Context, t testing.TB, scope string, sid es.StreamID, version uint64, typeURL string, payload []byte) uint64 {
			return insertFeedRowDirect(t, s, sid, version, typeURL, payload)
		},
		ReadDeadLetter: func(ctx context.Context, t testing.TB, scope, projection, instance string, position uint64) (feedtest.DeadLetterRow, bool) {
			var (
				row       feedtest.DeadLetterRow
				createdAt string
			)
			err := s.DB().QueryRowContext(ctx,
				`SELECT type_url, size_bytes, reason, actor, created_at FROM feed_dead_letters
				 WHERE scope = ? AND projection = ? AND instance = ? AND position = ?`,
				scope, projection, instance, position,
			).Scan(&row.TypeURL, &row.SizeBytes, &row.Reason, &row.Actor, &createdAt)
			if errors.Is(err, sql.ErrNoRows) {
				return feedtest.DeadLetterRow{}, false
			}
			if err != nil {
				t.Fatalf("sqlite: conformance store: read dead letter at %d: %v", position, err)
			}
			at, err := time.Parse(tsLayout, createdAt)
			if err != nil {
				t.Fatalf("sqlite: conformance store: dead letter created_at %q does not parse: %v", createdAt, err)
			}
			row.CreatedAt = at
			return row, true
		},
		CountDeadLetters: func(ctx context.Context, t testing.TB, scope string) int {
			var n int
			if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM feed_dead_letters`).Scan(&n); err != nil {
				t.Fatalf("sqlite: conformance store: count dead letters: %v", err)
			}
			return n
		},
	}, func() { _ = s.Close() }
}

// TestSQLiteFeedConformance runs the shared feed port conformance suite
// against the SQLite adapters. Everything the adapter tests used to pin
// in duplicate with Postgres — monotonic Save, rewinding Reset,
// empty-definition preserve, definition round-trip, the lease takeover
// and renew contracts, the bounded page and poison contracts, the
// dead-letter pointer contract, the end-to-end resume and skip runbooks
// — is pinned once, here, by feedtest.RunAll.
func TestSQLiteFeedConformance(t *testing.T) {
	feedtest.RunAll(t, feedtest.Fixture{NewStore: newSqliteFeedStore})
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
// SQLite-specific storage pins
// ---------------------------------------------------------------------------

// TestLeaseStoreKeyCompositionIsPinned pins the persisted storage contract:
// feed_leases.key holds the NUL-joined triple, byte-exactly. Lease rows are
// durable state an upgrade must keep reading — a composition change
// (different separator, different part order) would pass the collision test
// the suite runs yet orphan every existing row, splitting the lease domain
// and admitting duplicate subscribers. This is a regression pin,
// green-on-arrival by design: it pins current storage behavior so a future
// composition change fails loudly here. It reads the stored row directly,
// after insertFeedRowDirect's raw-SQL pattern, because the public API never
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

// TestDeadLetterCreatedAtIsCallerSupplied pins the SQLite half of the
// deliberate CreatedAt divergence (controller ruling): this store honors the
// caller's dl.CreatedAt — the feed core's Clock — writing it byte-exactly in
// the fixed-width layout, and stamps its own now when the caller supplies the
// zero time. The shared suite asserts only presence/reasonableness because
// the Postgres store stamps the database's clock instead; each backend keeps
// its own half exact.
func TestDeadLetterCreatedAtIsCallerSupplied(t *testing.T) {
	ctx := context.Background()
	s := openFeedStore(t)
	dls := sqlite.NewDeadLetterStore(s)
	at := time.Unix(1_700_000_000, 0).UTC()

	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: "ws", Projection: "p", Instance: "live",
		Position: 42, Reason: "caller-stamped", Actor: "oncall", CreatedAt: at,
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	var createdAt string
	if err := s.DB().QueryRowContext(ctx,
		`SELECT created_at FROM feed_dead_letters WHERE position = ?`, 42,
	).Scan(&createdAt); err != nil {
		t.Fatalf("read dead letter: %v", err)
	}
	if want := at.Format(tsLayout); createdAt != want {
		t.Fatalf("created_at = %q, want %q (the caller-supplied time, byte-exact)", createdAt, want)
	}

	// A zero CreatedAt is stamped here, like feed_checkpoints.updated_at.
	before := time.Now()
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: "ws", Projection: "p", Instance: "live",
		Position: 43, Reason: "store-stamped", Actor: "oncall",
	}); err != nil {
		t.Fatalf("record with a zero CreatedAt: %v", err)
	}
	var stamped string
	if err := s.DB().QueryRowContext(ctx,
		`SELECT created_at FROM feed_dead_letters WHERE position = ?`, 43,
	).Scan(&stamped); err != nil {
		t.Fatalf("read dead letter: %v", err)
	}
	parsed, err := time.Parse(tsLayout, stamped)
	if err != nil {
		t.Fatalf("stamped created_at %q does not parse: %v", stamped, err)
	}
	if parsed.Before(before.Add(-time.Second)) || parsed.After(time.Now().Add(time.Second)) {
		t.Fatalf("stamped created_at = %v, want a recent instant", parsed)
	}
}

// ---------------------------------------------------------------------------
// End to end across a real close/reopen: the SQLite durability shape
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

// TestFeedEndToEndResumesFromCheckpoint is the SQLite-specific durability
// shape: the contract behaviors (filtered delivery, fenced acks, resume
// past the checkpoint) are pinned by the shared suite; what only this
// backend can prove is that the checkpoint survives a real close/reopen of
// the file-backed store — the process-shaped restart — rather than
// in-process reuse.
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
