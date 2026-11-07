// Package feedtest is the shared conformance suite for the feed ports
// (spec §5.6): the portable statement of what every feed adapter owes
// the core, regardless of backend. Both adapters in this repository run
// it — sqlite and postgres — and the platform's future adapters must
// pass the same suite: a backend that wires its tests to this package
// inherits the full contract pin below, and the wiring is deliberately
// small (one constructor plus a capability flag).
//
// The suite pins:
//
//   - CheckpointStore: absent loads read 0, monotonic Save (a lower
//     position never rewinds a higher one), rewinding Reset, the
//     empty-definition preserve on both Save and Reset, the
//     definition-hash round-trip through Checkpoint, and Delete of
//     exactly one row.
//   - LeaseStore: exactly one winner per key, same-owner re-acquire
//     with the consumer refreshed, renew only by the owner, an
//     owner-guarded and idempotent Release, a holder only while
//     unexpired, takeover of an expired lease, and distinct
//     (scope, projection, instance) triples that never collide.
//   - Log: an empty type set returns no events, server-side type
//     filtering in global order, page limits with the 100 default,
//     payload-byte budgets (stop before the row that would overflow,
//     no truncated flag at an exact budget stop, the first row always
//     included even alone over budget), and the poison contract: a
//     first row over the store's cap surfaces *feed.EventTooLargeError
//     naming position, type URL, size and limit — never the payload —
//     while a later over-cap row stops the page before it with
//     truncated=true. The head views: RawHead never trails Head and
//     both equal the committed max once appends settle; MatchingHead
//     and MatchingRawHead are the filtered max over the requested
//     types, 0 for an empty type set, and the raw matching view never
//     trails the settled one.
//   - DeadLetterStore: one pointer row per skipped position, replaced
//     never duplicated, enriched from the log the store shares a
//     database with (metadata only: an over-cap poison row is named
//     without its payload ever being scanned), caller-supplied types
//     passing through, and absent positions recording the empty
//     pointer.
//   - End to end over the real feed core: a durable-checkpoint resume
//     and the skip-past-poison runbook.
//   - When Fixture.MultiWorkspace is set: cross-workspace isolation
//     through the adapters — checkpoints, leases and the dead-letter
//     enrichment are each pinned to one scope.
//
// Deliberate cross-backend divergences are normalized, never hidden. A
// backend may stamp recorded timestamps with its own database clock
// (Postgres clock_timestamp()) or the caller's (SQLite); the suite
// asserts such timestamps are present and plausible — within a generous
// window of the test's clock — while each backend's local tests pin its
// exact clock source.
package feedtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/projection"
)

// clockSlack is the cross-clock tolerance for timestamp and lease-expiry
// assertions: a backend may compute them from the database's own clock
// (Postgres clock_timestamp()) or the caller's (SQLite), so windows are
// bracketed around the test's clock with enough slack to absorb
// realistic skew between the two. The backend-local tests pin the exact
// clock source; this slack is only the suite's normalization.
const clockSlack = time.Minute

// The event types the suite seeds. They carry no schema — the feed hands
// payloads through opaquely (ADR 0025) — they only need to be distinct.
const (
	typConsumed = "conformance.v1.Consumed"
	typIgnored  = "conformance.v1.Ignored"
	typHappened = "conformance.v1.Happened"
	typPoison   = "conformance.v1.Poison"
)

// Store couples the four feed ports over one backing store with the
// seeding and inspection hooks the suite drives them with. The Log and
// DeadLetter sections must seed events and plant the one kind of row the
// append path refuses — an over-cap poison payload; the DeadLetter
// sections read the audit rows back because the port deliberately has no
// list API: triage reads the table directly over SQL.
//
// This is the stable wiring surface for future adapters: every field is
// required, and each section's tests fail with a named missing field
// rather than a nil dereference, so a new backend can assemble the
// fixture incrementally.
type Store struct {
	// Logs opens the per-scope Log under test. The suite reads through
	// it — never through the seeding hooks — so what is under test is
	// exactly the Log the feed core would open.
	Logs feed.Logs
	// Checkpoints is the CheckpointStore under test.
	Checkpoints feed.CheckpointStore
	// Leases is the LeaseStore under test.
	Leases feed.LeaseStore
	// DeadLetters is the DeadLetterStore under test.
	DeadLetters feed.DeadLetterStore

	// Append seeds one event to sid under scope at the given expected
	// stream version, through the backend's own append path, and returns
	// the committed envelope. Multi-workspace backends put the event in
	// scope's workspace; single-workspace backends ignore the scope (one
	// database is one workspace).
	Append func(ctx context.Context, t testing.TB, scope string, sid es.StreamID, version uint64, typeURL string, payload []byte) es.Envelope

	// PlantOverCap inserts one events row whose payload exceeds the
	// store's cap, bypassing the append-time cap — once the cap exists,
	// raw SQL is the only way such a row can exist — and returns its
	// global position.
	PlantOverCap func(ctx context.Context, t testing.TB, scope string, sid es.StreamID, version uint64, typeURL string, payload []byte) uint64

	// MaxPayloadBytes is the store's payload cap. It must be positive:
	// the poison contract (EventTooLargeError) is part of the port, and
	// the suite asserts the error's Limit equals this value.
	MaxPayloadBytes int

	// ReadDeadLetter reads one recorded dead-letter row back, or
	// found=false when no row exists for the key.
	ReadDeadLetter func(ctx context.Context, t testing.TB, scope, projection, instance string, position uint64) (DeadLetterRow, bool)

	// CountDeadLetters counts the recorded dead-letter rows under
	// scope. Single-workspace backends may ignore the scope as long as
	// the count covers only the fixture's own rows.
	CountDeadLetters func(ctx context.Context, t testing.TB, scope string) int
}

// DeadLetterRow is the audit row as the suite reads it back: the fields
// triage reads directly over SQL. CreatedAt is whatever the backend
// stamped — its own database clock or the caller-supplied time.
type DeadLetterRow struct {
	TypeURL   string
	SizeBytes int
	Reason    string
	Actor     string
	CreatedAt time.Time
}

// NewStore is the fixture constructor shape every Run* function takes:
// it opens a fresh backend — every port over one store, with seeding —
// and returns a cleanup that closes it. The suite calls it once per
// test, on that test's TB, and owns calling the cleanup.
type NewStore func(ctx context.Context, t testing.TB) (Store, func())

// NewCheckpointStore opens just the CheckpointStore under test, with a
// cleanup. The checkpoint suite needs no seeding: the port is
// self-contained.
type NewCheckpointStore func(ctx context.Context, t testing.TB) (feed.CheckpointStore, func())

// NewLeaseStore opens just the LeaseStore under test, with a cleanup.
// The lease suite needs no seeding either.
type NewLeaseStore func(ctx context.Context, t testing.TB) (feed.LeaseStore, func())

// Fixture carries a backend's constructor and capabilities into RunAll.
type Fixture struct {
	// NewStore opens the backend under the full suite. Required.
	NewStore NewStore
	// MultiWorkspace reports whether the backend isolates scopes from
	// one another (Postgres row-level security). It gates the
	// cross-workspace section; single-workspace backends — SQLite: one
	// database is one workspace, by design — leave it false and skip
	// it.
	MultiWorkspace bool
}

// validate fails with a named missing field so a new backend can wire
// the fixture incrementally instead of debugging a nil dereference.
func (st Store) validate(t testing.TB) {
	t.Helper()
	switch {
	case st.Logs == nil:
		t.Fatal("feedtest: Store.Logs is required")
	case st.Checkpoints == nil:
		t.Fatal("feedtest: Store.Checkpoints is required")
	case st.Leases == nil:
		t.Fatal("feedtest: Store.Leases is required")
	case st.DeadLetters == nil:
		t.Fatal("feedtest: Store.DeadLetters is required")
	case st.Append == nil:
		t.Fatal("feedtest: Store.Append is required")
	case st.PlantOverCap == nil:
		t.Fatal("feedtest: Store.PlantOverCap is required")
	case st.ReadDeadLetter == nil:
		t.Fatal("feedtest: Store.ReadDeadLetter is required")
	case st.CountDeadLetters == nil:
		t.Fatal("feedtest: Store.CountDeadLetters is required")
	case st.MaxPayloadBytes <= 0:
		t.Fatal("feedtest: Store.MaxPayloadBytes must be positive — the poison contract is part of the port")
	}
}

// RunAll runs the full conformance suite over the fixture: the Log,
// Checkpoint, Lease and DeadLetter sections, the end-to-end runs over
// the real feed core, and — when the backend reports MultiWorkspace —
// the cross-workspace isolation section.
func RunAll(t *testing.T, f Fixture) {
	t.Helper()
	if f.NewStore == nil {
		t.Fatal("feedtest: Fixture.NewStore is required")
	}
	RunLog(t, f.NewStore)
	RunCheckpoints(t, func(ctx context.Context, tb testing.TB) (feed.CheckpointStore, func()) {
		st, cleanup := f.NewStore(ctx, tb)
		return st.Checkpoints, cleanup
	})
	RunLeases(t, func(ctx context.Context, tb testing.TB) (feed.LeaseStore, func()) {
		st, cleanup := f.NewStore(ctx, tb)
		return st.Leases, cleanup
	})
	RunDeadLetters(t, f.NewStore)
	runEndToEnd(t, f.NewStore)
	if f.MultiWorkspace {
		runCrossWorkspace(t, f.NewStore)
	}
}

// ---------------------------------------------------------------------------
// Small helpers shared by the sections.
// ---------------------------------------------------------------------------

// testStream returns a "conformance"-typed stream id for name.
func testStream(t testing.TB, name string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID("conformance", name)
	if err != nil {
		t.Fatalf("feedtest: stream id: %v", err)
	}
	return sid
}

// positions collects a page's global positions for assertions and
// failure messages.
func positions(envs []es.Envelope) []uint64 {
	out := make([]uint64, len(envs))
	for i, e := range envs {
		out[i] = e.GlobalPosition
	}
	return out
}

// assertRecent fails unless at is a plausible recent instant: within
// clockSlack of the window [before, after] the caller bracketed. The
// normalization behind the controller ruling: the backend may stamp the
// database's clock or the caller's, so the suite pins that something
// recent landed, never which clock won.
func assertRecent(t testing.TB, what string, at, before, after time.Time) {
	t.Helper()
	if at.IsZero() {
		t.Fatalf("%s = the zero time, want a stamped instant", what)
	}
	lo, hi := before.Add(-clockSlack), after.Add(clockSlack)
	if at.Before(lo) || at.After(hi) {
		t.Fatalf("%s = %v, want a recent instant between %v and %v", what, at, lo, hi)
	}
}

// cancelSink records events, acks the final delivered position through
// ack (while the stream still holds its lease — acknowledgements are
// fenced on it), and cancels its subscription once want events have
// arrived.
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
				return fmt.Errorf("feedtest: ack at %d: %w", env.GlobalPosition, err)
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
			return fmt.Errorf("feedtest: ack at %d: %w", env.GlobalPosition, err)
		}
	}
	return nil
}

func (a *ackingSink) got() []es.Envelope {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]es.Envelope(nil), a.events...)
}

// newServer wires the real feed core over the bundle's ports with the
// same configuration shape the backend-local end-to-end tests used
// before the suite absorbed them.
func newServer(t *testing.T, st Store) *feed.Server {
	t.Helper()
	srv, err := feed.NewServer(feed.Config{
		Logs:        st.Logs,
		Checkpoints: st.Checkpoints,
		Leases:      st.Leases,
		DeadLetters: st.DeadLetters,
		Poll:        10 * time.Millisecond,
		Batch:       10,
		LeaseTTL:    time.Minute,
		RenewEvery:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("feedtest: new server: %v", err)
	}
	return srv
}

// ---------------------------------------------------------------------------
// Log: Head, ReadAllBounded, the byte budget, the poison contract.
// ---------------------------------------------------------------------------

// logScope is the workspace scope the Log section seeds and reads
// under. It carries no meaning of its own: multi-workspace backends pin
// every event to it, single-workspace backends ignore it.
const logScope = "ws_feedtest_log"

// RunLog runs the Log conformance suite over the fixture: Head, the
// other head views and ReadAllBounded through Store.Logs — exactly the
// Log the feed core would open — including the payload-byte budget and
// the over-cap poison contract.
func RunLog(t *testing.T, newLog NewStore) {
	t.Helper()
	ctx := context.Background()

	// open seeds-and-reads one log per test: the store, its cleanup, and
	// the Log opened through Logs for logScope.
	open := func(t *testing.T) (feed.Log, Store) {
		t.Helper()
		st, cleanup := newLog(ctx, t)
		st.validate(t)
		t.Cleanup(cleanup)
		lg, err := st.Logs.Open(ctx, logScope)
		if err != nil {
			t.Fatalf("feedtest: open log: %v", err)
		}
		return lg, st
	}

	t.Run("log", func(t *testing.T) {
		t.Run("head is the global position, not a stream version", func(t *testing.T) {
			lg, st := open(t)

			head, err := lg.Head(ctx)
			if err != nil {
				t.Fatalf("feedtest: head empty: %v", err)
			}
			if head != 0 {
				t.Fatalf("feedtest: head on an empty log = %d, want 0", head)
			}

			// Interleave two streams so the log head (the global
			// position) and the highest per-stream version diverge: a
			// version-based Head regression would return 2 here instead
			// of the third insert's global position.
			a := testStream(t, "head-a")
			b := testStream(t, "head-b")
			st.Append(ctx, t, logScope, a, 0, typHappened, []byte("one"))
			st.Append(ctx, t, logScope, b, 0, typHappened, []byte("two"))
			res := st.Append(ctx, t, logScope, a, 1, typHappened, []byte("three"))

			head, err = lg.Head(ctx)
			if err != nil {
				t.Fatalf("feedtest: head: %v", err)
			}
			if head != res.GlobalPosition {
				t.Fatalf("feedtest: head = %d, want the global position %d", head, res.GlobalPosition)
			}
			if head == res.Version {
				t.Fatalf("feedtest: head = %d, the stream version — a version-based Head regression", head)
			}
		})

		// The all-type head views: after appends have settled, Head —
		// which a fence-bounded backend may hold below the raw max
		// while a transaction is in flight — must report the full
		// committed maximum, and RawHead, which never waits, can never
		// be below it.
		t.Run("raw head never trails the settled head; both equal the max once appends settle", func(t *testing.T) {
			lg, st := open(t)

			// Interleave two streams so the log head (the global
			// position) and the highest per-stream version diverge,
			// like the Head test above.
			a := testStream(t, "heads-raw-a")
			b := testStream(t, "heads-raw-b")
			st.Append(ctx, t, logScope, a, 0, typHappened, []byte("one"))
			st.Append(ctx, t, logScope, b, 0, typHappened, []byte("two"))
			last := st.Append(ctx, t, logScope, a, 1, typHappened, []byte("three"))

			head, err := lg.Head(ctx)
			if err != nil {
				t.Fatalf("feedtest: head: %v", err)
			}
			raw, err := lg.RawHead(ctx)
			if err != nil {
				t.Fatalf("feedtest: raw head: %v", err)
			}
			if raw < head {
				t.Fatalf("feedtest: RawHead = %d below Head = %d, want raw >= settled (raw never waits)", raw, head)
			}
			if head != last.GlobalPosition {
				t.Fatalf("feedtest: Head = %d, want the settled max %d (every append has settled)", head, last.GlobalPosition)
			}
			if raw != last.GlobalPosition {
				t.Fatalf("feedtest: RawHead = %d, want the committed max %d", raw, last.GlobalPosition)
			}
		})

		// The matching head views: both filter to the requested types —
		// the subscription's own head, never the all-type maximum — the
		// raw one never trails the settled one, and an empty type set
		// (or one the log never saw) matches nothing.
		t.Run("matching heads are the filtered max; the raw view never trails the settled one", func(t *testing.T) {
			lg, st := open(t)
			sid := testStream(t, "heads-matching")

			var appended []es.Envelope
			for i, typ := range []string{typConsumed, typIgnored, typConsumed, typIgnored} {
				appended = append(appended, st.Append(ctx, t, logScope, sid, uint64(i), typ, []byte("x")))
			}
			lastConsumed := appended[2].GlobalPosition // the last typConsumed event
			lastIgnored := appended[3].GlobalPosition  // the last typIgnored event
			if lastConsumed >= lastIgnored {
				t.Fatalf("feedtest: insert order: the last consumed event %d must sit below the last ignored %d",
					lastConsumed, lastIgnored)
			}

			// Both matching views report each type set's own maximum,
			// never the all-type maximum.
			mh, err := lg.MatchingHead(ctx, []string{typConsumed})
			if err != nil {
				t.Fatalf("feedtest: matching head: %v", err)
			}
			if mh != lastConsumed {
				t.Fatalf("feedtest: MatchingHead(%s) = %d, want %d", typConsumed, mh, lastConsumed)
			}
			mrh, err := lg.MatchingRawHead(ctx, []string{typConsumed})
			if err != nil {
				t.Fatalf("feedtest: matching raw head: %v", err)
			}
			if mrh != lastConsumed {
				t.Fatalf("feedtest: MatchingRawHead(%s) = %d, want %d", typConsumed, mrh, lastConsumed)
			}
			if h, err := lg.MatchingHead(ctx, []string{typIgnored}); err != nil {
				t.Fatalf("feedtest: matching head: %v", err)
			} else if h != lastIgnored {
				t.Fatalf("feedtest: MatchingHead(%s) = %d, want %d", typIgnored, h, lastIgnored)
			}
			if h, err := lg.MatchingRawHead(ctx, []string{typIgnored}); err != nil {
				t.Fatalf("feedtest: matching raw head: %v", err)
			} else if h != lastIgnored {
				t.Fatalf("feedtest: MatchingRawHead(%s) = %d, want %d", typIgnored, h, lastIgnored)
			}

			// The raw matching view never trails the settled one: it
			// reads the same filter with no settle bound, so on a
			// fence-bounded backend it can only match or exceed it.
			if mrh < mh {
				t.Fatalf("feedtest: MatchingRawHead = %d below MatchingHead = %d, want raw >= settled", mrh, mh)
			}

			// An empty type set matches nothing — and so does a type
			// the log never saw — on both matching views.
			for _, types := range [][]string{
				nil,
				{},
				{"conformance.v1.Absent"}, // seeded nothing of this type
			} {
				if h, err := lg.MatchingHead(ctx, types); err != nil || h != 0 {
					t.Fatalf("feedtest: MatchingHead(%v) = %d err=%v, want 0/nil", types, h, err)
				}
				if h, err := lg.MatchingRawHead(ctx, types); err != nil || h != 0 {
					t.Fatalf("feedtest: MatchingRawHead(%v) = %d err=%v, want 0/nil", types, h, err)
				}
			}
		})

		t.Run("type filter, global order, page limit, resume position", func(t *testing.T) {
			lg, st := open(t)
			sid := testStream(t, "bounded")

			var appended []es.Envelope
			for i, typ := range []string{typConsumed, typIgnored, typConsumed, typIgnored, typConsumed} {
				appended = append(appended, st.Append(ctx, t, logScope, sid, uint64(i), typ, []byte("x")))
			}

			got, truncated, err := lg.ReadAllBounded(ctx, 0, 10, 0, []string{typConsumed})
			if err != nil {
				t.Fatalf("feedtest: read: %v", err)
			}
			if truncated {
				t.Fatal("feedtest: an unbounded read reported truncated")
			}
			if len(got) != 3 {
				t.Fatalf("feedtest: got %d events, want 3 (type-filtered)", len(got))
			}
			for i, env := range got {
				if want := appended[i*2].GlobalPosition; env.GlobalPosition != want {
					t.Fatalf("feedtest: event %d position = %d, want %d (global order)", i, env.GlobalPosition, want)
				}
			}

			got, _, err = lg.ReadAllBounded(ctx, 0, 2, 0, []string{typConsumed})
			if err != nil {
				t.Fatalf("feedtest: read limit: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("feedtest: limited read returned %d events, want 2", len(got))
			}

			got, _, err = lg.ReadAllBounded(ctx, appended[2].GlobalPosition, 10, 0, []string{typConsumed})
			if err != nil {
				t.Fatalf("feedtest: read from a position: %v", err)
			}
			if len(got) != 1 || got[0].GlobalPosition != appended[4].GlobalPosition {
				t.Fatalf("feedtest: read from position = %v, want only the last consumed event", positions(got))
			}
		})

		t.Run("an empty type set returns no events", func(t *testing.T) {
			lg, st := open(t)
			st.Append(ctx, t, logScope, testStream(t, "empty-types"), 0, typHappened, []byte("x"))

			got, truncated, err := lg.ReadAllBounded(ctx, 0, 10, 0, nil)
			if err != nil {
				t.Fatalf("feedtest: read: %v", err)
			}
			if got != nil || truncated {
				t.Fatalf("feedtest: empty types = (%v, %v), want (nil, false)", got, truncated)
			}
		})

		t.Run("the byte budget stops the page before the row that overflows", func(t *testing.T) {
			lg, st := open(t)
			sid := testStream(t, "budget")

			var appended []es.Envelope
			for i := 0; i < 4; i++ {
				appended = append(appended, st.Append(ctx, t, logScope, sid, uint64(i), typHappened, []byte("abcd"))) // 4 bytes each
			}

			// Budget 10: 4 + 4 fit; the third row (12) stops the page.
			got, truncated, err := lg.ReadAllBounded(ctx, 0, 10, 10, []string{typHappened})
			if err != nil {
				t.Fatalf("feedtest: read: %v", err)
			}
			if len(got) != 2 || !truncated {
				t.Fatalf("feedtest: budget page = %d events truncated=%v, want 2/true", len(got), truncated)
			}
			if got[0].GlobalPosition != appended[0].GlobalPosition || got[1].GlobalPosition != appended[1].GlobalPosition {
				t.Fatalf("feedtest: budget page positions = %v, want the first two",
					positions(got))
			}

			// Resuming from the last returned position makes progress.
			got, _, err = lg.ReadAllBounded(ctx, got[1].GlobalPosition, 10, 10, []string{typHappened})
			if err != nil {
				t.Fatalf("feedtest: read resume: %v", err)
			}
			if len(got) != 2 || got[0].GlobalPosition != appended[2].GlobalPosition {
				t.Fatalf("feedtest: resume page = %v, want the next two events", positions(got))
			}

			// maxBytes <= 0 disables the budget.
			got, truncated, err = lg.ReadAllBounded(ctx, 0, 10, -1, []string{typHappened})
			if err != nil {
				t.Fatalf("feedtest: read unbounded: %v", err)
			}
			if len(got) != 4 || truncated {
				t.Fatalf("feedtest: unbounded page = %d events truncated=%v, want 4/false", len(got), truncated)
			}
		})

		t.Run("a page that exactly meets the budget is not truncated", func(t *testing.T) {
			lg, st := open(t)
			sid := testStream(t, "exact-budget")

			st.Append(ctx, t, logScope, sid, 0, typHappened, []byte("abcd")) // 4 bytes
			st.Append(ctx, t, logScope, sid, 1, typHappened, []byte("efgh")) // 4 bytes

			// The page totals exactly the budget and no rows remain: it
			// must not be reported truncated. A >= comparison would stop
			// after the first row.
			got, truncated, err := lg.ReadAllBounded(ctx, 0, 10, 8, []string{typHappened})
			if err != nil {
				t.Fatalf("feedtest: read: %v", err)
			}
			if len(got) != 2 || truncated {
				t.Fatalf("feedtest: exact fit = %d events truncated=%v, want 2/false", len(got), truncated)
			}
		})

		t.Run("limit <= 0 selects the adapter default of 100", func(t *testing.T) {
			lg, st := open(t)
			sid := testStream(t, "default-limit")

			for i := 0; i < 101; i++ {
				st.Append(ctx, t, logScope, sid, uint64(i), typHappened, []byte("x"))
			}

			got, _, err := lg.ReadAllBounded(ctx, 0, 0, 0, []string{typHappened})
			if err != nil {
				t.Fatalf("feedtest: read: %v", err)
			}
			if len(got) != 100 {
				t.Fatalf("feedtest: default limit = %d events, want 100", len(got))
			}
		})

		t.Run("the first row is included even alone over budget", func(t *testing.T) {
			lg, st := open(t)
			sid := testStream(t, "oversized")

			st.Append(ctx, t, logScope, sid, 0, typHappened, []byte("12345")) // 5 bytes
			st.Append(ctx, t, logScope, sid, 1, typHappened, []byte("67890"))

			got, truncated, err := lg.ReadAllBounded(ctx, 0, 10, 3, []string{typHappened})
			if err != nil {
				t.Fatalf("feedtest: read: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("feedtest: got %d events, want the first oversized event only", len(got))
			}
			if !truncated {
				t.Fatal("feedtest: an oversized first event did not report truncated")
			}
		})

		t.Run("an over-cap first row is refused, not materialized", func(t *testing.T) {
			lg, st := open(t)
			oversized := bytes.Repeat([]byte("p"), st.MaxPayloadBytes*64)

			gp := st.PlantOverCap(ctx, t, logScope, testStream(t, "poison-first"), 1, typHappened, oversized)
			// A normal row follows the poison row.
			st.Append(ctx, t, logScope, testStream(t, "poison-first-ok"), 0, typHappened, []byte("ok"))

			got, _, err := lg.ReadAllBounded(ctx, 0, 10, 0, []string{typHappened})
			var tooLarge *feed.EventTooLargeError
			if !errors.As(err, &tooLarge) {
				t.Fatalf("feedtest: ReadAllBounded error = %v (%T), want *feed.EventTooLargeError", err, err)
			}
			if tooLarge.Position != gp || tooLarge.TypeURL != typHappened ||
				tooLarge.Size != len(oversized) || tooLarge.Limit != st.MaxPayloadBytes {
				t.Fatalf("feedtest: error fields = (pos %d, type %q, size %d, limit %d), want (pos %d, type %q, size %d, limit %d)",
					tooLarge.Position, tooLarge.TypeURL, tooLarge.Size, tooLarge.Limit, gp, typHappened, len(oversized), st.MaxPayloadBytes)
			}
			if len(got) != 0 {
				t.Fatalf("feedtest: the over-cap first row was materialized: %d envelopes, want none", len(got))
			}
		})

		t.Run("an over-cap later row stops the page before it", func(t *testing.T) {
			lg, st := open(t)
			oversized := bytes.Repeat([]byte("p"), st.MaxPayloadBytes*64)

			first := st.Append(ctx, t, logScope, testStream(t, "poison-later"), 0, typHappened, []byte("ok"))
			gp := st.PlantOverCap(ctx, t, logScope, testStream(t, "poison-later-b"), 1, typHappened, oversized)
			after := st.Append(ctx, t, logScope, testStream(t, "poison-later-after"), 0, typHappened, []byte("after"))
			if !(first.GlobalPosition < gp && gp < after.GlobalPosition) {
				t.Fatalf("feedtest: insert order: first=%d poison=%d after=%d, want the poison row between the normal rows",
					first.GlobalPosition, gp, after.GlobalPosition)
			}

			// The page delivers only the rows before the poison row and
			// stops before it: truncated=true is certain — the stopping
			// row was seen.
			got, truncated, err := lg.ReadAllBounded(ctx, 0, 10, 0, []string{typHappened})
			if err != nil {
				t.Fatalf("feedtest: ReadAllBounded: %v", err)
			}
			if len(got) != 1 || got[0].GlobalPosition != first.GlobalPosition {
				t.Fatalf("feedtest: page = %v, want only the event before the poison row (pos %d)",
					positions(got), first.GlobalPosition)
			}
			if !truncated {
				t.Fatal("feedtest: the page stopped before an over-cap row without truncated=true")
			}

			// The next page makes the poison row the first row: the
			// typed error, again with nothing materialized.
			got, _, err = lg.ReadAllBounded(ctx, first.GlobalPosition, 10, 0, []string{typHappened})
			var tooLarge *feed.EventTooLargeError
			if !errors.As(err, &tooLarge) {
				t.Fatalf("feedtest: next page error = %v (%T), want *feed.EventTooLargeError", err, err)
			}
			if tooLarge.Position != gp || tooLarge.TypeURL != typHappened ||
				tooLarge.Size != len(oversized) || tooLarge.Limit != st.MaxPayloadBytes {
				t.Fatalf("feedtest: error fields = (pos %d, type %q, size %d, limit %d), want (pos %d, type %q, size %d, limit %d)",
					tooLarge.Position, tooLarge.TypeURL, tooLarge.Size, tooLarge.Limit, gp, typHappened, len(oversized), st.MaxPayloadBytes)
			}
			if len(got) != 0 {
				t.Fatalf("feedtest: the over-cap first row was materialized: %d envelopes, want none", len(got))
			}
		})
	})
}

// ---------------------------------------------------------------------------
// CheckpointStore
// ---------------------------------------------------------------------------

// RunCheckpoints runs the CheckpointStore conformance suite: absent
// loads read 0, monotonic Save against rewinding Reset, the
// empty-definition preserve on both, the definition-hash round-trip
// through Checkpoint, and Delete of exactly one row.
func RunCheckpoints(t *testing.T, newStore NewCheckpointStore) {
	t.Helper()
	ctx := context.Background()
	open := func(t *testing.T) feed.CheckpointStore {
		t.Helper()
		st, cleanup := newStore(ctx, t)
		t.Cleanup(cleanup)
		return st
	}

	t.Run("checkpoints", func(t *testing.T) {
		t.Run("absent load, monotonic save, per-instance rows", func(t *testing.T) {
			cp := open(t)

			pos, err := cp.Load(ctx, "ws", "p", "live")
			if err != nil {
				t.Fatalf("feedtest: load absent: %v", err)
			}
			if pos != 0 {
				t.Fatalf("feedtest: absent load = %d, want 0", pos)
			}
			if _, found, err := cp.Checkpoint(ctx, "ws", "p", "live"); err != nil || found {
				t.Fatalf("feedtest: absent checkpoint = found=%v err=%v, want false/nil", found, err)
			}

			if err := cp.Save(ctx, "ws", "p", "live", "", 5); err != nil {
				t.Fatalf("feedtest: save: %v", err)
			}
			if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
				t.Fatalf("feedtest: load = %d, want 5", pos)
			}

			// Save never rewinds.
			if err := cp.Save(ctx, "ws", "p", "live", "", 3); err != nil {
				t.Fatalf("feedtest: save lower: %v", err)
			}
			if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
				t.Fatalf("feedtest: load after a lower save = %d, want 5 (monotonic)", pos)
			}

			// A different instance is a different row.
			if err := cp.Save(ctx, "ws", "p", "other", "", 2); err != nil {
				t.Fatalf("feedtest: save other: %v", err)
			}
			if pos, _ = cp.Load(ctx, "ws", "p", "other"); pos != 2 {
				t.Fatalf("feedtest: the other instance = %d, want 2", pos)
			}
			if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
				t.Fatalf("feedtest: the live instance = %d, want 5", pos)
			}
		})

		t.Run("reset rewinds, delete removes exactly one row", func(t *testing.T) {
			cp := open(t)

			before := time.Now()
			if err := cp.Save(ctx, "ws", "p", "live", "", 7); err != nil {
				t.Fatalf("feedtest: save: %v", err)
			}
			after := time.Now()
			checkpoint, found, err := cp.Checkpoint(ctx, "ws", "p", "live")
			if err != nil || !found {
				t.Fatalf("feedtest: checkpoint = found=%v err=%v, want true/nil", found, err)
			}
			if checkpoint.Position != 7 {
				t.Fatalf("feedtest: checkpoint position = %d, want 7", checkpoint.Position)
			}
			// UpdatedAt is stamped by the backend — its own database
			// clock or the caller's, the deliberate divergence — so the
			// suite pins only a plausible recent instant.
			assertRecent(t, "feedtest: checkpoint updated_at", checkpoint.UpdatedAt, before, after)

			// Reset overwrites, backwards included: a reset is a
			// deliberate replay decision, so unlike Save it may rewind.
			if err := cp.Reset(ctx, "ws", "p", "live", "", 1); err != nil {
				t.Fatalf("feedtest: reset: %v", err)
			}
			if pos, _ := cp.Load(ctx, "ws", "p", "live"); pos != 1 {
				t.Fatalf("feedtest: load after reset = %d, want 1 (reset rewinds)", pos)
			}
			if checkpoint, _, _ = cp.Checkpoint(ctx, "ws", "p", "live"); checkpoint.Position != 1 {
				t.Fatalf("feedtest: checkpoint after reset = %d, want 1", checkpoint.Position)
			}

			// Delete removes exactly that row.
			if err := cp.Save(ctx, "ws", "p", "other", "", 4); err != nil {
				t.Fatalf("feedtest: save other: %v", err)
			}
			if err := cp.Delete(ctx, "ws", "p", "live"); err != nil {
				t.Fatalf("feedtest: delete: %v", err)
			}
			if _, found, err := cp.Checkpoint(ctx, "ws", "p", "live"); err != nil || found {
				t.Fatalf("feedtest: checkpoint after delete = found=%v err=%v, want false/nil", found, err)
			}
			if pos, _ := cp.Load(ctx, "ws", "p", "other"); pos != 4 {
				t.Fatalf("feedtest: the other instance after delete = %d, want 4", pos)
			}
			// Deleting again is a no-op.
			if err := cp.Delete(ctx, "ws", "p", "live"); err != nil {
				t.Fatalf("feedtest: delete absent: %v", err)
			}
		})

		t.Run("definition hash round-trips and empty definitions preserve it", func(t *testing.T) {
			cp := open(t)

			h1 := projection.Definition{Name: "p", Consumes: []string{"a.v1.A"}}.Hash()
			h2 := projection.Definition{Name: "p", Consumes: []string{"a.v1.A", "b.v1.B"}}.Hash()

			// A save with no definition records no fingerprint.
			if err := cp.Save(ctx, "ws", "p", "live", "", 5); err != nil {
				t.Fatalf("feedtest: save without definition: %v", err)
			}
			got, found, err := cp.Checkpoint(ctx, "ws", "p", "live")
			if err != nil || !found || got.Position != 5 || got.DefinitionHash != "" {
				t.Fatalf("feedtest: checkpoint = %+v found=%v err=%v, want position 5 and no hash", got, found, err)
			}

			// A definition-carrying save records the fingerprint, and
			// Checkpoint returns what Save wrote.
			if err := cp.Save(ctx, "ws", "p", "live", h1, 6); err != nil {
				t.Fatalf("feedtest: save with definition: %v", err)
			}
			if got, _, _ = cp.Checkpoint(ctx, "ws", "p", "live"); got.DefinitionHash != h1 || got.Position != 6 {
				t.Fatalf("feedtest: checkpoint = %+v, want position 6 under hash %q", got, h1)
			}

			// An empty definition preserves the recorded hash on Save:
			// an ack that carries no identity cannot clear what a
			// previous one recorded.
			if err := cp.Save(ctx, "ws", "p", "live", "", 7); err != nil {
				t.Fatalf("feedtest: save without definition: %v", err)
			}
			if got, _, _ = cp.Checkpoint(ctx, "ws", "p", "live"); got.Position != 7 || got.DefinitionHash != h1 {
				t.Fatalf("feedtest: checkpoint = %+v, want position 7 with the hash preserved", got)
			}

			// ... and on Reset, which may rewind but still must not
			// clear it.
			if err := cp.Reset(ctx, "ws", "p", "live", "", 1); err != nil {
				t.Fatalf("feedtest: reset without definition: %v", err)
			}
			if got, _, _ = cp.Checkpoint(ctx, "ws", "p", "live"); got.Position != 1 || got.DefinitionHash != h1 {
				t.Fatalf("feedtest: checkpoint = %+v, want position 1 with the hash preserved", got)
			}

			// A reset under a new definition re-identifies the
			// checkpoint.
			if err := cp.Reset(ctx, "ws", "p", "live", h2, 0); err != nil {
				t.Fatalf("feedtest: reset with definition: %v", err)
			}
			if got, _, _ = cp.Checkpoint(ctx, "ws", "p", "live"); got.Position != 0 || got.DefinitionHash != h2 {
				t.Fatalf("feedtest: checkpoint = %+v, want position 0 under the new hash", got)
			}
		})
	})
}

// ---------------------------------------------------------------------------
// LeaseStore
// ---------------------------------------------------------------------------

// RunLeases runs the LeaseStore conformance suite: exactly one winner
// per key, same-owner re-acquire with the consumer refreshed, renew
// only by the owner, an owner-guarded and idempotent Release, a holder
// only while unexpired, takeover of an expired lease, and distinct
// (scope, projection, instance) triples that never collide.
func RunLeases(t *testing.T, newStore NewLeaseStore) {
	t.Helper()
	ctx := context.Background()
	open := func(t *testing.T) feed.LeaseStore {
		t.Helper()
		st, cleanup := newStore(ctx, t)
		t.Cleanup(cleanup)
		return st
	}

	t.Run("leases", func(t *testing.T) {
		t.Run("exactly one winner per key", func(t *testing.T) {
			leases := open(t)
			const scope, projection, instance = "ws", "p", "live"

			start := time.Now()
			ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", time.Minute)
			if err != nil {
				t.Fatalf("feedtest: acquire owner-a: %v", err)
			}
			if !ok {
				t.Fatal("feedtest: owner-a lost a free lease")
			}

			ok, err = leases.Acquire(ctx, scope, projection, instance, "owner-b", "consumer-b", time.Minute)
			if err != nil {
				t.Fatalf("feedtest: acquire owner-b: %v", err)
			}
			if ok {
				t.Fatal("feedtest: owner-b took a lease owner-a still holds; want exactly one winner")
			}

			lease, held, err := leases.Holder(ctx, scope, projection, instance)
			if err != nil {
				t.Fatalf("feedtest: holder: %v", err)
			}
			if !held || lease.Owner != "owner-a" || lease.Consumer != "consumer-a" {
				t.Fatalf("feedtest: holder = %+v held=%v, want owner-a/consumer-a", lease, held)
			}
			// The expiry is absolute ttl from the acquire, computed
			// from the backend's own clock or the caller's (the
			// deliberate divergence): the suite pins the plausible
			// window, the backend-local tests the exact clock.
			if lease.ExpiresAt.Before(start.Add(-clockSlack)) || lease.ExpiresAt.After(start.Add(time.Minute+clockSlack)) {
				t.Fatalf("feedtest: holder ExpiresAt = %v, want within the 1m TTL of the acquire at %v (cross-clock slack %v)",
					lease.ExpiresAt, start, clockSlack)
			}
		})

		t.Run("the same owner re-acquires and refreshes the consumer", func(t *testing.T) {
			leases := open(t)
			const scope, projection, instance = "ws", "p", "live"

			if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "pod-a", time.Minute); err != nil || !ok {
				t.Fatalf("feedtest: first acquire = %v err=%v, want true/nil", ok, err)
			}
			// A reconnect by the same owner must succeed and refresh
			// the consumer identity Status reports.
			if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "pod-b", time.Minute); err != nil || !ok {
				t.Fatalf("feedtest: same-owner acquire = %v err=%v, want true/nil", ok, err)
			}
			lease, held, err := leases.Holder(ctx, scope, projection, instance)
			if err != nil || !held {
				t.Fatalf("feedtest: holder = found=%v err=%v, want held", held, err)
			}
			if lease.Owner != "owner-a" || lease.Consumer != "pod-b" {
				t.Fatalf("feedtest: holder = %+v, want owner-a/pod-b", lease)
			}
		})

		t.Run("renew extends only for the owner", func(t *testing.T) {
			leases := open(t)
			const scope, projection, instance = "ws", "p", "live"

			if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", 200*time.Millisecond); err != nil || !ok {
				t.Fatalf("feedtest: acquire owner-a = %v err=%v", ok, err)
			}
			if ok, err := leases.Renew(ctx, scope, projection, instance, "owner-b", time.Minute); err != nil {
				t.Fatalf("feedtest: renew owner-b: %v", err)
			} else if ok {
				t.Fatal("feedtest: a non-owner renewed the lease")
			}
			if ok, err := leases.Renew(ctx, scope, projection, instance, "owner-a", 2*time.Hour); err != nil {
				t.Fatalf("feedtest: renew owner-a: %v", err)
			} else if !ok {
				t.Fatal("feedtest: the owner failed to renew its own lease")
			}

			// The renewal extended the expiry far past the original
			// 200ms TTL: the holder now reports a deadline an hour out.
			lease, held, err := leases.Holder(ctx, scope, projection, instance)
			if err != nil || !held {
				t.Fatalf("feedtest: holder after renew = found=%v err=%v, want held", held, err)
			}
			if !lease.ExpiresAt.After(time.Now().Add(time.Hour)) {
				t.Fatalf("feedtest: renewed ExpiresAt %v did not extend past the original TTL", lease.ExpiresAt)
			}
			if lease.Owner != "owner-a" {
				t.Fatalf("feedtest: holder owner = %q, want owner-a", lease.Owner)
			}
		})

		t.Run("release is owner-guarded and idempotent", func(t *testing.T) {
			leases := open(t)
			const scope, projection, instance = "ws", "p", "live"

			if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", time.Minute); err != nil || !ok {
				t.Fatalf("feedtest: acquire = %v err=%v", ok, err)
			}

			// A non-owner's release is a no-op, not an error.
			if err := leases.Release(ctx, scope, projection, instance, "owner-b"); err != nil {
				t.Fatalf("feedtest: release owner-b: %v", err)
			}
			if _, held, err := leases.Holder(ctx, scope, projection, instance); err != nil || !held {
				t.Fatalf("feedtest: holder after a non-owner release = held=%v err=%v, want still held", held, err)
			}

			if err := leases.Release(ctx, scope, projection, instance, "owner-a"); err != nil {
				t.Fatalf("feedtest: release owner-a: %v", err)
			}
			if _, held, err := leases.Holder(ctx, scope, projection, instance); err != nil || held {
				t.Fatalf("feedtest: holder after the owner's release = held=%v err=%v, want free", held, err)
			}
			// Releasing a free key is a no-op, and the next owner can
			// take it.
			if err := leases.Release(ctx, scope, projection, instance, "owner-a"); err != nil {
				t.Fatalf("feedtest: release a free key: %v", err)
			}
			if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-b", "consumer-b", time.Minute); err != nil || !ok {
				t.Fatalf("feedtest: acquire after release = %v err=%v, want true/nil", ok, err)
			}
		})

		// An expired lease stops fencing and stops being the holder:
		// Acquire treats an expired row as free (the <= takeover arm)
		// and Holder reports only strictly unexpired rows (the >
		// boundary). The suite waits out a real short TTL — portable
		// across backends; Postgres keeps a local test that pokes the
		// row with SQL for a deterministic boundary pin.
		t.Run("an expired lease stops fencing and is not the holder", func(t *testing.T) {
			leases := open(t)
			const scope, projection, instance = "ws", "p", "live"

			if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", 50*time.Millisecond); err != nil || !ok {
				t.Fatalf("feedtest: acquire owner-a = %v err=%v", ok, err)
			}
			// Unexpired: a live row IS the holder and DOES fence.
			if _, held, err := leases.Holder(ctx, scope, projection, instance); err != nil || !held {
				t.Fatalf("feedtest: holder before expiry = held=%v err=%v, want held", held, err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for {
				_, held, err := leases.Holder(ctx, scope, projection, instance)
				if err != nil {
					t.Fatalf("feedtest: holder: %v", err)
				}
				if !held {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("feedtest: Holder kept reporting an expired lease")
				}
				time.Sleep(5 * time.Millisecond)
			}

			// Another owner takes the expired lease over.
			if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-b", "consumer-b", time.Minute); err != nil || !ok {
				t.Fatalf("feedtest: acquire owner-b after expiry = %v err=%v, want takeover", ok, err)
			}
			lease, held, err := leases.Holder(ctx, scope, projection, instance)
			if err != nil || !held {
				t.Fatalf("feedtest: holder after takeover = found=%v err=%v, want the new owner", held, err)
			}
			if lease.Owner != "owner-b" || lease.Consumer != "consumer-b" {
				t.Fatalf("feedtest: holder after takeover = %+v, want owner-b/consumer-b", lease)
			}
		})

		// The subscription key is the (scope, projection, instance)
		// triple. Distinct triples must stay distinct rows even when a
		// part contains the separator-like "/" — ("ws","a/b","c") and
		// ("ws","a","b/c") are different subscriptions, never one lease.
		// This pins the collision-freedom the feed core's validateKey
		// guarantees for every adapter that joins parts into one key.
		t.Run("distinct triples never collide", func(t *testing.T) {
			leases := open(t)

			if ok, err := leases.Acquire(ctx, "ws", "a/b", "c", "owner-a", "consumer-a", time.Minute); err != nil || !ok {
				t.Fatalf("feedtest: acquire = %v err=%v, want true/nil", ok, err)
			}
			// The segment-shifted triple is a different subscription:
			// another owner can hold it alongside the first.
			if ok, err := leases.Acquire(ctx, "ws", "a", "b/c", "owner-b", "consumer-b", time.Minute); err != nil || !ok {
				t.Fatalf("feedtest: acquire of the shifted triple = %v err=%v, want true (distinct triples never collide)", ok, err)
			}
			// The original triple is still fenced by owner-a.
			if ok, err := leases.Acquire(ctx, "ws", "a/b", "c", "owner-b", "consumer-b", time.Minute); err != nil || ok {
				t.Fatalf("feedtest: acquire of the held triple = %v err=%v, want false (still held by owner-a)", ok, err)
			}
		})
	})
}

// ---------------------------------------------------------------------------
// DeadLetterStore
// ---------------------------------------------------------------------------

// RunDeadLetters runs the DeadLetterStore conformance suite: one
// pointer row per skipped position — replaced, never duplicated —
// enriched from the log the store shares a database with (metadata
// only: an over-cap poison row is named without its payload ever being
// scanned), caller-supplied types passing through, and absent positions
// recording the empty pointer. Recorded CreatedAt timestamps are
// asserted present and plausible only: the backend may stamp its own
// database clock or honor the caller's (the deliberate divergence).
func RunDeadLetters(t *testing.T, newStore NewStore) {
	t.Helper()
	ctx := context.Background()
	const (
		dlScope      = "ws_feedtest_dl"
		dlProjection = "p"
		dlInstance   = "live"
	)
	open := func(t *testing.T) Store {
		t.Helper()
		st, cleanup := newStore(ctx, t)
		st.validate(t)
		t.Cleanup(cleanup)
		return st
	}

	t.Run("dead letters", func(t *testing.T) {
		// The core delivers a pointer — no type, no size. The store
		// names the event from the log it shares a database with, and
		// the production-relevant case is the poison row: metadata
		// only, never the payload bytes.
		t.Run("the pointer names a poison event from the log, metadata only", func(t *testing.T) {
			st := open(t)
			oversized := bytes.Repeat([]byte("p"), st.MaxPayloadBytes*64)
			poisonGP := st.PlantOverCap(ctx, t, dlScope, testStream(t, "dl-poison"), 1, typPoison, oversized)

			before := time.Now()
			if err := st.DeadLetters.Record(ctx, feed.DeadLetter{
				Scope: dlScope, Projection: dlProjection, Instance: dlInstance,
				Position: poisonGP, Reason: "poison: over cap", Actor: "oncall",
				// A recent, caller-supplied time: the backend either
				// honors it or stamps its own clock — the suite pins
				// only that something recent lands.
				CreatedAt: before,
			}); err != nil {
				t.Fatalf("feedtest: record: %v", err)
			}
			after := time.Now()

			row, ok := st.ReadDeadLetter(ctx, t, dlScope, dlProjection, dlInstance, poisonGP)
			if !ok {
				t.Fatalf("feedtest: no dead letter row at %d", poisonGP)
			}
			if row.TypeURL != typPoison || row.SizeBytes != len(oversized) {
				t.Fatalf("feedtest: recorded row = %+v, want the poison named (%q, %d bytes)", row, typPoison, len(oversized))
			}
			if row.Reason != "poison: over cap" || row.Actor != "oncall" {
				t.Fatalf("feedtest: recorded row = %+v, want the audit fields kept", row)
			}
			assertRecent(t, "feedtest: dead letter created_at", row.CreatedAt, before, after)
		})

		t.Run("a repeat skip replaces the row, never duplicates it", func(t *testing.T) {
			st := open(t)
			oversized := bytes.Repeat([]byte("p"), st.MaxPayloadBytes*64)
			poisonGP := st.PlantOverCap(ctx, t, dlScope, testStream(t, "dl-replace"), 1, typPoison, oversized)

			if err := st.DeadLetters.Record(ctx, feed.DeadLetter{
				Scope: dlScope, Projection: dlProjection, Instance: dlInstance,
				Position: poisonGP, Reason: "poison: over cap", Actor: "oncall",
				CreatedAt: time.Now(),
			}); err != nil {
				t.Fatalf("feedtest: record: %v", err)
			}
			// The repeat skip of the same position: exactly one row,
			// the latest reason and actor.
			before := time.Now()
			if err := st.DeadLetters.Record(ctx, feed.DeadLetter{
				Scope: dlScope, Projection: dlProjection, Instance: dlInstance,
				Position: poisonGP, Reason: "triaged: cap raised", Actor: "oncall-2",
				CreatedAt: before,
			}); err != nil {
				t.Fatalf("feedtest: re-record: %v", err)
			}
			after := time.Now()

			if n := st.CountDeadLetters(ctx, t, dlScope); n != 1 {
				t.Fatalf("feedtest: rows after the repeat = %d, want 1 (replaced, never duplicated)", n)
			}
			row, ok := st.ReadDeadLetter(ctx, t, dlScope, dlProjection, dlInstance, poisonGP)
			if !ok {
				t.Fatalf("feedtest: no dead letter row at %d", poisonGP)
			}
			if row.Reason != "triaged: cap raised" || row.Actor != "oncall-2" {
				t.Fatalf("feedtest: replaced row = %+v, want the latest audit fields", row)
			}
			if row.TypeURL != typPoison || row.SizeBytes != len(oversized) {
				t.Fatalf("feedtest: replaced row = %+v, want the same named event kept", row)
			}
			assertRecent(t, "feedtest: dead letter created_at", row.CreatedAt, before, after)
		})

		t.Run("a caller-supplied type and size pass through", func(t *testing.T) {
			st := open(t)

			if err := st.DeadLetters.Record(ctx, feed.DeadLetter{
				Scope: dlScope, Projection: dlProjection, Instance: dlInstance,
				Position: 42, TypeURL: "custom.v1.Typed", SizeBytes: 99,
				Reason: "named by the caller", Actor: "oncall-3", CreatedAt: time.Now(),
			}); err != nil {
				t.Fatalf("feedtest: record with a type: %v", err)
			}
			row, ok := st.ReadDeadLetter(ctx, t, dlScope, dlProjection, dlInstance, 42)
			if !ok {
				t.Fatal("feedtest: no dead letter row at 42")
			}
			if row.TypeURL != "custom.v1.Typed" || row.SizeBytes != 99 {
				t.Fatalf("feedtest: row with a caller-supplied type = %+v, want the caller's type/size kept", row)
			}
		})

		t.Run("an absent position names nothing and still records", func(t *testing.T) {
			st := open(t)
			const absent = uint64(1) << 40

			// A position with no event row records the empty pointer:
			// Skip does not require the position to exist.
			if err := st.DeadLetters.Record(ctx, feed.DeadLetter{
				Scope: dlScope, Projection: dlProjection, Instance: dlInstance,
				Position: absent, Reason: "skipped past a restored log", Actor: "oncall",
				CreatedAt: time.Now(),
			}); err != nil {
				t.Fatalf("feedtest: record an absent position: %v", err)
			}
			row, ok := st.ReadDeadLetter(ctx, t, dlScope, dlProjection, dlInstance, absent)
			if !ok {
				t.Fatalf("feedtest: no dead letter row at %d", absent)
			}
			if row.TypeURL != "" || row.SizeBytes != 0 {
				t.Fatalf("feedtest: absent-position row = %+v, want the empty type and zero size", row)
			}
			if n := st.CountDeadLetters(ctx, t, dlScope); n != 1 {
				t.Fatalf("feedtest: rows = %d, want 1", n)
			}
		})
	})
}

// ---------------------------------------------------------------------------
// End to end over the real feed core
// ---------------------------------------------------------------------------

// e2eScope is the workspace scope the end-to-end section runs under.
const e2eScope = "ws_feedtest_e2e"

// runEndToEnd runs the two whole-core runs every backend must pass: a
// durable-checkpoint resume and the skip-past-poison runbook, exercising
// the adapters under the real feed.Server.
func runEndToEnd(t *testing.T, newStore NewStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("e2e resume from a durable checkpoint", func(t *testing.T) {
		st, cleanup := newStore(ctx, t)
		st.validate(t)
		t.Cleanup(cleanup)

		sid := testStream(t, "e2e-resume")
		first := st.Append(ctx, t, e2eScope, sid, 0, typConsumed, []byte("a1"))
		st.Append(ctx, t, e2eScope, sid, 1, typIgnored, []byte("b1"))
		third := st.Append(ctx, t, e2eScope, sid, 2, typConsumed, []byte("a2"))

		srv := newServer(t, st)
		req := feed.Request{
			Scope:      e2eScope,
			Projection: "p",
			Instance:   "live",
			Consumer:   "feedtest",
			// Caller-supplied owner: the fenced acks below must name it.
			Owner:      "feedtest-e2e-owner",
			Definition: projection.Definition{Name: "p", Consumes: []string{typConsumed}},
		}

		// First subscription: the two consumed events, acknowledged
		// while the stream holds its lease, then cancelled.
		ctx1, cancel1 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel1()
		sink := &cancelSink{want: 2, cancel: cancel1, ack: func(ackCtx context.Context, pos uint64) error {
			return srv.Checkpoint(ackCtx, feed.Ack{
				Scope: req.Scope, Projection: req.Projection, Instance: req.Instance,
				Owner: req.Owner, Position: pos,
				// The ack records the subscription's fingerprint, so
				// the resumed subscription crosses with a recorded
				// identity that must still match the definition.
				Definition: req.Definition.Hash(),
			})
		}}
		if err := srv.Subscribe(ctx1, req, sink); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("feedtest: subscribe 1: %v", err)
		}

		got := sink.got()
		if len(got) != 2 {
			t.Fatalf("feedtest: first subscription got %d events, want 2: %+v", len(got), got)
		}
		if got[0].GlobalPosition != first.GlobalPosition || got[1].GlobalPosition != third.GlobalPosition {
			t.Fatalf("feedtest: first subscription positions = %v, want %d,%d (the consumed events, in order)",
				positions(got), first.GlobalPosition, third.GlobalPosition)
		}

		// The ack landed a durable checkpoint under the acked
		// definition.
		cp, found, err := st.Checkpoints.Checkpoint(ctx, req.Scope, req.Projection, req.Instance)
		if err != nil || !found {
			t.Fatalf("feedtest: checkpoint after the acks = found %v, err %v, want found", found, err)
		}
		if cp.Position != third.GlobalPosition {
			t.Fatalf("feedtest: checkpoint = %d, want the acked %d", cp.Position, third.GlobalPosition)
		}
		if cp.DefinitionHash != req.Definition.Hash() {
			t.Fatalf("feedtest: checkpoint hash = %q, want the acked definition's %q", cp.DefinitionHash, req.Definition.Hash())
		}

		// One more event: the resumed subscription delivers exactly it
		// — never anything before the checkpoint.
		next := st.Append(ctx, t, e2eScope, sid, 3, typConsumed, []byte("a3"))
		ctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel2()
		sink2 := &cancelSink{want: 1, cancel: cancel2}
		if err := srv.Subscribe(ctx2, req, sink2); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("feedtest: subscribe 2: %v", err)
		}
		second := sink2.got()
		if len(second) != 1 || second[0].GlobalPosition != next.GlobalPosition {
			t.Fatalf("feedtest: resumed subscription = %v, want exactly the event at %d (after checkpoint %d)",
				positions(second), next.GlobalPosition, cp.Position)
		}
	})

	// The poison runbook end to end: the over-cap row kills a live
	// subscription with *feed.EventTooLargeError; the operator
	// deliberately skips past it; the dead letter lands as a complete
	// pointer; and the resubscription delivers exactly the events after
	// the poison position, never the poison itself.
	t.Run("e2e skip past a poison event", func(t *testing.T) {
		st, cleanup := newStore(ctx, t)
		st.validate(t)
		t.Cleanup(cleanup)
		oversized := bytes.Repeat([]byte("p"), st.MaxPayloadBytes*64)

		// Global order: before < poison < after1 < after2.
		before := st.Append(ctx, t, e2eScope, testStream(t, "poison-e2e-a"), 0, typPoison, []byte("ok"))
		poisonGP := st.PlantOverCap(ctx, t, e2eScope, testStream(t, "poison-e2e"), 1, typPoison, oversized)
		after1 := st.Append(ctx, t, e2eScope, testStream(t, "poison-e2e-b"), 0, typPoison, []byte("after-1"))
		after2 := st.Append(ctx, t, e2eScope, testStream(t, "poison-e2e-b"), 1, typPoison, []byte("after-2"))
		if !(before.GlobalPosition < poisonGP && poisonGP < after1.GlobalPosition && after1.GlobalPosition < after2.GlobalPosition) {
			t.Fatalf("feedtest: insert order: before=%d poison=%d after=%d,%d, want the poison between the normal rows",
				before.GlobalPosition, poisonGP, after1.GlobalPosition, after2.GlobalPosition)
		}

		srv := newServer(t, st)
		req := feed.Request{
			Scope:      e2eScope,
			Projection: "p",
			Instance:   "live",
			Consumer:   "feedtest",
			Owner:      "feedtest-skip-owner",
			Definition: projection.Definition{Name: "p", Consumes: []string{typPoison}},
		}

		// The live subscription delivers the event before the poison,
		// acks it under the definition, then dies on the poison row —
		// never materialized, never delivered.
		ctx1, cancel1 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel1()
		sink := &ackingSink{ack: func(ackCtx context.Context, pos uint64) error {
			return srv.Checkpoint(ackCtx, feed.Ack{
				Scope: req.Scope, Projection: req.Projection, Instance: req.Instance,
				Owner: req.Owner, Position: pos, Definition: req.Definition.Hash(),
			})
		}}
		err := srv.Subscribe(ctx1, req, sink)
		var tooLarge *feed.EventTooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("feedtest: subscribe error = %v (%T), want *feed.EventTooLargeError", err, err)
		}
		if tooLarge.Position != poisonGP || tooLarge.TypeURL != typPoison ||
			tooLarge.Size != len(oversized) || tooLarge.Limit != st.MaxPayloadBytes {
			t.Fatalf("feedtest: poison error = %+v, want position %d of type %q, %d bytes over the %d-byte cap",
				tooLarge, poisonGP, typPoison, len(oversized), st.MaxPayloadBytes)
		}
		if got := sink.got(); len(got) != 1 || got[0].GlobalPosition != before.GlobalPosition {
			t.Fatalf("feedtest: delivered before the poison = %v, want only the event at %d",
				positions(got), before.GlobalPosition)
		}

		// The operator skips past the poison. No force: the poison
		// error killed the stream and released its lease, so the fence
		// is free.
		reason := "poison: payload over the store cap"
		beforeSkip := time.Now()
		if err := srv.Skip(ctx, feed.SkipRequest{
			Scope: req.Scope, Projection: req.Projection, Instance: req.Instance,
			Position: poisonGP, Actor: "oncall@example.com", Reason: reason,
		}); err != nil {
			t.Fatalf("feedtest: Skip: %v", err)
		}
		afterSkip := time.Now()

		// The dead letter landed as a complete pointer: named from the
		// log (metadata only) with the operator's audit fields and a
		// stamped CreatedAt — presence/reasonableness only, the
		// deliberate divergence.
		row, ok := st.ReadDeadLetter(ctx, t, req.Scope, req.Projection, req.Instance, poisonGP)
		if !ok {
			t.Fatalf("feedtest: no dead letter row at %d", poisonGP)
		}
		if row.TypeURL != typPoison || row.SizeBytes != len(oversized) || row.Reason != reason || row.Actor != "oncall@example.com" {
			t.Fatalf("feedtest: dead letter row = %+v, want the poison named with the audit fields", row)
		}
		assertRecent(t, "feedtest: dead letter created_at", row.CreatedAt, beforeSkip, afterSkip)

		// The checkpoint advanced to the poison position with the
		// ack's definition fingerprint preserved — a skip is a position
		// advance, never a redefinition.
		cp, found, err := st.Checkpoints.Checkpoint(ctx, req.Scope, req.Projection, req.Instance)
		if err != nil || !found {
			t.Fatalf("feedtest: checkpoint after the skip = found %v, err %v", found, err)
		}
		if cp.Position != poisonGP {
			t.Fatalf("feedtest: checkpoint = %d after the skip, want the poison position %d", cp.Position, poisonGP)
		}
		if cp.DefinitionHash != req.Definition.Hash() {
			t.Fatalf("feedtest: definition hash = %q after the skip, want the acked %q preserved",
				cp.DefinitionHash, req.Definition.Hash())
		}

		// The resubscription delivers exactly the events after the
		// poison — never the poison itself.
		ctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel2()
		sink2 := &cancelSink{want: 2, cancel: cancel2}
		if err := srv.Subscribe(ctx2, req, sink2); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("feedtest: resubscribe: %v", err)
		}
		got := sink2.got()
		want := []uint64{after1.GlobalPosition, after2.GlobalPosition}
		if !slices.Equal(positions(got), want) {
			t.Fatalf("feedtest: resumed positions = %v, want %v (the events after the poison)",
				positions(got), want)
		}
		if slices.Contains(positions(got), poisonGP) {
			t.Fatalf("feedtest: the poison event at %d was delivered after the skip", poisonGP)
		}
	})
}

// ---------------------------------------------------------------------------
// Cross-workspace isolation (MultiWorkspace backends only)
// ---------------------------------------------------------------------------

// runCrossWorkspace runs the cross-workspace isolation section for
// multi-workspace backends: every feed surface stays pinned to one
// scope — B's rows never clobber A's, the same key in another workspace
// is free, and the dead-letter enrichment never names a foreign
// workspace's event. The RLS policy mechanics — probe roles, pg_policies
// shape, WITH CHECK — are backend-specific and stay in the backend's
// own tests.
func runCrossWorkspace(t *testing.T, newStore NewStore) {
	t.Helper()
	ctx := context.Background()
	const (
		wsA = "ws_feedtest_a"
		wsB = "ws_feedtest_b"
	)

	t.Run("cross workspace isolation", func(t *testing.T) {
		st, cleanup := newStore(ctx, t)
		st.validate(t)
		t.Cleanup(cleanup)

		t.Run("checkpoints never clobber across workspaces", func(t *testing.T) {
			if err := st.Checkpoints.Save(ctx, wsA, "cp", "live", "", 10); err != nil {
				t.Fatalf("feedtest: save under %s: %v", wsA, err)
			}
			if err := st.Checkpoints.Save(ctx, wsB, "cp", "live", "", 20); err != nil {
				t.Fatalf("feedtest: save under %s: %v", wsB, err)
			}
			pos, err := st.Checkpoints.Load(ctx, wsA, "cp", "live")
			if err != nil || pos != 10 {
				t.Fatalf("feedtest: load under %s = %d err=%v, want 10 (B's write must not clobber it)", wsA, pos, err)
			}
			pos, err = st.Checkpoints.Load(ctx, wsB, "cp", "live")
			if err != nil || pos != 20 {
				t.Fatalf("feedtest: load under %s = %d err=%v, want 20 — A's row leaked", wsB, pos, err)
			}
		})

		t.Run("the same lease key is free in another workspace", func(t *testing.T) {
			if ok, err := st.Leases.Acquire(ctx, wsA, "lease", "live", "owner-a", "consumer-a", time.Minute); err != nil || !ok {
				t.Fatalf("feedtest: acquire under %s = %v err=%v", wsA, ok, err)
			}
			if ok, err := st.Leases.Acquire(ctx, wsB, "lease", "live", "owner-b", "consumer-b", time.Minute); err != nil || !ok {
				t.Fatalf("feedtest: acquire under %s = %v err=%v, want true (the same key in another workspace is free)", wsB, ok, err)
			}
			leaseA, held, err := st.Leases.Holder(ctx, wsA, "lease", "live")
			if err != nil || !held || leaseA.Owner != "owner-a" {
				t.Fatalf("feedtest: holder under %s = %+v held=%v err=%v, want owner-a", wsA, leaseA, held, err)
			}
			leaseB, held, err := st.Leases.Holder(ctx, wsB, "lease", "live")
			if err != nil || !held || leaseB.Owner != "owner-b" {
				t.Fatalf("feedtest: holder under %s = %+v held=%v err=%v, want owner-b — A's lease leaked", wsB, leaseB, held, err)
			}
		})

		t.Run("dead letter enrichment never names a foreign event", func(t *testing.T) {
			own := st.Append(ctx, t, wsA, testStream(t, "cross-ws"), 0, typHappened, []byte("own"))
			if err := st.DeadLetters.Record(ctx, feed.DeadLetter{
				Scope: wsB, Projection: "dl", Instance: "live",
				Position: own.GlobalPosition, Reason: "probe", Actor: "feedtest",
				CreatedAt: time.Now(),
			}); err != nil {
				t.Fatalf("feedtest: record under %s: %v", wsB, err)
			}
			row, ok := st.ReadDeadLetter(ctx, t, wsB, "dl", "live", own.GlobalPosition)
			if !ok {
				t.Fatalf("feedtest: no dead letter row under %s", wsB)
			}
			if row.TypeURL != "" || row.SizeBytes != 0 {
				t.Fatalf("feedtest: foreign-position row = %+v, want ''/0 — the enrichment must not see across workspaces", row)
			}
		})
	})
}
