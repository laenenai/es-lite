package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// openAppendStore opens a fresh in-memory store with the given options. The
// DSN is the (sub)test name, so every case gets its own database.
func openAppendStore(t *testing.T, opts ...sqlite.Option) *sqlite.Store {
	t.Helper()
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared"
	s, err := sqlite.Open(context.Background(), dsn, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestAppendRejectsOversizedPayload pins the append-time payload cap
// (WithMaxPayloadBytes): an over-cap batch is rejected whole with
// es.ErrPayloadTooLarge before anything is inserted, 0 selects the 8 MiB
// default, a negative value lifts the cap, and the wrapped error carries
// stream, version, type URL and size — never the payload bytes.
func TestAppendRejectsOversizedPayload(t *testing.T) {
	ctx := context.Background()

	t.Run("over cap is rejected before any insert", func(t *testing.T) {
		s := openAppendStore(t, sqlite.WithMaxPayloadBytes(64))
		sid, _ := es.NewStreamID("thing", "cap")

		// Seed one event so the log has a non-zero baseline to compare
		// against after the rejection.
		if _, err := s.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Events:          []es.EventData{{TypeURL: "thing.v1.Created", SchemaVersion: 1, Payload: []byte("x")}},
		}); err != nil {
			t.Fatalf("seed append: %v", err)
		}
		head, err := s.Head(ctx)
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		if head == 0 {
			t.Fatalf("head = %d, want a non-zero baseline", head)
		}

		payload := bytes.Repeat([]byte("p"), 100)
		_, err = s.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 1,
			Events:          []es.EventData{{TypeURL: "thing.v1.Oversized", SchemaVersion: 1, Payload: payload}},
		})
		if !errors.Is(err, es.ErrPayloadTooLarge) {
			t.Fatalf("oversized append: got %v, want ErrPayloadTooLarge", err)
		}
		msg := err.Error()
		if !strings.Contains(msg, "thing.v1.Oversized") || !strings.Contains(msg, "100") {
			t.Fatalf("error %q lacks the type URL or the size", msg)
		}
		if strings.Contains(msg, string(payload)) {
			t.Fatal("the error leaks the payload bytes")
		}

		// Nothing was appended: the stream version and the log head are
		// exactly what they were before the rejected append.
		if v, err := s.CurrentStreamVersion(ctx, sid); err != nil || v != 1 {
			t.Fatalf("stream version after rejected append = %d (err %v), want 1", v, err)
		}
		if h, err := s.Head(ctx); err != nil || h != head {
			t.Fatalf("head after rejected append = %d (err %v), want %d", h, err, head)
		}
	})

	// A negative option lifts the cap: even a payload over the 8 MiB
	// default appends.
	t.Run("negative option means unlimited", func(t *testing.T) {
		s := openAppendStore(t, sqlite.WithMaxPayloadBytes(-1))
		sid, _ := es.NewStreamID("thing", "unlimited")
		if _, err := s.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Events:          []es.EventData{{TypeURL: "thing.v1.Oversized", SchemaVersion: 1, Payload: bytes.Repeat([]byte("p"), 8<<20+1)}},
		}); err != nil {
			t.Fatalf("unlimited append: %v", err)
		}
	})

	// Zero selects the default: 8 MiB exactly still fits, one byte over is
	// rejected.
	t.Run("zero option means the 8 MiB default", func(t *testing.T) {
		s := openAppendStore(t, sqlite.WithMaxPayloadBytes(0))
		sid, _ := es.NewStreamID("thing", "default")
		_, err := s.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Events:          []es.EventData{{TypeURL: "thing.v1.Oversized", SchemaVersion: 1, Payload: bytes.Repeat([]byte("p"), 8<<20+1)}},
		})
		if !errors.Is(err, es.ErrPayloadTooLarge) {
			t.Fatalf("over-default append: got %v, want ErrPayloadTooLarge", err)
		}
		if _, err := s.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Events:          []es.EventData{{TypeURL: "thing.v1.MaxSize", SchemaVersion: 1, Payload: bytes.Repeat([]byte("p"), 8<<20)}},
		}); err != nil {
			t.Fatalf("at-cap append: %v", err)
		}
	})
}

// TestDuplicateEventIDIsNotConflict pins the F25 classification: appending
// an EventID the log already stores — on ANOTHER stream, so no version is
// contested — fails with es.ErrDuplicateEventID, which is not
// es.ErrConflict. A caller that retried on ErrConflict would loop forever:
// the event is already in the log and the append can never win.
func TestDuplicateEventIDIsNotConflict(t *testing.T) {
	ctx := context.Background()
	s := openAppendStore(t)
	sidA, _ := es.NewStreamID("thing", "a")
	sidB, _ := es.NewStreamID("thing", "b")
	id := uuid.MustParse("11111111-1111-7111-8111-111111111111")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        sidA,
		ExpectedVersion: 0,
		Events:          []es.EventData{{EventID: id, TypeURL: "thing.v1.Created", SchemaVersion: 1, Payload: []byte("x")}},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}

	// The same EventID on a different stream: both appends expect v0 of a
	// fresh stream, so no version is contested — this is the events.event_id
	// UNIQUE constraint, not the (stream_id, version) backstop.
	_, err := s.Append(ctx, es.AppendParams{
		StreamID:        sidB,
		ExpectedVersion: 0,
		Events:          []es.EventData{{EventID: id, TypeURL: "thing.v1.Created", SchemaVersion: 1, Payload: []byte("y")}},
	})
	if !errors.Is(err, es.ErrDuplicateEventID) {
		t.Fatalf("duplicate event id append: got %v, want ErrDuplicateEventID", err)
	}
	if errors.Is(err, es.ErrConflict) {
		t.Fatalf("duplicate event id append misrouted to ErrConflict: %v", err)
	}

	// The rejected append wrote nothing: stream B is still unwritten.
	if v, err := s.CurrentStreamVersion(ctx, sidB); err != nil || v != 0 {
		t.Fatalf("stream B version after rejected append = %d (err %v), want 0", v, err)
	}
}

// TestScanRejectsCorruptEventID pins the F34 read-side contract: a stored
// audit-UUID column (event_id, correlation_id, causation_id, command_id)
// that is not a UUID fails the read instead of being silently zeroed into
// a forged envelope, and the error names the offending column and the
// value's LENGTH only — the corrupt bytes themselves must never leak into
// logs.
func TestScanRejectsCorruptEventID(t *testing.T) {
	ctx := context.Background()
	s := openAppendStore(t)

	for i, col := range []string{"event_id", "correlation_id", "causation_id", "command_id"} {
		t.Run(col, func(t *testing.T) {
			sid, err := es.NewStreamID("thing", "corrupt-"+col)
			if err != nil {
				t.Fatal(err)
			}
			// Each row gets a distinct valid EventID (the unique
			// constraint would otherwise reject the seed itself) and a
			// corrupt value in exactly one audit column. The timestamps
			// match the fixed-width layout the store writes so the scan
			// reaches the corrupt column rather than failing on the
			// clock fields first.
			good := fmt.Sprintf("11111111-1111-4111-8111-%012d", i)
			bad := "corrupt-" + col
			cols := map[string]string{
				"event_id":       good,
				"correlation_id": good,
				"causation_id":   good,
				"command_id":     good,
			}
			cols[col] = bad
			if _, err := s.DB().ExecContext(ctx,
				`INSERT INTO events (
					event_id, stream_type, stream_id, version,
					type_url, schema_version, occurred_at, recorded_at,
					correlation_id, causation_id, command_id,
					actor_type, actor_id, actor_principal, payload, prev_hash, hash
				) VALUES (?,?,?,?, ?,?,?, ?,?,?, ?,?,?,?, ?,?,?)`,
				cols["event_id"], sid.Type, sid.Canonical(), 1,
				"thing.v1.Corrupt", 1, "2026-01-01T00:00:00.000000000Z", "2026-01-01T00:00:00.000000000Z",
				cols["correlation_id"], cols["causation_id"], cols["command_id"],
				"thing", "seed", "thing:seed", []byte("x"), []byte{}, []byte{},
			); err != nil {
				t.Fatalf("seed corrupt row: %v", err)
			}

			_, rerr := s.ReadStream(ctx, sid, 0, 0)
			if rerr == nil {
				t.Fatalf("read of a corrupt %s succeeded: want the read to fail", col)
			}
			msg := rerr.Error()
			if !strings.Contains(msg, col) {
				t.Fatalf("error %q does not name the offending column %q", msg, col)
			}
			if !strings.Contains(msg, fmt.Sprintf("length %d", len(bad))) {
				t.Fatalf("error %q does not report the corrupt value's length %d", msg, len(bad))
			}
			if strings.Contains(msg, bad) {
				t.Fatalf("error %q leaks the corrupt value's bytes", msg)
			}
		})
	}
}
