package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

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
