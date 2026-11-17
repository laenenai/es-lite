package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

// The producer span context (ADR 0040) on the Postgres backend: the
// three nullable columns the migration adds, the write mapping, the
// read mapping and the all-or-nothing CHECK. Everything here skips
// without PG_DSN, like the rest of the Postgres suite.

// spanTypeURL is this file's event type, so the Logs page can filter on
// it server-side.
const spanTypeURL = "spancontext.v1.E"

// spanContextPopulated returns a valid, fully populated context: both
// ids non-zero and the W3C sampled flag set.
func spanContextPopulated() es.SpanContext {
	return es.SpanContext{
		TraceID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  [8]byte{21, 22, 23, 24, 25, 26, 27, 28},
		Flags:   0x01,
	}
}

// appendOne appends one spanTypeURL event to sid at the given expected
// version, carrying sc as the producer span context.
func appendOne(t testing.TB, ctx context.Context, ws es.Store, sid es.StreamID, version uint64, sc es.SpanContext) es.Envelope {
	t.Helper()
	res, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		SpanContext:     sc,
		Events:          []es.EventData{{TypeURL: spanTypeURL, SchemaVersion: 1, Payload: []byte("p")}},
	})
	if err != nil {
		t.Fatalf("append at v%d: %v", version+1, err)
	}
	return res.Envelopes[0]
}

// TestSpanContextRoundTrip pins the write and read mapping: a batch
// appended with a populated context comes back with the same valid
// context on both the stream read and the global-order read.
func TestSpanContextRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_span_rt")
	sid := streamID(t)
	sc := spanContextPopulated()

	appendOne(t, ctx, ws, sid, 0, sc)
	appendOne(t, ctx, ws, sid, 1, sc)

	stream, err := ws.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(stream) != 2 {
		t.Fatalf("stream has %d events, want 2", len(stream))
	}
	for i, e := range stream {
		if e.SpanContext != sc {
			t.Fatalf("ReadStream v%d carries %+v, want %+v", i+1, e.SpanContext, sc)
		}
		if !e.SpanContext.IsValid() {
			t.Fatalf("ReadStream v%d context is not valid", i+1)
		}
	}

	all, err := ws.ReadAll(ctx, 0, 100)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("log has %d events, want 2", len(all))
	}
	for i, e := range all {
		if e.SpanContext != sc {
			t.Fatalf("ReadAll event %d carries %+v, want %+v", i, e.SpanContext, sc)
		}
	}
}

// TestSpanContextAbsent pins the absent shape: an append without a
// context stores three NULLs and reads back as the zero SpanContext —
// on the stream read and through the feed's Logs paging, which must
// keep delivering the event.
func TestSpanContextAbsent(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_span_absent"
	store := s.Workspace(ws)
	sid := streamID(t)

	appendOne(t, ctx, store, sid, 0, es.SpanContext{})

	envs, err := store.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("stream has %d events, want 1", len(envs))
	}
	if envs[0].SpanContext != (es.SpanContext{}) || envs[0].SpanContext.IsValid() {
		t.Fatalf("context = %+v, want the zero value", envs[0].SpanContext)
	}

	// Logs still pages it: the envelope columns the pager selects cover
	// the span-context columns too.
	lg, err := postgres.NewLogs(s).Open(ctx, ws)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	page, truncated, err := lg.ReadAllBounded(ctx, 0, 100, 0, []string{spanTypeURL})
	if err != nil {
		t.Fatalf("page log: %v", err)
	}
	if truncated {
		t.Fatal("page of one event reported truncated")
	}
	if len(page) != 1 {
		t.Fatalf("page has %d events, want 1", len(page))
	}
	if page[0].SpanContext != (es.SpanContext{}) {
		t.Fatalf("paged context = %+v, want the zero value", page[0].SpanContext)
	}
}

// TestSpanContextPartialRejected pins the append-time guard: a non-zero
// but incomplete context is rejected with es.ErrMalformedSpanContext
// before anything is written, so the stream's version does not move.
func TestSpanContextPartialRejected(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_span_partial")
	sid := streamID(t)

	partial := es.SpanContext{TraceID: spanContextPopulated().TraceID} // span id left zero
	_, err := ws.Append(ctx, es.AppendParams{
		StreamID:    sid,
		SpanContext: partial,
		Events:      []es.EventData{{TypeURL: spanTypeURL, SchemaVersion: 1, Payload: []byte("p")}},
	})
	if !errors.Is(err, es.ErrMalformedSpanContext) {
		t.Fatalf("append with a partial context: error = %v, want es.ErrMalformedSpanContext", err)
	}

	v, err := ws.CurrentStreamVersion(ctx, sid)
	if err != nil {
		t.Fatalf("current version: %v", err)
	}
	if v != 0 {
		t.Fatalf("stream version after the rejected append = %d, want 0", v)
	}
}

// TestSpanContextCheckRejectsRawPartial pins the database-side guard:
// even a raw INSERT that bypasses the store cannot store a half context.
// The column list mirrors insertPoisonRow (feed_test.go) plus trace_id;
// trace_id set with span_id NULL trips the migration's all-or-nothing
// CHECK as SQLSTATE 23514.
func TestSpanContextCheckRejectsRawPartial(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := streamID(t)
	traceID := spanContextPopulated().TraceID

	_, err := s.Pool().Exec(ctx,
		`INSERT INTO events (
			workspace_id, event_id, stream_type, stream_id, version,
			type_url, schema_version, occurred_at, recorded_at,
			correlation_id, causation_id, command_id,
			actor_type, actor_id, actor_principal, payload,
			trace_id
		) VALUES ($1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11,$12, $13,$14,$15,$16, $17)`,
		"ws_span_raw", "11111111-1111-7111-8111-111111111111", sid.Type, sid.Canonical(), 1,
		spanTypeURL, 1, time.Now(), time.Now(),
		"", "", "",
		sid.Type, "raw", "conformance:raw", []byte("p"),
		traceID[:],
	)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" { // check_violation
		t.Fatalf("raw partial insert: error = %v, want SQLSTATE 23514 (check_violation)", err)
	}
}
