package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// The producer span context (ADR 0040) on the SQLite backend: the three
// nullable columns the 0009 rebuild adds, the write mapping, the read
// mapping and the all-or-nothing CHECK — the same contracts the Postgres
// suite pins, so the engines stay at parity.

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

// appendSpanEvent appends one spanTypeURL event to sid at the given
// expected version, carrying sc as the producer span context.
func appendSpanEvent(t testing.TB, ctx context.Context, s *sqlite.Store, sid es.StreamID, version uint64, sc es.SpanContext) es.Envelope {
	t.Helper()
	res, err := s.Append(ctx, es.AppendParams{
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

// insertRawEvent inserts one events row with raw SQL, bypassing the
// store — the poison-row pattern from insertFeedRowDirect (feed_test.go).
// position is written explicitly into global_position (the rowid): the
// rebuild test needs a gap to prove the migration's copy preserves
// positions, and every other caller simply passes a free one. traceID is
// the only span column it sets: nil writes all three NULL, which
// satisfies the migration's CHECK, and a non-nil trace id with
// span_id/trace_flags NULL trips it. The timestamp literal matches the
// fixed-width layout the store writes (tsLayout) so a materialized row
// still parses.
func insertRawEvent(t testing.TB, ctx context.Context, s *sqlite.Store, eventID string, sid es.StreamID, version, position uint64, traceID []byte) error {
	t.Helper()
	_, err := s.DB().ExecContext(ctx,
		`INSERT INTO events (
			global_position, event_id, stream_type, stream_id, version,
			type_url, schema_version, occurred_at, recorded_at,
			correlation_id, causation_id, command_id,
			actor_type, actor_id, actor_principal, payload,
			trace_id
		) VALUES (?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?,?, ?,?)`,
		position, eventID, sid.Type, sid.Canonical(), version,
		spanTypeURL, 1, "2026-01-01T00:00:00.000000000Z", "2026-01-01T00:00:00.000000000Z",
		"", "", "",
		sid.Type, "raw", "conformance:raw", []byte("p"),
		traceID,
	)
	return err
}

// TestSpanContextRoundTrip pins the write and read mapping: a two-event
// batch appended with a populated context comes back with the same valid
// context on both the stream read and the global-order read.
func TestSpanContextRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := mustStreamID(t, "counter", "main")
	sc := spanContextPopulated()

	res, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		SpanContext:     sc,
		Events: []es.EventData{
			{TypeURL: spanTypeURL, SchemaVersion: 1, Payload: []byte("p1")},
			{TypeURL: spanTypeURL, SchemaVersion: 1, Payload: []byte("p2")},
		},
	})
	if err != nil {
		t.Fatalf("append two-event batch: %v", err)
	}
	for i, env := range res.Envelopes {
		if env.SpanContext != sc {
			t.Fatalf("appended v%d carries %+v, want %+v", i+1, env.SpanContext, sc)
		}
	}

	stream, err := s.ReadStream(ctx, sid, 0, 0)
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

	all, err := s.ReadAll(ctx, 0, 100)
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
	sid := mustStreamID(t, "counter", "main")

	appendSpanEvent(t, ctx, s, sid, 0, es.SpanContext{})

	envs, err := s.ReadStream(ctx, sid, 0, 0)
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
	lg, err := sqlite.NewLogs(s).Open(ctx, "ignored")
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
	sid := mustStreamID(t, "counter", "main")

	partial := es.SpanContext{TraceID: spanContextPopulated().TraceID} // span id left zero
	_, err := s.Append(ctx, es.AppendParams{
		StreamID:    sid,
		SpanContext: partial,
		Events:      []es.EventData{{TypeURL: spanTypeURL, SchemaVersion: 1, Payload: []byte("p")}},
	})
	if !errors.Is(err, es.ErrMalformedSpanContext) {
		t.Fatalf("append with a partial context: error = %v, want es.ErrMalformedSpanContext", err)
	}

	v, err := s.CurrentStreamVersion(ctx, sid)
	if err != nil {
		t.Fatalf("current version: %v", err)
	}
	if v != 0 {
		t.Fatalf("stream version after the rejected append = %d, want 0", v)
	}
}

// TestSpanContextCheckRejectsRawPartial pins the database-side guard:
// even a raw INSERT that bypasses the store cannot store a half context.
// trace_id set with span_id/trace_flags NULL trips the migration's
// all-or-nothing CHECK.
func TestSpanContextCheckRejectsRawPartial(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := mustStreamID(t, "counter", "main")
	traceID := spanContextPopulated().TraceID

	err := insertRawEvent(t, ctx, s, "11111111-1111-7111-8111-111111111111", sid, 1, 1, traceID[:])
	if err == nil {
		t.Fatal("raw partial insert succeeded, want the CHECK to reject it")
	}
	if !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("raw partial insert: error = %v, want a CHECK constraint failure", err)
	}
}

// TestSpanContextRebuild re-runs the 0009 migration against a database
// that already holds rows — the rebuild production executes on a
// populated log, exercised here a second time on the finished schema so
// the copy, drop, rename, index recreation and sequence carry-over all
// run against real data.
//
// The store runs on ONE connection (SetMaxOpenConns(1)), so the
// rebuild's transaction is committed before the first assertion or
// store read: a read while the transaction still holds the connection
// would wait forever.
func TestSpanContextRebuild(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := mustStreamID(t, "counter", "main")

	// Three committed events — global_position 1, 2, 3 — written through
	// the store so the rebuild copies rows exactly as production finds
	// them, plus one raw row planted at position 9: a gap. Gap-free rows
	// renumber to themselves, so on [1 2 3] alone a copy that dropped the
	// explicit positions would be undetectable — with the gap, the same
	// copy surfaces as [1 2 3 4]. The gap row lives on its own stream (at
	// v1, context absent, like every pre-0009 row) so the sid append
	// below still expects v3.
	var firstEventID string
	for i := range 3 {
		env := appendSpanEvent(t, ctx, s, sid, uint64(i), es.SpanContext{})
		if i == 0 {
			firstEventID = env.EventID.String()
		}
	}
	if err := insertRawEvent(t, ctx, s, "33333333-3333-4333-8333-333333333333", mustStreamID(t, "counter", "gap"), 1, 9, nil); err != nil {
		t.Fatalf("plant the position-9 gap row: %v", err)
	}

	sqlText, err := os.ReadFile("migrations/0009_producer_span_context.sql")
	if err != nil {
		t.Fatalf("read 0009_producer_span_context.sql: %v", err)
	}
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin rebuild: %v", err)
	}
	if _, err := tx.ExecContext(ctx, string(sqlText)); err != nil {
		tx.Rollback()
		t.Fatalf("re-run 0009: %v", err)
	}
	// 0010's via columns live on the table this script DROPs, and
	// events_new is built to the 0009 column set — the re-run reverts
	// the table to the 0009 shape (0009's header warns against re-runs
	// on a database already migrated past it). A real rebuild runbook
	// re-applies every migration that came after the script it reruns;
	// mirror that here, or the store's next append finds no via_type.
	viaText, err := os.ReadFile("migrations/0010_delegated_actor.sql")
	if err != nil {
		tx.Rollback()
		t.Fatalf("read 0010_delegated_actor.sql: %v", err)
	}
	if _, err := tx.ExecContext(ctx, string(viaText)); err != nil {
		tx.Rollback()
		t.Fatalf("re-apply 0010: %v", err)
	}
	// Likewise 0012 adds the constraint_ops column the store now inserts.
	opsText, err := os.ReadFile("migrations/0012_event_constraint_ops.sql")
	if err != nil {
		tx.Rollback()
		t.Fatalf("read 0012_event_constraint_ops.sql: %v", err)
	}
	if _, err := tx.ExecContext(ctx, string(opsText)); err != nil {
		tx.Rollback()
		t.Fatalf("re-apply 0012: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit rebuild: %v", err)
	}

	// All four rows survive the rebuild, at their exact positions:
	// global_position is the log cursor and log_order's basis, and
	// renumbering it would rewrite history.
	var count int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 4 {
		t.Fatalf("row count after the rebuild = %d, want 4", count)
	}
	var positions []uint64
	rows, err := s.DB().QueryContext(ctx, `SELECT global_position FROM events ORDER BY global_position`)
	if err != nil {
		t.Fatalf("select positions: %v", err)
	}
	for rows.Next() {
		var p uint64
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			t.Fatalf("scan position: %v", err)
		}
		positions = append(positions, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatalf("iterate positions: %v", err)
	}
	if !slices.Equal(positions, []uint64{1, 2, 3, 9}) {
		t.Fatalf("global_position set after the rebuild = %v, want [1 2 3 9]", positions)
	}

	// sqlite_sequence carried over: the next append draws max+1 = 10 —
	// not 1 (a fresh sequence) and not 5 (a renumbered copy, whose max
	// would be 4). It carries a populated context, proving the rebuilt
	// table also accepts (and reads back) the three new columns.
	env := appendSpanEvent(t, ctx, s, sid, 3, spanContextPopulated())
	if env.GlobalPosition != 10 {
		t.Fatalf("global_position after the rebuild = %d, want max+1 = 10", env.GlobalPosition)
	}
	stream, err := s.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read stream after the rebuild: %v", err)
	}
	if len(stream) != 4 {
		t.Fatalf("stream has %d events after the rebuild, want 4", len(stream))
	}
	if stream[3].SpanContext != spanContextPopulated() {
		t.Fatalf("v4 context after the rebuild = %+v, want %+v", stream[3].SpanContext, spanContextPopulated())
	}

	// The five indexes were recreated with the table (they die with it):
	// a forgotten index turns feed reads into scans. index_list also
	// reports the sqlite_autoindex rows the UNIQUE constraints create,
	// so this is a subset check on the five named indexes.
	have := map[string]bool{}
	idxRows, err := s.DB().QueryContext(ctx, `PRAGMA index_list(events)`)
	if err != nil {
		t.Fatalf("index_list: %v", err)
	}
	for idxRows.Next() {
		var seq, unique, partial int
		var name, origin string
		if err := idxRows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			idxRows.Close()
			t.Fatalf("scan index_list: %v", err)
		}
		have[name] = true
	}
	err = idxRows.Err()
	idxRows.Close()
	if err != nil {
		t.Fatalf("iterate index_list: %v", err)
	}
	for _, name := range []string{
		"events_correlation_idx", "events_command_idx", "events_actor_idx",
		"events_stream_time_idx", "events_type_pos_idx",
	} {
		if !have[name] {
			t.Fatalf("index %s missing after the rebuild; index_list = %v", name, have)
		}
	}

	// Each index is also on the right columns, in order: a rebuild that
	// recreated the names from a wrong or older definition must fail
	// here, not at query time.
	for _, idx := range []struct {
		name string
		cols []string
	}{
		{"events_correlation_idx", []string{"correlation_id"}},
		{"events_command_idx", []string{"command_id"}},
		{"events_actor_idx", []string{"actor_principal"}},
		{"events_stream_time_idx", []string{"stream_id", "recorded_at"}},
		{"events_type_pos_idx", []string{"type_url", "global_position"}},
	} {
		infoRows, err := s.DB().QueryContext(ctx, fmt.Sprintf(`PRAGMA index_info(%s)`, idx.name))
		if err != nil {
			t.Fatalf("index_info(%s): %v", idx.name, err)
		}
		var cols []string
		for infoRows.Next() {
			var seqno, cid int
			var col string
			if err := infoRows.Scan(&seqno, &cid, &col); err != nil {
				infoRows.Close()
				t.Fatalf("scan index_info(%s): %v", idx.name, err)
			}
			// Rows come in seqno order; enforce it rather than trust it,
			// the comparison below is order-sensitive.
			if seqno != len(cols) {
				infoRows.Close()
				t.Fatalf("index_info(%s): seqno %d out of order", idx.name, seqno)
			}
			cols = append(cols, col)
		}
		err = infoRows.Err()
		infoRows.Close()
		if err != nil {
			t.Fatalf("iterate index_info(%s): %v", idx.name, err)
		}
		if !slices.Equal(cols, idx.cols) {
			t.Fatalf("index %s is on %v, want %v", idx.name, cols, idx.cols)
		}
	}

	// Both UNIQUE constraints survived: reusing v1's event_id on a fresh
	// stream, and a fresh event_id on v1's (stream, version), must each
	// fail — naming the constraint they hit, so a missing UNIQUE (the
	// silent loss of the optimistic-concurrency backstop) cannot pass as
	// something else.
	err = insertRawEvent(t, ctx, s, firstEventID, mustStreamID(t, "counter", "fresh-stream"), 1, 20, nil)
	if err == nil {
		t.Fatal("duplicate event_id insert succeeded; UNIQUE (event_id) did not survive the rebuild")
	}
	if !strings.Contains(err.Error(), "events.event_id") {
		t.Fatalf("duplicate event_id insert: error = %v, want the events.event_id UNIQUE", err)
	}
	err = insertRawEvent(t, ctx, s, "22222222-2222-4222-8222-222222222222", sid, 1, 21, nil)
	if err == nil {
		t.Fatal("duplicate (stream_id, version) insert succeeded; the version backstop did not survive the rebuild")
	}
	if !strings.Contains(err.Error(), "events.stream_id, events.version") {
		t.Fatalf("duplicate (stream_id, version) insert: error = %v, want the events.stream_id, events.version UNIQUE", err)
	}

	// Exactly the 25 expected columns, each with its declared type and
	// nullability: 0001's sixteen, 0002's prev_hash/hash, the three
	// span-context ones, 0010's three via columns, and 0012's
	// constraint_ops — no more, no fewer. global_position is the rowid
	// alias, which table_info reports as notnull=0, pk=1.
	type colSpec struct {
		typ         string
		notnull, pk int
	}
	wantCols := map[string]colSpec{
		"global_position": {"INTEGER", 0, 1},
		"event_id":        {"TEXT", 1, 0},
		"stream_type":     {"TEXT", 1, 0},
		"stream_id":       {"TEXT", 1, 0},
		"version":         {"INTEGER", 1, 0},
		"type_url":        {"TEXT", 1, 0},
		"schema_version":  {"INTEGER", 1, 0},
		"occurred_at":     {"TEXT", 1, 0},
		"recorded_at":     {"TEXT", 1, 0},
		"correlation_id":  {"TEXT", 1, 0},
		"causation_id":    {"TEXT", 1, 0},
		"command_id":      {"TEXT", 1, 0},
		"actor_type":      {"TEXT", 1, 0},
		"actor_id":        {"TEXT", 1, 0},
		"actor_principal": {"TEXT", 1, 0},
		"payload":         {"BLOB", 1, 0},
		"prev_hash":       {"BLOB", 0, 0},
		"hash":            {"BLOB", 0, 0},
		"trace_id":        {"BLOB", 0, 0},
		"span_id":         {"BLOB", 0, 0},
		"trace_flags":     {"INTEGER", 0, 0},
		"via_type":        {"TEXT", 0, 0},
		"via_id":          {"TEXT", 0, 0},
		"via_principal":   {"TEXT", 0, 0},
		"constraint_ops":  {"BLOB", 0, 0},
	}
	seen := make(map[string]bool, len(wantCols))
	colRows, err := s.DB().QueryContext(ctx, `PRAGMA table_info(events)`)
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	for colRows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var dflt any
		if err := colRows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			colRows.Close()
			t.Fatalf("scan table_info: %v", err)
		}
		want, ok := wantCols[name]
		if !ok {
			colRows.Close()
			t.Fatalf("unexpected column %s (%s) after the rebuild", name, typ)
		}
		if typ != want.typ || notnull != want.notnull || pk != want.pk {
			colRows.Close()
			t.Fatalf("column %s = (%s, notnull %d, pk %d), want (%s, notnull %d, pk %d)",
				name, typ, notnull, pk, want.typ, want.notnull, want.pk)
		}
		seen[name] = true
	}
	err = colRows.Err()
	colRows.Close()
	if err != nil {
		t.Fatalf("iterate table_info: %v", err)
	}
	var missing []string
	for name := range wantCols {
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		t.Fatalf("columns missing after the rebuild: %v", missing)
	}
}
