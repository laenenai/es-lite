package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

func multiAppend(t *testing.T, command uuid.UUID, sid es.StreamID, expected uint64, payload string) es.AppendParams {
	t.Helper()
	return es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: expected,
		Events: []es.EventData{{
			TypeURL:       "thing.v1.Happened",
			SchemaVersion: 1,
			Payload:       []byte(payload),
		}},
		CommandID: command,
	}
}

func TestAppendMultiTxCommitsBothStreams(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-a")
	sidB, _ := es.NewStreamID("counter", "multi-b")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	res, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{
		multiAppend(t, command, sidA, 0, "a"),
		multiAppend(t, command, sidB, 0, "b"),
	}})
	if err != nil {
		t.Fatalf("AppendMultiTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("results = %d, want 2", len(res))
	}
	for i, sid := range []es.StreamID{sidA, sidB} {
		if res[i].FromVersion != 0 || res[i].ToVersion != 1 {
			t.Fatalf("result %d = v%d->v%d, want v0->v1", i, res[i].FromVersion, res[i].ToVersion)
		}
		envs, err := ws.ReadStream(ctx, sid, 0, 0)
		if err != nil {
			t.Fatalf("ReadStream %d: %v", i, err)
		}
		if len(envs) != 1 || string(envs[0].Payload) != string([]byte{byte('a' + i)}) {
			t.Fatalf("stream %d = %d events, payload %q", i, len(envs), envs[0].Payload)
		}
	}
	if res[0].Envelopes[0].GlobalPosition >= res[1].Envelopes[0].GlobalPosition {
		t.Fatalf("input-order positions %d >= %d", res[0].Envelopes[0].GlobalPosition, res[1].Envelopes[0].GlobalPosition)
	}
}

func TestAppendMultiTxRollsBackOnConflict(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-rollback-a")
	sidB, _ := es.NewStreamID("counter", "multi-rollback-b")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err = ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{
		multiAppend(t, command, sidA, 0, "a"),
		multiAppend(t, command, sidB, 5, "b"),
	}})
	if !errors.Is(err, es.ErrConflict) {
		_ = tx.Rollback(ctx)
		t.Fatalf("AppendMultiTx error = %v, want ErrConflict", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	for _, sid := range []es.StreamID{sidA, sidB} {
		envs, err := ws.ReadStream(ctx, sid, 0, 0)
		if err != nil && !errors.Is(err, es.ErrStreamNotFound) {
			t.Fatalf("ReadStream: %v", err)
		}
		if len(envs) != 0 {
			t.Fatalf("stream %s has %d events after rollback, want 0", sid, len(envs))
		}
	}
}

func TestAppendMultiTxRejectsDuplicateStream(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sid, _ := es.NewStreamID("counter", "multi-dup")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	_, err = ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{
		multiAppend(t, command, sid, 0, "a"),
		multiAppend(t, command, sid, 1, "b"),
	}})
	if !errors.Is(err, es.ErrDuplicateMultiStream) {
		t.Fatalf("error = %v, want ErrDuplicateMultiStream", err)
	}
}

func TestAppendMultiTxRejectsEmpty(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	if _, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{}); !errors.Is(err, es.ErrEmptyMultiAppend) {
		t.Fatalf("empty error = %v, want ErrEmptyMultiAppend", err)
	}

	command := uuid.New()
	sid, _ := es.NewStreamID("counter", "multi-empty")
	noEvents := multiAppend(t, command, sid, 0, "a")
	noEvents.Events = nil
	if _, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{noEvents}}); !errors.Is(err, es.ErrEmptyMultiAppend) {
		t.Fatalf("zero-event error = %v, want ErrEmptyMultiAppend", err)
	}
}

func TestAppendMultiTxSharedMetadata(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-meta-a")
	sidB, _ := es.NewStreamID("counter", "multi-meta-b")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	b := multiAppend(t, uuid.New(), sidB, 0, "b")
	if _, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{
		multiAppend(t, command, sidA, 0, "a"),
		b,
	}}); !errors.Is(err, es.ErrConflictingMultiAppend) {
		t.Fatalf("error = %v, want ErrConflictingMultiAppend", err)
	}
}

func TestAppendMultiTxEventIDs(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-id-a")
	sidB, _ := es.NewStreamID("counter", "multi-id-b")
	supplied := uuid.New()

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	res, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{
		{
			StreamID: sidA, ExpectedVersion: 0, CommandID: command,
			Events: []es.EventData{{EventID: supplied, TypeURL: "thing.v1.Happened", Payload: []byte("a")}},
		},
		{
			StreamID: sidB, ExpectedVersion: 0, CommandID: command,
			Events: []es.EventData{{TypeURL: "thing.v1.Happened", Payload: []byte("b")}},
		},
	}})
	if err != nil {
		t.Fatalf("AppendMultiTx: %v", err)
	}
	if res[0].Envelopes[0].EventID != supplied {
		t.Fatalf("supplied id = %v, want %v", res[0].Envelopes[0].EventID, supplied)
	}
	if res[1].Envelopes[0].EventID == uuid.Nil {
		t.Fatal("zero EventID was not minted")
	}
}

func TestAppendMultiTxDeterministicIDsCollide(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-det-a")
	sidB, _ := es.NewStreamID("counter", "multi-det-b")
	// The runtime's per-append derivation restarts at index 0: two streams
	// with the same command id derive the same event id.
	derived := uuid.NewSHA1(uuid.NameSpaceOID, []byte(command.String()+":0"))

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err = ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{
		{
			StreamID: sidA, ExpectedVersion: 0, CommandID: command,
			Events: []es.EventData{{EventID: derived, TypeURL: "thing.v1.Happened", Payload: []byte("a")}},
		},
		{
			StreamID: sidB, ExpectedVersion: 0, CommandID: command,
			Events: []es.EventData{{EventID: derived, TypeURL: "thing.v1.Happened", Payload: []byte("b")}},
		},
	}})
	if !errors.Is(err, es.ErrDuplicateEventID) {
		_ = tx.Rollback(ctx)
		t.Fatalf("error = %v, want ErrDuplicateEventID (the documented per-append derivation caveat)", err)
	}
	_ = tx.Rollback(ctx)
}

func TestAppendMultiTxInputOrderPositions(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-order-a")
	sidB, _ := es.NewStreamID("counter", "multi-order-b")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	res, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{
		multiAppend(t, command, sidA, 0, "a"),
		multiAppend(t, command, sidB, 0, "b"),
	}})
	if err != nil {
		t.Fatalf("AppendMultiTx: %v", err)
	}
	if res[0].Envelopes[0].GlobalPosition >= res[1].Envelopes[0].GlobalPosition {
		t.Fatalf("positions %d >= %d, want input order", res[0].Envelopes[0].GlobalPosition, res[1].Envelopes[0].GlobalPosition)
	}
}

func TestAppendMultiTxTags(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-tags-a")
	sidB, _ := es.NewStreamID("counter", "multi-tags-b")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	a := multiAppend(t, command, sidA, 0, "a")
	a.Tags = map[string]string{"flowdoc.space": "s1"}
	a.Events[0].Tags = map[string]string{"flowdoc.block": "b1"}
	b := multiAppend(t, command, sidB, 0, "b")
	b.Tags = map[string]string{"flowdoc.space": "s1"}
	b.Events[0].Tags = map[string]string{"flowdoc.block": "b2"}

	res, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{a, b}})
	if err != nil {
		t.Fatalf("AppendMultiTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if res[0].Envelopes[0].Tags["flowdoc.block"] != "b1" || res[1].Envelopes[0].Tags["flowdoc.block"] != "b2" {
		t.Fatalf("result tags = %#v / %#v", res[0].Envelopes[0].Tags, res[1].Envelopes[0].Tags)
	}

	for _, sid := range []es.StreamID{sidA, sidB} {
		envs, err := ws.ReadStream(ctx, sid, 0, 0)
		if err != nil {
			t.Fatalf("ReadStream: %v", err)
		}
		if envs[0].Tags["flowdoc.space"] != "s1" {
			t.Fatalf("read tags = %#v, want flowdoc.space=s1", envs[0].Tags)
		}
	}
	var n int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM event_tags WHERE workspace_id = 'ws_a'`).Scan(&n); err != nil {
		t.Fatalf("count tags: %v", err)
	}
	if n != 4 {
		t.Fatalf("event_tags rows = %d, want 4", n)
	}
}

func TestAppendMultiTxRollbackLeavesNoTags(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sid, _ := es.NewStreamID("counter", "multi-tags-rollback")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	p := multiAppend(t, command, sid, 0, "a")
	p.Tags = map[string]string{"flowdoc.space": "s1"}
	p.Events[0].Tags = map[string]string{"flowdoc.block": "b1"}
	if _, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{p}}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("AppendMultiTx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	var n int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM event_tags WHERE workspace_id = 'ws_a'`).Scan(&n); err != nil {
		t.Fatalf("count tags: %v", err)
	}
	if n != 0 {
		t.Fatalf("event_tags rows after rollback = %d, want 0", n)
	}
}

// TestAppendMultiNotifyFiresOnceOnCommit pins the ADR 0039 half of the
// multi-append contract: the per-append hints are suppressed, and exactly one
// hint is emitted after the last append and becomes visible on commit.
func TestAppendMultiNotifyFiresOnceOnCommit(t *testing.T) {
	const channel = "es_lite_notify_multi_commit"
	const wsID = "ws_notify_multi"
	ctx := context.Background()
	lc := listenConn(t, channel)
	s := openStoreWithOptions(t, postgres.WithAppendNotify(channel))
	ws := s.WorkspaceStore(wsID)
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "notify-multi-a")
	sidB, _ := es.NewStreamID("counter", "notify-multi-b")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{
		multiAppend(t, command, sidA, 0, "a"),
		multiAppend(t, command, sidB, 0, "b"),
	}}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("AppendMultiTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	n, err := lc.WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("wait for notification: %v", err)
	}
	if n.Channel != channel || n.Payload != wsID {
		t.Fatalf("notification = (%q, %q), want (%q, %q)", n.Channel, n.Payload, channel, wsID)
	}
	assertNoNotification(t, lc, 30*time.Millisecond)
}

// TestAppendMultiNotifyRollbackDoesNotFire pins the transactional half: a
// multi-append that rolls back claims no durable events and notifies nobody.
func TestAppendMultiNotifyRollbackDoesNotFire(t *testing.T) {
	const channel = "es_lite_notify_multi_rollback"
	const wsID = "ws_notify_multi_rollback"
	ctx := context.Background()
	lc := listenConn(t, channel)
	s := openStoreWithOptions(t, postgres.WithAppendNotify(channel))
	ws := s.WorkspaceStore(wsID)
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "notify-multi-rb-a")
	sidB, _ := es.NewStreamID("counter", "notify-multi-rb-b")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{Appends: []es.AppendParams{
		multiAppend(t, command, sidA, 0, "a"),
		multiAppend(t, command, sidB, 0, "b"),
	}}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("AppendMultiTx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	assertNoNotification(t, lc, 50*time.Millisecond)
}

// TestAppendMultiTxPreChecksPayloads pins the pre-pass: an oversized payload
// on the second append is refused before the first one is written, even inside
// the caller's transaction.
func TestAppendMultiTxPreChecksPayloads(t *testing.T) {
	ctx := context.Background()
	s := openStoreWithOptions(t, postgres.WithMaxPayloadBytes(8))
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-payload-a")
	sidB, _ := es.NewStreamID("counter", "multi-payload-b")

	first := multiAppend(t, command, sidA, 0, "a")
	second := multiAppend(t, command, sidB, 0, "0123456789")

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{
		Appends: []es.AppendParams{first, second},
	}); !errors.Is(err, es.ErrPayloadTooLarge) {
		t.Fatalf("AppendMultiTx error = %v, want ErrPayloadTooLarge", err)
	}

	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM events WHERE workspace_id = 'ws_a'`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 0 {
		t.Fatalf("events in the transaction after a refused multi-append = %d, want 0", n)
	}
}

// TestAppendMultiTxPreChecksTags pins the tag half of the pre-pass: an invalid
// tag on the second append is refused before the first one is written.
func TestAppendMultiTxPreChecksTags(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-tag-pre-a")
	sidB, _ := es.NewStreamID("counter", "multi-tag-pre-b")

	first := multiAppend(t, command, sidA, 0, "a")
	first.Tags = map[string]string{"flowdoc.space": "s1"}
	second := multiAppend(t, command, sidB, 0, "b")
	second.Tags = map[string]string{"Bad": "v"}

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := ws.AppendMultiTx(ctx, tx, es.MultiAppend{
		Appends: []es.AppendParams{first, second},
	}); !errors.Is(err, es.ErrInvalidTag) {
		t.Fatalf("AppendMultiTx error = %v, want ErrInvalidTag", err)
	}

	var events, tags int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM events WHERE workspace_id = 'ws_a'`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM event_tags WHERE workspace_id = 'ws_a'`).Scan(&tags); err != nil {
		t.Fatalf("count event_tags: %v", err)
	}
	if events != 0 || tags != 0 {
		t.Fatalf("refused multi-append wrote events=%d tags=%d, want 0/0", events, tags)
	}
}
