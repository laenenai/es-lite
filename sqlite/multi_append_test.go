package sqlite_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
)

func sqliteMultiAppend(t *testing.T, command uuid.UUID, sid es.StreamID, expected uint64, payload string) es.AppendParams {
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

func TestSQLiteAppendMultiCommitsBothStreams(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-a")
	sidB, _ := es.NewStreamID("counter", "multi-b")

	res, err := s.AppendMulti(ctx, es.MultiAppend{Appends: []es.AppendParams{
		sqliteMultiAppend(t, command, sidA, 0, "a"),
		sqliteMultiAppend(t, command, sidB, 0, "b"),
	}})
	if err != nil {
		t.Fatalf("AppendMulti: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("results = %d, want 2", len(res))
	}
	for i, sid := range []es.StreamID{sidA, sidB} {
		if res[i].FromVersion != 0 || res[i].ToVersion != 1 {
			t.Fatalf("result %d = v%d->v%d, want v0->v1", i, res[i].FromVersion, res[i].ToVersion)
		}
		envs, err := s.ReadStream(ctx, sid, 0, 0)
		if err != nil {
			t.Fatalf("ReadStream %d: %v", i, err)
		}
		if len(envs) != 1 || string(envs[0].Payload) != string([]byte{byte('a' + i)}) {
			t.Fatalf("stream %d payload = %q", i, envs[0].Payload)
		}
	}
	if res[0].Envelopes[0].GlobalPosition >= res[1].Envelopes[0].GlobalPosition {
		t.Fatalf("input-order positions %d >= %d", res[0].Envelopes[0].GlobalPosition, res[1].Envelopes[0].GlobalPosition)
	}
}

func TestSQLiteAppendMultiRollsBackOnConflict(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-rollback-a")
	sidB, _ := es.NewStreamID("counter", "multi-rollback-b")

	_, err := s.AppendMulti(ctx, es.MultiAppend{Appends: []es.AppendParams{
		sqliteMultiAppend(t, command, sidA, 0, "a"),
		sqliteMultiAppend(t, command, sidB, 5, "b"),
	}})
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("AppendMulti error = %v, want ErrConflict", err)
	}
	for _, sid := range []es.StreamID{sidA, sidB} {
		envs, err := s.ReadStream(ctx, sid, 0, 0)
		if err != nil && !errors.Is(err, es.ErrStreamNotFound) {
			t.Fatalf("ReadStream: %v", err)
		}
		if len(envs) != 0 {
			t.Fatalf("stream %s has %d events after rollback, want 0", sid, len(envs))
		}
	}
}

func TestSQLiteAppendMultiRejectsDuplicateStream(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	command := uuid.New()
	sid, _ := es.NewStreamID("counter", "multi-dup")

	_, err := s.AppendMulti(ctx, es.MultiAppend{Appends: []es.AppendParams{
		sqliteMultiAppend(t, command, sid, 0, "a"),
		sqliteMultiAppend(t, command, sid, 1, "b"),
	}})
	if !errors.Is(err, es.ErrDuplicateMultiStream) {
		t.Fatalf("error = %v, want ErrDuplicateMultiStream", err)
	}
}

func TestSQLiteAppendMultiRejectsEmpty(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	if _, err := s.AppendMulti(ctx, es.MultiAppend{}); !errors.Is(err, es.ErrEmptyMultiAppend) {
		t.Fatalf("empty error = %v, want ErrEmptyMultiAppend", err)
	}
}

func TestSQLiteAppendMultiSharedMetadata(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-meta-a")
	sidB, _ := es.NewStreamID("counter", "multi-meta-b")

	_, err := s.AppendMulti(ctx, es.MultiAppend{Appends: []es.AppendParams{
		sqliteMultiAppend(t, command, sidA, 0, "a"),
		sqliteMultiAppend(t, uuid.New(), sidB, 0, "b"),
	}})
	if !errors.Is(err, es.ErrConflictingMultiAppend) {
		t.Fatalf("error = %v, want ErrConflictingMultiAppend", err)
	}
}

func TestSQLiteAppendMultiEventIDs(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-id-a")
	sidB, _ := es.NewStreamID("counter", "multi-id-b")
	supplied := uuid.New()

	res, err := s.AppendMulti(ctx, es.MultiAppend{Appends: []es.AppendParams{
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
		t.Fatalf("AppendMulti: %v", err)
	}
	if res[0].Envelopes[0].EventID != supplied {
		t.Fatalf("supplied id = %v, want %v", res[0].Envelopes[0].EventID, supplied)
	}
	if res[1].Envelopes[0].EventID == uuid.Nil {
		t.Fatal("zero EventID was not minted")
	}
}

func TestSQLiteAppendMultiTags(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	command := uuid.New()
	sidA, _ := es.NewStreamID("counter", "multi-tags-a")
	sidB, _ := es.NewStreamID("counter", "multi-tags-b")

	a := sqliteMultiAppend(t, command, sidA, 0, "a")
	a.Tags = map[string]string{"flowdoc.space": "s1"}
	a.Events[0].Tags = map[string]string{"flowdoc.block": "b1"}
	b := sqliteMultiAppend(t, command, sidB, 0, "b")
	b.Tags = map[string]string{"flowdoc.space": "s1"}
	b.Events[0].Tags = map[string]string{"flowdoc.block": "b2"}

	res, err := s.AppendMulti(ctx, es.MultiAppend{Appends: []es.AppendParams{a, b}})
	if err != nil {
		t.Fatalf("AppendMulti: %v", err)
	}
	if res[0].Envelopes[0].Tags["flowdoc.block"] != "b1" || res[1].Envelopes[0].Tags["flowdoc.block"] != "b2" {
		t.Fatalf("result tags = %#v / %#v", res[0].Envelopes[0].Tags, res[1].Envelopes[0].Tags)
	}
	for _, sid := range []es.StreamID{sidA, sidB} {
		envs, err := s.ReadStream(ctx, sid, 0, 0)
		if err != nil {
			t.Fatalf("ReadStream: %v", err)
		}
		if envs[0].Tags["flowdoc.space"] != "s1" {
			t.Fatalf("read tags = %#v, want flowdoc.space=s1", envs[0].Tags)
		}
	}
	var n int
	if err := s.DB().QueryRowContext(ctx, `SELECT count(*) FROM event_tags`).Scan(&n); err != nil {
		t.Fatalf("count event_tags: %v", err)
	}
	if n != 4 {
		t.Fatalf("event_tags rows = %d, want 4", n)
	}
}
