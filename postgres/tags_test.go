package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

func appendTagged(t *testing.T, ctx context.Context, ws *postgres.Workspace, sid es.StreamID, expected uint64, batch map[string]string, eventTags ...map[string]string) es.AppendResult {
	t.Helper()
	events := make([]es.EventData, len(eventTags))
	for i, tags := range eventTags {
		events[i] = es.EventData{
			TypeURL:       "thing.v1.Happened",
			SchemaVersion: 1,
			Payload:       []byte{byte(i + 1)},
			Tags:          tags,
		}
	}
	res, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: expected,
		Events:          events,
		Tags:            batch,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	return res
}

func TestAppendPersistsTagsAndEnvelope(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, err := es.NewStreamID("counter", "tagged")
	if err != nil {
		t.Fatal(err)
	}

	res := appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
		map[string]string{"flowdoc.block": "b2"},
	)
	for i, wantBlock := range []string{"b1", "b2"} {
		if got := res.Envelopes[i].Tags["flowdoc.space"]; got != "s1" {
			t.Fatalf("result envelope %d space = %q, want s1", i, got)
		}
		if got := res.Envelopes[i].Tags["flowdoc.block"]; got != wantBlock {
			t.Fatalf("result envelope %d block = %q, want %q", i, got, wantBlock)
		}
	}

	envs, err := ws.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(envs) != 2 {
		t.Fatalf("ReadStream returned %d events, want 2", len(envs))
	}
	for i, wantBlock := range []string{"b1", "b2"} {
		if got := envs[i].Tags["flowdoc.space"]; got != "s1" {
			t.Fatalf("read envelope %d space = %q, want s1", i, got)
		}
		if got := envs[i].Tags["flowdoc.block"]; got != wantBlock {
			t.Fatalf("read envelope %d block = %q, want %q", i, got, wantBlock)
		}
	}

	var rows int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM event_tags WHERE workspace_id = 'ws_a'`).Scan(&rows); err != nil {
		t.Fatalf("count event_tags: %v", err)
	}
	if rows != 4 {
		t.Fatalf("event_tags rows = %d, want 4", rows)
	}

	var tagsHash []byte
	if err := s.Pool().QueryRow(ctx,
		`SELECT tags_hash FROM log_order WHERE workspace_id = 'ws_a'`).Scan(&tagsHash); err != nil {
		t.Fatalf("read tags_hash: %v", err)
	}
	if len(tagsHash) != es.HashSize {
		t.Fatalf("tags_hash length = %d, want %d", len(tagsHash), es.HashSize)
	}
}

func TestAppendWithoutTagsWritesNullAndNoRows(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, err := es.NewStreamID("counter", "untagged")
	if err != nil {
		t.Fatal(err)
	}

	appendTagged(t, ctx, ws, sid, 0, nil, nil)

	var tags []byte
	if err := s.Pool().QueryRow(ctx, `
		SELECT tags FROM events WHERE workspace_id = 'ws_a' AND stream_id = $1
	`, sid.Canonical()).Scan(&tags); err != nil {
		t.Fatalf("read tags: %v", err)
	}
	if tags != nil {
		t.Fatalf("events.tags = %q, want NULL", tags)
	}

	var rows int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM event_tags WHERE workspace_id = 'ws_a'`).Scan(&rows); err != nil {
		t.Fatalf("count event_tags: %v", err)
	}
	if rows != 0 {
		t.Fatalf("event_tags rows = %d, want 0", rows)
	}

	var tagsHash []byte
	if err := s.Pool().QueryRow(ctx,
		`SELECT tags_hash FROM log_order WHERE workspace_id = 'ws_a'`).Scan(&tagsHash); err != nil {
		t.Fatalf("read tags_hash: %v", err)
	}
	if tagsHash != nil {
		t.Fatalf("tags_hash = %x, want NULL", tagsHash)
	}
}

func TestAppendRollbackWritesNoTags(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, err := es.NewStreamID("counter", "rollback")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := ws.AppendTx(ctx, tx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventData{{
			TypeURL: "thing.v1.Happened",
			Payload: []byte("x"),
			Tags:    map[string]string{"flowdoc.block": "b1"},
		}},
		Tags: map[string]string{"flowdoc.space": "s1"},
	}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("AppendTx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	var rows int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM event_tags WHERE workspace_id = 'ws_a'`).Scan(&rows); err != nil {
		t.Fatalf("count event_tags: %v", err)
	}
	if rows != 0 {
		t.Fatalf("event_tags rows after rollback = %d, want 0", rows)
	}
}

func TestAppendRejectsInvalidTags(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, err := es.NewStreamID("counter", "invalid-tags")
	if err != nil {
		t.Fatal(err)
	}

	tooMany := make(map[string]string, es.MaxTagsPerEvent+1)
	for i := 0; i <= es.MaxTagsPerEvent; i++ {
		tooMany[strings.ToLower(string(rune('a'+i%26)))+string(rune('0'+i/26))] = "v"
	}

	cases := []struct {
		name   string
		batch  map[string]string
		events []map[string]string
		want   error
	}{
		{"bad key", map[string]string{"Bad": "v"}, []map[string]string{nil}, es.ErrInvalidTag},
		{"long value", map[string]string{"k": strings.Repeat("v", es.MaxTagValueBytes+1)}, []map[string]string{nil}, es.ErrInvalidTag},
		{"nul", map[string]string{"k": "a\x00b"}, []map[string]string{nil}, es.ErrInvalidTag},
		{"too many", tooMany, []map[string]string{{}}, es.ErrTooManyTags},
		{"conflict", map[string]string{"k": "batch"}, []map[string]string{{"k": "event"}}, es.ErrConflictingTag},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := make([]es.EventData, len(tc.events))
			for i := range tc.events {
				events[i] = es.EventData{TypeURL: "thing.v1.Happened", Payload: []byte("x"), Tags: tc.events[i]}
			}
			_, err := ws.Append(ctx, es.AppendParams{
				StreamID:        sid,
				ExpectedVersion: 0,
				Events:          events,
				Tags:            tc.batch,
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("Append error = %v, want %v", err, tc.want)
			}

			var eventsN, tagsN int
			if err := s.Pool().QueryRow(ctx,
				`SELECT count(*) FROM events WHERE workspace_id = 'ws_a'`).Scan(&eventsN); err != nil {
				t.Fatalf("count events: %v", err)
			}
			if err := s.Pool().QueryRow(ctx,
				`SELECT count(*) FROM event_tags WHERE workspace_id = 'ws_a'`).Scan(&tagsN); err != nil {
				t.Fatalf("count event_tags: %v", err)
			}
			if eventsN != 0 || tagsN != 0 {
				t.Fatalf("rejected append wrote events=%d tags=%d, want 0/0", eventsN, tagsN)
			}
		})
	}
}

func TestReadCorruptTagsFailsLoudly(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, err := es.NewStreamID("counter", "corrupt-tags")
	if err != nil {
		t.Fatal(err)
	}
	appendTagged(t, ctx, ws, sid, 0,
		map[string]string{"flowdoc.space": "s1"},
		map[string]string{"flowdoc.block": "b1"},
	)

	// Both values are JSON objects (the DB CHECK admits them) but are not
	// the flat string map the decoder requires. A non-object is rejected by
	// the CHECK at write time, so only the object-shaped corruption reaches
	// the read path exercised here.
	for _, corrupt := range []string{`{"k":1}`, `{"Bad":"v"}`} {
		if _, err := s.Pool().Exec(ctx, `
			UPDATE events SET tags = $1 WHERE workspace_id = 'ws_a' AND stream_id = $2
		`, corrupt, sid.Canonical()); err != nil {
			t.Fatalf("corrupt tags: %v", err)
		}
		if _, err := ws.ReadStream(ctx, sid, 0, 0); err == nil {
			t.Fatalf("ReadStream with tags=%s returned no error", corrupt)
		}
	}
}

func TestTagsDoNotChangeEventHash(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, err := es.NewStreamID("counter", "tag-hash")
	if err != nil {
		t.Fatal(err)
	}

	res := appendTagged(t, ctx, ws, sid, 0,
		nil,
		nil,
		map[string]string{"flowdoc.block": "b1"},
	)
	for i, env := range res.Envelopes {
		got := es.EnvelopeHash(env)
		if !bytes.Equal(got[:], env.Hash) {
			t.Fatalf("envelope %d hash = %x, recomputed %x", i, env.Hash, got)
		}
		withoutTags := env
		withoutTags.Tags = nil
		if es.EnvelopeHash(withoutTags) != got {
			t.Fatalf("envelope %d hash changed when tags were removed", i)
		}
	}
}
