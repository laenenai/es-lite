package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

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

// seedThreeTagged appends three one-event batches with the shared tag
// k=range, different spaces/blocks, and returns their envelopes.
func seedThreeTagged(t *testing.T, ctx context.Context, ws *postgres.Workspace, sid es.StreamID) []es.Envelope {
	t.Helper()
	batches := []map[string]string{
		{"k": "range", "flowdoc.space": "s1"},
		{"k": "range", "flowdoc.space": "s1"},
		{"k": "range", "flowdoc.space": "s2"},
	}
	blocks := []string{"b1", "b2", "b1"}
	var out []es.Envelope
	for i := range batches {
		res := appendTagged(t, ctx, ws, sid, uint64(i), batches[i], map[string]string{"flowdoc.block": blocks[i]})
		out = append(out, res.Envelopes...)
	}
	return out
}

func TestReadByTagsAND(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "tags-and")
	envs := seedThreeTagged(t, ctx, ws, sid)

	got, err := ws.ReadByTags(ctx, es.TagQuery{Tags: map[string]string{
		"flowdoc.space": "s1",
		"flowdoc.block": "b1",
	}})
	if err != nil {
		t.Fatalf("ReadByTags: %v", err)
	}
	if len(got) != 1 || got[0].GlobalPosition != envs[0].GlobalPosition {
		t.Fatalf("ReadByTags returned %v, want only position %d", tagEnvelopePositions(got), envs[0].GlobalPosition)
	}
}

func TestReadByTagsRangeAndOrder(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "tags-range")
	envs := seedThreeTagged(t, ctx, ws, sid)

	got, err := ws.ReadByTags(ctx, es.TagQuery{
		Tags:         map[string]string{"k": "range"},
		FromPosition: envs[0].GlobalPosition,
		ToPosition:   envs[2].GlobalPosition,
	})
	if err != nil {
		t.Fatalf("ReadByTags: %v", err)
	}
	if want := []uint64{envs[1].GlobalPosition, envs[2].GlobalPosition}; !equalTagEnvelopePositions(got, want) {
		t.Fatalf("ReadByTags = %v, want %v", tagEnvelopePositions(got), want)
	}

	got, err = ws.ReadByTags(ctx, es.TagQuery{
		Tags:       map[string]string{"k": "range"},
		ToPosition: envs[1].GlobalPosition,
	})
	if err != nil {
		t.Fatalf("ReadByTags: %v", err)
	}
	if want := []uint64{envs[0].GlobalPosition, envs[1].GlobalPosition}; !equalTagEnvelopePositions(got, want) {
		t.Fatalf("ReadByTags = %v, want %v", tagEnvelopePositions(got), want)
	}
}

func TestReadByTagsPagination(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "tags-page")
	envs := seedThreeTagged(t, ctx, ws, sid)

	first, err := ws.ReadByTags(ctx, es.TagQuery{
		Tags:  map[string]string{"k": "range"},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("ReadByTags page 1: %v", err)
	}
	if len(first) != 1 || first[0].GlobalPosition != envs[0].GlobalPosition {
		t.Fatalf("page 1 = %v, want position %d", positions(first), envs[0].GlobalPosition)
	}

	second, err := ws.ReadByTags(ctx, es.TagQuery{
		Tags:         map[string]string{"k": "range"},
		FromPosition: first[0].GlobalPosition,
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("ReadByTags page 2: %v", err)
	}
	if len(second) != 1 || second[0].GlobalPosition != envs[1].GlobalPosition {
		t.Fatalf("page 2 = %v, want position %d", positions(second), envs[1].GlobalPosition)
	}
}

func TestReadByTagsLimit(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "tags-limit")
	seedThreeTagged(t, ctx, ws, sid)

	got, err := ws.ReadByTags(ctx, es.TagQuery{Tags: map[string]string{"k": "range"}})
	if err != nil {
		t.Fatalf("ReadByTags: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ReadByTags default limit returned %d events, want 3", len(got))
	}

	for _, limit := range []int{-1, es.MaxTagReadLimit + 1} {
		if _, err := ws.ReadByTags(ctx, es.TagQuery{
			Tags:  map[string]string{"k": "range"},
			Limit: limit,
		}); !errors.Is(err, es.ErrInvalidTagQuery) {
			t.Fatalf("Limit=%d error = %v, want ErrInvalidTagQuery", limit, err)
		}
	}
}

func TestReadByTagsRejectsEmptyQuery(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	if _, err := ws.ReadByTags(ctx, es.TagQuery{}); !errors.Is(err, es.ErrInvalidTagQuery) {
		t.Fatalf("empty query error = %v, want ErrInvalidTagQuery", err)
	}
}

func TestReadByTagsRejectsInvertedRange(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	if _, err := ws.ReadByTags(ctx, es.TagQuery{
		Tags:         map[string]string{"k": "v"},
		FromPosition: 5,
		ToPosition:   4,
	}); !errors.Is(err, es.ErrInvalidTagQuery) {
		t.Fatalf("inverted range error = %v, want ErrInvalidTagQuery", err)
	}
}

func TestReadByTagsWorkspaceIsolation(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, _ := es.NewStreamID("counter", "tags-isolation")

	a := s.WorkspaceStore("ws_a")
	b := s.WorkspaceStore("ws_b")
	if _, err := a.Append(ctx, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0,
		Events: []es.EventData{{TypeURL: "thing.v1.Happened", Payload: []byte("a"), Tags: map[string]string{"k": "shared"}}},
	}); err != nil {
		t.Fatalf("append ws_a: %v", err)
	}
	if _, err := b.Append(ctx, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0,
		Events: []es.EventData{{TypeURL: "thing.v1.Happened", Payload: []byte("b"), Tags: map[string]string{"k": "shared"}}},
	}); err != nil {
		t.Fatalf("append ws_b: %v", err)
	}

	got, err := a.ReadByTags(ctx, es.TagQuery{Tags: map[string]string{"k": "shared"}})
	if err != nil {
		t.Fatalf("ReadByTags ws_a: %v", err)
	}
	if len(got) != 1 || got[0].Workspace != "ws_a" {
		t.Fatalf("ws_a read = %v, want exactly its own event", tagEnvelopePositions(got))
	}
}

// TestReadByTagsAsOf pins ToPosition as an as-of cursor: the second event's
// position includes events 1 and 2 and excludes the third.
func TestReadByTagsAsOf(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_a")
	sid, _ := es.NewStreamID("counter", "tags-asof")
	envs := seedThreeTagged(t, ctx, ws, sid)

	got, err := ws.ReadByTags(ctx, es.TagQuery{
		Tags:       map[string]string{"k": "range"},
		ToPosition: envs[1].GlobalPosition,
	})
	if err != nil {
		t.Fatalf("ReadByTags: %v", err)
	}
	if want := []uint64{envs[0].GlobalPosition, envs[1].GlobalPosition}; !equalTagEnvelopePositions(got, want) {
		t.Fatalf("as-of read = %v, want %v", tagEnvelopePositions(got), want)
	}
}

// TestEventTagsBypassRequiresRelay pins the projection's role gate: a pinned
// non-member sees only its workspace, and the bypass arm needs both
// app.bypass_rls and es_relay membership.
func TestEventTagsBypassRequiresRelay(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ensureRLSProbeRole(t, ctx, s)
	sid, _ := es.NewStreamID("counter", "tags-rls")

	for _, ws := range []string{"ws_a", "ws_b"} {
		if _, err := s.WorkspaceStore(ws).Append(ctx, es.AppendParams{
			StreamID: sid, ExpectedVersion: 0,
			Events: []es.EventData{{TypeURL: "thing.v1.Happened", Payload: []byte("x"), Tags: map[string]string{"k": "shared"}}},
		}); err != nil {
			t.Fatalf("append %s: %v", ws, err)
		}
	}

	if _, err := s.Pool().Exec(ctx, `GRANT SELECT ON event_tags TO `+rlsProbeRole); err != nil {
		t.Fatalf("grant %s on event_tags: %v", rlsProbeRole, err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool().Exec(context.Background(), `REVOKE SELECT ON event_tags FROM `+rlsProbeRole); err != nil {
			t.Errorf("revoke %s on event_tags: %v", rlsProbeRole, err)
		}
	})

	if err := probePinned(t, ctx, s, "ws_a", func(tx pgx.Tx) error {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM event_tags`).Scan(&n); err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("pinned ws_a count = %d, want 1", n)
		}
		return nil
	}); err != nil {
		t.Fatalf("pinned probe: %v", err)
	}

	bypassCount := func() (int, error) {
		tx, err := s.Pool().Begin(ctx)
		if err != nil {
			return 0, err
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+rlsProbeRole); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('app.bypass_rls', 'on', true)`); err != nil {
			return 0, err
		}
		var n int
		err = tx.QueryRow(ctx, `SELECT count(*) FROM event_tags`).Scan(&n)
		return n, err
	}

	if n, err := bypassCount(); err != nil || n != 0 {
		t.Fatalf("non-member bypass count = %d (err %v), want 0", n, err)
	}
	if _, err := s.Pool().Exec(ctx, `GRANT `+relayRole+` TO `+rlsProbeRole); err != nil {
		t.Fatalf("grant %s to %s: %v", relayRole, rlsProbeRole, err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool().Exec(context.Background(), `REVOKE `+relayRole+` FROM `+rlsProbeRole); err != nil {
			t.Errorf("revoke %s from %s: %v", relayRole, rlsProbeRole, err)
		}
	})
	if n, err := bypassCount(); err != nil || n != 2 {
		t.Fatalf("member bypass count = %d (err %v), want 2", n, err)
	}
}

func tagEnvelopePositions(envs []es.Envelope) []uint64 {
	out := make([]uint64, len(envs))
	for i := range envs {
		out[i] = envs[i].GlobalPosition
	}
	return out
}

func equalTagEnvelopePositions(envs []es.Envelope, want []uint64) bool {
	got := positions(envs)
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
